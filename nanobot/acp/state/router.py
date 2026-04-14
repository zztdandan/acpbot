"""请求级路由器：把 ACP 回流更新分派到处理器，并协调结构锁与死手出包链。"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from nanobot.acp.contracts import (
    ACPPermissionOption,
    ACPToolCall,
    build_permission_selected_payload,
)
from nanobot.acp.state.handlers import StateUpdateHandler, build_default_handlers
from nanobot.acp.state.models import PoolKey
from nanobot.acp.state.permission_coordinator import PermissionCoordinator
from nanobot.acp.state.permission_events import PermissionReplyEvent, PermissionRequestEvent

if TYPE_CHECKING:
    from nanobot.acp.state.manager import SessionStateManager
    from nanobot.acp.state.models import FlushResult, PoolRuntimeEntry


class HandlerRegistry:
    """处理器注册表：在单请求状态域内维护 update -> handler 的解析顺序。"""

    def __init__(self, *, handlers: list[StateUpdateHandler] | None = None) -> None:
        """建立处理器列表；未显式注入时使用状态目录约定的默认处理器链。"""

        self._handlers = list(handlers or build_default_handlers())

    def resolve(self, update: object) -> StateUpdateHandler:
        """解析一条 ACP 更新对应的处理器；保证同一条更新只进入一个 owner。"""

        for handler in self._handlers:
            if handler.match(update):
                return handler
        raise RuntimeError("state handler registry must contain a fallback handler")


class ProgressRouter:
    """请求级进度路由器：协调结构锁、死手调度、权限子域与 progress 出包。"""

    def __init__(
        self,
        *,
        state_manager: SessionStateManager,
        permission_coordinator: PermissionCoordinator,
        handler_registry: HandlerRegistry | None = None,
    ) -> None:
        """建立请求级路由器；router 只在当前 request 生命周期内存活。"""

        self._state_manager = state_manager
        self._permission_coordinator = permission_coordinator
        self._handler_registry = handler_registry or HandlerRegistry()
        self._closing = False
        self._closed = False

    def has_pending_permission(self) -> bool:
        """返回当前 request 是否仍有待回复权限；供 inbound 与 runtime 快速筛选 reply 目标。"""

        return self._permission_coordinator.has_pending_permission()

    def looks_like_permission_reply(self, reply_text: str) -> bool:
        """判断一条入站文本是否像当前权限请求的 reply；仅做识别，不改写状态。"""

        return self._permission_coordinator.looks_like_permission_reply(reply_text)

    async def handle_permission_reply(self, *, reply_text: str) -> str:
        """消费一条权限回复文本；命中时把 reply 事件送入统一 handler/pool/router 主链。"""

        if not self._permission_coordinator.has_pending_permission():
            await self._permission_coordinator.emit_permission_reply_not_found()
            return "No pending permission request for this session."
        await self.handle_update(PermissionReplyEvent(reply_text=reply_text))
        if self._permission_coordinator.has_pending_permission():
            return "Permission reply not understood. Reply with /permission <number>."
        return "Permission reply received."

    async def handle_permission_request(
        self,
        *,
        options: list[ACPPermissionOption],
        tool_call: ACPToolCall | None = None,
    ) -> object:
        """等待一次权限请求结果并回传给 ACP SDK；对外隐藏 permission waiter 细节。"""

        if self._closing or self._closed or self._state_manager.is_closed():
            raise RuntimeError("permission request received after state closed")

        from acp.schema import RequestPermissionResponse

        pending_request = self._permission_coordinator.start_request(
            options=options,
            tool_call=tool_call,
        )
        await self.handle_update(PermissionRequestEvent(pending_request=pending_request))

        try:
            selected_option_id = await self._permission_coordinator.wait_for_reply()
        except asyncio.TimeoutError as exc:
            await self._permission_coordinator.emit_permission_timeout()
            raise RuntimeError("permission reply timed out") from exc
        finally:
            self._permission_coordinator.clear()

        return RequestPermissionResponse.model_validate(
            build_permission_selected_payload(selected_option_id)
        )

    async def handle_update(self, update: object) -> None:
        """消费一条 session update；锁内只做结构操作，锁外统一处理 flush 产物。"""

        if self._closing or self._closed:
            return
        handler = self._handler_registry.resolve(update)
        pool_key = handler.build_pool_key(update)
        lock = self._state_manager.get_pool_lock(pool_key)
        flushed: list[tuple[StateUpdateHandler, FlushResult]] = []
        async with lock:
            entry = self._state_manager.get_pool_entry(pool_key)
            if entry is None:
                entry = self._state_manager.install_pool_entry(
                    pool_key=pool_key,
                    pool=handler.create_pool(bucket_key=pool_key.bucket_key),
                    handler=handler,
                    lock=lock,
                )
            result = handler.consume(
                state_manager=self._state_manager,
                update=update,
                pool=entry.pool,
            )
            if result.immediate_finalize or entry.pool.is_terminal():
                flush_result = entry.pool.flush()
                self._state_manager.drop_pool_entry(pool_key)
                if flush_result is not None:
                    flushed.append((entry.handler, flush_result))
            else:
                self._schedule_deadhand_locked(entry)
        for flushed_handler, flush_result in flushed:
            await self.publish_progress_flush(
                flushed_handler=flushed_handler,
                flush_result=flush_result,
            )

    async def publish_progress_flush(
        self,
        *,
        flushed_handler: StateUpdateHandler,
        flush_result: FlushResult,
    ) -> None:
        """发布一次 progress flush；由 owner handler 编制 outbound，再统一走 runtime publish。"""

        if self._closed:
            return
        outbound = flushed_handler.build_progress_outbound(
            state_manager=self._state_manager,
            flush_result=flush_result,
        )
        if outbound is None:
            return
        await self._state_manager.publish_progress_outbound(outbound=outbound)

    def _schedule_deadhand_locked(self, entry: PoolRuntimeEntry) -> None:
        """按池自己维护的 deadline 更新死手定时器；router 只同步 timeout handle。"""

        if entry.timeout_handle is not None:
            entry.timeout_handle.cancel()
            entry.timeout_handle = None
        deadline = entry.pool.deadline_monotonic
        if deadline is None:
            return
        entry.timeout_token += 1
        token = entry.timeout_token
        delay = max(0.0, deadline - time.monotonic())
        loop = asyncio.get_running_loop()
        entry.timeout_handle = loop.call_later(
            delay,
            lambda: asyncio.create_task(self._run_deadhand(pool_key=entry.pool_key, token=token)),
        )

    async def _run_deadhand(self, *, pool_key: PoolKey, token: int) -> None:
        """执行某个池的死手 flush；锁内拿产物和摘结构，锁外统一发布。"""

        if self._closing or self._closed:
            return
        entry = self._state_manager.get_pool_entry(pool_key)
        if entry is None:
            return
        outbound = None
        async with entry.lock:
            current_entry = self._state_manager.get_pool_entry(pool_key)
            if current_entry is None or current_entry.timeout_token != token:
                return
            outbound = current_entry.handler.do_deadhand_operate_and_build_progress_outbound(
                state_manager=self._state_manager,
                pool=current_entry.pool,
            )
            self._state_manager.drop_pool_entry(pool_key)
        if outbound is not None:
            await self._state_manager.publish_progress_outbound(outbound=outbound)

    async def flush_all(self) -> None:
        """显式排空当前请求的全部池；用于测试、调试或需要立即观察 flush 结果的场景。"""

        for entry in list(self._state_manager.iter_pool_entries()):
            async with entry.lock:
                current_entry = self._state_manager.get_pool_entry(entry.pool_key)
                if current_entry is None:
                    continue
                flush_result = current_entry.pool.flush()
                flushed_handler = current_entry.handler
            if flush_result is not None:
                await self.publish_progress_flush(
                    flushed_handler=flushed_handler,
                    flush_result=flush_result,
                )

    async def close(self) -> None:
        """关闭请求级路由器并排空剩余池；request 收尾时仍允许发布最后一批 flush。"""

        if self._closed:
            return
        self._closing = True
        flushed: list[tuple[StateUpdateHandler, FlushResult]] = []
        for entry in list(self._state_manager.iter_pool_entries()):
            async with entry.lock:
                current_entry = self._state_manager.get_pool_entry(entry.pool_key)
                if current_entry is None:
                    continue
                flush_result = current_entry.pool.flush()
                self._state_manager.drop_pool_entry(current_entry.pool_key)
                if flush_result is not None:
                    flushed.append((current_entry.handler, flush_result))
        for flushed_handler, flush_result in flushed:
            await self.publish_progress_flush(
                flushed_handler=flushed_handler,
                flush_result=flush_result,
            )
        self._permission_coordinator.trigger_timeout()
        self._closed = True


__all__ = ["HandlerRegistry", "ProgressRouter"]
