"""单请求状态管理器：统一持有池运行时封装、请求聚合与状态子域组件。"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

from loguru import logger

from nanobot.acp.contracts import (
    ACP_META_KIND,
    ACP_META_KIND_FINAL,
    ACP_META_PROGRESS,
    ACP_META_RENDER_AS,
    ACP_META_RENDER_AS_FINAL,
    ACPCallbackUpdate,
    ObservabilityEventName,
    ObservabilityScopeName,
)
from nanobot.acp.observability import ObservabilityEvent
from nanobot.acp.runtime_models import ProgressCallback
from nanobot.acp.state.handlers.base import sanitize_json_value
from nanobot.acp.state.models import ACPPool, PoolKey, PoolRuntimeEntry, RequestScopeState
from nanobot.acp.state.permission_coordinator import PermissionCoordinator
from nanobot.bus.events import OutboundMessage

if TYPE_CHECKING:
    from nanobot.acp.state.handlers.base import StateUpdateHandler
    from nanobot.acp.state.router import ProgressRouter


FINAL_OUTBOUND_SETTLE_SECONDS = 0.8


class _RuntimeObservabilityOwner(Protocol):
    """运行时桥接协议：state 只通过这组窄接口触达 runtime。"""

    async def push_observability(self, event: ObservabilityEvent) -> None:
        """上报一条结构化观测事件；适用于 state 发现 late update 或权限异常。"""

        ...

    def resolve_acp_workspace_path(self) -> Path:
        """返回 ACP 工作目录；适用于消息媒体 handler 计算 request 级落地目录。"""

        ...

    async def global_publish_progress_outbound(
        self,
        *,
        outbound: OutboundMessage,
        request_key: str | None = None,
        drop_if_inactive: bool = False,
    ) -> None:
        """发布 progress outbound 到全局总线；state 只把最终 outbound 交给 runtime。"""

        ...


class SessionStateManager:
    """单请求状态管理器：归属 pool runtime entry、request-scope 聚合与权限子域组件。

    职责：
        - 持有 `PoolKey -> PoolRuntimeEntry` 真相，统一管理池实例、结构锁与 timeout handle
        - 持有 request-scope 最终文本、媒体与 metadata 聚合结果，作为 final materialize 的事实源
        - 把权限 waiter 交给独立的 `PermissionCoordinator`，避免把权限流程继续堆在 manager 方法上
    """

    def __init__(
        self,
        *,
        runtime: _RuntimeObservabilityOwner,
        request_key: str,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
        channel: str,
        chat_id: str,
        on_progress: ProgressCallback | None,
    ) -> None:
        """初始化单请求状态 owner；所有依赖都按 request 粒度注入。"""

        self._runtime = runtime
        self.request_key = request_key
        self.nanobot_side_session_key = nanobot_side_session_key
        self.acp_side_session_id = acp_side_session_id
        self.channel = channel
        self.chat_id = chat_id
        self.on_progress = on_progress
        self.request_scope = RequestScopeState()
        self._pool_entries: dict[PoolKey, PoolRuntimeEntry] = {}
        self._detached_pool_locks: dict[PoolKey, asyncio.Lock] = {}
        self._closed = False
        self._permission_coordinator = PermissionCoordinator(
            runtime=runtime,
            request_key=request_key,
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
        )
        from nanobot.acp.state.router import ProgressRouter

        self._progress_router: ProgressRouter = ProgressRouter(
            state_manager=self,
            permission_coordinator=self._permission_coordinator,
        )

    def is_closed(self) -> bool:
        """返回当前 request state 是否已经关闭；用于 late update 的防御性拦截。"""

        return self._closed

    @property
    def progress_router(self) -> ProgressRouter:
        """返回请求级 progress router；适用于 runtime active entry 与测试代码复用同一实例。"""

        return self._progress_router

    @property
    def permission_coordinator(self) -> PermissionCoordinator:
        """返回权限子域组件；handler/router 通过它访问 waiter，而不是让 manager 直接 owner。"""

        return self._permission_coordinator

    def get_pool_lock(self, pool_key: PoolKey) -> asyncio.Lock:
        """返回指定池键的结构锁；创建、摘除和立即结束都必须通过它串行化。"""

        entry = self._pool_entries.get(pool_key)
        if entry is not None:
            return entry.lock
        lock = self._detached_pool_locks.get(pool_key)
        if lock is None:
            lock = asyncio.Lock()
            self._detached_pool_locks[pool_key] = lock
        return lock

    def get_pool_entry(self, pool_key: PoolKey) -> PoolRuntimeEntry | None:
        """读取指定池键的运行时封装；供 router 与死手 actor 查询当前池状态。"""

        return self._pool_entries.get(pool_key)

    def install_pool_entry(
        self,
        *,
        pool_key: PoolKey,
        pool: ACPPool,
        handler: StateUpdateHandler,
        lock: asyncio.Lock,
    ) -> PoolRuntimeEntry:
        """安装新的池运行时封装；适用于某个 `PoolKey` 首次出现时建立 owner 记录。"""

        entry = PoolRuntimeEntry(pool_key=pool_key, pool=pool, handler=handler, lock=lock)
        self._pool_entries[pool_key] = entry
        self._detached_pool_locks.pop(pool_key, None)
        return entry

    def iter_pool_entries(self) -> Iterable[PoolRuntimeEntry]:
        """遍历当前请求全部活跃池；用于 flush_all 与 request close 收尾。"""

        return tuple(self._pool_entries.values())

    def drop_pool_entry(self, pool_key: PoolKey) -> None:
        """摘除一个池运行时封装；适用于立即结束池、死手到点和 request close 场景。"""

        entry = self._pool_entries.pop(pool_key, None)
        if entry is None:
            self._detached_pool_locks.pop(pool_key, None)
            return
        if entry.timeout_handle is not None:
            entry.timeout_handle.cancel()
            entry.timeout_handle = None
        self._detached_pool_locks.pop(pool_key, None)

    def update_partial_text(self, text: str) -> None:
        """刷新 request-scope 的 partial 文本快照；供异常回退读取最新文本。"""

        self.request_scope.partial_text = text.strip()

    def append_final_text(self, text: str) -> None:
        """把一次文本 flush 结果并入 final 文本区，兼容全量快照与增量片段。"""

        segment = text.strip()
        if not segment:
            return
        current = self.request_scope.final_text
        if not current:
            self.request_scope.final_text = segment
            return
        current_spaced = current.replace("\n", " ").strip()

        if segment == current or segment == current_spaced:
            # 与当前聚合快照等价（仅分隔符不同也算）时忽略。
            return
        if current.endswith(segment) or current_spaced.endswith(segment):
            # segment 已被 final 覆盖（同一快照重放或旧尾段回放）时不重复追加。
            return
        if segment.startswith(current):
            # 上游把全量快照重新发送时，仅并入新增尾段。
            delta = segment[len(current) :].strip()
            if not delta:
                return
            self.request_scope.final_text = f"{current}\n{delta}"
            return
        if current_spaced and segment.startswith(current_spaced):
            # 已写入 final 的段间换行会破坏前缀匹配，这里按空格等价值再做一轮判定。
            delta = segment[len(current_spaced) :].strip()
            if not delta:
                return
            self.request_scope.final_text = f"{current}\n{delta}"
            return

        overlap = max(
            self._suffix_prefix_overlap(current, segment),
            self._suffix_prefix_overlap(current_spaced, segment),
        )
        if overlap > 0:
            delta = segment[overlap:].strip()
            if not delta:
                return
            self.request_scope.final_text = f"{current}\n{delta}"
            return

        # 无重叠时按新段追加，段间保持一个换行。
        self.request_scope.final_text = f"{current}\n{segment}"

    @staticmethod
    def _suffix_prefix_overlap(left: str, right: str) -> int:
        """返回 `left` 后缀与 `right` 前缀的最大重叠长度。"""

        max_len = min(len(left), len(right))
        for size in range(max_len, 0, -1):
            if left[-size:] == right[:size]:
                return size
        return 0

    def append_media_path(self, media_path: str) -> None:
        """把消息媒体路径写入 request-scope 聚合；当前 state 明确忽略 tool 媒体。"""

        if not media_path:
            return
        if media_path not in self.request_scope.message_media_paths:
            self.request_scope.message_media_paths.append(media_path)
        self.request_scope.media_paths = list(self.request_scope.message_media_paths)

    def append_named_metadata(self, key: str, payload: object) -> None:
        """在 最终 metadata中建立一个数组，如果这个key不是数组则格式化为一个空数组。
        如果是数组那么把 payload 加入其中作为一项"""
        pass

    def update_named_metadata(self, key: str, payload: object) -> None:
        """把显式采纳的结构化事实写入 final metadata；适用于 plan/thought 等聚合投影场景。"""

        self.request_scope.final_metadata[key] = sanitize_json_value(payload)

    def append_other_update(self, *, label: str, payload: object) -> None:
        """记录未识别更新痕迹；用于审计 other 池是否仍有漏网类型。"""

        existing = self.request_scope.final_metadata.get("other_updates")
        items = list(existing) if isinstance(existing, list) else []
        items.append({"label": label, "payload": sanitize_json_value(payload)})
        self.request_scope.final_metadata["other_updates"] = items

    def media_landing_root(self, family: str) -> Path:
        """返回当前请求的媒体落地目录；消息媒体 handler 会在这里写出本地 resource-link 文件。"""

        workspace = self._runtime.resolve_acp_workspace_path()
        return workspace / ".nanobot" / "acp-state-media" / self.request_key / family

    async def state_publish_progress_outbound(self, *, outbound: OutboundMessage) -> None:
        """发布 state progress outbound，并在 state owner 边界完成 `on_progress` 兼容镜像。"""

        await self._runtime.global_publish_progress_outbound(
            outbound=outbound,
            request_key=self.request_key,
        )
        if not outbound.content or self.on_progress is None:
            return
        callback_kwargs: dict[str, object] = {}
        if outbound.metadata.get("_tool_hint") is not None:
            # on_progress 兼容签名仍使用 tool_hint 参数名，但事实源统一为 _tool_hint。
            callback_kwargs["tool_hint"] = outbound.metadata["_tool_hint"]
        if outbound.metadata.get("tool_event") is not None:
            callback_kwargs["tool_event"] = outbound.metadata["tool_event"]
        callback = cast(Any, self.on_progress)
        try:
            await callback(outbound.content, **callback_kwargs)
        except TypeError:
            try:
                if "tool_hint" in callback_kwargs:
                    await callback(outbound.content, tool_hint=callback_kwargs["tool_hint"])
                else:
                    await callback(outbound.content)
            except Exception:
                logger.exception("ACP progress on_progress fallback failed")
        except Exception:
            logger.exception("ACP progress on_progress emit failed")

    def schedule_progress_outbound(self, *, outbound: OutboundMessage) -> None:
        """调度一次异步 progress 发布；供 handler 在同步 consume 中触发即时上发。"""

        asyncio.create_task(self.state_publish_progress_outbound(outbound=outbound))

    async def consume_session_update(
        self,
        update: ACPCallbackUpdate,
    ) -> None:
        """消费一条 ACP session update；适用于 runtime 回调进入状态域主链的场景。"""

        if self._closed:
            # 如果state关闭后还有消费传入，那么只能打日志，无法正常处理了
            session_update_name = getattr(update, "session_update", None) or getattr(
                update, "sessionUpdate", None
            )
            await self._runtime.push_observability(
                ObservabilityEvent(
                    scope=ObservabilityScopeName.STATE,
                    event=ObservabilityEventName.LATE_SESSION_UPDATE,
                    request_key=self.request_key,
                    nanobot_side_session_key=self.nanobot_side_session_key,
                    acp_side_session_id=self.acp_side_session_id,
                    payload={
                        "reason": "state_closed",
                        "update_type": type(update).__name__,
                        "session_update": str(session_update_name or ""),
                    },
                )
            )
            return
        await self._progress_router.handle_update(update)

    async def finalize_and_materialize_final_outbound(self) -> OutboundMessage:
        """等待 state 完整收尾后再物化 final outbound；用于 runtime 执行完成时的统一出口。

        处理流程：
            - 先等待短暂 settle 窗口，给尾部 text/media/metadata 聚合留出收口时间
            - 先执行 `close()`，强制触发 router 尾刷并终止 permission waiter
            - 再基于 request-scope 聚合事实物化最终结果
        """

        # ACP 尾部 update 在执行完成后仍可能有一个很短的收口窗口，这里先等待再 close，
        # 避免 final outbound 过早物化而丢失最后一批聚合结果。
        await asyncio.sleep(FINAL_OUTBOUND_SETTLE_SECONDS)
        await self.close()
        return self.materialize_final_outbound(partial=False)

    def materialize_final_outbound(self, *, partial: bool = False) -> OutboundMessage:
        """按 request-scope 聚合事实物化最终 outbound；适用于 request 完成或异常回退场景。"""

        if partial:
            content = self.request_scope.partial_text
        else:
            # final 区意外为空时回退 partial，避免最后响应丢正文。
            content = self.request_scope.final_text or self.request_scope.partial_text
        if not partial:
            # 统一最终消息协议：所有通道都看到三行 final 包裹结构，避免通道各自拼装。
            content = self._format_final_content(content)
        final_metadata = dict(self.request_scope.final_metadata)
        if not partial:
            # 最终出站前统一补正语义元数据，避免前端把 final 误判成 progress。
            final_metadata[ACP_META_KIND] = ACP_META_KIND_FINAL
            final_metadata[ACP_META_RENDER_AS] = ACP_META_RENDER_AS_FINAL
            final_metadata[ACP_META_PROGRESS] = False
        return OutboundMessage(
            channel=self.channel,
            chat_id=self.chat_id,
            content=content or "",
            media=list(self.request_scope.media_paths),
            metadata=final_metadata,
        )

    @staticmethod
    def _format_final_content(content: str) -> str:
        """把最终正文统一为 `<final>\n...\n</final>` 三行格式。"""

        stripped = (content or "").strip()
        if stripped.startswith("<final>") and stripped.endswith("</final>"):
            stripped = stripped[len("<final>") : -len("</final>")].strip()
        return f"<final>\n{stripped}\n</final>"

    async def close(self) -> None:
        """关闭当前 request state；适用于 request 完成、失败或外部中止后的统一收尾场景。"""

        if self._closed:
            return
        self._closed = True
        await self._progress_router.close()


__all__ = ["SessionStateManager"]
