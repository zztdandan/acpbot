"""ProgressRouter 与 HandlerRegistry：把 update 路由到具体 handler，并统一做 flush/close 收口。"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nanobot.acp.state.handlers import StateUpdateHandler, build_default_handlers
from nanobot.acp.state.outbound_schema import build_progress_payload

if TYPE_CHECKING:
    from nanobot.acp.state.manager import SessionStateManager
    from nanobot.acp.state.models import FlushResult


class HandlerRegistry:
    """handler 注册表：按顺序解析 update，对外暴露唯一的 resolve 入口。"""

    def __init__(self, *, handlers: list[StateUpdateHandler] | None = None) -> None:
        """建立 handler 列表；默认加载 state 目录定义的全量 handler 链。"""

        self._handlers = list(handlers or build_default_handlers())

    def resolve(self, update: object) -> StateUpdateHandler:
        """查找第一个匹配该 update 的 handler；末尾 other handler 负责兜底。"""

        for handler in self._handlers:
            if handler.match(update):
                return handler
        raise RuntimeError("state handler registry must contain a fallback handler")


class ProgressRouter:
    """请求级 progress 路由器：协调 handler、池索引、flush 输出与 close 尾刷。"""

    def __init__(
        self,
        *,
        state_manager: SessionStateManager,
        handler_registry: HandlerRegistry | None = None,
    ) -> None:
        """绑定 state manager 与 handler registry；每个请求独立创建一份实例。"""

        self._state_manager = state_manager
        self._handler_registry = handler_registry or HandlerRegistry()
        self._closed = False

    async def handle_update(self, update: object) -> None:
        """处理一条 session_update；按 handler 规则建立/复用池并执行 flush/destroy。"""

        if self._closed:
            return
        handler = self._handler_registry.resolve(update)
        pool_key = handler.build_pool_key(update)
        pool = self._state_manager.get_or_create_pool(
            pool_key,
            factory=lambda: handler.create_pool(bucket_key=pool_key.bucket_key),
        )
        result = handler.consume(state_manager=self._state_manager, update=update, pool=pool)
        for flush_result in result.flush_results:
            await self.emit(flush_result)
        if pool.is_terminal():
            self._state_manager.destroy_pool(pool_key)
        for destroy_pool_key in result.destroy_pool_keys:
            self._state_manager.destroy_pool(destroy_pool_key)

    async def emit(self, result: FlushResult | None) -> None:
        """把一次 flush 结果镜像给 on_progress；空结果或关闭态直接忽略。"""

        if self._closed or result is None:
            return
        content, metadata, _media = build_progress_payload(result)
        await self._state_manager.emit_progress(content=content, metadata=metadata)

    async def flush_all(self) -> None:
        """刷新当前请求内全部存活池；常用于调试或显式 drain。"""

        for _pool_key, pool in self._state_manager.iter_pools():
            await self.emit(pool.flush())

    async def close(self) -> None:
        """关闭 router 并尾刷所有存活池；close 之后所有池索引都会被销毁。"""

        if self._closed:
            return
        for pool_key, pool in list(self._state_manager.iter_pools()):
            await self.emit(pool.close())
            self._state_manager.destroy_pool(pool_key)
        self._closed = True


__all__ = ["HandlerRegistry", "ProgressRouter"]
