"""请求级路由器：把 ACP 回流更新分派到具体处理器，并统一收口刷新与关闭。"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nanobot.acp.state.handlers import StateUpdateHandler, build_default_handlers
from nanobot.acp.state.outbound_schema import build_progress_payload

if TYPE_CHECKING:
    from nanobot.acp.state.manager import SessionStateManager
    from nanobot.acp.state.models import FlushResult


class HandlerRegistry:
    """处理器注册表：按优先级解析更新应交给哪个处理器。

    职责：
        - 维护请求级可用处理器的有序链路
        - 通过 `resolve` 返回首个命中的处理器，并保证末尾存在兜底分支
    """

    def __init__(self, *, handlers: list[StateUpdateHandler] | None = None) -> None:
        """建立处理器列表；未显式传入时使用状态目录约定的默认链路。"""

        self._handlers = list(handlers or build_default_handlers())

    def resolve(self, update: object) -> StateUpdateHandler:
        """查找首个匹配当前更新的处理器；未命中时依赖末尾兜底分支。"""

        for handler in self._handlers:
            if handler.match(update):
                return handler
        raise RuntimeError("state handler registry must contain a fallback handler")


class ProgressRouter:
    """请求级进度路由器：协调处理器、池索引、刷新输出与关闭尾刷。

    职责：
        - 把单条会话更新解析到正确处理器，并驱动池创建、复用与销毁
        - 把各池的 `FlushResult` 统一转换为 `on_progress` 可消费的进度片段

    生命周期：
        - 创建：每个请求建立一份，与 `SessionStateManager` 绑定
        - 销毁：请求结束时调用 `close`，尾刷并销毁全部仍存活的池
    """

    def __init__(
        self,
        *,
        state_manager: SessionStateManager,
        handler_registry: HandlerRegistry | None = None,
    ) -> None:
        """绑定状态管理器与处理器注册表；每个请求独立创建一份实例。"""

        self._state_manager = state_manager
        self._handler_registry = handler_registry or HandlerRegistry()
        self._closed = False

    async def handle_update(self, update: object) -> None:
        """处理一条会话更新；按处理器规则驱动池的消费、刷新与销毁。

        处理流程：
            - 根据注册表解析当前更新对应的处理器
            - 通过处理器计算池键，并从 `SessionStateManager` 复用或创建池
            - 执行消费逻辑，依次镜像 flush 结果，再按终态或显式列表销毁池
        """

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
        """把一次刷新结果镜像给 `on_progress`；空结果或关闭态直接忽略。"""

        if self._closed or result is None:
            return
        content, metadata, _media = build_progress_payload(result)
        await self._state_manager.emit_progress(content=content, metadata=metadata)

    async def flush_all(self) -> None:
        """刷新当前请求内全部存活池；适用于调试或显式排空场景。"""

        for _pool_key, pool in self._state_manager.iter_pools():
            await self.emit(pool.flush())

    async def close(self) -> None:
        """关闭路由器并尾刷所有存活池；关闭后不再接受新的进度镜像。

        处理流程：
            - 遍历当前仍存活的全部池
            - 对每个池执行 `close`，把尾部片段继续镜像给请求级回调
            - 销毁对应池索引并标记路由器为关闭态
        """

        if self._closed:
            return
        for pool_key, pool in list(self._state_manager.iter_pools()):
            await self.emit(pool.close())
            self._state_manager.destroy_pool(pool_key)
        self._closed = True


__all__ = ["HandlerRegistry", "ProgressRouter"]
