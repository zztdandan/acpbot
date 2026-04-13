"""单请求状态聚合与进度发布层。"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nanobot.acp.state.outbound_schema import build_progress_payload
from nanobot.acp.state.pools import MediaPool, MessageTextPool, PermissionPool, ToolPool

if TYPE_CHECKING:
    from nanobot.acp.state.manager import SessionStateManager
    from nanobot.acp.state.models import FlushResult


class ProgressRouter:
    """负责事件路由与进度输出编排。"""

    def __init__(self, *, state_manager: SessionStateManager) -> None:
        """初始化当前对象并建立必要状态。"""
        self._state_manager = state_manager
        self._closed = False

    async def emit(self, result: FlushResult | None) -> None:
        """执行该方法定义的处理流程并返回结果。"""
        if self._closed or result is None:
            return
        content, metadata, _media = build_progress_payload(result)
        await self._state_manager.emit_progress(content=content, metadata=metadata)

    async def flush_all(self) -> None:
        """执行该方法定义的处理流程并返回结果。"""

        pools = (
            self._state_manager.message_text_pool,
            self._state_manager.media_pool,
            self._state_manager.tool_pool,
            self._state_manager.permission_pool,
        )
        for pool in pools:
            await self.emit(pool.flush())

    async def close(self) -> None:
        """关闭运行时并释放资源。"""
        if self._closed:
            return
        pools = (
            self._state_manager.message_text_pool,
            self._state_manager.media_pool,
            self._state_manager.tool_pool,
            self._state_manager.permission_pool,
        )
        for pool in pools:
            await self.emit(pool.close())
        self._closed = True


__all__ = ["MediaPool", "MessageTextPool", "PermissionPool", "ProgressRouter", "ToolPool"]
