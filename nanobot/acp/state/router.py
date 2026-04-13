"""ACP state router and progress mirroring boundary."""

from __future__ import annotations

from typing import TYPE_CHECKING

from nanobot.acp.state.outbound_schema import build_progress_payload
from nanobot.acp.state.pools import MediaPool, MessageTextPool, PermissionPool, ToolPool

if TYPE_CHECKING:
    from nanobot.acp.state.manager import SessionStateManager
    from nanobot.acp.state.models import FlushResult


class ProgressRouter:
    """State-owned progress sink that mirrors structured facts to on_progress."""

    def __init__(self, *, state_manager: SessionStateManager) -> None:
        self._state_manager = state_manager
        self._closed = False

    async def emit(self, result: FlushResult | None) -> None:
        if self._closed or result is None:
            return
        # 中文注释：router 负责把 pool flush 结果统一变成对外 progress 负载，
        # 各 handler/pool 不允许各自直接调 on_progress，避免出口分裂。
        content, metadata, _media = build_progress_payload(result)
        await self._state_manager.emit_progress(content=content, metadata=metadata)

    async def flush_all(self) -> None:
        """Flush all known pools through the centralized progress sink."""

        pools = (
            self._state_manager.message_text_pool,
            self._state_manager.media_pool,
            self._state_manager.tool_pool,
            self._state_manager.permission_pool,
        )
        for pool in pools:
            await self.emit(pool.flush())

    async def close(self) -> None:
        if self._closed:
            return
        # 中文注释：close 时要做尾部 flush，确保最后一批 text/media/tool/permission
        # 不会因为 request 收尾而丢失。
        pools = (
            self._state_manager.message_text_pool,
            self._state_manager.media_pool,
            self._state_manager.tool_pool,
            self._state_manager.permission_pool,
        )
        for pool in pools:
            await self.emit(pool.close())
        self._closed = True


# Re-export concrete pools so reader-facing API stays near the router boundary.
__all__ = ["MediaPool", "MessageTextPool", "PermissionPool", "ProgressRouter", "ToolPool"]
