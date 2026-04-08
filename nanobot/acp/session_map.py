"""ACP session_map family 入口。"""

from __future__ import annotations

from typing import Any

from nanobot.acp.acp_factory import _acp_text_block
from nanobot.acp.session_map_binding_manager import _SessionMapBindingManager


class _SessionMapSupport:
    """为 ACPDispatcher 提供会话映射管理能力的混入类。"""

    _conn: Any = None
    _session_map_binding_manager: _SessionMapBindingManager
    _session_map_bootstrapped: bool = False
    _session_bootstrap_activated_keys: set[str]

    @staticmethod
    def _acp_text_block(content: str) -> Any:
        return _acp_text_block(content)

    def _ensure_session_map_binding_manager(self) -> _SessionMapBindingManager:
        manager = getattr(self, "_session_map_binding_manager", None)
        if manager is None:
            manager = _SessionMapBindingManager(self)
            self._session_map_binding_manager = manager
        return manager

    def _resolved_acp_cwd(self) -> str:
        return self._ensure_session_map_binding_manager()._resolved_acp_cwd()

    def _persist_session_map(self) -> None:
        self._ensure_session_map_binding_manager().persist()

    async def _bootstrap_session_map(self) -> None:
        manager = self._ensure_session_map_binding_manager()
        activated_keys = await manager.bootstrap()
        self._session_bootstrap_activated_keys = set(activated_keys)
        self._session_map_bootstrapped = True

    async def send_heartbeat_to_active_sessions(self, heartbeat_instruction: str) -> int:
        """向当前活跃渠道映射对应的 ACP 会话逐个发送 heartbeat 指令。"""
        await self._ensure_connection()  # type: ignore[attr-defined]
        if self._conn is None:
            raise RuntimeError("ACP connection is not available")
        pairs = self._ensure_session_map_binding_manager().list_active_channel_pairs()
        delivered = 0
        for _, acp_side_session_id in pairs:
            try:
                await self._conn.prompt(
                    prompt=[self._acp_text_block(heartbeat_instruction)],
                    session_id=acp_side_session_id,
                )
                delivered += 1
            except Exception:
                # 中文注释：heartbeat 属于巡检信号，单会话失败记录后继续，避免阻断其它会话投递。
                from loguru import logger

                logger.exception("ACP heartbeat prompt failed for session {}", acp_side_session_id)
        return delivered
