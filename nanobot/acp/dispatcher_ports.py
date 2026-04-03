"""ACP dispatcher 端口协议定义。

当前版本仅提供最小骨架，供命令分发辅助模块进行类型约束。
"""

from __future__ import annotations

from typing import Any, ClassVar, Protocol

from nanobot.bus.events import OutboundMessage


class _DispatcherCommandPorts(Protocol):
    """slash 命令处理所需的 dispatcher 端口集合。"""

    _HELP_TEXT: ClassVar[Any]
    _conn: Any
    _session_map: dict[str, str]
    _session_caps: dict[str, Any]
    _connection_epoch: int
    _session_activation_ensure_epoch: dict[str, int]

    async def _ensure_connection(self) -> None: ...

    async def _ensure_session(
        self,
        session_key: str,
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
    ) -> str: ...

    async def _list_models_command(self, session_id: str) -> str: ...

    async def _list_agents_command(self, session_id: str) -> str: ...

    async def _refresh_session_caps_from_server(self, session_id: str) -> bool: ...

    def _persist_session_map(self) -> None: ...

    async def _publish_outbound_with_debug(
        self,
        *,
        msg: OutboundMessage,
        reason: str,
        session_key: str,
    ) -> None: ...
