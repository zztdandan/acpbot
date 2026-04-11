"""ACP permission 反向池桥接。"""

from __future__ import annotations

import re
from typing import Any

from acp.schema import RequestPermissionResponse

from nanobot.acp.state import SessionStateManager
from nanobot.acp.state.handlers.permission import PermissionHandler
from nanobot.acp.state.permission_events import (
    PermissionReplyEvent,
    PermissionRequestEvent,
)
from nanobot.bus.events import InboundMessage

_PERM_PATTERN = re.compile(r"^perm:(?P<request_id>\S+)\s+(?P<token>\S+)\s*$", re.IGNORECASE)


class PermissionBridge:
    """实现 request_permission pending 池与 inbound 决策回填。"""

    def __init__(self, dispatcher: Any) -> None:
        self._dispatcher = dispatcher
        manager = getattr(dispatcher, "_session_state_manager", None)
        if manager is None:
            manager = SessionStateManager()
            dispatcher._session_state_manager = manager
        self._handler = PermissionHandler(dispatcher=dispatcher, manager=manager)

    @property
    def _pending(self) -> dict[tuple[str, str], Any]:
        return self._handler._pending

    async def request_permission(
        self, *, options: list[Any], session_id: str, tool_call: Any
    ) -> RequestPermissionResponse:
        return await self._handler.handle_request(
            PermissionRequestEvent(
                options=list(options), session_id=session_id, tool_call=tool_call
            )
        )

    async def try_consume_permission_reply(self, msg: InboundMessage) -> bool:
        """预锁拦截 inbound permission 回执；命中则返回 True。"""

        request_id: str | None = None
        token: str | None = None
        source = "text"

        md = msg.metadata or {}
        decision_md = md.get("permission_decision")
        if isinstance(decision_md, dict):
            request_id_raw = decision_md.get("request_id") or decision_md.get("requestId")
            token_raw = (
                decision_md.get("token")
                or decision_md.get("option_id")
                or decision_md.get("optionId")
            )
            if isinstance(request_id_raw, str) and isinstance(token_raw, str):
                request_id, token = request_id_raw.strip(), token_raw.strip()
                source = "metadata"

        if not request_id or not token:
            m = _PERM_PATTERN.match((msg.content or "").strip())
            if m:
                request_id = m.group("request_id").strip()
                token = m.group("token").strip()
                source = "text"

        if not request_id or not token:
            return False

        session_key = msg.session_key
        session_id = self._dispatcher._session_map.get(session_key)
        if not session_id:
            # 中文注释：若 session_key 无映射，尝试全局 request_id 匹配，兼容 channel 回执未带 session_key 场景。
            session_id = self._handler.find_session_id_by_request(request_id)
        if not session_id:
            return False

        return await self._handler.handle_reply(
            PermissionReplyEvent(
                session_id=session_id,
                request_id=request_id,
                token=token,
                source=source,
                session_key=session_key,
            )
        )

    async def close(self) -> None:
        await self._handler.close()
