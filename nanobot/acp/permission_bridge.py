"""ACP permission 反向池桥接。"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from typing import Any

from acp.schema import RequestPermissionResponse

from nanobot.acp.outbound_content_schema import build_permission_ack, build_permission_request
from nanobot.bus.events import InboundMessage, OutboundMessage

_PERM_PATTERN = re.compile(r"^perm:(?P<request_id>\S+)\s+(?P<token>\S+)\s*$", re.IGNORECASE)


@dataclass
class _PendingPermission:
    session_id: str
    request_id: str
    options: list[Any]
    future: asyncio.Future[dict[str, Any]]
    expires_at: float
    state: str = "pending"


class PermissionBridge:
    """实现 request_permission pending 池与 inbound 决策回填。"""

    def __init__(self, dispatcher: Any) -> None:
        self._dispatcher = dispatcher
        self._pending: dict[tuple[str, str], _PendingPermission] = {}
        self._lock = asyncio.Lock()

    async def request_permission(
        self, *, options: list[Any], session_id: str, tool_call: Any
    ) -> RequestPermissionResponse:
        request_id = str(
            getattr(tool_call, "tool_call_id", None)
            or getattr(tool_call, "toolCallId", None)
            or f"perm-{int(time.time() * 1000)}"
        )
        timeout_seconds = max(
            1, int(getattr(self._dispatcher.acp_config, "permission_timeout_seconds", 90))
        )
        expires_at = time.time() + timeout_seconds
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        pending = _PendingPermission(
            session_id=session_id,
            request_id=request_id,
            options=list(options),
            future=fut,
            expires_at=expires_at,
        )
        key = (session_id, request_id)
        async with self._lock:
            self._pending[key] = pending

        await self._publish_permission_request(pending, timeout_seconds)

        try:
            decision = await asyncio.wait_for(fut, timeout=timeout_seconds)
        except asyncio.TimeoutError:
            await self._resolve_timeout(key)
            return RequestPermissionResponse.model_validate({"outcome": {"outcome": "cancelled"}})

        option_id = decision.get("option_id")
        if not option_id:
            return RequestPermissionResponse.model_validate({"outcome": {"outcome": "cancelled"}})
        return RequestPermissionResponse.model_validate(
            {"outcome": {"outcome": "selected", "optionId": option_id}}
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
            session_id = self._find_session_id_by_request(request_id)
        if not session_id:
            return False

        pending_key = (session_id, request_id)
        async with self._lock:
            pending = self._pending.get(pending_key)
            if pending is None or pending.state != "pending":
                return False
            option_id = self._resolve_option_id(pending.options, token)
            pending.state = "resolved"
            if not pending.future.done():
                pending.future.set_result({"option_id": option_id, "source": source})
            # 中文注释：命中终态后立即移除 pending，避免长期运行内存累积。
            self._pending.pop(pending_key, None)

        await self._publish_permission_ack(
            session_key=session_key,
            session_id=session_id,
            request_id=request_id,
            selected_option_id=option_id,
            decision_source=source,
            reason="permission_ack",
        )
        return True

    async def close(self) -> None:
        async with self._lock:
            pendings = list(self._pending.items())
            self._pending.clear()
        for _key, item in pendings:
            if not item.future.done():
                item.future.set_result({"option_id": None, "source": "close"})

    def _find_session_id_by_request(self, request_id: str) -> str | None:
        for session_id, rid in ((k[0], k[1]) for k in self._pending.keys()):
            if rid == request_id:
                return session_id
        return None

    async def _resolve_timeout(self, key: tuple[str, str]) -> None:
        async with self._lock:
            pending = self._pending.get(key)
            if pending is None or pending.state != "pending":
                return
            pending.state = "timed_out"
            if not pending.future.done():
                pending.future.set_result({"option_id": None, "source": "timeout"})
            # 中文注释：timeout 进入终态后同步清理 pending，生命周期闭环。
            self._pending.pop(key, None)
        session_id, request_id = key
        session_key = self._dispatcher._session_id_to_session_key.get(session_id, "")
        await self._publish_permission_ack(
            session_key=session_key,
            session_id=session_id,
            request_id=request_id,
            selected_option_id=None,
            decision_source="timeout",
            reason="permission_timeout",
        )

    def _resolve_option_id(self, options: list[Any], token: str) -> str | None:
        # 中文注释：支持 1..n 下标回复，也支持 option_id 直填。
        if token.isdigit():
            idx = int(token)
            if 1 <= idx <= len(options):
                opt = options[idx - 1]
                return (
                    str(getattr(opt, "option_id", None) or getattr(opt, "optionId", None) or "")
                    or None
                )
        for opt in options:
            option_id = str(getattr(opt, "option_id", None) or getattr(opt, "optionId", None) or "")
            if option_id and option_id == token:
                return option_id
        return None

    async def _publish_permission_request(
        self, pending: _PendingPermission, timeout_seconds: int
    ) -> None:
        session_target = self._dispatcher._session_targets.get(pending.session_id)
        if session_target is None:
            return
        channel, chat_id, session_key = session_target
        options_payload = [
            {
                "option_id": getattr(opt, "option_id", None) or getattr(opt, "optionId", None),
                "name": getattr(opt, "name", None),
                "kind": getattr(getattr(opt, "kind", None), "value", getattr(opt, "kind", None)),
            }
            for opt in pending.options
        ]
        content, metadata = build_permission_request(
            request_id=pending.request_id,
            options=options_payload,
            timeout_seconds=timeout_seconds,
            expires_at=pending.expires_at,
            session_id=pending.session_id,
            route_key=pending.request_id,
            flush_reason="permission_request",
        )
        await self._dispatcher._publish_outbound_with_debug(
            msg=OutboundMessage(
                channel=channel, chat_id=chat_id, content=content, metadata=metadata
            ),
            reason="permission_request",
            session_key=session_key,
        )

    async def _publish_permission_ack(
        self,
        *,
        session_key: str,
        session_id: str,
        request_id: str,
        selected_option_id: str | None,
        decision_source: str,
        reason: str,
    ) -> None:
        session_target = self._dispatcher._session_targets.get(session_id)
        if session_target is None:
            return
        channel, chat_id, routed_session_key = session_target
        content, metadata = build_permission_ack(
            request_id=request_id,
            selected_option_id=selected_option_id,
            decision_source=decision_source,
            session_id=session_id,
            route_key=request_id,
            flush_reason=reason,
        )
        await self._dispatcher._publish_outbound_with_debug(
            msg=OutboundMessage(
                channel=channel, chat_id=chat_id, content=content, metadata=metadata
            ),
            reason=reason,
            session_key=session_key or routed_session_key,
        )
