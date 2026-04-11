from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, cast

from acp.schema import RequestPermissionResponse

from nanobot.acp.outbound_content_schema import build_permission_ack, build_permission_request
from nanobot.acp.state import ACPBucketType, ACPOutboundKind
from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.permission_events import (
    PermissionReplyEvent,
    PermissionRequestEvent,
    PermissionTimeoutEvent,
)
from nanobot.acp.state.pools import PermissionPool
from nanobot.bus.events import OutboundMessage


@dataclass
class _PendingPermission:
    session_id: str
    request_id: str
    options: list[Any]
    future: asyncio.Future[dict[str, Any]]
    expires_at: float
    state: str = "pending"


class PermissionHandler:
    name = "permission"
    supported_update_types = frozenset()
    bucket_type = ACPBucketType.PERMISSION
    outbound_kind = ACPOutboundKind.PERMISSION
    auto_dispatch = False

    def __init__(self, *, dispatcher: Any, manager: SessionStateManager) -> None:
        self._dispatcher = dispatcher
        self._manager = manager
        self._pending: dict[tuple[str, str], _PendingPermission] = {}
        self._lock = asyncio.Lock()

    def find_session_id_by_request(self, request_id: str) -> str | None:
        for session_id, rid in ((key[0], key[1]) for key in self._pending.keys()):
            if rid == request_id:
                return session_id
        return None

    async def handle_request(self, event: PermissionRequestEvent) -> RequestPermissionResponse:
        request_id = str(
            getattr(event.tool_call, "tool_call_id", None)
            or getattr(event.tool_call, "toolCallId", None)
            or f"perm-{int(time.time() * 1000)}"
        )
        timeout_seconds = max(
            1, int(getattr(self._dispatcher.acp_config, "permission_timeout_seconds", 90))
        )
        expires_at = time.time() + timeout_seconds
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        pending = _PendingPermission(
            session_id=event.session_id,
            request_id=request_id,
            options=list(event.options),
            future=future,
            expires_at=expires_at,
        )
        key = (event.session_id, request_id)
        async with self._lock:
            self._pending[key] = pending
            pool = cast(
                PermissionPool,
                self._manager.register_pool(
                    PermissionPool(
                        pool_id=f"permission:{event.session_id}:{request_id}",
                        session_id=event.session_id,
                    ),
                    bucket_key=request_id,
                ),
            )
        await pool.accept({"request_id": request_id, "state": "requested"})
        await self._publish_permission_request(pending, timeout_seconds)

        try:
            decision = await asyncio.wait_for(future, timeout=timeout_seconds)
        except asyncio.TimeoutError:
            await self.handle_timeout(
                PermissionTimeoutEvent(session_id=event.session_id, request_id=request_id)
            )
            return RequestPermissionResponse.model_validate({"outcome": {"outcome": "cancelled"}})

        option_id = decision.get("option_id")
        if not option_id:
            return RequestPermissionResponse.model_validate({"outcome": {"outcome": "cancelled"}})
        return RequestPermissionResponse.model_validate(
            {"outcome": {"outcome": "selected", "optionId": option_id}}
        )

    async def handle_reply(self, event: PermissionReplyEvent) -> bool:
        pending_key = (event.session_id, event.request_id)
        async with self._lock:
            pending = self._pending.get(pending_key)
            if pending is None or pending.state != "pending":
                return False
            option_id = self._resolve_option_id(pending.options, event.token)
            pending.state = "resolved"
            if not pending.future.done():
                pending.future.set_result({"option_id": option_id, "source": event.source})
            self._pending.pop(pending_key, None)

        await self._publish_permission_ack(
            session_key=event.session_key,
            session_id=event.session_id,
            request_id=event.request_id,
            selected_option_id=option_id,
            decision_source=event.source,
            reason="permission_ack",
        )
        await self._destroy_permission_pool(event.session_id, event.request_id)
        return True

    async def handle_timeout(self, event: PermissionTimeoutEvent) -> None:
        pending_key = (event.session_id, event.request_id)
        async with self._lock:
            pending = self._pending.get(pending_key)
            if pending is None or pending.state != "pending":
                return
            pending.state = "timed_out"
            if not pending.future.done():
                pending.future.set_result({"option_id": None, "source": "timeout"})
            self._pending.pop(pending_key, None)

        session_key = self._dispatcher._session_id_to_session_key.get(event.session_id, "")
        await self._publish_permission_ack(
            session_key=session_key,
            session_id=event.session_id,
            request_id=event.request_id,
            selected_option_id=None,
            decision_source="timeout",
            reason="permission_timeout",
        )
        await self._destroy_permission_pool(event.session_id, event.request_id)

    async def close(self) -> None:
        async with self._lock:
            pendings = list(self._pending.items())
            self._pending.clear()
        for (session_id, request_id), pending in pendings:
            if not pending.future.done():
                pending.future.set_result({"option_id": None, "source": "close"})
            await self._destroy_permission_pool(session_id, request_id)

    def _resolve_option_id(self, options: list[Any], token: str) -> str | None:
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

    async def _destroy_permission_pool(self, session_id: str, request_id: str) -> None:
        pool = self._manager.get_pool_by_key(
            bucket_type=ACPBucketType.PERMISSION,
            session_id=session_id,
            bucket_key=request_id,
        )
        if pool is not None:
            await self._manager.destroy_pool(pool.pool_id)

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
