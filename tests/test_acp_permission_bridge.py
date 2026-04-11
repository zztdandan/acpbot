from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from nanobot.acp.state import ACPUpdateType, SessionStateManager
from nanobot.acp.state.handlers.other import OtherHandler
from nanobot.acp.state.handlers.permission import PermissionHandler
from nanobot.acp.state.permission_events import (
    PermissionReplyEvent,
    PermissionRequestEvent,
    PermissionTimeoutEvent,
)
from nanobot.acp.state.router import HandlerRegistry
from nanobot.acp.permission_bridge import PermissionBridge
from nanobot.bus.events import InboundMessage


class _FakeDispatcher:
    def __init__(self, timeout_seconds: int = 1) -> None:
        self.acp_config = SimpleNamespace(permission_timeout_seconds=timeout_seconds)
        self._session_map = {"websocket:chat": "sid-1"}
        self._session_id_to_session_key = {"sid-1": "websocket:chat"}
        self._session_targets = {"sid-1": ("websocket", "chat", "websocket:chat")}
        self.published: list[tuple[str, Any]] = []

    async def _publish_outbound_with_debug(self, *, msg, reason: str, session_key: str) -> None:
        del session_key
        self.published.append((reason, msg))


def _options() -> list[object]:
    return [
        SimpleNamespace(
            option_id="allow_once", kind=SimpleNamespace(value="allow_once"), name="Allow once"
        ),
        SimpleNamespace(option_id="deny", kind=SimpleNamespace(value="deny"), name="Deny"),
    ]


@pytest.mark.asyncio
async def test_permission_bridge_metadata_reply_resolves_pending() -> None:
    dispatcher = _FakeDispatcher(timeout_seconds=3)
    bridge = PermissionBridge(dispatcher)

    task = asyncio.create_task(
        bridge.request_permission(
            options=_options(), session_id="sid-1", tool_call=SimpleNamespace(tool_call_id="t-1")
        )
    )
    await asyncio.sleep(0.05)
    consumed = await bridge.try_consume_permission_reply(
        InboundMessage(
            channel="websocket",
            sender_id="u",
            chat_id="chat",
            content="",
            metadata={"permission_decision": {"request_id": "t-1", "token": "allow_once"}},
            session_key_override="websocket:chat",
        )
    )

    result = await asyncio.wait_for(task, timeout=1.0)
    assert consumed is True
    assert result.outcome.outcome == "selected"
    assert result.outcome.option_id == "allow_once"
    assert any(reason == "permission_request" for reason, _ in dispatcher.published)
    assert any(reason == "permission_ack" for reason, _ in dispatcher.published)
    assert bridge._pending == {}

    request_msg = next(
        msg for reason, msg in dispatcher.published if reason == "permission_request"
    )
    request_metadata = dict(request_msg.metadata or {})
    assert request_metadata.get("_acp_kind") == "permission/request"
    assert request_metadata.get("_acp_version") == "1"
    assert request_metadata.get("_acp_session_id") == "sid-1"
    assert request_metadata.get("_acp_route_key") == "t-1"
    assert "_acp_payload" in request_metadata

    ack_msg = next(msg for reason, msg in dispatcher.published if reason == "permission_ack")
    ack_metadata = dict(ack_msg.metadata or {})
    assert ack_metadata.get("_acp_kind") == "permission/ack"
    assert ack_metadata.get("_acp_version") == "1"
    assert ack_metadata.get("_acp_session_id") == "sid-1"
    assert ack_metadata.get("_acp_route_key") == "t-1"
    assert "_acp_payload" in ack_metadata


@pytest.mark.asyncio
async def test_permission_bridge_text_reply_supports_ordinal_token() -> None:
    dispatcher = _FakeDispatcher(timeout_seconds=3)
    bridge = PermissionBridge(dispatcher)

    task = asyncio.create_task(
        bridge.request_permission(
            options=_options(), session_id="sid-1", tool_call=SimpleNamespace(tool_call_id="t-2")
        )
    )
    await asyncio.sleep(0.05)
    consumed = await bridge.try_consume_permission_reply(
        InboundMessage(
            channel="websocket",
            sender_id="u",
            chat_id="chat",
            content="perm:t-2 1",
            session_key_override="websocket:chat",
        )
    )

    result = await asyncio.wait_for(task, timeout=1.0)
    assert consumed is True
    assert result.outcome.outcome == "selected"
    assert result.outcome.option_id == "allow_once"


@pytest.mark.asyncio
async def test_permission_bridge_timeout_returns_cancelled() -> None:
    dispatcher = _FakeDispatcher(timeout_seconds=1)
    bridge = PermissionBridge(dispatcher)

    result = await bridge.request_permission(
        options=_options(),
        session_id="sid-1",
        tool_call=SimpleNamespace(tool_call_id="t-timeout"),
    )
    assert result.outcome.outcome == "cancelled"
    assert any(reason == "permission_timeout" for reason, _ in dispatcher.published)
    assert bridge._pending == {}


@pytest.mark.asyncio
async def test_permission_handler_receives_manual_request_reply_timeout_events() -> None:
    dispatcher = _FakeDispatcher(timeout_seconds=3)
    manager = SessionStateManager()
    handler = PermissionHandler(dispatcher=dispatcher, manager=manager)

    request_task = asyncio.create_task(
        handler.handle_request(
            PermissionRequestEvent(
                options=_options(),
                session_id="sid-1",
                tool_call=SimpleNamespace(tool_call_id="t-manual"),
            )
        )
    )
    await asyncio.sleep(0.05)
    consumed = await handler.handle_reply(
        PermissionReplyEvent(
            session_id="sid-1",
            request_id="t-manual",
            token="allow_once",
            source="metadata",
            session_key="websocket:chat",
        )
    )

    result = await asyncio.wait_for(request_task, timeout=1.0)
    assert consumed is True
    assert result.outcome.outcome == "selected"
    assert result.outcome.option_id == "allow_once"

    timeout_task = asyncio.create_task(
        handler.handle_request(
            PermissionRequestEvent(
                options=_options(),
                session_id="sid-1",
                tool_call=SimpleNamespace(tool_call_id="t-timeout-manual"),
            )
        )
    )
    await asyncio.sleep(0.05)
    await handler.handle_timeout(
        PermissionTimeoutEvent(session_id="sid-1", request_id="t-timeout-manual")
    )

    timeout_result = await asyncio.wait_for(timeout_task, timeout=1.0)
    assert timeout_result.outcome.outcome == "cancelled"
    assert manager.iter_pools_for_session(session_id="sid-1") == []


def test_permission_events_do_not_use_auto_update_registry() -> None:
    registry = HandlerRegistry(
        handlers=[PermissionHandler(dispatcher=_FakeDispatcher(), manager=SessionStateManager())],
        fallback_handler=OtherHandler(),
    )

    assert registry.handlers_for(ACPUpdateType.UNKNOWN) == []
