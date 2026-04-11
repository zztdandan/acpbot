from __future__ import annotations

from acp.schema import CurrentModeUpdate

import pytest

from nanobot.acp.progress_event_types import ACPProgressEvent
from nanobot.acp.session_update_events import to_progress_event
from nanobot.acp.state import ACPBucketType, ACPOutboundKind, ACPUpdateType, SessionStateManager
from nanobot.acp.state.handlers.other import OtherHandler
from nanobot.acp.state.handlers.user_message import UserMessageHandler
from nanobot.acp.state.router import HandlerRegistry
from nanobot.acp.state.router import SessionStateRouter


class ToolCallUpdate:
    def __init__(self, *, tool_call_id: str, status: str) -> None:
        self.tool_call_id = tool_call_id
        self.status = status

    def model_dump(self, **kwargs: object) -> dict[str, str]:
        del kwargs
        return {
            "toolCallId": self.tool_call_id,
            "status": self.status,
        }


def test_normalize_current_mode_update_to_explicit_update_type() -> None:
    event = to_progress_event(
        session_id="session-1",
        update=CurrentModeUpdate.model_validate(
            {
                "currentModeId": "agent-1",
                "sessionUpdate": "current_mode_update",
            }
        ),
    )

    assert event.update_type is ACPUpdateType.CURRENT_MODE_UPDATE
    assert event.route_key == "session-1"
    assert event.raw_json["currentModeId"] == "agent-1"


def test_normalize_internal_tool_call_update_keeps_compatibility_type() -> None:
    event = to_progress_event(
        session_id="session-2",
        update=ToolCallUpdate(tool_call_id="tool-1", status="completed"),
    )

    assert event.update_type is ACPUpdateType.TOOL_CALL_UPDATE
    assert event.route_key == "tool-1"
    assert event.extracted["status"] == "completed"


class _FakeHandler:
    def __init__(
        self,
        *,
        name: str,
        supported_update_types: frozenset[ACPUpdateType],
        bucket_type: ACPBucketType = ACPBucketType.NONE,
        outbound_kind: ACPOutboundKind = ACPOutboundKind.NONE,
        auto_dispatch: bool = True,
        match_result: bool = True,
    ) -> None:
        self.name = name
        self.supported_update_types = supported_update_types
        self.bucket_type = bucket_type
        self.outbound_kind = outbound_kind
        self.auto_dispatch = auto_dispatch
        self._match_result = match_result

    def match(self, event: ACPProgressEvent) -> bool:
        del event
        return self._match_result

    def build_bucket_key(self, event: ACPProgressEvent) -> str | None:
        return event.route_key

    async def consume(self, manager: object, item: object) -> None:
        del manager, item

    async def enqueue(self, manager: object, item: object) -> object:
        del manager, item
        return object()

    async def flush(
        self,
        manager: object,
        *,
        session_id: str,
        bucket_key: str,
        reason: str,
    ) -> object:
        del manager, session_id, bucket_key, reason
        return None


def _agent_message_event() -> ACPProgressEvent:
    return ACPProgressEvent(
        session_id="session-agent",
        raw_update={"content": {"type": "text", "text": "hello"}},
        raw_json={"content": {"type": "text", "text": "hello"}},
        update_type=ACPUpdateType.AGENT_MESSAGE_CHUNK,
        family="text",
        route_key="session-agent",
    )


def test_agent_message_prefers_text_handler_before_media_handler() -> None:
    registry = HandlerRegistry(
        handlers=[
            _FakeHandler(
                name="text",
                supported_update_types=frozenset({ACPUpdateType.AGENT_MESSAGE_CHUNK}),
                bucket_type=ACPBucketType.TEXT,
            ),
            _FakeHandler(
                name="media",
                supported_update_types=frozenset({ACPUpdateType.AGENT_MESSAGE_CHUNK}),
                bucket_type=ACPBucketType.MEDIA,
                match_result=False,
            ),
        ],
        fallback_handler=_FakeHandler(name="other", supported_update_types=frozenset()),
    )

    assert registry.resolve(_agent_message_event()).name == "text"


def test_unmatched_update_uses_other_handler() -> None:
    registry = HandlerRegistry(
        handlers=[
            _FakeHandler(
                name="text",
                supported_update_types=frozenset({ACPUpdateType.AGENT_MESSAGE_CHUNK}),
                bucket_type=ACPBucketType.TEXT,
                match_result=False,
            )
        ],
        fallback_handler=_FakeHandler(name="other", supported_update_types=frozenset()),
    )

    assert registry.resolve(_agent_message_event()).name == "other"


def test_non_agent_message_update_type_allows_only_one_formal_handler() -> None:
    with pytest.raises(ValueError, match="current_mode_update"):
        HandlerRegistry(
            handlers=[
                _FakeHandler(
                    name="current-mode-1",
                    supported_update_types=frozenset({ACPUpdateType.CURRENT_MODE_UPDATE}),
                ),
                _FakeHandler(
                    name="current-mode-2",
                    supported_update_types=frozenset({ACPUpdateType.CURRENT_MODE_UPDATE}),
                ),
            ],
            fallback_handler=_FakeHandler(name="other", supported_update_types=frozenset()),
        )


def test_permission_handler_is_not_registered_in_auto_dispatch() -> None:
    registry = HandlerRegistry(
        handlers=[
            _FakeHandler(
                name="permission",
                supported_update_types=frozenset({ACPUpdateType.TOOL_CALL_UPDATE}),
                bucket_type=ACPBucketType.PERMISSION,
                outbound_kind=ACPOutboundKind.PERMISSION,
                auto_dispatch=False,
            )
        ],
        fallback_handler=_FakeHandler(name="other", supported_update_types=frozenset()),
    )

    assert registry.handlers_for(ACPUpdateType.TOOL_CALL_UPDATE) == []


def test_agent_message_dual_formal_match_is_treated_as_error() -> None:
    registry = HandlerRegistry(
        handlers=[
            _FakeHandler(
                name="text",
                supported_update_types=frozenset({ACPUpdateType.AGENT_MESSAGE_CHUNK}),
                bucket_type=ACPBucketType.TEXT,
            ),
            _FakeHandler(
                name="media",
                supported_update_types=frozenset({ACPUpdateType.AGENT_MESSAGE_CHUNK}),
                bucket_type=ACPBucketType.MEDIA,
            ),
        ],
        fallback_handler=_FakeHandler(name="other", supported_update_types=frozenset()),
    )

    with pytest.raises(ValueError, match="agent_message_chunk"):
        registry.resolve(_agent_message_event())


@pytest.mark.asyncio
async def test_user_message_uses_consume_only_pool_and_is_destroyed_immediately() -> None:
    manager = SessionStateManager()
    router = SessionStateRouter(
        HandlerRegistry(handlers=[UserMessageHandler()], fallback_handler=OtherHandler()),
        manager=manager,
        publish=_noop_publish,
    )

    await router.handle_event(
        ACPProgressEvent(
            session_id="session-user",
            raw_update={"content": {"text": "hello"}},
            raw_json={"content": {"text": "hello"}},
            update_type=ACPUpdateType.USER_MESSAGE_CHUNK,
            family="user",
            route_key="session-user",
        )
    )

    assert manager.iter_pools_for_session(session_id="session-user") == []


@pytest.mark.asyncio
async def test_other_handler_is_the_only_fallback_and_uses_consume_only_pool() -> None:
    manager = SessionStateManager()
    router = SessionStateRouter(
        HandlerRegistry(handlers=[UserMessageHandler()], fallback_handler=OtherHandler()),
        manager=manager,
        publish=_noop_publish,
    )

    await router.handle_event(
        ACPProgressEvent(
            session_id="session-other",
            raw_update={"raw": "mystery"},
            raw_json={"raw": "mystery"},
            update_type=ACPUpdateType.UNKNOWN,
            family="other",
            route_key="session-other",
        )
    )

    assert manager.iter_pools_for_session(session_id="session-other") == []


async def _noop_publish(content: str, metadata: dict[str, object], reason: str) -> None:
    del content, metadata, reason
