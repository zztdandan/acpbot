from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Awaitable, Callable

import pytest
from acp.interfaces import AgentMessageChunk
from acp.schema import CurrentModeUpdate, ToolCallProgress

from nanobot.acp.dispatcher import ACPDispatcher
from nanobot.acp.progress_event_types import ACPProgressEvent
from nanobot.acp.state import ACPBucketType, ACPUpdateType, SessionStateManager
from nanobot.acp.state.handlers.agent_message_media import AgentMessageMediaHandler
from nanobot.acp.state.handlers.agent_message_text import AgentMessageTextHandler
from nanobot.acp.state.handlers.other import OtherHandler
from nanobot.acp.state.handlers.tool import ToolHandler
from nanobot.acp.state.handlers.user_message import UserMessageHandler
from nanobot.acp.state.router import HandlerRegistry, SessionStateRouter
from nanobot.bus.events import InboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import ACPBackendConfig, ChannelsConfig


class _FakePromptConn:
    """Drive ACP prompt calls with deterministic in-test events."""

    def __init__(
        self,
        prompt_hook: Callable[[str], Awaitable[None]],
    ) -> None:
        self._prompt_hook = prompt_hook

    async def prompt(self, *, session_id: str, **kwargs: Any) -> None:
        del kwargs
        await self._prompt_hook(session_id)


def _install_fake_runtime(
    monkeypatch: pytest.MonkeyPatch,
    dispatcher: ACPDispatcher,
    prompt_hook: Callable[[str], Awaitable[None]],
    *,
    session_id: str,
) -> None:
    """Stub ACP connection/session helpers so tests can exercise the real progress pipeline."""

    dispatcher._conn = _FakePromptConn(prompt_hook)
    dispatcher._build_inbound_prompt_blocks = lambda *args, **kwargs: [object()]  # type: ignore[method-assign]

    async def _noop_ensure_connection(runtime: object) -> None:
        del runtime

    async def _fixed_ensure_session(runtime: object, session_key: str, **kwargs: Any) -> str:
        del runtime, session_key, kwargs
        return session_id

    monkeypatch.setattr("nanobot.acp.session_runtime._ensure_connection", _noop_ensure_connection)
    monkeypatch.setattr("nanobot.acp.session_runtime._ensure_session", _fixed_ensure_session)


@pytest.mark.asyncio
async def test_acp_progress_text_uses_new_metadata_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(progress_text_idle_seconds=0.01),
        channels_config=ChannelsConfig(send_final=True),
    )

    async def _prompt_hook(session_id: str) -> None:
        await dispatcher._handle_session_update(
            session_id,
            AgentMessageChunk.model_validate(
                {
                    "content": {"type": "text", "text": "hello "},
                    "sessionUpdate": "agent_message_chunk",
                }
            ),
        )
        await dispatcher._handle_session_update(
            session_id,
            AgentMessageChunk.model_validate(
                {
                    "content": {"type": "text", "text": "world"},
                    "sessionUpdate": "agent_message_chunk",
                }
            ),
        )

    _install_fake_runtime(monkeypatch, dispatcher, _prompt_hook, session_id="sess-1")

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    progress = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    final = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    assert progress.content == "hello world"
    assert progress.metadata.get("_acp_kind") == "text"
    assert progress.metadata.get("_acp_flush_reason") in {"close", "deadman", "size"}
    assert final.content == "<final>hello world</final>"


@pytest.mark.asyncio
async def test_acp_progress_tool_routes_by_tool_call_id(monkeypatch: pytest.MonkeyPatch) -> None:
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(
            progress_tool_idle_seconds=0.2,
            progress_tool_terminal_delay_seconds=0.01,
        ),
        channels_config=ChannelsConfig(send_final=True),
    )

    async def _prompt_hook(session_id: str) -> None:
        await dispatcher._handle_session_update(
            session_id,
            ToolCallProgress.model_validate(
                {
                    "toolCallId": "tc-1",
                    "status": "completed",
                    "title": "ls",
                    "sessionUpdate": "tool_call_update",
                }
            ),
        )
        await asyncio.sleep(0.03)

    _install_fake_runtime(monkeypatch, dispatcher, _prompt_hook, session_id="sess-2")

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    progress = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    final = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    assert progress.metadata.get("_acp_kind") == "tool"
    assert progress.metadata.get("_acp_route_key") == "tc-1"
    assert progress.content.startswith("[tool]")
    assert final.content == "<final></final>"


@pytest.mark.asyncio
async def test_acp_text_flush_reason_never_uses_family_switch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(progress_text_idle_seconds=5.0),
        channels_config=ChannelsConfig(send_final=True),
    )

    async def _prompt_hook(session_id: str) -> None:
        await dispatcher._handle_session_update(
            session_id,
            AgentMessageChunk.model_validate(
                {
                    "content": {"type": "text", "text": "text-before-tool"},
                    "sessionUpdate": "agent_message_chunk",
                }
            ),
        )
        await dispatcher._handle_session_update(
            session_id,
            ToolCallProgress.model_validate(
                {
                    "toolCallId": "tc-3",
                    "status": "in_progress",
                    "title": "ls",
                    "sessionUpdate": "tool_call_update",
                }
            ),
        )

    _install_fake_runtime(monkeypatch, dispatcher, _prompt_hook, session_id="sess-3")

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    progress_frames = []
    for _ in range(3):
        progress_frames.append(await asyncio.wait_for(bus.consume_outbound(), timeout=1.0))

    text_progress = next(
        frame for frame in progress_frames if (frame.metadata or {}).get("_acp_kind") == "text"
    )
    assert text_progress.metadata.get("_acp_flush_reason") in {"deadman", "size", "close"}
    assert text_progress.metadata.get("_acp_flush_reason") != "family_switch"


@pytest.mark.asyncio
async def test_process_direct_mirrors_progress_to_on_progress_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(progress_text_idle_seconds=0.01),
        channels_config=ChannelsConfig(send_final=True),
    )
    progress_lines: list[str] = []

    async def _prompt_hook(session_id: str) -> None:
        await dispatcher._handle_session_update(
            session_id,
            AgentMessageChunk.model_validate(
                {
                    "content": {"type": "text", "text": "compat-off"},
                    "sessionUpdate": "agent_message_chunk",
                }
            ),
        )

    _install_fake_runtime(monkeypatch, dispatcher, _prompt_hook, session_id="sess-4")

    async def _capture_progress(content: str) -> None:
        progress_lines.append(content)

    response = await dispatcher.process_direct(
        "run",
        session_key="cli:direct",
        channel="cli",
        chat_id="c",
        on_progress=_capture_progress,
    )

    progress = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    assert progress.content == "compat-off"
    assert progress_lines == ["compat-off"]
    assert response.content == "compat-off"


@pytest.mark.asyncio
async def test_process_direct_marks_tool_progress_as_tool_hint_for_on_progress_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(progress_tool_terminal_delay_seconds=0.01),
        channels_config=ChannelsConfig(send_final=True),
    )
    mirrored: list[tuple[str, bool]] = []

    async def _prompt_hook(session_id: str) -> None:
        await dispatcher._handle_session_update(
            session_id,
            ToolCallProgress.model_validate(
                {
                    "toolCallId": "tc-hint",
                    "status": "completed",
                    "title": "ls",
                    "sessionUpdate": "tool_call_update",
                }
            ),
        )
        await asyncio.sleep(0.03)

    _install_fake_runtime(monkeypatch, dispatcher, _prompt_hook, session_id="sess-tool")

    async def _capture_progress(content: str, *, tool_hint: bool = False) -> None:
        mirrored.append((content, tool_hint))

    response = await dispatcher.process_direct(
        "run",
        session_key="cli:tool",
        channel="cli",
        chat_id="c",
        on_progress=_capture_progress,
    )

    progress = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    assert progress.metadata.get("_acp_kind") == "tool"
    assert mirrored == [(progress.content, True)]
    assert response.content == ""


@pytest.mark.asyncio
async def test_text_handler_uses_message_text_pool_and_preserves_deadman_size_close() -> None:
    manager = SessionStateManager()
    published: list[tuple[str, dict[str, Any], str]] = []
    router = SessionStateRouter(
        HandlerRegistry(
            handlers=[AgentMessageTextHandler(), AgentMessageMediaHandler()],
            fallback_handler=AgentMessageTextHandler(),
        ),
        manager=manager,
        publish=lambda content, metadata, reason: _capture_publish(
            published, content, metadata, reason
        ),
        text_idle_seconds=0.01,
        text_max_chars=4,
    )

    await router.handle_event(
        ACPProgressEvent(
            session_id="sess-text",
            raw_update={"content": {"text": "hi"}},
            raw_json={"content": {"text": "hi"}},
            update_type=ACPUpdateType.AGENT_MESSAGE_CHUNK,
            family="text",
            route_key="sess-text",
        )
    )
    assert (
        manager.get_pool_by_key(
            bucket_type=ACPBucketType.TEXT,
            session_id="sess-text",
            bucket_key="sess-text",
        )
        is not None
    )

    await asyncio.sleep(0.03)

    await router.handle_event(
        ACPProgressEvent(
            session_id="sess-text",
            raw_update={"content": {"text": "ab"}},
            raw_json={"content": {"text": "ab"}},
            update_type=ACPUpdateType.AGENT_MESSAGE_CHUNK,
            family="text",
            route_key="sess-text",
        )
    )
    await router.handle_event(
        ACPProgressEvent(
            session_id="sess-text",
            raw_update={"content": {"text": "cd"}},
            raw_json={"content": {"text": "cd"}},
            update_type=ACPUpdateType.AGENT_MESSAGE_CHUNK,
            family="text",
            route_key="sess-text",
        )
    )
    await router.handle_event(
        ACPProgressEvent(
            session_id="sess-text",
            raw_update={"content": {"text": "z"}},
            raw_json={"content": {"text": "z"}},
            update_type=ACPUpdateType.AGENT_MESSAGE_CHUNK,
            family="text",
            route_key="sess-text",
        )
    )
    await router.close("sess-text")

    flush_reasons = [metadata.get("_acp_flush_reason") for _, metadata, _ in published]
    assert flush_reasons == ["deadman", "size", "close"]
    finalized = manager.finalize_request_scope("sess-text")
    assert finalized.final_text == "hiabcdz"
    assert (
        manager.iter_pools_for_session(session_id="sess-text", bucket_type=ACPBucketType.TEXT) == []
    )


@pytest.mark.asyncio
async def test_media_handler_uses_media_pool_and_updates_final_media() -> None:
    manager = SessionStateManager()
    published: list[tuple[str, dict[str, Any], str]] = []
    router = SessionStateRouter(
        HandlerRegistry(
            handlers=[AgentMessageTextHandler(), AgentMessageMediaHandler()],
            fallback_handler=AgentMessageTextHandler(),
        ),
        manager=manager,
        publish=lambda content, metadata, reason: _capture_publish(
            published, content, metadata, reason
        ),
        media_idle_seconds=0.01,
    )

    event = ACPProgressEvent(
        session_id="sess-media",
        raw_update={"content": {"type": "image", "uri": "file:///tmp/image.png"}},
        raw_json={"content": {"type": "image", "uri": "file:///tmp/image.png"}},
        update_type=ACPUpdateType.AGENT_MESSAGE_CHUNK,
        family="media",
        route_key="file:///tmp/image.png",
        extracted={"content_type": "image"},
        ext={"media_path": "/tmp/image.png"},
    )
    await router.handle_event(event)

    assert (
        manager.get_pool_by_key(
            bucket_type=ACPBucketType.MEDIA,
            session_id="sess-media",
            bucket_key="file:///tmp/image.png",
        )
        is not None
    )

    await asyncio.sleep(0.03)
    await router.close("sess-media")

    assert published[0][1].get("_acp_kind") == "media"
    assert len(published) == 1
    finalized = manager.finalize_request_scope("sess-media")
    assert finalized.media_paths == ["/tmp/image.png"]
    assert (
        manager.iter_pools_for_session(session_id="sess-media", bucket_type=ACPBucketType.MEDIA)
        == []
    )


@pytest.mark.asyncio
async def test_tool_handler_uses_tool_pool_history_and_terminal_delay() -> None:
    manager = SessionStateManager()
    published: list[tuple[str, dict[str, Any], str]] = []
    router = SessionStateRouter(
        HandlerRegistry(
            handlers=[
                AgentMessageTextHandler(),
                AgentMessageMediaHandler(),
                ToolHandler(),
                UserMessageHandler(),
            ],
            fallback_handler=OtherHandler(),
        ),
        manager=manager,
        publish=lambda content, metadata, reason: _capture_publish(
            published, content, metadata, reason
        ),
        tool_idle_seconds=5.0,
        tool_terminal_delay_seconds=0.01,
    )

    await router.handle_event(
        ACPProgressEvent(
            session_id="sess-tool",
            raw_update={"toolCallId": "tc-1", "status": "in_progress", "title": "ls"},
            raw_json={"toolCallId": "tc-1", "status": "in_progress", "title": "ls"},
            update_type=ACPUpdateType.TOOL_CALL_PROGRESS,
            family="tool",
            route_key="tc-1",
            extracted={"status": "in_progress"},
        )
    )
    await router.handle_event(
        ACPProgressEvent(
            session_id="sess-tool",
            raw_update={"toolCallId": "tc-1", "status": "completed", "title": "ls"},
            raw_json={"toolCallId": "tc-1", "status": "completed", "title": "ls"},
            update_type=ACPUpdateType.TOOL_CALL_PROGRESS,
            family="tool",
            route_key="tc-1",
            extracted={"status": "completed"},
        )
    )

    assert (
        manager.get_pool_by_key(
            bucket_type=ACPBucketType.TOOL,
            session_id="sess-tool",
            bucket_key="tc-1",
        )
        is not None
    )

    await asyncio.sleep(0.03)

    payload = published[0][1].get("_acp_payload") or {}
    assert published[0][1].get("_acp_kind") == "tool"
    assert published[0][1].get("_acp_route_key") == "tc-1"
    assert len(payload.get("history") or []) == 2
    assert payload.get("status") == "completed"


@pytest.mark.asyncio
async def test_process_direct_uses_session_state_manager_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(progress_text_idle_seconds=0.01),
        channels_config=ChannelsConfig(send_final=True),
    )

    async def _prompt_hook(session_id: str) -> None:
        await dispatcher._handle_session_update(
            session_id,
            AgentMessageChunk.model_validate(
                {
                    "content": {"type": "text", "text": "through-manager"},
                    "sessionUpdate": "agent_message_chunk",
                }
            ),
        )
        await asyncio.sleep(0.03)

    _install_fake_runtime(monkeypatch, dispatcher, _prompt_hook, session_id="sess-cutover")

    response = await dispatcher.process_direct(
        "run",
        session_key="cli:cutover",
        channel="cli",
        chat_id="c",
    )

    progress = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    assert progress.metadata.get("_acp_kind") == "text"
    assert response.content == "through-manager"


@pytest.mark.asyncio
async def test_stage_a_current_mode_compatibility_is_preserved_during_cutover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(progress_text_idle_seconds=0.01),
        channels_config=ChannelsConfig(send_final=True),
    )
    seen_session_ids: list[str] = []

    async def _prompt_hook(session_id: str) -> None:
        seen_session_ids.append(session_id)
        await dispatcher._handle_session_update(
            session_id,
            CurrentModeUpdate.model_validate(
                {
                    "currentModeId": "agent-cutover",
                    "sessionUpdate": "current_mode_update",
                }
            ),
        )

    _install_fake_runtime(monkeypatch, dispatcher, _prompt_hook, session_id="sess-mode")

    await dispatcher.process_direct(
        "run",
        session_key="cli:mode-cutover",
        channel="cli",
        chat_id="c",
    )

    assert seen_session_ids == ["sess-mode"]
    assert dispatcher._session_caps["sess-mode"].current_agent == "agent-cutover"


async def _capture_publish(
    published: list[tuple[str, dict[str, Any], str]],
    content: str,
    metadata: dict[str, Any],
    reason: str,
) -> None:
    published.append((content, metadata, reason))


def test_channels_send_final_alias_parsing() -> None:
    cfg = ChannelsConfig.model_validate({"sendFinal": False})
    assert cfg.send_final is False
