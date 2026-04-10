from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Awaitable, Callable

import pytest

from nanobot.acp.dispatcher import ACPDispatcher
from nanobot.acp.progress_event_types import ACPProgressEvent
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


async def _emit_progress_event(
    dispatcher: ACPDispatcher,
    session_id: str,
    event: ACPProgressEvent,
) -> None:
    state = dispatcher._session_states[session_id]
    await state.on_progress_event(event)


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
        await _emit_progress_event(
            dispatcher,
            session_id,
            ACPProgressEvent(
                session_id=session_id,
                raw_update={"content": {"text": "hello "}},
                raw_json={"content": {"text": "hello "}},
                update_type="AgentMessageChunk",
                family="text",
                route_key=session_id,
            ),
        )
        await _emit_progress_event(
            dispatcher,
            session_id,
            ACPProgressEvent(
                session_id=session_id,
                raw_update={"content": {"text": "world"}},
                raw_json={"content": {"text": "world"}},
                update_type="AgentMessageChunk",
                family="text",
                route_key=session_id,
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
        await _emit_progress_event(
            dispatcher,
            session_id,
            ACPProgressEvent(
                session_id=session_id,
                raw_update={"toolCallId": "tc-1", "status": "completed", "title": "ls"},
                raw_json={"toolCallId": "tc-1", "status": "completed", "title": "ls"},
                update_type="ToolCallProgress",
                family="tool",
                route_key="tc-1",
                extracted={"status": "completed"},
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
        await _emit_progress_event(
            dispatcher,
            session_id,
            ACPProgressEvent(
                session_id=session_id,
                raw_update={"content": {"text": "text-before-tool"}},
                raw_json={"content": {"text": "text-before-tool"}},
                update_type="AgentMessageChunk",
                family="text",
                route_key=session_id,
            ),
        )
        await _emit_progress_event(
            dispatcher,
            session_id,
            ACPProgressEvent(
                session_id=session_id,
                raw_update={"toolCallId": "tc-3", "status": "in_progress", "title": "ls"},
                raw_json={"toolCallId": "tc-3", "status": "in_progress", "title": "ls"},
                update_type="ToolCallProgress",
                family="tool",
                route_key="tc-3",
                extracted={"status": "in_progress"},
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
        await _emit_progress_event(
            dispatcher,
            session_id,
            ACPProgressEvent(
                session_id=session_id,
                raw_update={"content": {"text": "compat-off"}},
                raw_json={"content": {"text": "compat-off"}},
                update_type="AgentMessageChunk",
                family="text",
                route_key=session_id,
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
        await _emit_progress_event(
            dispatcher,
            session_id,
            ACPProgressEvent(
                session_id=session_id,
                raw_update={"toolCallId": "tc-hint", "status": "completed", "title": "ls"},
                raw_json={"toolCallId": "tc-hint", "status": "completed", "title": "ls"},
                update_type="ToolCallProgress",
                family="tool",
                route_key="tc-hint",
                extracted={"status": "completed"},
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


def test_channels_send_final_alias_parsing() -> None:
    cfg = ChannelsConfig.model_validate({"sendFinal": False})
    assert cfg.send_final is False
