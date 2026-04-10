from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from nanobot.acp.dispatcher import ACPDispatcher
from nanobot.acp.progress_event_types import ACPProgressEvent
from nanobot.bus.events import InboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import ACPBackendConfig, ChannelsConfig


@pytest.mark.asyncio
async def test_acp_progress_text_uses_new_metadata_schema() -> None:
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(progress_text_idle_seconds=0.01),
        channels_config=ChannelsConfig(send_final=True),
    )

    async def fake_process_direct(
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress=None,
        on_progress_event=None,
    ) -> str:
        del content, session_key, channel, chat_id, preferred_model, preferred_agent, on_progress
        assert on_progress_event is not None
        await on_progress_event(
            ACPProgressEvent(
                session_id="sess-1",
                raw_update={"content": {"text": "hello "}},
                raw_json={"content": {"text": "hello "}},
                update_type="AgentMessageChunk",
                family="text",
                route_key="sess-1",
            )
        )
        await on_progress_event(
            ACPProgressEvent(
                session_id="sess-1",
                raw_update={"content": {"text": "world"}},
                raw_json={"content": {"text": "world"}},
                update_type="AgentMessageChunk",
                family="text",
                route_key="sess-1",
            )
        )
        return "done"

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    progress = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    final = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    assert progress.content == "hello world"
    assert progress.metadata.get("_acp_kind") == "text"
    assert progress.metadata.get("_acp_flush_reason") in {
        "close",
        "deadman",
        "family_switch",
        "size",
    }
    assert final.content == "<final>done</final>"


@pytest.mark.asyncio
async def test_acp_progress_tool_routes_by_tool_call_id() -> None:
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

    async def fake_process_direct(
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress=None,
        on_progress_event=None,
    ) -> str:
        del content, session_key, channel, chat_id, preferred_model, preferred_agent, on_progress
        assert on_progress_event is not None
        await on_progress_event(
            ACPProgressEvent(
                session_id="sess-2",
                raw_update={"toolCallId": "tc-1", "status": "completed", "title": "ls"},
                raw_json={"toolCallId": "tc-1", "status": "completed", "title": "ls"},
                update_type="ToolCallProgress",
                family="tool",
                route_key="tc-1",
                extracted={"status": "completed"},
            )
        )
        await asyncio.sleep(0.03)
        return "done"

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    progress = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    final = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    assert progress.metadata.get("_acp_kind") == "tool"
    assert progress.metadata.get("_acp_route_key") == "tc-1"
    assert progress.content.startswith("[tool]")
    assert final.content == "<final>done</final>"


@pytest.mark.asyncio
async def test_acp_text_flush_reason_never_uses_family_switch() -> None:
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(progress_text_idle_seconds=5.0),
        channels_config=ChannelsConfig(send_final=True),
    )

    async def fake_process_direct(
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress=None,
        on_progress_event=None,
    ) -> str:
        del content, session_key, channel, chat_id, preferred_model, preferred_agent, on_progress
        assert on_progress_event is not None
        await on_progress_event(
            ACPProgressEvent(
                session_id="sess-3",
                raw_update={"content": {"text": "text-before-tool"}},
                raw_json={"content": {"text": "text-before-tool"}},
                update_type="AgentMessageChunk",
                family="text",
                route_key="sess-3",
            )
        )
        await on_progress_event(
            ACPProgressEvent(
                session_id="sess-3",
                raw_update={"toolCallId": "tc-3", "status": "in_progress", "title": "ls"},
                raw_json={"toolCallId": "tc-3", "status": "in_progress", "title": "ls"},
                update_type="ToolCallProgress",
                family="tool",
                route_key="tc-3",
                extracted={"status": "in_progress"},
            )
        )
        return "done"

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

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
async def test_dispatch_uses_event_callback_without_progress_compat() -> None:
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(progress_text_idle_seconds=0.01),
        channels_config=ChannelsConfig(send_final=True),
    )
    captured: dict[str, object] = {}

    async def fake_process_direct(
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress=None,
        on_progress_event=None,
    ) -> str:
        del content, session_key, channel, chat_id, preferred_model, preferred_agent
        captured["on_progress"] = on_progress
        captured["on_progress_event"] = on_progress_event
        assert on_progress_event is not None
        await on_progress_event(
            ACPProgressEvent(
                session_id="sess-4",
                raw_update={"content": {"text": "compat-off"}},
                raw_json={"content": {"text": "compat-off"}},
                update_type="AgentMessageChunk",
                family="text",
                route_key="sess-4",
            )
        )
        return "done"

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    assert captured.get("on_progress") is None
    assert captured.get("on_progress_event") is not None


def test_channels_send_final_alias_parsing() -> None:
    cfg = ChannelsConfig.model_validate({"sendFinal": False})
    assert cfg.send_final is False
