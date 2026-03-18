from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import ACPBackendConfig, ChannelsConfig
from nanobot.dispatch.acp import ACPDispatcher, _ACPDispatchError


def test_channels_send_final_alias_parsing() -> None:
    # Given: 配置文件常用 camelCase，sendFinal 需要正确映射到 send_final。
    cfg = ChannelsConfig.model_validate({"sendFinal": False})
    assert cfg.send_final is False


def test_acp_outbound_debug_payload_contains_tool_items() -> None:
    # Given: _tool_hint=true 时，调试 JSON 中应显式记录 tool 的分项内容。
    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
        channels_config=ChannelsConfig(send_final=True),
    )
    payload = dispatcher._build_outbound_debug_payload(
        msg=OutboundMessage(
            channel="telegram",
            chat_id="123",
            content="glob\nread",
            metadata={"_tool_hint": True, "_progress": True},
        ),
        reason="progress_tool_hint",
        session_key="telegram:123",
    )
    assert payload["tool"]["is_tool_hint"] is True
    assert payload["tool"]["items"] == ["glob", "read"]
    assert payload["content"] == "glob\nread"


@pytest.mark.asyncio
async def test_acp_progress_flush_on_type_switch_and_final_wrapper() -> None:
    # Given: 构造真实 dispatcher，但用 fake process_direct 注入 progress 序列，避免依赖外部 ACP 进程。
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
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
    ) -> str:
        del content, session_key, channel, chat_id, preferred_model, preferred_agent
        assert on_progress is not None
        await on_progress("alpha ")
        await on_progress("beta")
        await on_progress("tool-a", tool_hint=True)
        await on_progress("tool-b", tool_hint=True)
        return "done"

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    first = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    second = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    third = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    fourth = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)

    assert first.content == "alpha beta"
    assert first.metadata.get("_progress") is True
    assert first.metadata.get("_tool_hint") is False

    # 中文注释：默认模式改为“tool hint 逐条直发 + JSON 数组消息体（单事件数组）”。
    second_payload = json.loads(second.content)
    assert isinstance(second_payload, list)
    assert len(second_payload) == 1
    assert second_payload[0]["raw_hint"] == "tool-a"
    assert second.metadata.get("_progress") is True
    assert second.metadata.get("_tool_hint") is True

    third_payload = json.loads(third.content)
    assert isinstance(third_payload, list)
    assert len(third_payload) == 1
    assert third_payload[0]["raw_hint"] == "tool-b"
    assert third.metadata.get("_progress") is True
    assert third.metadata.get("_tool_hint") is True

    assert fourth.content == "<final>done</final>"
    assert fourth.metadata.get("_progress") is None


@pytest.mark.asyncio
async def test_acp_progress_deadman_flush_after_idle_2s() -> None:
    # Given: 验证“死手机制”——2 秒内无新消息时自动 flush，且在 final 前已落盘到 bus。
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
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
    ) -> str:
        del content, session_key, channel, chat_id, preferred_model, preferred_agent
        assert on_progress is not None
        await on_progress("idle-flush")
        await asyncio.sleep(2.2)
        return "done"

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    progress = await asyncio.wait_for(bus.consume_outbound(), timeout=0.5)
    final = await asyncio.wait_for(bus.consume_outbound(), timeout=0.5)
    assert progress.content == "idle-flush"
    assert progress.metadata.get("_progress") is True
    assert final.content == "<final>done</final>"


@pytest.mark.asyncio
async def test_acp_can_disable_final_by_config() -> None:
    # Given: send_final=false 时，仅允许 progress/tool-hint 消息，不发送 final。
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
        channels_config=ChannelsConfig(send_final=False),
    )

    async def fake_process_direct(
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress=None,
    ) -> str:
        del content, session_key, channel, chat_id, preferred_model, preferred_agent
        assert on_progress is not None
        await on_progress("only-progress")
        return "hidden-final"

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    progress = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    assert progress.content == "only-progress"
    assert progress.metadata.get("_progress") is True

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(bus.consume_outbound(), timeout=0.2)


@pytest.mark.asyncio
async def test_acp_tool_hint_merge_by_tool_call_id_with_per_id_deadman() -> None:
    # Given: merge_by_tool_call_id 模式下，同一 toolCallId 聚合，且按每个 ID 独立死手 flush。
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
        channels_config=ChannelsConfig(
            send_final=True,
            tool_hint_publish_mode="merge_by_tool_call_id",
            tool_hint_merge_idle_seconds=0.05,
            tool_hint_payload_mode="array",
        ),
    )

    async def fake_process_direct(
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress=None,
    ) -> str:
        del content, session_key, channel, chat_id, preferred_model, preferred_agent
        assert on_progress is not None
        # 中文注释：两个不同 toolCallId 的事件交错输入，验证不会互相混桶。
        await on_progress(
            "in_progress",
            tool_hint=True,
            tool_event={
                "event": "tool_progress",
                "status": "in_progress",
                "tool_call": {"toolCallId": "A", "status": "in_progress"},
            },
        )
        await on_progress(
            "completed",
            tool_hint=True,
            tool_event={
                "event": "tool_progress",
                "status": "completed",
                "tool_call": {"toolCallId": "A", "status": "completed"},
            },
        )
        await on_progress(
            "in_progress",
            tool_hint=True,
            tool_event={
                "event": "tool_progress",
                "status": "in_progress",
                "tool_call": {"toolCallId": "B", "status": "in_progress"},
            },
        )
        await asyncio.sleep(0.08)
        return "done"

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    tool_a = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    tool_b = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    final = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)

    payload_a = json.loads(tool_a.content)
    payload_b = json.loads(tool_b.content)
    assert [item["tool_call"]["toolCallId"] for item in payload_a] == ["A", "A"]
    assert [item["tool_call"]["toolCallId"] for item in payload_b] == ["B"]
    assert final.content == "<final>done</final>"


@pytest.mark.asyncio
async def test_acp_tool_hint_status_with_compact_payload_mode() -> None:
    # Given: status_with_compact 模式下，completed/failed 事件成为主体，其他事件进入 compact_events。
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
        channels_config=ChannelsConfig(
            send_final=True,
            tool_hint_publish_mode="merge_by_tool_call_id",
            tool_hint_merge_idle_seconds=0.05,
            tool_hint_payload_mode="status_with_compact",
        ),
    )

    async def fake_process_direct(
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress=None,
    ) -> str:
        del content, session_key, channel, chat_id, preferred_model, preferred_agent
        assert on_progress is not None
        await on_progress(
            "in_progress",
            tool_hint=True,
            tool_event={
                "event": "tool_progress",
                "session_id": "ses-x",
                "status": "in_progress",
                "tool_call": {"toolCallId": "X", "status": "in_progress"},
            },
        )
        await on_progress(
            "completed",
            tool_hint=True,
            tool_event={
                "event": "tool_progress",
                "session_id": "ses-x",
                "status": "completed",
                "tool_call": {"toolCallId": "X", "status": "completed"},
            },
        )
        return "done"

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    tool = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    final = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    payload = json.loads(tool.content)
    assert payload["session_id"] == "ses-x"
    assert payload["status"] == "completed"
    assert payload["tool_call"]["toolCallId"] == "X"
    assert len(payload["progress_info"]) == 1
    assert payload["progress_info"][0]["tool_call"]["status"] == "in_progress"
    assert payload["progress_info"][0]["status"] == "in_progress"
    assert final.content == "<final>done</final>"


@pytest.mark.asyncio
async def test_acp_tool_hint_status_with_compact_supports_custom_terminal_status() -> None:
    # Given: terminal status 可配置，便于后续扩展 cancelled/timeout 等状态。
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
        channels_config=ChannelsConfig(
            send_final=True,
            tool_hint_publish_mode="merge_by_tool_call_id",
            tool_hint_merge_idle_seconds=0.05,
            tool_hint_payload_mode="status_with_compact",
            tool_hint_terminal_statuses=["completed", "failed", "cancelled"],
        ),
    )

    async def fake_process_direct(
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress=None,
    ) -> str:
        del content, session_key, channel, chat_id, preferred_model, preferred_agent
        assert on_progress is not None
        await on_progress(
            "in_progress",
            tool_hint=True,
            tool_event={
                "event": "tool_progress",
                "session_id": "ses-y",
                "status": "in_progress",
                "tool_call": {"toolCallId": "Y", "status": "in_progress"},
            },
        )
        await on_progress(
            "cancelled",
            tool_hint=True,
            tool_event={
                "event": "tool_progress",
                "session_id": "ses-y",
                "status": "cancelled",
                "tool_call": {"toolCallId": "Y", "status": "cancelled"},
            },
        )
        return "done"

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    tool = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    final = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    payload = json.loads(tool.content)
    assert payload["status"] == "cancelled"
    assert payload["tool_call"]["toolCallId"] == "Y"
    assert len(payload["progress_info"]) == 1
    assert payload["progress_info"][0]["status"] == "in_progress"
    assert final.content == "<final>done</final>"


@pytest.mark.asyncio
async def test_acp_dispatch_error_without_partial_keeps_error_reply() -> None:
    # Given: 当 ACP 异常没有 partial 时，应该走统一错误回复而不是静默吞掉。
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
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
    ) -> str:
        del content, session_key, channel, chat_id, preferred_model, preferred_agent, on_progress
        raise _ACPDispatchError(partial_response="")

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    err = await asyncio.wait_for(bus.consume_outbound(), timeout=1.0)
    assert err.content == "Sorry, I encountered an error."


@pytest.mark.asyncio
async def test_acp_partial_with_send_final_false_emits_no_error() -> None:
    # Given: 有 partial 但 send_final=false 时，不应发送 final，也不应误发通用错误。
    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
        channels_config=ChannelsConfig(send_final=False),
    )

    async def fake_process_direct(
        content: str,
        session_key: str = "cli:direct",
        channel: str = "cli",
        chat_id: str = "direct",
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
        on_progress=None,
    ) -> str:
        del content, session_key, channel, chat_id, preferred_model, preferred_agent, on_progress
        raise _ACPDispatchError(partial_response="partial-ready")

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(bus.consume_outbound(), timeout=0.2)
