from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from nanobot.bus.events import InboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import ACPBackendConfig, ChannelsConfig
from nanobot.dispatch.acp import ACPDispatcher, _ACPDispatchError


def test_channels_send_final_alias_parsing() -> None:
    # Given: 配置文件常用 camelCase，sendFinal 需要正确映射到 send_final。
    cfg = ChannelsConfig.model_validate({"sendFinal": False})
    assert cfg.send_final is False


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
        on_progress=None,
    ) -> str:
        del content, session_key, channel, chat_id
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

    assert first.content == "alpha beta"
    assert first.metadata.get("_progress") is True
    assert first.metadata.get("_tool_hint") is False

    assert second.content == "tool-a\ntool-b"
    assert second.metadata.get("_progress") is True
    assert second.metadata.get("_tool_hint") is True

    assert third.content == "<final>done</final>"
    assert third.metadata.get("_progress") is None


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
        on_progress=None,
    ) -> str:
        del content, session_key, channel, chat_id
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
        on_progress=None,
    ) -> str:
        del content, session_key, channel, chat_id
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
        on_progress=None,
    ) -> str:
        del content, session_key, channel, chat_id, on_progress
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
        on_progress=None,
    ) -> str:
        del content, session_key, channel, chat_id, on_progress
        raise _ACPDispatchError(partial_response="partial-ready")

    dispatcher.process_direct = fake_process_direct  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(channel="cli", sender_id="u", chat_id="c", content="run")
    )

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(bus.consume_outbound(), timeout=0.2)
