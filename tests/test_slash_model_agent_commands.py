from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from acp.schema import SetSessionModelResponse

from nanobot.bus.events import InboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import ACPBackendConfig
from nanobot.dispatch.acp import ACPDispatcher, _SessionCapabilities


def _make_loop():
    from nanobot.agent.loop import AgentLoop

    bus = MessageBus()
    provider = MagicMock()
    provider.get_default_model.return_value = "test-model"
    workspace = MagicMock()
    workspace.__truediv__ = MagicMock(return_value=MagicMock())

    with (
        patch("nanobot.agent.loop.ContextBuilder"),
        patch("nanobot.agent.loop.SessionManager"),
        patch("nanobot.agent.loop.SubagentManager") as mock_sub_mgr,
    ):
        mock_sub_mgr.return_value.cancel_by_session = AsyncMock(return_value=0)
        loop = AgentLoop(bus=bus, provider=provider, workspace=workspace)
    return loop


@pytest.mark.asyncio
async def test_agentloop_help_matches_main_command_set() -> None:
    loop = _make_loop()

    response = await loop._process_message(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/help",
        )
    )

    assert response is not None
    assert "/restart" in response.content
    assert "/models" not in response.content
    assert "/set_model" not in response.content


class _DummyACPConn:
    def __init__(self) -> None:
        self.model_calls: list[tuple[str, str]] = []
        self.mode_calls: list[tuple[str, str]] = []
        self.current_model = "opencode/big-pickle"
        self.current_mode = "build"

    async def set_session_model(self, model_id: str, session_id: str):
        # 中文注释：沿用 python-sdk 的 set_session_model 形参顺序，确保 nanobot 调用方式与 SDK 一致。
        self.model_calls.append((session_id, model_id))
        self.current_model = model_id
        return SetSessionModelResponse()

    async def set_session_mode(self, mode_id: str, session_id: str):
        self.mode_calls.append((session_id, mode_id))
        self.current_mode = mode_id

    async def load_session(self, cwd: str, session_id: str, mcp_servers: list[object]):
        del cwd, session_id, mcp_servers
        return SimpleNamespace(
            models=SimpleNamespace(
                current_model_id=self.current_model,
                available_models=[
                    SimpleNamespace(model_id="opencode/big-pickle"),
                    SimpleNamespace(model_id="anthropic/claude-sonnet-4"),
                ],
            ),
            modes=SimpleNamespace(
                current_mode_id=self.current_mode,
                available_modes=[SimpleNamespace(id="build"), SimpleNamespace(id="plan")],
            ),
        )


class _BlockingACPConn(_DummyACPConn):
    def __init__(self) -> None:
        super().__init__()
        self._release = asyncio.Event()

    async def set_session_model(self, model_id: str, session_id: str):
        self.model_calls.append((session_id, model_id))
        await self._release.wait()
        self.current_model = model_id
        return SetSessionModelResponse()

    def release(self) -> None:
        self._release.set()


class _ModelMismatchACPConn(_DummyACPConn):
    async def set_session_model(self, model_id: str, session_id: str):
        self.model_calls.append((session_id, model_id))
        # 中文注释：模拟 ACP 接收到了 set 请求，但会话状态仍停留在旧模型。
        self.current_model = "opencode/big-pickle"
        return SetSessionModelResponse()


class _EventuallyConsistentModelACPConn(_DummyACPConn):
    def __init__(self) -> None:
        super().__init__()
        self._load_calls = 0

    async def load_session(self, cwd: str, session_id: str, mcp_servers: list[object]):
        del cwd, session_id, mcp_servers
        self._load_calls += 1
        observed_model = "opencode/big-pickle" if self._load_calls == 1 else self.current_model
        return SimpleNamespace(
            models=SimpleNamespace(
                current_model_id=observed_model,
                available_models=[
                    SimpleNamespace(model_id="opencode/big-pickle"),
                    SimpleNamespace(model_id="anthropic/claude-sonnet-4"),
                ],
            ),
            modes=SimpleNamespace(
                current_mode_id=self.current_mode,
                available_modes=[SimpleNamespace(id="build"), SimpleNamespace(id="plan")],
            ),
        )


class _EpochAwareSlashConn(_DummyACPConn):
    def __init__(self) -> None:
        super().__init__()
        self.resume_calls: list[str] = []

    async def resume_session(self, cwd: str, session_id: str, mcp_servers: list[object]):
        del cwd, mcp_servers
        self.resume_calls.append(session_id)
        return SimpleNamespace(
            models=SimpleNamespace(
                current_model_id=self.current_model,
                available_models=[
                    SimpleNamespace(model_id="opencode/big-pickle"),
                    SimpleNamespace(model_id="anthropic/claude-sonnet-4"),
                ],
            ),
            modes=SimpleNamespace(
                current_mode_id=self.current_mode,
                available_modes=[SimpleNamespace(id="build"), SimpleNamespace(id="plan")],
            ),
        )


class _ConcurrentSlashPromptConn(_DummyACPConn):
    def __init__(self) -> None:
        super().__init__()
        self._release_model = asyncio.Event()
        self.prompt_calls = 0

    async def set_session_model(self, model_id: str, session_id: str):
        self.model_calls.append((session_id, model_id))
        await self._release_model.wait()
        self.current_model = model_id
        return SetSessionModelResponse()

    async def prompt(self, prompt: list[object], session_id: str, **kwargs: object):
        del prompt, session_id, kwargs
        self.prompt_calls += 1

    def release_model(self) -> None:
        self._release_model.set()


def _make_acp_dispatcher() -> tuple[ACPDispatcher, MessageBus, _DummyACPConn]:
    bus = MessageBus()
    conn = _DummyACPConn()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
    )
    dispatcher._conn = cast(Any, conn)
    caps = _SessionCapabilities()
    caps.available_models = ["opencode/big-pickle", "anthropic/claude-sonnet-4"]
    caps.current_model = "opencode/big-pickle"
    caps.available_agents = ["build", "plan"]
    caps.current_agent = "build"
    dispatcher._session_caps["sess-1"] = caps

    async def _ensure_session(session_key: str) -> str:
        _ = session_key
        return "sess-1"

    dispatcher._ensure_session = _ensure_session  # type: ignore[method-assign]
    return dispatcher, bus, conn


@pytest.mark.asyncio
async def test_acp_models_command_lists_cached_models() -> None:
    dispatcher, bus, _ = _make_acp_dispatcher()
    await dispatcher._dispatch(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/models",
        )
    )
    out = await bus.consume_outbound()
    assert "Available models:" in out.content
    assert "opencode/big-pickle" in out.content


@pytest.mark.asyncio
async def test_acp_set_model_and_set_agent_call_connection() -> None:
    dispatcher, bus, conn = _make_acp_dispatcher()

    await dispatcher._dispatch(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/set_model anthropic/claude-sonnet-4",
        )
    )
    out_model = await bus.consume_outbound()
    assert out_model.content == "Model switched to: anthropic/claude-sonnet-4"
    assert conn.model_calls == [("sess-1", "anthropic/claude-sonnet-4")]

    await dispatcher._dispatch(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/set_agent plan",
        )
    )
    out_agent = await bus.consume_outbound()
    assert out_agent.content == "Agent switched to: plan"
    assert conn.mode_calls == [("sess-1", "plan")]


@pytest.mark.asyncio
async def test_acp_set_model_success_updates_desired_and_persists() -> None:
    dispatcher, bus, _ = _make_acp_dispatcher()
    dispatcher._session_map["cli:chat"] = "sess-1"
    persist_mock = MagicMock()
    dispatcher._persist_session_map = persist_mock  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/set_model anthropic/claude-sonnet-4",
        )
    )
    out = await bus.consume_outbound()

    assert out.content == "Model switched to: anthropic/claude-sonnet-4"
    assert dispatcher._session_desired["cli:chat"]["model"] == "anthropic/claude-sonnet-4"
    persist_mock.assert_called_once_with()


@pytest.mark.asyncio
async def test_acp_set_agent_success_updates_desired_and_persists() -> None:
    dispatcher, bus, _ = _make_acp_dispatcher()
    dispatcher._session_map["cli:chat"] = "sess-1"
    dispatcher._session_desired["cli:chat"] = {"model": "opencode/big-pickle"}
    persist_mock = MagicMock()
    dispatcher._persist_session_map = persist_mock  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/set_agent plan",
        )
    )
    out = await bus.consume_outbound()

    assert out.content == "Agent switched to: plan"
    assert dispatcher._session_desired["cli:chat"] == {
        "model": "opencode/big-pickle",
        "agent": "plan",
    }
    persist_mock.assert_called_once_with()


@pytest.mark.asyncio
async def test_acp_set_model_updates_models_view_after_inbound_command() -> None:
    dispatcher, bus, _ = _make_acp_dispatcher()

    await dispatcher._dispatch(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/set_model anthropic/claude-sonnet-4",
        )
    )
    out_model = await bus.consume_outbound()
    assert out_model.content == "Model switched to: anthropic/claude-sonnet-4"

    # 中文注释：从 inbound 命令起步，再走 /models 验证最终可见状态，确保“设置结束后结果可确认”。
    await dispatcher._dispatch(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/models",
        )
    )
    out_models = await bus.consume_outbound()
    assert "Current model: anthropic/claude-sonnet-4" in out_models.content


@pytest.mark.asyncio
async def test_acp_set_model_waits_sdk_completion_before_success_outbound() -> None:
    bus = MessageBus()
    conn = _BlockingACPConn()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
    )
    dispatcher._conn = cast(Any, conn)

    caps = _SessionCapabilities()
    caps.available_models = ["opencode/big-pickle", "anthropic/claude-sonnet-4"]
    caps.current_model = "opencode/big-pickle"
    dispatcher._session_caps["sess-1"] = caps

    async def _ensure_session(session_key: str) -> str:
        _ = session_key
        return "sess-1"

    dispatcher._ensure_session = _ensure_session  # type: ignore[method-assign]

    # 中文注释：把 dispatch 放到后台，先确认在 SDK 请求未返回前不会提前发布“切换成功”。
    task = asyncio.create_task(
        dispatcher._dispatch(
            InboundMessage(
                channel="cli",
                sender_id="user",
                chat_id="chat",
                content="/set_model anthropic/claude-sonnet-4",
            )
        )
    )

    await asyncio.sleep(0.02)
    assert bus.outbound.empty()

    conn.release()
    await task

    out_model = await bus.consume_outbound()
    assert out_model.content == "Model switched to: anthropic/claude-sonnet-4"


@pytest.mark.asyncio
async def test_acp_set_model_reports_verification_failure_when_server_model_mismatches() -> None:
    bus = MessageBus()
    conn = _ModelMismatchACPConn()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
    )
    dispatcher._conn = cast(Any, conn)

    caps = _SessionCapabilities()
    caps.available_models = ["opencode/big-pickle", "anthropic/claude-sonnet-4"]
    caps.current_model = "opencode/big-pickle"
    dispatcher._session_caps["sess-1"] = caps

    async def _ensure_session(session_key: str) -> str:
        _ = session_key
        return "sess-1"

    dispatcher._ensure_session = _ensure_session  # type: ignore[method-assign]

    await dispatcher._dispatch(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/set_model anthropic/claude-sonnet-4",
        )
    )
    out = await bus.consume_outbound()
    assert "Model switch verification failed." in out.content
    assert "Requested: anthropic/claude-sonnet-4" in out.content
    assert "ACP current: opencode/big-pickle" in out.content


@pytest.mark.asyncio
async def test_acp_set_model_tolerates_eventual_session_state_refresh() -> None:
    bus = MessageBus()
    conn = _EventuallyConsistentModelACPConn()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
    )
    dispatcher._conn = cast(Any, conn)

    caps = _SessionCapabilities()
    caps.available_models = ["opencode/big-pickle", "anthropic/claude-sonnet-4"]
    caps.current_model = "opencode/big-pickle"
    dispatcher._session_caps["sess-1"] = caps

    async def _ensure_session(session_key: str) -> str:
        _ = session_key
        return "sess-1"

    dispatcher._ensure_session = _ensure_session  # type: ignore[method-assign]

    # 中文注释：模拟首次回读仍是旧模型，第二次回读才一致，避免把短暂不一致误判成失败。
    await dispatcher._dispatch(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/set_model anthropic/claude-sonnet-4",
        )
    )
    out = await bus.consume_outbound()
    assert out.content == "Model switched to: anthropic/claude-sonnet-4"


@pytest.mark.asyncio
async def test_slash_commands_activation_once_per_epoch_then_reactivate_after_epoch_change() -> (
    None
):
    bus = MessageBus()
    conn = _EpochAwareSlashConn()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
    )
    dispatcher._conn = cast(Any, conn)
    dispatcher._session_map = {"cli:chat": "sess-1"}

    await dispatcher._dispatch(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/models",
        )
    )
    _ = await bus.consume_outbound()

    await dispatcher._dispatch(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/agents",
        )
    )
    _ = await bus.consume_outbound()

    assert conn.resume_calls == ["sess-1"]

    # 中文注释：模拟连接重建进入新 epoch，后续 slash 命令应允许再次激活一次。
    dispatcher._connection_epoch += 1
    await dispatcher._dispatch(
        InboundMessage(
            channel="cli",
            sender_id="user",
            chat_id="chat",
            content="/models",
        )
    )
    _ = await bus.consume_outbound()

    assert conn.resume_calls == ["sess-1", "sess-1"]


@pytest.mark.asyncio
async def test_same_session_concurrent_set_model_and_prompt_are_serialized() -> None:
    bus = MessageBus()
    conn = _ConcurrentSlashPromptConn()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=Path("/tmp"),
        acp_config=ACPBackendConfig(),
    )
    dispatcher._conn = cast(Any, conn)
    dispatcher._session_map = {"cli:chat": "sess-1"}

    caps = _SessionCapabilities()
    caps.available_models = ["opencode/big-pickle", "anthropic/claude-sonnet-4"]
    caps.current_model = "opencode/big-pickle"
    dispatcher._session_caps["sess-1"] = caps

    async def _ensure_session(session_key: str) -> str:
        _ = session_key
        return "sess-1"

    dispatcher._ensure_session = _ensure_session  # type: ignore[method-assign]

    set_task = asyncio.create_task(
        dispatcher._dispatch(
            InboundMessage(
                channel="cli",
                sender_id="user",
                chat_id="chat",
                content="/set_model anthropic/claude-sonnet-4",
            )
        )
    )
    await asyncio.sleep(0.02)

    prompt_task = asyncio.create_task(
        dispatcher._dispatch(
            InboundMessage(
                channel="cli",
                sender_id="user",
                chat_id="chat",
                content="hello",
            )
        )
    )
    await asyncio.sleep(0.02)

    # 中文注释：若同 session_key 未串行，普通 prompt 会在 set_model 完成前抢跑，导致状态覆盖风险。
    assert conn.prompt_calls == 0

    conn.release_model()
    await set_task
    await prompt_task

    out1 = await bus.consume_outbound()
    out2 = await bus.consume_outbound()
    assert out1.content == "Model switched to: anthropic/claude-sonnet-4"
    assert out2.content == "<final></final>"
    assert conn.prompt_calls == 1
