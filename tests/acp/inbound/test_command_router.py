from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from nanobot.config.loader import get_config_path, set_config_path
from tests.acp.sessionmap.helpers import build_runtime, close_runtime_quietly, write_runtime_config


def _build_session_key(tag: str) -> str:
    """为每个用例创建显式 session key，避免污染默认 direct identity。"""

    return f"websocket:inbound-command-{tag}-{uuid4().hex}"


def _parse_catalog_ids(content: str) -> list[str]:
    """从 /models 或 /agents 的格式化文本中提取可选 id 列表。"""

    ids: list[str] = []
    for line in content.splitlines():
        if line.startswith("* "):
            ids.append(line[2:].strip())
            continue
        if line.startswith("  "):
            ids.append(line[2:].strip())
    return [item for item in ids if item]


def _parse_current_value(content: str, label: str) -> str | None:
    """提取 `Current xxx: ...` 里的当前值。"""

    prefix = f"{label}: "
    for line in content.splitlines():
        if line.startswith(prefix):
            value = line[len(prefix) :].strip()
            return value or None
    return None


async def _run_direct_command(
    *,
    runtime,
    command: str,
    session_key: str,
    chat_id: str,
) -> tuple[str, list[dict[str, Any]], int]:
    """统一执行 process_direct 命令并采集 on_progress + outbound bus 指标。"""

    progress_events: list[dict[str, Any]] = []

    async def _on_progress(content: str, **metadata: Any) -> None:
        progress_events.append({"content": content, "metadata": dict(metadata)})

    outbound = await runtime.process_direct(
        command,
        session_key=session_key,
        channel="websocket",
        chat_id=chat_id,
        on_progress=_on_progress,
    )
    return outbound.content, progress_events, runtime.bus.outbound_size


def _assert_direct_command_observability(
    *,
    progress_events: list[dict[str, Any]],
    outbound_bus_size: int,
) -> None:
    """命令直返路径不在本轮测试 runtime progress/bus 行为。"""

    assert progress_events == []
    assert outbound_bus_size == 0


@pytest.mark.asyncio
async def test_help_command_returns_direct_response(tmp_path: Path) -> None:
    """验证 /help 会通过 process_direct 命中 command router 并直返帮助文本。"""

    config_path = write_runtime_config(tmp_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key = _build_session_key("help")
    chat_id = session_key.split(":", maxsplit=1)[1]
    try:
        content, progress_events, outbound_bus_size = await _run_direct_command(
            runtime=runtime,
            command="/help",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert "acp runtime commands" in content
        assert "/set_model <model_id>" in content
        _assert_direct_command_observability(
            progress_events=progress_events,
            outbound_bus_size=outbound_bus_size,
        )
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_new_command_rotates_session_after_rebootstrap(tmp_path: Path) -> None:
    """验证 /new 会清理当前绑定，后续命令可重建新会话。"""

    config_path = write_runtime_config(tmp_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key = _build_session_key("new")
    chat_id = session_key.split(":", maxsplit=1)[1]
    try:
        # 先触发一次会话就绪，确保已有绑定可被 /new 清理。
        await runtime.process_direct(
            "/models",
            session_key=session_key,
            channel="websocket",
            chat_id=chat_id,
        )
        first_session_id = runtime.sessionmap_binding_manager.resolve_session_id(session_key)
        assert isinstance(first_session_id, str) and first_session_id

        content, progress_events, outbound_bus_size = await _run_direct_command(
            runtime=runtime,
            command="/new",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert content == "New session started."
        _assert_direct_command_observability(
            progress_events=progress_events,
            outbound_bus_size=outbound_bus_size,
        )
        assert runtime.sessionmap_binding_manager.resolve_session_id(session_key) is None

        # /new 后再次访问命令，应可重建新会话。
        await runtime.process_direct(
            "/models",
            session_key=session_key,
            channel="websocket",
            chat_id=chat_id,
        )
        second_session_id = runtime.sessionmap_binding_manager.resolve_session_id(session_key)
        assert isinstance(second_session_id, str) and second_session_id
        assert second_session_id != first_session_id
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_models_command_returns_direct_response(tmp_path: Path) -> None:
    """验证 /models 会直返模型列表文本。"""

    config_path = write_runtime_config(tmp_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key = _build_session_key("models")
    chat_id = session_key.split(":", maxsplit=1)[1]
    try:
        content, progress_events, outbound_bus_size = await _run_direct_command(
            runtime=runtime,
            command="/models",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert "Current model:" in content
        _assert_direct_command_observability(
            progress_events=progress_events,
            outbound_bus_size=outbound_bus_size,
        )
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_agents_command_returns_direct_response(tmp_path: Path) -> None:
    """验证 /agents 会直返代理列表文本。"""

    config_path = write_runtime_config(tmp_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key = _build_session_key("agents")
    chat_id = session_key.split(":", maxsplit=1)[1]
    try:
        content, progress_events, outbound_bus_size = await _run_direct_command(
            runtime=runtime,
            command="/agents",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert "Current agent:" in content
        _assert_direct_command_observability(
            progress_events=progress_events,
            outbound_bus_size=outbound_bus_size,
        )
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_set_model_command_returns_direct_response(tmp_path: Path) -> None:
    """验证 /set_model 会真实调用 ACP 接口并更新当前模型。"""

    config_path = write_runtime_config(tmp_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key = _build_session_key("set-model")
    chat_id = session_key.split(":", maxsplit=1)[1]
    try:
        models_content, _, _ = await _run_direct_command(
            runtime=runtime,
            command="/models",
            session_key=session_key,
            chat_id=chat_id,
        )
        available_models = _parse_catalog_ids(models_content)
        if not available_models:
            pytest.skip("ACP backend returned no available models for /set_model test")

        current_model = _parse_current_value(models_content, "Current model")
        target_model = next(
            (model_id for model_id in available_models if model_id != current_model),
            available_models[0],
        )

        content, progress_events, outbound_bus_size = await _run_direct_command(
            runtime=runtime,
            command=f"/set_model {target_model}",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert content == f"Model switched to: {target_model}"
        _assert_direct_command_observability(
            progress_events=progress_events,
            outbound_bus_size=outbound_bus_size,
        )

        verify_models_content, _, _ = await _run_direct_command(
            runtime=runtime,
            command="/models",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert _parse_current_value(verify_models_content, "Current model") == target_model
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_set_model_command_rejects_invalid_model_id(tmp_path: Path) -> None:
    """验证 /set_model 对无效 model_id 返回失败且不污染当前选择。"""

    config_path = write_runtime_config(tmp_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key = _build_session_key("set-model-invalid")
    chat_id = session_key.split(":", maxsplit=1)[1]
    try:
        models_content, _, _ = await _run_direct_command(
            runtime=runtime,
            command="/models",
            session_key=session_key,
            chat_id=chat_id,
        )
        available_models = _parse_catalog_ids(models_content)
        if not available_models:
            pytest.skip("ACP backend returned no available models for invalid /set_model test")

        before_current = _parse_current_value(models_content, "Current model")
        invalid_model = "__invalid_model_for_test__"
        assert invalid_model not in available_models

        content, progress_events, outbound_bus_size = await _run_direct_command(
            runtime=runtime,
            command=f"/set_model {invalid_model}",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert content.startswith(f"Model switch failed: {invalid_model}.")
        assert "invalid model id" in content
        _assert_direct_command_observability(
            progress_events=progress_events,
            outbound_bus_size=outbound_bus_size,
        )

        verify_models_content, _, _ = await _run_direct_command(
            runtime=runtime,
            command="/models",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert _parse_current_value(verify_models_content, "Current model") == before_current
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_set_agent_command_returns_direct_response(tmp_path: Path) -> None:
    """验证 /set_agent 会真实调用 ACP 接口并更新当前代理。"""

    config_path = write_runtime_config(tmp_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key = _build_session_key("set-agent")
    chat_id = session_key.split(":", maxsplit=1)[1]
    try:
        agents_content, _, _ = await _run_direct_command(
            runtime=runtime,
            command="/agents",
            session_key=session_key,
            chat_id=chat_id,
        )
        available_agents = _parse_catalog_ids(agents_content)
        if not available_agents:
            pytest.skip("ACP backend returned no available agents for /set_agent test")

        current_agent = _parse_current_value(agents_content, "Current agent")
        target_agent = next(
            (agent_id for agent_id in available_agents if agent_id != current_agent),
            available_agents[0],
        )

        content, progress_events, outbound_bus_size = await _run_direct_command(
            runtime=runtime,
            command=f"/set_agent {target_agent}",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert content == f"Agent switched to: {target_agent}"
        _assert_direct_command_observability(
            progress_events=progress_events,
            outbound_bus_size=outbound_bus_size,
        )

        verify_agents_content, _, _ = await _run_direct_command(
            runtime=runtime,
            command="/agents",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert _parse_current_value(verify_agents_content, "Current agent") == target_agent
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_stop_command_returns_direct_response(tmp_path: Path) -> None:
    """验证 /stop 在 process_direct 命令路径下总能直返命令结果。"""

    config_path = write_runtime_config(tmp_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key = _build_session_key("stop")
    chat_id = session_key.split(":", maxsplit=1)[1]
    try:
        content, progress_events, outbound_bus_size = await _run_direct_command(
            runtime=runtime,
            command="/stop",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert content in {
            "Nothing active or queued for this session.",
            "Stop requested. active_cancel_requested=True dropped_queued=0",
            "Stop requested. active_cancel_requested=False dropped_queued=0",
        }
        _assert_direct_command_observability(
            progress_events=progress_events,
            outbound_bus_size=outbound_bus_size,
        )
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_unknown_slash_command_returns_direct_response(tmp_path: Path) -> None:
    """验证未知 slash 命令会被 command router 显式处理并直返错误提示。"""

    config_path = write_runtime_config(tmp_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key = _build_session_key("unknown")
    chat_id = session_key.split(":", maxsplit=1)[1]
    try:
        content, progress_events, outbound_bus_size = await _run_direct_command(
            runtime=runtime,
            command="/unknown_command_for_test",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert content == "Unknown ACP slash command: /unknown_command_for_test"
        _assert_direct_command_observability(
            progress_events=progress_events,
            outbound_bus_size=outbound_bus_size,
        )
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)
