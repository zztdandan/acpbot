from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest

from nanobot.acp.contracts import (
    ACP_META_KIND,
    ACP_META_KIND_COMMAND,
    ACP_META_PROGRESS,
    ACP_META_RENDER_AS,
    ACP_META_RENDER_AS_COMMAND,
    ACP_META_RENDER_AS_LIST,
    ACP_META_RENDER_AS_TEXT,
)
from nanobot.acp.inbound.command_router import CommandRouter
from nanobot.acp.runtime_models import InboundContext, SessionSelectionResult
from nanobot.config.loader import get_config_path, set_config_path
from tests.acp.sessionmap.helpers import build_runtime, close_runtime_quietly, write_runtime_config


def _build_session_key(tag: str) -> str:
    """为每个用例创建显式 session key，避免污染默认 direct identity。"""

    return f"websocket:inbound-command-{tag}-{uuid4().hex}"


def _parse_catalog_ids(content: str) -> list[str]:
    """从 /models 的格式化文本中提取可选 id 列表。"""

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


class _FakeSessionRuntimeManager:
    def __init__(
        self,
        *,
        ensure_ready_session_result: str = "ses-test",
        set_model_result: SessionSelectionResult | None = None,
    ) -> None:
        self.ensure_ready_session_result = ensure_ready_session_result
        self.set_model_result = set_model_result or SessionSelectionResult(
            success=True,
            target="model",
            value="default/model",
            reason="ok",
            acp_side_session_id=ensure_ready_session_result,
        )
        self.ensure_calls: list[str] = []
        self.set_model_calls: list[tuple[str, str]] = []

    async def ensure_ready_session(
        self,
        *,
        nanobot_side_session_key: str,
        preferred_model: str | None = None,
    ) -> str:
        self.ensure_calls.append(nanobot_side_session_key)
        return self.ensure_ready_session_result

    async def set_model_safe(
        self,
        *,
        nanobot_side_session_key: str,
        model_id: str,
    ) -> SessionSelectionResult:
        self.set_model_calls.append((nanobot_side_session_key, model_id))
        return self.set_model_result


class _FakeRuntime:
    def __init__(self, *, models_content: str, set_model_result: SessionSelectionResult | None = None) -> None:
        self.session_runtime_manager = _FakeSessionRuntimeManager(set_model_result=set_model_result)
        self.models_content = models_content

    async def list_models_command(self, acp_side_session_id: str) -> str:
        assert acp_side_session_id == self.session_runtime_manager.ensure_ready_session_result
        return self.models_content


def _build_inbound_context(content: str) -> InboundContext:
    return InboundContext(
        request_key="req-test",
        nanobot_side_session_key="websocket:test-chat",
        channel="websocket",
        chat_id="test-chat",
        content=content,
    )


@pytest.mark.asyncio
async def test_help_command_sets_text_render_metadata() -> None:
    """/help 直返应显式标记 text render，而不是落回默认 command render。"""

    router = CommandRouter(runtime=cast(Any, _FakeRuntime(models_content="unused")))

    outbound = await router.maybe_handle(_build_inbound_context("/help"))

    assert outbound is not None
    assert outbound.metadata[ACP_META_KIND] == ACP_META_KIND_COMMAND
    assert outbound.metadata[ACP_META_PROGRESS] is False
    assert outbound.metadata[ACP_META_RENDER_AS] == ACP_META_RENDER_AS_TEXT


@pytest.mark.asyncio
async def test_models_command_sets_list_render_metadata_for_config_options_source() -> None:
    """configOptions-only source 的 /models 用户路径应返回 list render metadata。"""

    router = CommandRouter(
        runtime=cast(Any, _FakeRuntime(
            models_content="Current model: opencode/big-pickle\nAvailable models:\n* opencode/big-pickle\n  openai/gpt-4.1"
        ))
    )

    outbound = await router.maybe_handle(_build_inbound_context("/models"))

    assert outbound is not None
    assert outbound.content.startswith("Current model: opencode/big-pickle")
    assert outbound.metadata[ACP_META_RENDER_AS] == ACP_META_RENDER_AS_LIST


@pytest.mark.asyncio
async def test_models_command_returns_default_only_catalog_for_unknown_source() -> None:
    """unknown source 时，/models 用户路径应回显 runtime 注入的 default-only catalog。"""

    router = CommandRouter(
        runtime=_FakeRuntime(
            models_content="Current model: deepseek/deepseek-v4-pro\nAvailable models:\n* deepseek/deepseek-v4-pro"
        )
    )

    outbound = await router.maybe_handle(_build_inbound_context("/models"))

    assert outbound is not None
    assert outbound.content == "Current model: deepseek/deepseek-v4-pro\nAvailable models:\n* deepseek/deepseek-v4-pro"
    assert outbound.metadata[ACP_META_RENDER_AS] == ACP_META_RENDER_AS_LIST


@pytest.mark.asyncio
async def test_set_model_success_uses_default_command_render_metadata() -> None:
    """未显式指定 render_as 时，命令直返默认应保持 command render。"""

    router = CommandRouter(
        runtime=cast(Any, _FakeRuntime(
            models_content="unused",
            set_model_result=SessionSelectionResult(
                success=True,
                target="model",
                value="openai/gpt-5.4",
                reason="ok",
                acp_side_session_id="ses-test",
            ),
        ))
    )

    outbound = await router.maybe_handle(_build_inbound_context("/set_model openai/gpt-5.4"))

    assert outbound is not None
    assert outbound.content == "Model switched to: openai/gpt-5.4"
    assert outbound.metadata[ACP_META_RENDER_AS] == ACP_META_RENDER_AS_COMMAND


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
        assert "/agents" not in content
        assert "/set_agent" not in content
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
async def test_agents_command_returns_unknown_command(tmp_path: Path) -> None:
    """验证 /agents 已删除，会按未知 slash 命令直返。"""

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
        assert content == "Unknown ACP slash command: /agents"
        _assert_direct_command_observability(
            progress_events=progress_events,
            outbound_bus_size=outbound_bus_size,
        )
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_set_agent_command_returns_unknown_command(tmp_path: Path) -> None:
    """验证 /set_agent 已删除，会按未知 slash 命令直返。"""

    config_path = write_runtime_config(tmp_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key = _build_session_key("set-agent")
    chat_id = session_key.split(":", maxsplit=1)[1]
    try:
        content, progress_events, outbound_bus_size = await _run_direct_command(
            runtime=runtime,
            command="/set_agent build",
            session_key=session_key,
            chat_id=chat_id,
        )
        assert content == "Unknown ACP slash command: /set_agent"
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
        assert "invalid model id for current ACP backend catalog" in content
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
