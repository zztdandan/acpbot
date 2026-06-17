from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from nanobot.acp.sessionmap.internal.session_caps import ModelSource, _SessionCapabilities
from nanobot.acp.sessionmap.models import SessionRuntimeEntry
from nanobot.acp.sessionmap.runtime_manager import SessionRuntimeManager


class _FakeSessionPayload:
    def __init__(
        self,
        *,
        session_id: str,
        models: object | None = None,
        config_options: list[dict[str, object]] | None = None,
    ) -> None:
        self.session_id = session_id
        self.models = models
        self.configOptions = config_options


class _FakeConnection:
    def __init__(
        self,
        *,
        new_session_payload: _FakeSessionPayload | None = None,
        set_config_response: object = None,
        set_session_response: object = None,
    ) -> None:
        self.new_session_payload = new_session_payload or _FakeSessionPayload(session_id="ses-new")
        self.set_config_response = set_config_response
        self.set_session_response = set_session_response
        self.new_session_calls: list[str] = []
        self.set_config_calls: list[tuple[str, str, str | bool]] = []
        self.set_session_calls: list[tuple[str, str]] = []
        self.resume_calls: list[tuple[str, str]] = []
        self.load_calls: list[tuple[str, str]] = []

    async def new_session(self, *, cwd: str) -> _FakeSessionPayload:
        self.new_session_calls.append(cwd)
        return self.new_session_payload

    async def set_config_option(self, config_id: str, session_id: str, value: str | bool) -> object:
        self.set_config_calls.append((config_id, session_id, value))
        return self.set_config_response

    async def set_session_model(self, model_id: str, session_id: str) -> object:
        self.set_session_calls.append((model_id, session_id))
        return self.set_session_response

    async def resume_session(self, *, cwd: str, session_id: str) -> object:
        self.resume_calls.append((cwd, session_id))
        return self.new_session_payload

    async def load_session(self, *, cwd: str, session_id: str) -> object:
        self.load_calls.append((cwd, session_id))
        return self.new_session_payload


class _FakeBindingManager:
    def __init__(self, *, bound_model: str | None = None, bootstrapped: bool = True) -> None:
        self.bound_model = bound_model
        self.bootstrapped = bootstrapped
        self.resolved_session_id: str | None = None
        self.bound_updates: list[tuple[str, str]] = []
        self.bind_calls: list[tuple[str, str]] = []
        self.clear_calls: list[str] = []
        self.load_calls = 0

    def resolve_session_id(self, nanobot_side_session_key: str) -> str | None:
        return self.resolved_session_id

    def is_bootstrapped(self) -> bool:
        return self.bootstrapped

    async def load_persistent_truth(self) -> None:
        self.load_calls += 1
        self.bootstrapped = True

    def bind_session(self, nanobot_side_session_key: str, acp_side_session_id: str) -> None:
        self.bind_calls.append((nanobot_side_session_key, acp_side_session_id))
        self.resolved_session_id = acp_side_session_id

    def get_bound_model(self, nanobot_side_session_key: str) -> str | None:
        return self.bound_model

    def update_bound_model(self, nanobot_side_session_key: str, model: str) -> None:
        self.bound_updates.append((nanobot_side_session_key, model))
        self.bound_model = model

    def clear_bound_model(self, nanobot_side_session_key: str) -> None:
        self.clear_calls.append(nanobot_side_session_key)
        self.bound_model = None


class _FakeRuntime:
    def __init__(self, *, conn: _FakeConnection, default_model: str) -> None:
        self._acp_client_conn = conn
        self.acp_config = SimpleNamespace(default_model=default_model)
        self.ensure_connection_calls = 0

    async def ensure_connection(self) -> None:
        self.ensure_connection_calls += 1

    def resolve_acp_workspace_path(self) -> Path:
        return Path("/tmp/acp-workspace")


def _caps_from_payload(payload: dict[str, object]) -> _SessionCapabilities:
    caps = _SessionCapabilities()
    caps.apply_session_payload(payload)
    return caps


@pytest.mark.asyncio
async def test_ensure_ready_session_applies_default_model_via_config_option_source() -> None:
    """new_session 返回 configOptions-only catalog 时，default model 应优先走 set_config_option。"""

    conn = _FakeConnection(
        new_session_payload=_FakeSessionPayload(
            session_id="ses-config",
            config_options=[
                {
                    "id": "model",
                    "currentValue": "openai/gpt-4.1",
                    "options": [
                        {"value": "openai/gpt-4.1"},
                        {"value": "openai/gpt-5.4"},
                    ],
                }
            ],
        ),
        set_config_response={},
    )
    runtime = _FakeRuntime(conn=conn, default_model="openai/gpt-5.4")
    binding_manager = _FakeBindingManager()
    manager = SessionRuntimeManager(runtime=cast(Any, runtime), binding_manager=cast(Any, binding_manager))

    session_id = await manager.ensure_ready_session(nanobot_side_session_key="websocket:test")

    assert session_id == "ses-config"
    assert conn.set_config_calls == [("model", "ses-config", "openai/gpt-5.4")]
    assert conn.set_session_calls == []
    assert binding_manager.bound_updates == [("websocket:test", "openai/gpt-5.4")]


@pytest.mark.asyncio
async def test_ensure_ready_session_skips_default_model_when_not_in_catalog() -> None:
    """default model 不在 catalog 时不应盲试任何 set 通道。"""

    conn = _FakeConnection(
        new_session_payload=_FakeSessionPayload(
            session_id="ses-skip",
            config_options=[
                {
                    "id": "model",
                    "currentValue": "openai/gpt-4.1",
                    "options": [{"value": "openai/gpt-4.1"}],
                }
            ],
        ),
        set_config_response={},
    )
    runtime = _FakeRuntime(conn=conn, default_model="openai/gpt-5.4")
    binding_manager = _FakeBindingManager()
    manager = SessionRuntimeManager(runtime=cast(Any, runtime), binding_manager=cast(Any, binding_manager))

    session_id = await manager.ensure_ready_session(nanobot_side_session_key="websocket:test")

    assert session_id == "ses-skip"
    assert conn.set_config_calls == []
    assert conn.set_session_calls == []
    assert binding_manager.bound_updates == []


@pytest.mark.asyncio
async def test_ensure_ready_session_unknown_source_materializes_default_model_locally() -> None:
    """unknown source 时应把 configured default model 作为本地只读 fallback 暴露出来。"""

    conn = _FakeConnection(new_session_payload=_FakeSessionPayload(session_id="ses-unknown"))
    runtime = _FakeRuntime(conn=conn, default_model="deepseek/deepseek-v4-pro")
    binding_manager = _FakeBindingManager()
    manager = SessionRuntimeManager(runtime=cast(Any, runtime), binding_manager=cast(Any, binding_manager))

    session_id = await manager.ensure_ready_session(nanobot_side_session_key="websocket:test")

    assert session_id == "ses-unknown"
    entry = manager.get_by_acp_side_session_id("ses-unknown")
    assert entry is not None
    assert entry.capabilities.model_source is ModelSource.UNKNOWN
    assert entry.capabilities.current_model == "deepseek/deepseek-v4-pro"
    assert conn.set_config_calls == []
    assert conn.set_session_calls == []
    assert binding_manager.bound_updates == [("websocket:test", "deepseek/deepseek-v4-pro")]


@pytest.mark.asyncio
async def test_apply_explicit_selection_safe_unknown_source_default_model_is_local_noop() -> None:
    """unknown source 下切换到 default model 只做本地成功，不调用 backend set。"""

    conn = _FakeConnection(set_config_response={}, set_session_response={})
    runtime = _FakeRuntime(conn=conn, default_model="deepseek/deepseek-v4-pro")
    binding_manager = _FakeBindingManager()
    manager = SessionRuntimeManager(runtime=cast(Any, runtime), binding_manager=cast(Any, binding_manager))
    entry = SessionRuntimeEntry(
        nanobot_side_session_key="websocket:test",
        acp_side_session_id="ses-unknown",
        ready=True,
        capabilities=_caps_from_payload({"sessionId": "ses-unknown"}),
    )
    entry.capabilities.current_model = "deepseek/deepseek-v4-pro"
    entry.capabilities.available_models = ["deepseek/deepseek-v4-pro"]
    manager._by_nanobot_side_session_key["websocket:test"] = entry
    manager._by_acp_side_session_id["ses-unknown"] = entry

    result = await manager.apply_explicit_selection_safe(
        nanobot_side_session_key="websocket:test",
        acp_side_session_id="ses-unknown",
        model_id="deepseek/deepseek-v4-pro",
    )

    assert result.success is True
    assert result.reason == "ok"
    assert conn.set_config_calls == []
    assert conn.set_session_calls == []
    assert binding_manager.bound_updates == [("websocket:test", "deepseek/deepseek-v4-pro")]


@pytest.mark.asyncio
async def test_apply_explicit_selection_safe_unknown_source_non_default_still_fails_without_backend_call() -> None:
    """unknown source 下非 default model 仍应失败，且不调用 backend set。"""

    conn = _FakeConnection(set_config_response={}, set_session_response={})
    runtime = _FakeRuntime(conn=conn, default_model="deepseek/deepseek-v4-pro")
    binding_manager = _FakeBindingManager()
    manager = SessionRuntimeManager(runtime=cast(Any, runtime), binding_manager=cast(Any, binding_manager))
    entry = SessionRuntimeEntry(
        nanobot_side_session_key="websocket:test",
        acp_side_session_id="ses-unknown",
        ready=True,
        capabilities=_caps_from_payload({"sessionId": "ses-unknown"}),
    )
    entry.capabilities.current_model = "deepseek/deepseek-v4-pro"
    entry.capabilities.available_models = ["deepseek/deepseek-v4-pro"]
    manager._by_nanobot_side_session_key["websocket:test"] = entry
    manager._by_acp_side_session_id["ses-unknown"] = entry

    result = await manager.apply_explicit_selection_safe(
        nanobot_side_session_key="websocket:test",
        acp_side_session_id="ses-unknown",
        model_id="deepseek/deepseek-v4-flash",
    )

    assert result.success is False
    assert "invalid model id for current ACP backend catalog" in result.reason
    assert conn.set_config_calls == []
    assert conn.set_session_calls == []


@pytest.mark.asyncio
async def test_apply_explicit_selection_safe_does_not_refresh_after_success() -> None:
    """显式 /set_model 成功后只做 local remember/binding，不应触发 resume/load/new refresh。"""

    conn = _FakeConnection(set_session_response={})
    runtime = _FakeRuntime(conn=conn, default_model="openai/gpt-5.4")
    binding_manager = _FakeBindingManager()
    manager = SessionRuntimeManager(runtime=cast(Any, runtime), binding_manager=cast(Any, binding_manager))
    caps = _caps_from_payload(
        {
            "models": {
                "currentModelId": "openai/gpt-4.1",
                "availableModels": [
                    {"modelId": "openai/gpt-4.1"},
                    {"modelId": "openai/gpt-5.4"},
                ],
            }
        }
    )
    entry = SessionRuntimeEntry(
        nanobot_side_session_key="websocket:test",
        acp_side_session_id="ses-model",
        ready=True,
        capabilities=caps,
    )
    manager._by_nanobot_side_session_key["websocket:test"] = entry
    manager._by_acp_side_session_id["ses-model"] = entry

    result = await manager.apply_explicit_selection_safe(
        nanobot_side_session_key="websocket:test",
        acp_side_session_id="ses-model",
        model_id="openai/gpt-5.4",
    )

    assert result.success is True
    assert conn.set_session_calls == [("openai/gpt-5.4", "ses-model")]
    assert conn.set_config_calls == []
    assert conn.resume_calls == []
    assert conn.load_calls == []
    assert conn.new_session_calls == []
    assert binding_manager.bound_updates == [("websocket:test", "openai/gpt-5.4")]
    assert entry.capabilities.current_model == "openai/gpt-5.4"
