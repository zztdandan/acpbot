from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from nanobot.acp.dispatcher import ACPDispatcher
from nanobot.acp.state import _StreamState
from nanobot.acp.session_update_tool import _handle_tool_progress
from nanobot.bus.events import InboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import ACPBackendConfig


class _FakeConn:
    def __init__(self, existing_session_ids: set[str] | None = None) -> None:
        self._existing_session_ids = set(existing_session_ids or set())
        self._seq = 0
        self.prompt_session_ids: list[str] = []
        self.prompt_payloads: list[list[object]] = []
        self.model_calls: list[tuple[str, str]] = []
        self.mode_calls: list[tuple[str, str]] = []

    async def new_session(self, cwd: str, mcp_servers: list[object]):
        del cwd, mcp_servers
        self._seq += 1
        session_id = f"acp-session-{self._seq}"
        self._existing_session_ids.add(session_id)
        return SimpleNamespace(session_id=session_id)

    async def list_sessions(self):
        return {
            "sessions": [{"sessionId": sid} for sid in sorted(self._existing_session_ids)],
        }

    async def prompt(self, prompt: list[object], session_id: str, **kwargs: object):
        del kwargs
        self.prompt_session_ids.append(session_id)
        self.prompt_payloads.append(prompt)

    async def set_session_model(self, model_id: str, session_id: str) -> None:
        self.model_calls.append((session_id, model_id))

    async def set_session_mode(self, mode_id: str, session_id: str) -> None:
        self.mode_calls.append((session_id, mode_id))


class _FakeConnListUnavailable:
    async def ext_method(self, rpc_name: str, params: dict[str, object]):
        del params
        raise RuntimeError(f"unsupported rpc: {rpc_name}")


class _FakeConnListRequiresCwd:
    def __init__(self, expected_cwd: str, existing_session_ids: set[str]) -> None:
        self._expected_cwd = expected_cwd
        self._existing_session_ids = set(existing_session_ids)

    async def list_sessions(self, cwd: str | None = None):
        if cwd != self._expected_cwd:
            raise RuntimeError("cwd is required")
        return {
            "sessions": [{"sessionId": sid} for sid in sorted(self._existing_session_ids)],
        }


class _FakeConnListAuthoritativeEmpty:
    async def list_sessions(self, cwd: str | None = None):
        del cwd
        return {"sessions": []}


class _FakeConnWithActivation(_FakeConn):
    def __init__(self, existing_session_ids: set[str] | None = None) -> None:
        super().__init__(existing_session_ids=existing_session_ids)
        self.resume_calls: list[str] = []

    async def resume_session(self, cwd: str, session_id: str, mcp_servers: list[object]):
        del cwd, mcp_servers
        self.resume_calls.append(session_id)
        return SimpleNamespace(
            models=SimpleNamespace(
                current_model_id="opencode/big-pickle",
                available_models=[SimpleNamespace(model_id="opencode/big-pickle")],
            ),
            modes=SimpleNamespace(
                current_mode_id="build",
                available_modes=[SimpleNamespace(id="build")],
            ),
        )


class _FakeConnWithActivationReplayFailureOnce(_FakeConnWithActivation):
    def __init__(self, existing_session_ids: set[str] | None = None) -> None:
        super().__init__(existing_session_ids=existing_session_ids)
        self._fail_model_once = True

    async def set_session_model(self, model_id: str, session_id: str) -> None:
        self.model_calls.append((session_id, model_id))
        if self._fail_model_once:
            self._fail_model_once = False
            # 中文注释：模拟 desired 回放首轮失败，下一轮 ensure 再重试成功。
            raise RuntimeError("replay model apply failed")


class _FakeConnInvalidParamsRebind(_FakeConnWithActivation):
    def __init__(self, existing_session_ids: set[str] | None = None) -> None:
        super().__init__(existing_session_ids=existing_session_ids)
        self._prompt_calls = 0

    async def prompt(self, prompt: list[object], session_id: str, **kwargs: object):
        del prompt, kwargs
        self._prompt_calls += 1
        if self._prompt_calls == 1:
            # 中文注释：模拟 ACP 端“旧会话参数失效”，触发 invalid params 自愈分支。
            raise RuntimeError("invalid params")
        self.prompt_session_ids.append(session_id)


class _FakeConnAlwaysClosed(_FakeConnWithActivation):
    async def prompt(self, prompt: list[object], session_id: str, **kwargs: object):
        del prompt, session_id, kwargs
        raise ConnectionError("connection closed")


class _FakeConnContextManager:
    def __init__(self) -> None:
        self.exit_calls = 0

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        self.exit_calls += 1


def _map_file(root: Path) -> Path:
    return root / "acp-session-map.json"


def _read_mappings(path: Path) -> list[dict[str, str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload.get("mappings", [])


@pytest.mark.asyncio
async def test_session_map_is_persisted_with_cwd_and_keys(monkeypatch, tmp_path: Path) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-a",
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConn()
    dispatcher._conn = conn
    dispatcher._convert_mcp_servers = lambda: []

    async def _noop() -> None:
        return None

    dispatcher._ensure_connection = _noop
    created_session_id = await dispatcher._ensure_session("telegram:1001")

    assert created_session_id.startswith("acp-session-")
    map_path = _map_file(config_root)
    assert map_path.exists()

    mappings = _read_mappings(map_path)
    assert len(mappings) == 1
    assert mappings[0]["cwd"] == str((tmp_path / "workspace-a").resolve())
    assert mappings[0]["nanobotSideSessionKey"] == "telegram:1001"
    assert mappings[0]["acpSideSessionId"] == created_session_id


@pytest.mark.asyncio
async def test_bootstrap_reconciles_missing_acp_sessions(monkeypatch, tmp_path: Path) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    config_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    workspace = tmp_path / "workspace-b"
    cwd_value = str(workspace.resolve())
    stale_id = "acp-stale"
    live_id = "acp-live"
    _map_file(config_root).write_text(
        json.dumps(
            {
                "version": 1,
                "mappings": [
                    {
                        "cwd": cwd_value,
                        "nanobotSideSessionKey": "telegram:stale",
                        "acpSideSessionId": stale_id,
                    },
                    {
                        "cwd": cwd_value,
                        "nanobotSideSessionKey": "telegram:live",
                        "acpSideSessionId": live_id,
                    },
                    {
                        "cwd": str((tmp_path / "workspace-other").resolve()),
                        "nanobotSideSessionKey": "telegram:other",
                        "acpSideSessionId": "acp-other",
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=workspace,
        acp_config=ACPBackendConfig(),
    )
    dispatcher._conn = _FakeConn(existing_session_ids={live_id})

    await dispatcher._bootstrap_session_map()

    assert dispatcher._session_map == {"telegram:live": live_id}
    mappings = _read_mappings(_map_file(config_root))
    current_cwd_mappings = [m for m in mappings if m.get("cwd") == cwd_value]
    assert current_cwd_mappings == [
        {
            "cwd": cwd_value,
            "nanobotSideSessionKey": "telegram:live",
            "acpSideSessionId": live_id,
        }
    ]
    assert any(m.get("cwd") != cwd_value for m in mappings)


@pytest.mark.asyncio
async def test_bootstrap_keeps_local_map_when_session_list_unavailable(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    config_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    workspace = tmp_path / "workspace-b"
    cwd_value = str(workspace.resolve())
    _map_file(config_root).write_text(
        json.dumps(
            {
                "version": 1,
                "mappings": [
                    {
                        "cwd": cwd_value,
                        "nanobotSideSessionKey": "websocket:web-chat-b",
                        "acpSideSessionId": "ses-existing-1",
                    },
                    {
                        "cwd": cwd_value,
                        "nanobotSideSessionKey": "telegram:live",
                        "acpSideSessionId": "ses-existing-2",
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=workspace,
        acp_config=ACPBackendConfig(),
    )
    dispatcher._conn = _FakeConnListUnavailable()

    await dispatcher._bootstrap_session_map()

    assert dispatcher._session_map == {
        "websocket:web-chat-b": "ses-existing-1",
        "telegram:live": "ses-existing-2",
    }

    mappings = _read_mappings(_map_file(config_root))
    current_cwd_mappings = [m for m in mappings if m.get("cwd") == cwd_value]
    assert current_cwd_mappings == [
        {
            "cwd": cwd_value,
            "nanobotSideSessionKey": "telegram:live",
            "acpSideSessionId": "ses-existing-2",
        },
        {
            "cwd": cwd_value,
            "nanobotSideSessionKey": "websocket:web-chat-b",
            "acpSideSessionId": "ses-existing-1",
        },
    ]


@pytest.mark.asyncio
async def test_bootstrap_reconcile_supports_list_sessions_with_cwd(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    config_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    workspace = tmp_path / "workspace-list-cwd"
    cwd_value = str(workspace.resolve())
    _map_file(config_root).write_text(
        json.dumps(
            {
                "version": 1,
                "mappings": [
                    {
                        "cwd": cwd_value,
                        "nanobotSideSessionKey": "websocket:web-chat-b",
                        "acpSideSessionId": "ses-missing",
                    },
                    {
                        "cwd": cwd_value,
                        "nanobotSideSessionKey": "telegram:live",
                        "acpSideSessionId": "ses-live",
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=workspace,
        acp_config=ACPBackendConfig(),
    )
    dispatcher._conn = _FakeConnListRequiresCwd(
        expected_cwd=cwd_value,
        existing_session_ids={"ses-live"},
    )

    await dispatcher._bootstrap_session_map()

    assert dispatcher._session_map == {"telegram:live": "ses-live"}


@pytest.mark.asyncio
async def test_bootstrap_reconcile_keeps_map_when_authoritative_list_is_empty(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    config_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    workspace = tmp_path / "workspace-empty-list"
    cwd_value = str(workspace.resolve())
    _map_file(config_root).write_text(
        json.dumps(
            {
                "version": 1,
                "mappings": [
                    {
                        "cwd": cwd_value,
                        "nanobotSideSessionKey": "websocket:web-chat-b",
                        "acpSideSessionId": "ses-old-1",
                    },
                    {
                        "cwd": cwd_value,
                        "nanobotSideSessionKey": "telegram:live",
                        "acpSideSessionId": "ses-old-2",
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=workspace,
        acp_config=ACPBackendConfig(),
    )
    dispatcher._conn = _FakeConnListAuthoritativeEmpty()

    await dispatcher._bootstrap_session_map()

    assert dispatcher._session_map == {
        "websocket:web-chat-b": "ses-old-1",
        "telegram:live": "ses-old-2",
    }


@pytest.mark.asyncio
async def test_new_command_removes_old_mapping_and_new_mapping_replaces_it(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=tmp_path / "workspace-c",
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConn()
    dispatcher._conn = conn
    dispatcher._convert_mcp_servers = lambda: []

    async def _noop() -> None:
        return None

    dispatcher._ensure_connection = _noop
    first_session_id = await dispatcher._ensure_session("telegram:chat-1")
    dispatcher._session_desired["telegram:chat-1"] = {
        "model": "anthropic/claude-sonnet-4",
        "agent": "plan",
    }
    dispatcher._persist_session_map()

    await dispatcher._dispatch(
        InboundMessage(
            channel="telegram",
            sender_id="u",
            chat_id="chat-1",
            content="/new",
        )
    )
    outbound = await bus.consume_outbound()
    assert outbound.content == "New session started."
    assert "telegram:chat-1" not in dispatcher._session_map
    assert "telegram:chat-1" not in dispatcher._session_desired

    mappings_after_new = _read_mappings(_map_file(config_root))
    assert mappings_after_new == []

    second_session_id = await dispatcher._ensure_session("telegram:chat-1")
    assert second_session_id != first_session_id

    mappings = _read_mappings(_map_file(config_root))
    assert len(mappings) == 1
    assert mappings[0]["nanobotSideSessionKey"] == "telegram:chat-1"
    assert mappings[0]["acpSideSessionId"] == second_session_id


@pytest.mark.asyncio
async def test_ensure_connection_rollback_cleans_state_even_when_aexit_raises(
    monkeypatch,
    tmp_path: Path,
) -> None:
    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-rollback-clean",
        acp_config=ACPBackendConfig(),
    )

    warning_calls: list[str] = []

    def _capture_warning(message: str, *args: object, **kwargs: object) -> None:
        del kwargs
        warning_calls.append(message.format(*args))

    class _FailingRollbackConn:
        async def initialize(self, **kwargs: object) -> None:
            del kwargs
            raise RuntimeError("initialize failed")

    class _FailingRollbackCM:
        async def __aenter__(self):
            return _FailingRollbackConn(), object()

        async def __aexit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb
            raise RuntimeError("aexit cleanup failed")

    def _spawn_stub():
        def _factory(*args: object, **kwargs: object):
            del args, kwargs
            return _FailingRollbackCM()

        return _factory

    monkeypatch.setattr("nanobot.acp.session_runtime._acp_spawn_agent_process", _spawn_stub)
    monkeypatch.setattr("nanobot.acp.session_runtime.logger.warning", _capture_warning)

    with pytest.raises(RuntimeError, match="initialize failed"):
        await dispatcher._ensure_connection()

    assert dispatcher._conn is None
    assert dispatcher._conn_cm is None
    assert dispatcher._proc is None
    assert dispatcher._session_map_bootstrapped is False
    assert any("rollback __aexit__ failed" in item for item in warning_calls)


@pytest.mark.asyncio
async def test_heartbeat_only_targets_active_channel_sessions(monkeypatch, tmp_path: Path) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-d",
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConn(existing_session_ids={"sid-a", "sid-b", "sid-c"})
    dispatcher._conn = conn
    dispatcher._session_map = {
        "telegram:chat-a": "sid-a",
        "cron:job-1": "sid-b",
        "cli:direct": "sid-c",
    }

    async def _noop() -> None:
        return None

    dispatcher._ensure_connection = _noop
    monkeypatch.setitem(sys.modules, "acp", SimpleNamespace(text_block=lambda text: {"text": text}))

    delivered = await dispatcher.send_heartbeat_to_active_sessions("hb-check")

    assert delivered == 1
    assert conn.prompt_session_ids == ["sid-a"]


@pytest.mark.asyncio
async def test_ws_metadata_preferences_override_default_model_and_agent(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=tmp_path / "workspace-e",
        acp_config=ACPBackendConfig(
            default_model="openai/gpt-default",
            default_mode="build",
        ),
    )
    conn = _FakeConn()
    dispatcher._conn = conn
    dispatcher._convert_mcp_servers = lambda: []

    async def _noop() -> None:
        return None

    dispatcher._ensure_connection = _noop
    monkeypatch.setitem(sys.modules, "acp", SimpleNamespace(text_block=lambda text: {"text": text}))

    await dispatcher._dispatch(
        InboundMessage(
            channel="websocket",
            sender_id="u",
            chat_id="chat-pref",
            content="hello",
            metadata={
                "_acp_session_model": "anthropic/claude-sonnet-4",
                "_acp_session_agent": "plan",
            },
        )
    )

    out = await bus.consume_outbound()
    assert out.content == "<final></final>"
    # 中文注释：首帧偏好优先于 default；session 创建后会调用 ACP set_session_*。
    assert len(conn.model_calls) == 1
    assert len(conn.mode_calls) == 1
    assert conn.model_calls[0][1] == "anthropic/claude-sonnet-4"
    assert conn.mode_calls[0][1] == "plan"


@pytest.mark.asyncio
async def test_default_model_and_agent_apply_when_ws_preferences_absent(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=tmp_path / "workspace-f",
        acp_config=ACPBackendConfig(
            default_model="openai/gpt-default",
            default_mode="build",
        ),
    )
    conn = _FakeConn()
    dispatcher._conn = conn
    dispatcher._convert_mcp_servers = lambda: []

    async def _noop() -> None:
        return None

    dispatcher._ensure_connection = _noop
    monkeypatch.setitem(sys.modules, "acp", SimpleNamespace(text_block=lambda text: {"text": text}))

    await dispatcher._dispatch(
        InboundMessage(
            channel="websocket",
            sender_id="u",
            chat_id="chat-default",
            content="hello",
            metadata={},
        )
    )

    _ = await bus.consume_outbound()
    assert len(conn.model_calls) == 1
    assert len(conn.mode_calls) == 1
    assert conn.model_calls[0][1] == "openai/gpt-default"
    assert conn.mode_calls[0][1] == "build"


@pytest.mark.asyncio
async def test_reused_session_does_not_reapply_new_ws_preferences(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=tmp_path / "workspace-g",
        acp_config=ACPBackendConfig(
            default_model="openai/gpt-default",
            default_mode="build",
        ),
    )
    conn = _FakeConn()
    dispatcher._conn = conn
    dispatcher._convert_mcp_servers = lambda: []

    async def _noop() -> None:
        return None

    dispatcher._ensure_connection = _noop
    monkeypatch.setitem(sys.modules, "acp", SimpleNamespace(text_block=lambda text: {"text": text}))

    await dispatcher._dispatch(
        InboundMessage(
            channel="websocket",
            sender_id="u",
            chat_id="chat-reuse",
            content="first",
            metadata={
                "_acp_session_model": "anthropic/claude-sonnet-4",
                "_acp_session_agent": "plan",
            },
        )
    )
    _ = await bus.consume_outbound()

    await dispatcher._dispatch(
        InboundMessage(
            channel="websocket",
            sender_id="u",
            chat_id="chat-reuse",
            content="second",
            metadata={
                "_acp_session_model": "openai/gpt-4.1",
                "_acp_session_agent": "build",
            },
        )
    )
    _ = await bus.consume_outbound()

    assert len(conn.model_calls) == 1
    assert len(conn.mode_calls) == 1
    assert conn.model_calls[0][1] == "anthropic/claude-sonnet-4"
    assert conn.mode_calls[0][1] == "plan"


@pytest.mark.asyncio
async def test_outbound_metadata_strips_ws_preference_control_keys(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=tmp_path / "workspace-h",
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConn()
    dispatcher._conn = conn
    dispatcher._convert_mcp_servers = lambda: []

    async def _noop() -> None:
        return None

    dispatcher._ensure_connection = _noop
    monkeypatch.setitem(sys.modules, "acp", SimpleNamespace(text_block=lambda text: {"text": text}))

    await dispatcher._dispatch(
        InboundMessage(
            channel="websocket",
            sender_id="u",
            chat_id="chat-meta",
            content="hello",
            metadata={
                "_acp_session_model": "anthropic/claude-sonnet-4",
                "_acp_session_agent": "plan",
                "client_trace_id": "trace-1",
            },
        )
    )
    out = await bus.consume_outbound()
    assert out.metadata == {"client_trace_id": "trace-1"}


@pytest.mark.asyncio
async def test_dispatch_includes_workspace_media_as_resource_link_in_prompt(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    bus = MessageBus()
    workspace = tmp_path / "workspace-media-in"
    workspace.mkdir(parents=True, exist_ok=True)
    media_file = workspace / "sample.md"
    media_file.write_text("# sample\n" + ("A" * (100 * 1024)), encoding="utf-8")
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=workspace,
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConn()
    dispatcher._conn = conn
    dispatcher._convert_mcp_servers = lambda: []

    async def _noop() -> None:
        return None

    dispatcher._ensure_connection = _noop

    await dispatcher._dispatch(
        InboundMessage(
            channel="websocket",
            sender_id="u",
            chat_id="chat-media-in",
            content="check file",
            media=[str(media_file)],
        )
    )

    _ = await bus.consume_outbound()
    assert conn.prompt_payloads
    prompt_blocks = conn.prompt_payloads[0]
    assert len(prompt_blocks) >= 2
    assert getattr(prompt_blocks[0], "type", None) == "text"
    assert getattr(prompt_blocks[1], "type", None) == "resource_link"


@pytest.mark.asyncio
async def test_handle_session_update_resource_link_writes_outbound_media_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    source = tmp_path / "agent-output.txt"
    source.write_text("hello from acp", encoding="utf-8")

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-media-out",
        acp_config=ACPBackendConfig(),
    )
    state_session = "session-1"
    dispatcher._session_states[state_session] = _StreamState()

    from acp.helpers import resource_link_block, update_agent_message

    update = update_agent_message(
        resource_link_block(
            name=source.name,
            uri=source.resolve().as_uri(),
            mime_type="text/plain",
            size=source.stat().st_size,
        )
    )

    await dispatcher._handle_session_update(state_session, update)

    media_paths = dispatcher._session_states[state_session].final_media()
    assert len(media_paths) == 1
    saved = Path(media_paths[0])
    assert saved.exists()
    assert saved.read_text(encoding="utf-8") == "hello from acp"


@pytest.mark.asyncio
async def test_tool_progress_completed_metadata_writes_outbound_media_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    source = tmp_path / "agent-output.txt"
    source.write_text("hello from tool metadata", encoding="utf-8")

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-tool-out",
        acp_config=ACPBackendConfig(),
    )
    session_id = "session-tool-1"
    state = _StreamState()
    dispatcher._session_states[session_id] = state
    dispatcher._session_active_tool_name[session_id] = "acp_send_file"

    update = SimpleNamespace(
        status="completed",
        title="acp_send_file",
        rawOutput=SimpleNamespace(
            metadata={
                "acp_send_file": {
                    "file": str(source),
                    "filename": "copied-doctor.txt",
                    "mime": "text/plain",
                }
            }
        ),
    )

    await _handle_tool_progress(dispatcher, session_id, state, update)

    media_paths = state.final_media()
    assert len(media_paths) == 1
    saved = Path(media_paths[0])
    assert saved.exists()
    assert saved.read_text(encoding="utf-8") == "hello from tool metadata"


def test_dispatcher_initializes_runtime_state_maps(monkeypatch, tmp_path: Path) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-init-state",
        acp_config=ACPBackendConfig(),
    )

    assert dispatcher._running is False
    assert dispatcher._conn is None
    assert dispatcher._proc is None
    assert dispatcher._session_map == {}
    assert dispatcher._session_locks == {}
    assert dispatcher._process_locks == {}
    assert dispatcher._session_states == {}
    assert dispatcher._session_caps == {}
    assert dispatcher._active_tasks == {}
    assert dispatcher._session_id_to_session_key == {}
    assert dispatcher._session_active_tool_name == {}
    assert dispatcher._session_result_media == {}
    assert dispatcher._session_pending_media == {}
    assert dispatcher._session_desired == {}
    assert dispatcher._session_activation_ensure_epoch == {}
    assert dispatcher._session_map_file == config_root / "acp-session-map.json"


@pytest.mark.asyncio
async def test_session_map_desired_roundtrip_persistence(monkeypatch, tmp_path: Path) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    workspace = tmp_path / "workspace-desired"
    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=workspace,
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConn()
    dispatcher._conn = conn
    dispatcher._convert_mcp_servers = lambda: []

    async def _noop() -> None:
        return None

    dispatcher._ensure_connection = _noop
    created_session_id = await dispatcher._ensure_session(
        "telegram:desired",
        preferred_model="anthropic/claude-sonnet-4",
        preferred_agent="plan",
    )

    mappings = _read_mappings(_map_file(config_root))
    assert mappings == [
        {
            "cwd": str(workspace.resolve()),
            "nanobotSideSessionKey": "telegram:desired",
            "acpSideSessionId": created_session_id,
            "desiredModel": "anthropic/claude-sonnet-4",
            "desiredAgent": "plan",
        }
    ]

    reloaded = ACPDispatcher(
        bus=MessageBus(),
        workspace=workspace,
        acp_config=ACPBackendConfig(),
    )
    reloaded._conn = _FakeConn(existing_session_ids={created_session_id})
    await reloaded._bootstrap_session_map()

    assert reloaded._session_map == {"telegram:desired": created_session_id}
    assert reloaded._session_desired == {
        "telegram:desired": {
            "model": "anthropic/claude-sonnet-4",
            "agent": "plan",
        }
    }


@pytest.mark.asyncio
async def test_session_map_legacy_format_loads_without_desired_fields(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    config_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    workspace = tmp_path / "workspace-legacy"
    cwd_value = str(workspace.resolve())
    _map_file(config_root).write_text(
        json.dumps(
            {
                "version": 1,
                "mappings": [
                    {
                        "cwd": cwd_value,
                        "nanobotSideSessionKey": "telegram:legacy",
                        "acpSideSessionId": "legacy-sid",
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=workspace,
        acp_config=ACPBackendConfig(),
    )
    dispatcher._conn = _FakeConn(existing_session_ids={"legacy-sid"})

    await dispatcher._bootstrap_session_map()

    assert dispatcher._session_map == {"telegram:legacy": "legacy-sid"}
    assert dispatcher._session_desired == {}


@pytest.mark.asyncio
async def test_session_map_malformed_top_level_list_is_ignored(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    config_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    map_path = _map_file(config_root)
    map_path.write_text("[]", encoding="utf-8")

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-malformed",
        acp_config=ACPBackendConfig(),
    )

    await dispatcher._bootstrap_session_map()

    assert dispatcher._session_map == {}
    assert dispatcher._session_desired == {}


@pytest.mark.asyncio
async def test_session_map_cwd_isolation_preserves_other_cwd_desired_fields(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    config_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    workspace_current = tmp_path / "workspace-cwd-current"
    workspace_other = tmp_path / "workspace-cwd-other"
    cwd_current = str(workspace_current.resolve())
    cwd_other = str(workspace_other.resolve())
    _map_file(config_root).write_text(
        json.dumps(
            {
                "version": 1,
                "mappings": [
                    {
                        "cwd": cwd_current,
                        "nanobotSideSessionKey": "telegram:current",
                        "acpSideSessionId": "sid-current",
                        "desiredModel": "model-current",
                        "desiredAgent": "agent-current",
                    },
                    {
                        "cwd": cwd_other,
                        "nanobotSideSessionKey": "telegram:other",
                        "acpSideSessionId": "sid-other",
                        "desiredModel": "model-other",
                        "desiredAgent": "agent-other",
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=workspace_current,
        acp_config=ACPBackendConfig(),
    )
    dispatcher._conn = _FakeConn(existing_session_ids={"sid-current"})

    await dispatcher._bootstrap_session_map()

    mappings = _read_mappings(_map_file(config_root))
    assert mappings == [
        {
            "cwd": cwd_other,
            "nanobotSideSessionKey": "telegram:other",
            "acpSideSessionId": "sid-other",
            "desiredModel": "model-other",
            "desiredAgent": "agent-other",
        },
        {
            "cwd": cwd_current,
            "nanobotSideSessionKey": "telegram:current",
            "acpSideSessionId": "sid-current",
            "desiredModel": "model-current",
            "desiredAgent": "agent-current",
        },
    ]


def test_session_map_activation_epoch_is_runtime_only_not_persisted(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-epoch",
        acp_config=ACPBackendConfig(),
    )
    dispatcher._session_map = {"telegram:epoch": "sid-epoch"}
    dispatcher._session_desired = {
        "telegram:epoch": {"model": "model-epoch", "agent": "agent-epoch"}
    }
    dispatcher._session_activation_ensure_epoch = {"telegram:epoch": 42}

    dispatcher._persist_session_map()

    mappings = _read_mappings(_map_file(config_root))
    assert mappings == [
        {
            "cwd": str((tmp_path / "workspace-epoch").resolve()),
            "nanobotSideSessionKey": "telegram:epoch",
            "acpSideSessionId": "sid-epoch",
            "desiredModel": "model-epoch",
            "desiredAgent": "agent-epoch",
        }
    ]
    assert "activationEnsureEpoch" not in mappings[0]


@pytest.mark.asyncio
async def test_activate_once_per_epoch_for_same_session_key(tmp_path: Path) -> None:
    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-activate-once",
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConnWithActivation(existing_session_ids={"sid-once"})
    dispatcher._conn = conn
    dispatcher._session_map = {"telegram:activate-once": "sid-once"}

    first_session_id = await dispatcher._ensure_session("telegram:activate-once")
    second_session_id = await dispatcher._ensure_session("telegram:activate-once")

    assert first_session_id == "sid-once"
    assert second_session_id == "sid-once"
    assert conn.resume_calls == ["sid-once"]
    assert (
        dispatcher._session_activation_ensure_epoch["telegram:activate-once"]
        == dispatcher._connection_epoch
    )


@pytest.mark.asyncio
async def test_reactivate_once_when_connection_epoch_changes(tmp_path: Path) -> None:
    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-reactivate",
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConnWithActivation(existing_session_ids={"sid-rebind"})
    dispatcher._conn = conn
    dispatcher._session_map = {"telegram:rebind": "sid-rebind"}

    _ = await dispatcher._ensure_session("telegram:rebind")
    # 中文注释：模拟连接重建后进入新 epoch，同一 session_key 允许再次 activate 一次。
    dispatcher._connection_epoch += 1
    _ = await dispatcher._ensure_session("telegram:rebind")

    assert conn.resume_calls == ["sid-rebind", "sid-rebind"]
    assert (
        dispatcher._session_activation_ensure_epoch["telegram:rebind"]
        == dispatcher._connection_epoch
    )


@pytest.mark.asyncio
async def test_activation_replays_desired_once_and_non_activation_prompt_does_not_replay(
    tmp_path: Path,
) -> None:
    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-replay-desired",
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConnWithActivation(existing_session_ids={"sid-replay"})
    dispatcher._conn = conn
    dispatcher._session_map = {"telegram:replay": "sid-replay"}
    dispatcher._session_desired = {
        "telegram:replay": {
            "model": "anthropic/claude-sonnet-4",
            "agent": "plan",
        }
    }

    first = await dispatcher._ensure_session("telegram:replay")
    second = await dispatcher._ensure_session("telegram:replay")

    assert first == "sid-replay"
    assert second == "sid-replay"
    assert conn.resume_calls == ["sid-replay"]
    # 中文注释：desired 只在激活成功时回放一次；同 epoch 下常规轮次不重复 set。
    assert conn.model_calls == [("sid-replay", "anthropic/claude-sonnet-4")]
    assert conn.mode_calls == [("sid-replay", "plan")]


@pytest.mark.asyncio
async def test_activation_replay_failure_skips_ensure_marker_and_retries_next_round(
    monkeypatch,
    tmp_path: Path,
) -> None:
    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-replay-fail",
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConnWithActivationReplayFailureOnce(existing_session_ids={"sid-replay-fail"})
    dispatcher._conn = conn
    dispatcher._session_map = {"telegram:replay-fail": "sid-replay-fail"}
    dispatcher._session_desired = {
        "telegram:replay-fail": {
            "model": "anthropic/claude-sonnet-4",
        }
    }

    warning_calls: list[str] = []

    def _capture_warning(message: str, *args: object, **kwargs: object) -> None:
        del kwargs
        warning_calls.append(message.format(*args))

    monkeypatch.setattr("nanobot.acp.session_runtime.logger.warning", _capture_warning)

    first = await dispatcher._ensure_session("telegram:replay-fail")

    assert first == "sid-replay-fail"
    assert "telegram:replay-fail" not in dispatcher._session_activation_ensure_epoch

    second = await dispatcher._ensure_session("telegram:replay-fail")

    assert second == "sid-replay-fail"
    assert conn.resume_calls == ["sid-replay-fail", "sid-replay-fail"]
    assert conn.model_calls == [
        ("sid-replay-fail", "anthropic/claude-sonnet-4"),
        ("sid-replay-fail", "anthropic/claude-sonnet-4"),
    ]
    assert dispatcher._session_activation_ensure_epoch["telegram:replay-fail"] == (
        dispatcher._connection_epoch
    )
    assert any("desired replay failed" in item for item in warning_calls)


@pytest.mark.asyncio
async def test_ensure_marker_not_written_when_ensure_connection_fails_then_retry(
    monkeypatch,
    tmp_path: Path,
) -> None:
    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-ensure-fail",
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConnWithActivation(existing_session_ids={"sid-ensure-fail"})
    dispatcher._conn = conn
    dispatcher._session_map = {"telegram:ensure-fail": "sid-ensure-fail"}

    attempts = {"count": 0}

    async def _flaky_ensure_connection(runtime: object) -> None:
        del runtime
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("temporary ensure failure")

    monkeypatch.setattr("nanobot.acp.session_runtime._ensure_connection", _flaky_ensure_connection)

    with pytest.raises(RuntimeError, match="temporary ensure failure"):
        await dispatcher._ensure_session("telegram:ensure-fail")

    assert "telegram:ensure-fail" not in dispatcher._session_activation_ensure_epoch

    recovered_session_id = await dispatcher._ensure_session("telegram:ensure-fail")
    assert recovered_session_id == "sid-ensure-fail"
    assert conn.resume_calls == ["sid-ensure-fail"]
    assert (
        dispatcher._session_activation_ensure_epoch["telegram:ensure-fail"]
        == dispatcher._connection_epoch
    )


@pytest.mark.asyncio
async def test_invalid_params_rebind_keeps_activate_marker_semantics(tmp_path: Path) -> None:
    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-invalid-params-rebind",
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConnInvalidParamsRebind(existing_session_ids={"sid-stale"})
    dispatcher._conn = conn
    dispatcher._session_map = {"telegram:invalid-params-rebind": "sid-stale"}

    # 中文注释：避免依赖 ACP block 工厂，测试聚焦在 rebind + ensure 行为。
    dispatcher._build_inbound_prompt_blocks = lambda *args, **kwargs: [object()]  # type: ignore[method-assign]

    result = await dispatcher.process_direct(
        "hello",
        session_key="telegram:invalid-params-rebind",
        channel="telegram",
        chat_id="chat-rebind",
    )

    assert result == ""
    assert conn.resume_calls == ["sid-stale"]
    rebound_session_id = dispatcher._session_map["telegram:invalid-params-rebind"]
    assert rebound_session_id != "sid-stale"
    assert (
        dispatcher._session_activation_ensure_epoch["telegram:invalid-params-rebind"]
        == dispatcher._connection_epoch
    )


@pytest.mark.asyncio
async def test_concurrent_ensure_same_key_activates_once_per_epoch(tmp_path: Path) -> None:
    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-concurrent-ensure",
        acp_config=ACPBackendConfig(),
    )
    conn = _FakeConnWithActivation(existing_session_ids={"sid-concurrent"})
    dispatcher._conn = conn
    dispatcher._session_map = {"telegram:concurrent": "sid-concurrent"}

    async def _noop_ensure_connection(runtime: Any) -> None:
        del runtime

    activation_calls = {"count": 0}

    async def _slow_activate(runtime: Any, *, session_key: str, session_id: str) -> bool:
        del runtime, session_key
        activation_calls["count"] += 1
        await asyncio.sleep(0.02)
        conn.resume_calls.append(session_id)
        return True

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("nanobot.acp.session_runtime._ensure_connection", _noop_ensure_connection)
    monkeypatch.setattr("nanobot.acp.session_runtime._activate_existing_session", _slow_activate)

    first, second = await asyncio.gather(
        dispatcher._ensure_session("telegram:concurrent"),
        dispatcher._ensure_session("telegram:concurrent"),
    )

    monkeypatch.undo()

    assert first == "sid-concurrent"
    assert second == "sid-concurrent"
    assert activation_calls["count"] == 1
    assert conn.resume_calls == ["sid-concurrent"]


@pytest.mark.asyncio
async def test_connection_closed_rebind_resets_state_and_rebuilds_once(
    monkeypatch,
    tmp_path: Path,
) -> None:
    dispatcher = ACPDispatcher(
        bus=MessageBus(),
        workspace=tmp_path / "workspace-reconnect-once",
        acp_config=ACPBackendConfig(),
    )
    old_conn = _FakeConnAlwaysClosed(existing_session_ids={"sid-reconnect"})
    old_cm = _FakeConnContextManager()
    new_conn = _FakeConnWithActivation(existing_session_ids={"sid-reconnect"})
    new_cm = _FakeConnContextManager()

    dispatcher._conn = old_conn
    dispatcher._conn_cm = old_cm
    dispatcher._proc = object()
    dispatcher._session_map_bootstrapped = True
    dispatcher._session_map = {"telegram:reconnect": "sid-reconnect"}

    rebuilds = {"count": 0}

    async def _reconnect_once(runtime: Any) -> None:
        if runtime._conn is not None:
            return
        # 中文注释：连接关闭自愈后，重建前必须已经清空 bootstrap 标志，防止旧连接状态泄漏。
        assert runtime._session_map_bootstrapped is False
        rebuilds["count"] += 1
        runtime._conn = new_conn
        runtime._conn_cm = new_cm
        runtime._proc = object()
        runtime._connection_epoch += 1

    monkeypatch.setattr("nanobot.acp.session_runtime._ensure_connection", _reconnect_once)
    dispatcher._build_inbound_prompt_blocks = lambda *args, **kwargs: [object()]  # type: ignore[method-assign]

    result = await dispatcher.process_direct(
        "hello",
        session_key="telegram:reconnect",
        channel="telegram",
        chat_id="chat-reconnect",
    )

    assert result == ""
    assert old_cm.exit_calls == 1
    assert rebuilds["count"] == 1
    assert dispatcher._conn is new_conn
