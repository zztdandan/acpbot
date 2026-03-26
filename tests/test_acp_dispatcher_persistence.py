from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from nanobot.acp.dispatcher import ACPDispatcher
from nanobot.acp.state import _StreamState
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

    async def prompt(self, prompt: list[object], session_id: str):
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

    second_session_id = await dispatcher._ensure_session("telegram:chat-1")
    assert second_session_id != first_session_id

    mappings = _read_mappings(_map_file(config_root))
    assert len(mappings) == 1
    assert mappings[0]["nanobotSideSessionKey"] == "telegram:chat-1"
    assert mappings[0]["acpSideSessionId"] == second_session_id


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
async def test_dispatch_includes_text_media_as_resource_block_in_prompt(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_root = tmp_path / ".nanobot" / "config"
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: config_root)

    media_file = tmp_path / "sample.md"
    media_file.write_text("# sample\n" + ("A" * (100 * 1024)), encoding="utf-8")

    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=tmp_path / "workspace-media-in",
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
    assert getattr(prompt_blocks[1], "type", None) == "resource"


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
