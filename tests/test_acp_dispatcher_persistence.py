from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from nanobot.acp.dispatcher import ACPDispatcher
from nanobot.bus.events import InboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import ACPBackendConfig


class _FakeConn:
    def __init__(self, existing_session_ids: set[str] | None = None) -> None:
        self._existing_session_ids = set(existing_session_ids or set())
        self._seq = 0
        self.prompt_session_ids: list[str] = []

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
        del prompt
        self.prompt_session_ids.append(session_id)


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
