from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.websocket import WebSocketChannel, WebSocketConfig

from .helpers import file_transport_fixture_source_dir, require_e2e_enabled


@dataclass(eq=False)
class _FakeConnection:
    """In-memory websocket connection used for deterministic WS E2E.

    中文注释：这里不走真实网络 socket，避免网络抖动影响 blob/resource_link
    协议语义验证；重点仍是 WS channel 的入站/出站行为。
    """

    incoming: list[object]
    path: str = "/ws"
    sent: list[str] = field(default_factory=list)
    closed: tuple[int, str] | None = None
    request: object | None = None

    def __post_init__(self) -> None:
        self.request = type("Req", (), {"path": self.path})()

    async def recv(self):
        if not self.incoming:
            raise RuntimeError("connection closed")
        item = self.incoming.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = (code, reason)


def _make_channel(*, allow_from: list[str], tokens: list[str]) -> WebSocketChannel:
    config = WebSocketConfig(enabled=True, allow_from=allow_from)
    config.auth.tokens = tokens
    return WebSocketChannel(config, MessageBus())


def _doctor_fixture_bytes() -> bytes:
    """Load canonical doctor.txt bytes used by FT/WS E2E."""

    return (file_transport_fixture_source_dir() / "doctor.txt").read_bytes()


@pytest.mark.asyncio
async def test_e2e_ws_001_inbound_blob_is_materialized_as_local_media_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """E2E-WS-001: WS inbound blob -> inbound 端口表现为本地 resource_link 语义。"""

    require_e2e_enabled()
    monkeypatch.setattr(
        "nanobot.channels.websocket.get_media_dir",
        lambda channel=None: (tmp_path / "media-root" / (channel or "root")),
    )

    channel = _make_channel(allow_from=["*"], tokens=["ok-token"])
    payload = _doctor_fixture_bytes()
    conn = _FakeConnection(
        incoming=[
            json.dumps({"type": "auth", "token": "ok-token", "principalId": "ws-u-1"}),
            json.dumps(
                {
                    "type": "send",
                    "chatId": "ws-chat-in",
                    "content": "blob inbound",
                    "media": [
                        {
                            "mode": "blob",
                            "filename": "doctor.txt",
                            "mimeType": "text/plain",
                            "data": base64.b64encode(payload).decode("ascii"),
                        }
                    ],
                }
            ),
            RuntimeError("client done"),
        ]
    )

    await channel._on_connection(conn)
    msg = await asyncio.wait_for(channel.bus.consume_inbound(), timeout=1.0)

    assert msg.chat_id == "ws-chat-in"
    assert len(msg.media) == 1
    media_path = Path(msg.media[0])
    assert media_path.exists()
    assert media_path.read_bytes() == payload


@pytest.mark.asyncio
async def test_e2e_ws_002_outbound_media_path_is_encoded_to_blob_for_ws_peer(
    tmp_path: Path,
) -> None:
    """E2E-WS-002: outbound resource_link(path) -> WS 对端收到 blob。"""

    require_e2e_enabled()
    channel = _make_channel(allow_from=["*"], tokens=["ok-token"])
    conn = _FakeConnection(incoming=[])
    await channel._register_connection(conn, "ws-u-2")
    channel._chat_subscribers.setdefault("ws-chat-out", set()).add(conn)

    sample = tmp_path / "doctor.txt"
    sample.write_bytes(_doctor_fixture_bytes())

    await channel.send(
        OutboundMessage(
            channel="websocket",
            chat_id="ws-chat-out",
            content="outbound with media",
            media=[str(sample)],
        )
    )
    await asyncio.sleep(0)

    assert conn.sent, "Expected at least one outbound frame"
    frame = json.loads(conn.sent[-1])
    assert frame.get("type") == "final"
    media = frame.get("media")
    assert isinstance(media, list) and media
    assert media[0].get("mode") == "blob"
    filename = str(media[0].get("filename") or "")
    assert filename.endswith("doctor.txt")
    assert media[0].get("data") == base64.b64encode(_doctor_fixture_bytes()).decode("ascii")

    await channel._cleanup_connection(conn, close_code=None, reason="done")


@pytest.mark.asyncio
async def test_e2e_ws_003_blob_roundtrip_inbound_to_outbound_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """E2E-WS-003: 复合回路验证（blob 入 + blob 出）。"""

    require_e2e_enabled()
    monkeypatch.setattr(
        "nanobot.channels.websocket.get_media_dir",
        lambda channel=None: (tmp_path / "media-root" / (channel or "root")),
    )

    channel = _make_channel(allow_from=["*"], tokens=["ok-token"])

    inbound_payload = _doctor_fixture_bytes()
    conn_in = _FakeConnection(
        incoming=[
            json.dumps({"type": "auth", "token": "ok-token", "principalId": "ws-u-in"}),
            json.dumps(
                {
                    "type": "send",
                    "chatId": "ws-chat-rt",
                    "content": "roundtrip",
                    "media": [
                        {
                            "mode": "blob",
                            "filename": "doctor.txt",
                            "data": base64.b64encode(inbound_payload).decode("ascii"),
                        }
                    ],
                }
            ),
            RuntimeError("client done"),
        ]
    )
    await channel._on_connection(conn_in)
    inbound_msg = await asyncio.wait_for(channel.bus.consume_inbound(), timeout=1.0)
    assert inbound_msg.media

    conn_out = _FakeConnection(incoming=[])
    await channel._register_connection(conn_out, "ws-u-out")
    channel._chat_subscribers.setdefault("ws-chat-rt", set()).add(conn_out)
    await channel.send(
        OutboundMessage(
            channel="websocket",
            chat_id="ws-chat-rt",
            content="roundtrip-outbound",
            media=[inbound_msg.media[0]],
        )
    )
    await asyncio.sleep(0)

    assert conn_out.sent
    frame = json.loads(conn_out.sent[-1])
    assert frame.get("type") == "final"
    media = frame.get("media")
    assert isinstance(media, list) and media and media[0].get("mode") == "blob"
    filename = str(media[0].get("filename") or "")
    assert filename.endswith("doctor.txt")
    assert media[0].get("data") == base64.b64encode(_doctor_fixture_bytes()).decode("ascii")

    await channel._cleanup_connection(conn_out, close_code=None, reason="done")
