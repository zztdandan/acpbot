import asyncio
import base64
import json
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.websocket import WebSocketChannel, WebSocketConfig


@dataclass(eq=False)
class _FakeConnection:
    # 中文注释：用内存队列模拟客户端输入帧，避免真实网络依赖。
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


@pytest.mark.asyncio
async def test_first_frame_must_be_auth() -> None:
    channel = _make_channel(allow_from=["*"], tokens=["ok-token"])
    conn = _FakeConnection(incoming=[json.dumps({"type": "ping"})])

    await channel._on_connection(conn)

    assert conn.closed == (1008, "first frame must be auth")


@pytest.mark.asyncio
async def test_connection_can_switch_chat_and_publish_inbound() -> None:
    channel = _make_channel(allow_from=["*"], tokens=["ok-token"])
    conn = _FakeConnection(
        incoming=[
            json.dumps({"type": "auth", "token": "ok-token", "principalId": "u-1"}),
            json.dumps({"type": "bind_chat", "chatId": "chat-A"}),
            json.dumps({"type": "send", "chatId": "chat-A", "content": "hello A"}),
            json.dumps(
                {
                    "type": "send",
                    "chatId": "chat-B",
                    "content": "hello B",
                    "sessionKey": "websocket:chat-B:tab-2",
                }
            ),
            RuntimeError("client done"),
        ]
    )

    await channel._on_connection(conn)

    msg_a = await asyncio.wait_for(channel.bus.consume_inbound(), timeout=1)
    msg_b = await asyncio.wait_for(channel.bus.consume_inbound(), timeout=1)

    # 中文注释：同一连接可切换 chat_id，且每条消息保留各自会话键语义。
    assert msg_a.chat_id == "chat-A"
    assert msg_a.session_key == "websocket:chat-A"
    assert msg_b.chat_id == "chat-B"
    assert msg_b.session_key == "websocket:chat-B:tab-2"


@pytest.mark.asyncio
async def test_auth_first_frame_can_set_acp_preferences_for_inbound_metadata() -> None:
    channel = _make_channel(allow_from=["*"], tokens=["ok-token"])
    conn = _FakeConnection(
        incoming=[
            json.dumps(
                {
                    "type": "auth",
                    "token": "ok-token",
                    "principalId": "u-2",
                    "model": "anthropic/claude-sonnet-4",
                    "agent": "build",
                }
            ),
            json.dumps({"type": "send", "chatId": "chat-C", "content": "hello C"}),
            RuntimeError("client done"),
        ]
    )

    await channel._on_connection(conn)

    msg = await asyncio.wait_for(channel.bus.consume_inbound(), timeout=1)
    # 中文注释：验证 WS 首帧中的 model/agent 已注入 metadata，供 ACP 首次建会话选默认策略。
    assert msg.metadata["_acp_session_model"] == "anthropic/claude-sonnet-4"
    assert msg.metadata["_acp_session_agent"] == "build"


@pytest.mark.asyncio
async def test_auth_ack_send_failure_still_cleans_connection_state() -> None:
    channel = _make_channel(allow_from=["*"], tokens=["ok-token"])
    conn = _FakeConnection(
        incoming=[json.dumps({"type": "auth", "token": "ok-token", "principalId": "u-3"})]
    )

    async def _fail_send(payload: str) -> None:
        del payload
        raise RuntimeError("send failed")

    conn.send = _fail_send  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="send failed"):
        await channel._on_connection(conn)

    assert conn not in channel._connections
    assert conn not in channel._connection_principals
    assert conn not in channel._connection_acp_preferences


@pytest.mark.asyncio
async def test_outbound_only_fanout_to_subscribed_chat() -> None:
    channel = _make_channel(allow_from=["*"], tokens=["ok-token"])
    conn_a = _FakeConnection(incoming=[])
    conn_b = _FakeConnection(incoming=[])

    await channel._register_connection(conn_a, "u-a")
    await channel._register_connection(conn_b, "u-b")
    channel._chat_subscribers.setdefault("chat-A", set()).add(conn_a)
    channel._chat_subscribers.setdefault("chat-B", set()).add(conn_b)

    await channel.send(
        OutboundMessage(channel="websocket", chat_id="chat-A", content="only A should receive")
    )
    await asyncio.sleep(0)

    # 中文注释：验证路由仅命中订阅了 chat-A 的连接，不会串话到 chat-B。
    assert len(conn_a.sent) == 1
    assert len(conn_b.sent) == 0
    frame = json.loads(conn_a.sent[0])
    assert frame["type"] == "final"
    assert frame["chatId"] == "chat-A"

    await channel._cleanup_connection(conn_a, close_code=None, reason="done")
    await channel._cleanup_connection(conn_b, close_code=None, reason="done")


@pytest.mark.asyncio
async def test_send_frame_accepts_100kb_blob_media_and_publishes_local_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "nanobot.channels.websocket.get_media_dir",
        lambda channel=None: (tmp_path / "media-root" / (channel or "root")),
    )
    channel = _make_channel(allow_from=["*"], tokens=["ok-token"])
    payload = b"x" * (100 * 1024)
    conn = _FakeConnection(
        incoming=[
            json.dumps({"type": "auth", "token": "ok-token", "principalId": "u-blob"}),
            json.dumps(
                {
                    "type": "send",
                    "chatId": "chat-media",
                    "content": "text only",
                    "media": [
                        {
                            "mode": "blob",
                            "filename": "sample.txt",
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

    msg = await asyncio.wait_for(channel.bus.consume_inbound(), timeout=1)
    assert msg.content == "text only"
    assert len(msg.media) == 1
    saved = Path(msg.media[0])
    assert saved.exists()
    assert saved.stat().st_size == 100 * 1024
    assert saved.read_bytes() == payload


@pytest.mark.asyncio
async def test_send_frame_rejects_blob_media_over_2mb_limit() -> None:
    channel = _make_channel(allow_from=["*"], tokens=["ok-token"])
    payload = b"x" * ((2 * 1024 * 1024) + 1)
    conn = _FakeConnection(
        incoming=[
            json.dumps({"type": "auth", "token": "ok-token", "principalId": "u-limit"}),
            json.dumps(
                {
                    "type": "send",
                    "chatId": "chat-limit",
                    "content": "too big",
                    "requestId": "req-1",
                    "media": [
                        {
                            "mode": "blob",
                            "filename": "oversize.bin",
                            "data": base64.b64encode(payload).decode("ascii"),
                        }
                    ],
                }
            ),
            RuntimeError("client done"),
        ]
    )

    await channel._on_connection(conn)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(channel.bus.consume_inbound(), timeout=0.2)

    error_frames = [json.loads(item) for item in conn.sent if isinstance(item, str)]
    assert any(frame.get("code") == "BAD_MEDIA" for frame in error_frames)


@pytest.mark.asyncio
async def test_outbound_media_default_blob_mode_encodes_100kb_file(tmp_path: Path) -> None:
    channel = _make_channel(allow_from=["*"], tokens=["ok-token"])
    conn = _FakeConnection(incoming=[])
    await channel._register_connection(conn, "u-blob-out")
    channel._chat_subscribers.setdefault("chat-out", set()).add(conn)

    path = tmp_path / "outbound.txt"
    path.write_bytes(b"y" * (100 * 1024))

    await channel.send(
        OutboundMessage(
            channel="websocket",
            chat_id="chat-out",
            content="with file",
            media=[str(path)],
        )
    )
    await asyncio.sleep(0)

    assert conn.sent
    payload = json.loads(conn.sent[0])
    assert payload["chatId"] == "chat-out"
    assert payload["type"] == "final"
    assert isinstance(payload.get("media"), list)
    assert payload["media"][0]["mode"] == "blob"
    assert payload["media"][0]["size"] == 100 * 1024
    decoded = base64.b64decode(payload["media"][0]["data"], validate=True)
    assert len(decoded) == 100 * 1024

    await channel._cleanup_connection(conn, close_code=None, reason="done")
