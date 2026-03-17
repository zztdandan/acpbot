import asyncio
import json
from dataclasses import dataclass, field

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
