from __future__ import annotations

import asyncio
import base64
import binascii
import json
import mimetypes
import re
import ssl
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import unquote, urlsplit

from loguru import logger
from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base


class WebSocketAuthConfig(Base):
    # 中文注释：首版固定使用“首帧 token”，避免浏览器 Header 兼容性问题。
    mode: Literal["firstFrameToken"] = "firstFrameToken"
    # 中文注释：内存 token 白名单；后续可替换为 JWT/JWKS 验签。
    tokens: list[str] = Field(default_factory=list)


class WebSocketTLSConfig(Base):
    # 中文注释：内网可以先用 ws://，需要链路加密时切到 wss://。
    enabled: bool = False
    cert_file: str = ""
    key_file: str = ""
    ca_file: str = ""
    verify_client: bool = False


class WebSocketLimitsConfig(Base):
    # 中文注释：连接数上限用于防止异常客户端拖垮进程。
    max_connections: int = 200
    max_message_bytes: int = 128 * 1024
    auth_timeout_seconds: int = 10
    idle_timeout_seconds: int = 30 * 60
    outbound_queue_size: int = 200
    # 中文注释：按需求限制 WS 文件传输大小，默认单文件 2MB。
    max_media_bytes: int = 2 * 1024 * 1024


class WebSocketConfig(Base):
    enabled: bool = False
    host: str = "0.0.0.0"
    port: int = 18790
    path: str = "/ws"
    allow_from: list[str] = Field(default_factory=list)
    # 中文注释：出站文件默认发 blob，跨主机/跨进程场景无需共享磁盘。
    outbound_media_mode: Literal["blob", "filepath"] = "blob"
    auth: WebSocketAuthConfig = Field(default_factory=WebSocketAuthConfig)
    tls: WebSocketTLSConfig = Field(default_factory=WebSocketTLSConfig)
    limits: WebSocketLimitsConfig = Field(default_factory=WebSocketLimitsConfig)


@dataclass(slots=True)
class _OutboundFrame:
    payload: dict[str, Any]
    is_progress: bool


class WebSocketChannel(BaseChannel):
    name = "websocket"
    display_name = "WebSocket"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return WebSocketConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        if isinstance(config, dict):
            config = WebSocketConfig.model_validate(config)
        super().__init__(config, bus)
        self._server = None
        self._connections: set[Any] = set()
        # 中文注释：每连接独立出站队列，确保慢连接不会阻塞全局 outbound 分发。
        self._connection_queues: dict[Any, asyncio.Queue[_OutboundFrame]] = {}
        self._writer_tasks: dict[Any, asyncio.Task[None]] = {}
        self._connection_principals: dict[Any, str] = {}
        # 中文注释：记录连接首帧传入的 ACP 会话偏好（agent/model），供首次建会话时透传。
        self._connection_acp_preferences: dict[Any, dict[str, str]] = {}
        self._current_chat: dict[Any, str] = {}
        # 中文注释：chat_id -> 订阅连接集合，用于多人并发与多会话并存场景。
        self._chat_subscribers: dict[str, set[Any]] = {}

    async def start(self) -> None:
        import websockets

        # 中文注释：首版强制首帧 token，避免误配置成“裸开放”服务。
        if not self.config.auth.tokens:
            raise RuntimeError(
                "websocket.auth.tokens is empty; first-frame token auth requires tokens"
            )

        ssl_context = self._build_ssl_context()
        self._running = True
        # 中文注释：先统一接入 TCP 监听，再在业务层做 path 校验，便于保持实现简单。
        self._server = await websockets.serve(
            self._on_connection,
            self.config.host,
            self.config.port,
            max_size=self.config.limits.max_message_bytes,
            ssl=ssl_context,
            compression=None,
        )
        scheme = "wss" if ssl_context else "ws"
        logger.info(
            "WebSocket channel listening on {}://{}:{}{}",
            scheme,
            self.config.host,
            self.config.port,
            self.config.path,
        )

        try:
            await self._server.wait_closed()
        finally:
            self._running = False

    async def stop(self) -> None:
        self._running = False

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        # 中文注释：逐个关闭连接，确保 writer 任务和订阅索引全部回收。
        for conn in list(self._connections):
            await self._cleanup_connection(conn, close_code=1001, reason="channel stopping")

    async def send(self, msg: OutboundMessage) -> None:
        # 中文注释：只向目标 chat_id 的订阅者 fanout，不跨 chat 串话。
        recipients = list(self._chat_subscribers.get(msg.chat_id, set()))
        if not recipients:
            return

        is_progress = bool(msg.metadata.get("_progress"))
        frame_type = "progress" if is_progress else "final"
        payload = {
            "type": frame_type,
            "chatId": msg.chat_id,
            "content": msg.content,
            "metadata": msg.metadata,
        }
        if msg.media:
            # 中文注释：WS 出站附件支持 filepath/blob 双模式；默认 blob 便于远端直接消费。
            payload["media"] = await self._build_outbound_media_payload(msg.media)

        for conn in recipients:
            await self._enqueue_frame(
                conn, _OutboundFrame(payload=payload, is_progress=is_progress)
            )

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", (name or "").strip())
        return safe or "ws-file.bin"

    def _media_storage_dir(self) -> Path:
        # 中文注释：WS 入站 blob 文件统一落盘到独立子目录，便于后续审计与清理。
        target = get_media_dir("websocket") / "inbound"
        target.mkdir(parents=True, exist_ok=True)
        return target

    def _persist_inbound_blob(self, *, data: bytes, filename: str) -> str:
        safe_name = self._sanitize_filename(filename)
        short_hash = f"{(hash(data) & 0xFFFFFFFF):08x}"
        output = self._media_storage_dir() / f"{int(time.time() * 1000)}-{short_hash}-{safe_name}"
        output.write_bytes(data)
        return str(output)

    async def _build_outbound_media_payload(self, media_paths: list[str]) -> list[dict[str, Any]]:
        mode = str(self.config.outbound_media_mode or "blob").strip().lower()
        max_bytes = max(int(self.config.limits.max_media_bytes), 0)
        payloads: list[dict[str, Any]] = []
        for raw_path in media_paths:
            path = Path(str(raw_path)).expanduser()
            if not path.is_file():
                logger.warning("websocket outbound media skipped (not file): {}", raw_path)
                continue
            try:
                size = path.stat().st_size
            except OSError as e:
                logger.warning("websocket outbound media stat failed path={} err={}", path, e)
                continue
            if max_bytes > 0 and size > max_bytes:
                logger.warning(
                    "websocket outbound media skipped (too large) path={} size={} limit={}",
                    path,
                    size,
                    max_bytes,
                )
                continue
            if mode == "filepath":
                payloads.append(
                    {
                        "mode": "filepath",
                        "path": str(path.resolve()),
                        "filename": path.name,
                        "size": size,
                    }
                )
                continue

            try:
                raw = path.read_bytes()
            except OSError as e:
                logger.warning("websocket outbound media read failed path={} err={}", path, e)
                continue
            mime_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            payloads.append(
                {
                    "mode": "blob",
                    "filename": path.name,
                    "mimeType": mime_type,
                    "size": size,
                    "data": base64.b64encode(raw).decode("ascii"),
                }
            )
        return payloads

    async def _normalize_inbound_media(self, media_payload: Any) -> list[str]:
        """把 WS 帧里的文件描述统一为本地 filepath 列表。"""
        if media_payload is None:
            return []
        if not isinstance(media_payload, list):
            raise ValueError("media must be a list")

        resolved: list[str] = []
        max_bytes = max(int(self.config.limits.max_media_bytes), 0)
        for item in media_payload:
            if isinstance(item, str):
                if item.startswith("http://") or item.startswith("https://"):
                    raise ValueError("remote URL media is not allowed")
                raw_path = item
                if item.startswith("file://"):
                    raw_path = unquote(urlsplit(item).path)
                path = Path(raw_path).expanduser()
                if not path.is_file():
                    raise ValueError(f"media file not found: {item}")
                try:
                    size = path.stat().st_size
                except OSError as e:
                    raise ValueError(f"cannot read media file stat: {item}") from e
                if max_bytes > 0 and size > max_bytes:
                    raise ValueError(f"media file too large: {item}")
                resolved.append(str(path.resolve()))
                continue

            if not isinstance(item, dict):
                raise ValueError("media item must be string path or object")
            mode = str(item.get("mode") or "").strip().lower()
            if mode in {"path", "filepath"}:
                path_value = str(item.get("path") or "")
                if not path_value:
                    raise ValueError("path media requires path")
                path = Path(path_value).expanduser()
                if not path.is_file():
                    raise ValueError(f"media file not found: {path_value}")
                try:
                    size = path.stat().st_size
                except OSError as e:
                    raise ValueError(f"cannot read media file stat: {path_value}") from e
                if max_bytes > 0 and size > max_bytes:
                    raise ValueError(f"media file too large: {path_value}")
                resolved.append(str(path.resolve()))
                continue

            # 中文注释：默认将 dict 当作 blob/base64 处理，兼容 mode 缺失但包含 data 的请求。
            b64_data = str(item.get("data") or "")
            if not b64_data:
                raise ValueError("blob media requires data")
            try:
                raw = base64.b64decode(b64_data, validate=True)
            except (ValueError, binascii.Error) as e:
                raise ValueError("invalid base64 media data") from e
            if max_bytes > 0 and len(raw) > max_bytes:
                raise ValueError("media blob too large")
            filename = self._sanitize_filename(str(item.get("filename") or "blob.bin"))
            resolved.append(self._persist_inbound_blob(data=raw, filename=filename))
        return resolved

    def _build_ssl_context(self) -> ssl.SSLContext | None:
        if not self.config.tls.enabled:
            return None
        if not self.config.tls.cert_file or not self.config.tls.key_file:
            raise RuntimeError(
                "websocket.tls.certFile/keyFile must be provided when tls.enabled=true"
            )

        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ctx.load_cert_chain(self.config.tls.cert_file, self.config.tls.key_file)
        if self.config.tls.ca_file:
            ctx.load_verify_locations(self.config.tls.ca_file)
        if self.config.tls.verify_client:
            ctx.verify_mode = ssl.CERT_REQUIRED
        return ctx

    async def _on_connection(self, connection: Any) -> None:
        if len(self._connections) >= self.config.limits.max_connections:
            await connection.close(code=1013, reason="too many connections")
            return

        if not self._path_allowed(connection):
            await connection.close(code=1008, reason="invalid path")
            return

        principal = await self._authenticate(connection)
        if principal is None:
            return

        await self._register_connection(connection, principal)
        try:
            await connection.send(
                json.dumps({"type": "authed", "principalId": principal}, ensure_ascii=False)
            )
            while True:
                try:
                    raw = await asyncio.wait_for(
                        connection.recv(),
                        timeout=self.config.limits.idle_timeout_seconds,
                    )
                except asyncio.TimeoutError:
                    await connection.close(code=1000, reason="idle timeout")
                    break
                except Exception:
                    break
                await self._handle_client_frame(connection, raw)
        finally:
            # 中文注释：无论正常断开还是异常退出，统一走清理流程，避免索引泄漏。
            await self._cleanup_connection(connection, close_code=None, reason="connection closed")

    def _path_allowed(self, connection: Any) -> bool:
        expected = (self.config.path or "/").strip() or "/"
        request = getattr(connection, "request", None)
        if request is not None:
            raw_path = getattr(request, "path", "")
        else:
            raw_path = getattr(connection, "path", "")
        path = urlsplit(raw_path or "").path or "/"
        return path == expected

    async def _authenticate(self, connection: Any) -> str | None:
        try:
            raw = await asyncio.wait_for(
                connection.recv(), timeout=self.config.limits.auth_timeout_seconds
            )
        except asyncio.TimeoutError:
            await connection.close(code=1008, reason="auth timeout")
            return None
        except Exception:
            await connection.close(code=1008, reason="auth required")
            return None

        data = self._decode_json_frame(raw)
        if data is None or data.get("type") != "auth":
            await connection.close(code=1008, reason="first frame must be auth")
            return None

        token = str(data.get("token") or "")
        if not token or token not in set(self.config.auth.tokens):
            await connection.close(code=1008, reason="invalid token")
            return None

        principal = str(data.get("principalId") or data.get("clientId") or token)
        # 中文注释：首帧 token 决定连接身份，再复用 allowFrom 做二次访问控制。
        if not self.is_allowed(principal):
            await connection.close(code=1008, reason="principal not allowed")
            return None
        # 中文注释：首帧可选携带 agent/model；仅记录非空字符串，后续按“首帧优先，default 回落”策略使用。
        model = str(data.get("model") or "").strip()
        agent = str(data.get("agent") or "").strip()
        prefs: dict[str, str] = {}
        if model:
            prefs["model"] = model
        if agent:
            prefs["agent"] = agent
        self._connection_acp_preferences[connection] = prefs
        return principal

    async def _register_connection(self, connection: Any, principal: str) -> None:
        self._connections.add(connection)
        self._connection_principals[connection] = principal
        queue: asyncio.Queue[_OutboundFrame] = asyncio.Queue(
            maxsize=self.config.limits.outbound_queue_size
        )
        self._connection_queues[connection] = queue
        self._writer_tasks[connection] = asyncio.create_task(self._writer_loop(connection, queue))

    async def _cleanup_connection(
        self, connection: Any, close_code: int | None, reason: str
    ) -> None:
        if close_code is not None:
            try:
                await connection.close(code=close_code, reason=reason)
            except Exception:
                pass

        self._connections.discard(connection)
        self._connection_principals.pop(connection, None)
        self._connection_acp_preferences.pop(connection, None)
        self._current_chat.pop(connection, None)

        queue = self._connection_queues.pop(connection, None)
        if queue is not None:
            # 中文注释：写入 None 作为结束信号，让 writer 自然退出。
            try:
                queue.put_nowait(_OutboundFrame(payload={"type": "_close"}, is_progress=False))
            except asyncio.QueueFull:
                pass

        task = self._writer_tasks.pop(connection, None)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        # 中文注释：逆向索引必须同步清理，避免旧连接持续收到消息。
        empty_keys: list[str] = []
        for chat_id, conns in self._chat_subscribers.items():
            conns.discard(connection)
            if not conns:
                empty_keys.append(chat_id)
        for chat_id in empty_keys:
            self._chat_subscribers.pop(chat_id, None)

    async def _handle_client_frame(self, connection: Any, raw: Any) -> None:
        data = self._decode_json_frame(raw)
        if data is None:
            await self._send_to_connection(
                connection, {"type": "error", "code": "BAD_JSON", "message": "invalid json payload"}
            )
            return

        frame_type = str(data.get("type") or "")
        if frame_type == "ping":
            await self._send_to_connection(connection, {"type": "pong"})
            return

        if frame_type == "bind_chat":
            chat_id = str(data.get("chatId") or "")
            if not chat_id:
                await self._send_to_connection(
                    connection,
                    {"type": "error", "code": "MISSING_CHAT", "message": "chatId is required"},
                )
                return
            self._current_chat[connection] = chat_id
            # 中文注释：bind_chat 默认即订阅该 chat，方便 Web UI 直接切换会话。
            self._chat_subscribers.setdefault(chat_id, set()).add(connection)
            await self._send_to_connection(connection, {"type": "bound", "chatId": chat_id})
            return

        if frame_type == "subscribe":
            chat_id = str(data.get("chatId") or "")
            if chat_id:
                self._chat_subscribers.setdefault(chat_id, set()).add(connection)
                await self._send_to_connection(
                    connection, {"type": "subscribed", "chatId": chat_id}
                )
            return

        if frame_type == "unsubscribe":
            chat_id = str(data.get("chatId") or "")
            if chat_id in self._chat_subscribers:
                self._chat_subscribers[chat_id].discard(connection)
                if not self._chat_subscribers[chat_id]:
                    self._chat_subscribers.pop(chat_id, None)
            await self._send_to_connection(connection, {"type": "unsubscribed", "chatId": chat_id})
            return

        if frame_type in {"send", "stop"}:
            # 中文注释：允许客户端显式传 chatId；未传时回落到 bind_chat 的当前会话。
            chat_id = str(data.get("chatId") or self._current_chat.get(connection, ""))
            if not chat_id:
                await self._send_to_connection(
                    connection,
                    {"type": "error", "code": "MISSING_CHAT", "message": "chatId is required"},
                )
                return

            session_key = data.get("sessionKey")
            content = "/stop" if frame_type == "stop" else str(data.get("content") or "")
            metadata_raw = data.get("metadata")
            metadata: dict[str, Any] = metadata_raw if isinstance(metadata_raw, dict) else {}
            metadata["_websocket_request_id"] = data.get("requestId")
            try:
                media = await self._normalize_inbound_media(data.get("media"))
            except ValueError as e:
                await self._send_to_connection(
                    connection,
                    {
                        "type": "error",
                        "code": "BAD_MEDIA",
                        "message": str(e),
                        "requestId": data.get("requestId"),
                    },
                )
                return
            prefs = self._connection_acp_preferences.get(connection, {})
            if prefs.get("model"):
                metadata["_acp_session_model"] = prefs["model"]
            if prefs.get("agent"):
                metadata["_acp_session_agent"] = prefs["agent"]

            await self._handle_message(
                sender_id=self._connection_principals.get(connection, "unknown"),
                chat_id=chat_id,
                content=content,
                media=media,
                metadata=metadata,
                # 中文注释：sessionKey 可选覆盖默认 websocket:<chat_id>，用于同 chat 多线程并行对话。
                session_key=str(session_key) if session_key else None,
            )
            await self._send_to_connection(
                connection, {"type": "ack", "requestId": data.get("requestId")}
            )
            return

        await self._send_to_connection(
            connection,
            {
                "type": "error",
                "code": "UNSUPPORTED",
                "message": f"unsupported frame type: {frame_type}",
            },
        )

    async def _enqueue_frame(self, connection: Any, frame: _OutboundFrame) -> None:
        queue = self._connection_queues.get(connection)
        if queue is None:
            return

        try:
            queue.put_nowait(frame)
            return
        except asyncio.QueueFull:
            pass

        if frame.is_progress:
            # 中文注释：回压时优先丢进度帧，保证 final 帧尽可能送达。
            return

        # 中文注释：final 消息尽量保留；若队列满则淘汰一个最旧消息再写入。
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass

        try:
            queue.put_nowait(frame)
        except asyncio.QueueFull:
            await self._cleanup_connection(
                connection, close_code=1013, reason="connection backpressure"
            )

    async def _writer_loop(self, connection: Any, queue: asyncio.Queue[_OutboundFrame]) -> None:
        try:
            while True:
                frame = await queue.get()
                if frame.payload.get("type") == "_close":
                    break
                await connection.send(json.dumps(frame.payload, ensure_ascii=False))
        except asyncio.CancelledError:
            pass
        except Exception:
            await self._cleanup_connection(connection, close_code=1011, reason="writer failed")

    async def _send_to_connection(self, connection: Any, payload: dict[str, Any]) -> None:
        try:
            await connection.send(json.dumps(payload, ensure_ascii=False))
        except Exception:
            await self._cleanup_connection(connection, close_code=None, reason="send failed")

    def _decode_json_frame(self, raw: Any) -> dict[str, Any] | None:
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                return None
        if not isinstance(raw, str):
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        return data
