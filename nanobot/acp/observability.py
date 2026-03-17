"""ACP dispatcher 可观测性与审计能力。

本模块集中放置：
1) outbound JSON debug 事件；
2) inbound/outbound 分离 JSONL 落盘；
3) 按工具拆分的细粒度审计日志。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

from loguru import logger

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.config.paths import get_data_dir


class _ACPObservabilityMixin:
    """提供 dispatcher 的调试日志与审计文件能力。"""

    _session_id_to_session_key: dict[str, str]
    _audit_run_id: str
    _audit_run_dir: Path
    _audit_tool_dir: Path
    _audit_lock: asyncio.Lock
    _audit_inbound_fp: TextIO | None
    _audit_outbound_fp: TextIO | None
    _audit_tool_fps: dict[str, TextIO]
    _audit_enabled: bool

    @staticmethod
    def _format_final_content(content: str) -> str:
        """为 final 输出加可识别包裹，降低接收侧重复判定歧义。"""
        stripped = content.strip()
        if stripped.startswith("<final>") and stripped.endswith("</final>"):
            return content
        return f"<final>{content}</final>"

    @staticmethod
    def _to_jsonable(value: Any) -> Any:
        """将 ACP/消息对象安全转换为 JSON 可序列化结构。"""
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, dict):
            return {str(k): _ACPObservabilityMixin._to_jsonable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_ACPObservabilityMixin._to_jsonable(v) for v in value]
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            try:
                return _ACPObservabilityMixin._to_jsonable(
                    model_dump(by_alias=True, exclude_none=True, mode="json")
                )
            except Exception:
                pass
        return str(value)

    @staticmethod
    def _safe_filename(name: str) -> str:
        """把工具名转换为可落盘文件名。"""
        sanitized = re.sub(r"[^a-zA-Z0-9._-]+", "_", name.strip())
        sanitized = sanitized.strip("._")
        return sanitized or "unknown_tool"

    def _init_observability_state(self) -> None:
        """初始化审计运行态；每次 dispatcher 实例化对应一个 run 目录。"""
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
        self._audit_run_id = f"{stamp}-{os.getpid()}"
        self._audit_run_dir = get_data_dir() / "acp-audit" / f"run-{self._audit_run_id}"
        self._audit_tool_dir = self._audit_run_dir / "tools"
        self._audit_lock = asyncio.Lock()
        self._audit_inbound_fp = None
        self._audit_outbound_fp = None
        self._audit_tool_fps = {}
        # 中文注释：文件系统不可写时自动降级为“仅控制台 debug”，不影响主业务链路。
        self._audit_enabled = True

    def _log_acp_json(self, *, event: str, payload: dict[str, Any]) -> None:
        # 中文注释：统一输出 ACP 调试 JSON，便于平台侧按 event 检索与回放。
        record = {
            "event": event,
            "payload": self._to_jsonable(payload),
        }
        logger.debug(
            "ACP debug json {}", json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        )

    def _build_outbound_debug_payload(
        self,
        *,
        msg: OutboundMessage,
        reason: str,
        session_key: str,
    ) -> dict[str, Any]:
        metadata = dict(msg.metadata or {})
        is_tool_hint = bool(metadata.get("_tool_hint"))
        tool_items = (
            [line for line in msg.content.splitlines() if line.strip()] if is_tool_hint else []
        )
        return {
            "reason": reason,
            "session_key": session_key,
            "channel": msg.channel,
            "chat_id": msg.chat_id,
            "content": msg.content,
            "reply_to": msg.reply_to,
            "media": list(msg.media or []),
            "metadata": metadata,
            "tool": {
                "is_tool_hint": is_tool_hint,
                "items": tool_items,
                "raw": msg.content if is_tool_hint else "",
            },
        }

    def _ensure_audit_streams(self) -> None:
        """懒初始化审计文件句柄，按运行周期固定到当前 run 目录。"""
        if not self._audit_enabled:
            return
        if self._audit_inbound_fp is not None and self._audit_outbound_fp is not None:
            return
        try:
            self._audit_run_dir.mkdir(parents=True, exist_ok=True)
            self._audit_tool_dir.mkdir(parents=True, exist_ok=True)
            if self._audit_inbound_fp is None:
                self._audit_inbound_fp = (self._audit_run_dir / "inbound.jsonl").open(
                    "a", encoding="utf-8"
                )
            if self._audit_outbound_fp is None:
                self._audit_outbound_fp = (self._audit_run_dir / "outbound.jsonl").open(
                    "a", encoding="utf-8"
                )
        except OSError as exc:
            self._audit_enabled = False
            logger.warning(
                "ACP audit disabled due to filesystem error run_dir={} error_type={} error={}",
                self._audit_run_dir,
                type(exc).__name__,
                exc,
            )
            self._close_observability()

    async def _append_audit_jsonl(self, *, fp: TextIO, record: dict[str, Any]) -> None:
        """串行写入 jsonl，避免并发会话下记录穿插。"""
        serialized = json.dumps(
            self._to_jsonable(record),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        async with self._audit_lock:
            try:
                fp.write(serialized + "\n")
                fp.flush()
            except OSError as exc:
                self._audit_enabled = False
                logger.warning(
                    "ACP audit write disabled due to filesystem error error_type={} error={}",
                    type(exc).__name__,
                    exc,
                )
                self._close_observability()

    async def _append_tool_audit_jsonl(self, *, tool_name: str, record: dict[str, Any]) -> None:
        """按工具维度拆分审计文件；同一工具在同次运行只打开一个句柄。"""
        self._ensure_audit_streams()
        if not self._audit_enabled:
            return
        safe_name = self._safe_filename(tool_name)
        fp = self._audit_tool_fps.get(safe_name)
        if fp is None:
            try:
                fp = (self._audit_tool_dir / f"{safe_name}.jsonl").open("a", encoding="utf-8")
                self._audit_tool_fps[safe_name] = fp
            except OSError as exc:
                self._audit_enabled = False
                logger.warning(
                    "ACP tool audit disabled due to filesystem error tool_dir={} tool={} error_type={} error={}",
                    self._audit_tool_dir,
                    safe_name,
                    type(exc).__name__,
                    exc,
                )
                self._close_observability()
                return
        await self._append_audit_jsonl(fp=fp, record=record)

    @staticmethod
    def _tool_name_from_hint_line(line: str) -> str | None:
        """从 tool hint 单行提取工具名，兼容 `name(args)` / `name: ...` / 纯文本。"""
        stripped = line.strip()
        if not stripped:
            return None
        if "(" in stripped:
            return stripped.split("(", 1)[0].strip() or None
        if ":" in stripped:
            return stripped.split(":", 1)[0].strip() or None
        return stripped

    def _extract_tool_name(self, update: Any) -> str:
        """尽量从 ACP tool update 对象中提取工具名。"""
        # 中文注释：_pick 由 dispatcher 提供；这里复用其 snake/camel 兼容取值。
        direct_name = self._pick(update, "tool_name", "toolName", "name")  # type: ignore[attr-defined]
        if isinstance(direct_name, str) and direct_name.strip():
            return direct_name.strip()
        tool_call = self._pick(update, "tool_call", "toolCall")  # type: ignore[attr-defined]
        nested_name = self._pick(tool_call, "tool_name", "toolName", "name")  # type: ignore[attr-defined]
        if isinstance(nested_name, str) and nested_name.strip():
            return nested_name.strip()
        title = self._pick(update, "title")  # type: ignore[attr-defined]
        if isinstance(title, str) and title.strip():
            parsed = self._tool_name_from_hint_line(title)
            if parsed:
                return parsed
        return "unknown_tool"

    async def _audit_inbound(self, *, msg: InboundMessage, session_key: str) -> None:
        """将 inbound 事件落盘到独立文件，便于回放输入链路。"""
        self._ensure_audit_streams()
        if not self._audit_enabled:
            return
        if self._audit_inbound_fp is None:
            return
        await self._append_audit_jsonl(
            fp=self._audit_inbound_fp,
            record={
                "event": "inbound",
                "recordedAt": datetime.now(timezone.utc).isoformat(),
                "runId": self._audit_run_id,
                "sessionKey": session_key,
                "message": {
                    "channel": msg.channel,
                    "senderId": msg.sender_id,
                    "chatId": msg.chat_id,
                    "content": msg.content,
                    "timestamp": msg.timestamp.isoformat(),
                    "media": list(msg.media or []),
                    "metadata": dict(msg.metadata or {}),
                },
            },
        )

    async def _audit_outbound(
        self,
        *,
        msg: OutboundMessage,
        reason: str,
        session_key: str,
    ) -> None:
        """将 outbound 事件落盘到独立文件，并把工具相关事件拆分到工具文件。"""
        self._ensure_audit_streams()
        if not self._audit_enabled:
            return
        if self._audit_outbound_fp is None:
            return
        payload = self._build_outbound_debug_payload(msg=msg, reason=reason, session_key=session_key)
        now = datetime.now(timezone.utc).isoformat()
        await self._append_audit_jsonl(
            fp=self._audit_outbound_fp,
            record={
                "event": "outbound",
                "recordedAt": now,
                "runId": self._audit_run_id,
                "payload": payload,
            },
        )
        # 中文注释：tool-hint 事件按“每个工具一个文件”落盘，便于逐工具全链路审计。
        if not bool((msg.metadata or {}).get("_tool_hint")):
            return
        hint_lines = [line for line in msg.content.splitlines() if line.strip()]
        for line in hint_lines:
            tool_name = self._tool_name_from_hint_line(line) or "unknown_tool"
            await self._append_tool_audit_jsonl(
                tool_name=tool_name,
                record={
                    "event": "tool_hint_outbound",
                    "recordedAt": now,
                    "runId": self._audit_run_id,
                    "sessionKey": session_key,
                    "reason": reason,
                    "line": line,
                    "payload": payload,
                },
            )

    async def _audit_tool_event(
        self,
        *,
        tool_name: str,
        session_id: str,
        event: str,
        payload: dict[str, Any],
    ) -> None:
        """记录 ACP session_update 中的工具事件，形成每个工具的完整过程日志。"""
        session_key = self._session_id_to_session_key.get(session_id, "")
        await self._append_tool_audit_jsonl(
            tool_name=tool_name,
            record={
                "event": event,
                "recordedAt": datetime.now(timezone.utc).isoformat(),
                "runId": self._audit_run_id,
                "sessionId": session_id,
                "sessionKey": session_key,
                "payload": payload,
            },
        )

    async def _publish_outbound_with_debug(
        self,
        *,
        msg: OutboundMessage,
        reason: str,
        session_key: str,
    ) -> None:
        self._log_acp_json(
            event="acp_outbound",
            payload=self._build_outbound_debug_payload(
                msg=msg,
                reason=reason,
                session_key=session_key,
            ),
        )
        await self._audit_outbound(
            msg=msg,
            reason=reason,
            session_key=session_key,
        )
        await self.bus.publish_outbound(msg)  # type: ignore[attr-defined]

    def _close_observability(self) -> None:
        """关闭审计文件句柄，避免进程退出后跨次写入。"""
        if self._audit_inbound_fp is not None:
            self._audit_inbound_fp.close()
            self._audit_inbound_fp = None
        if self._audit_outbound_fp is not None:
            self._audit_outbound_fp.close()
            self._audit_outbound_fp = None
        for fp in self._audit_tool_fps.values():
            fp.close()
        self._audit_tool_fps.clear()
