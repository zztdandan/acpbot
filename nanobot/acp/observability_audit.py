"""ACP 可观测性审计落盘能力。"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

from loguru import logger

from nanobot.bus.events import InboundMessage, OutboundMessage


class _ACPObservabilityAuditMixin:
    """提供 inbound/outbound/tool 维度的 JSONL 审计能力。"""

    _audit_run_id: str
    _audit_run_dir: Path
    _audit_tool_dir: Path
    _audit_lock: Any
    _session_id_to_session_key: dict[str, str]
    _audit_enabled: bool
    _audit_inbound_fp: TextIO | None
    _audit_outbound_fp: TextIO | None
    _audit_tool_fps: dict[str, TextIO]

    @staticmethod
    def _to_jsonable(value: Any) -> Any:
        raise NotImplementedError

    @staticmethod
    def _safe_filename(name: str) -> str:
        raise NotImplementedError

    def _build_outbound_debug_payload(
        self, *, msg: OutboundMessage, reason: str, session_key: str
    ) -> dict[str, Any]:
        raise NotImplementedError

    @staticmethod
    def _tool_name_from_hint_line(line: str) -> str | None:
        raise NotImplementedError

    def _close_observability(self) -> None:
        raise NotImplementedError

    def _ensure_audit_streams(self) -> None:
        """懒初始化审计文件句柄，按运行周期固定到当前 run 目录。"""
        if not self._audit_enabled:
            return
        if self._audit_inbound_fp is not None and self._audit_outbound_fp is not None:
            return
        try:
            self._audit_run_dir.mkdir(parents=True, exist_ok=True)  # type: ignore[attr-defined]
            self._audit_tool_dir.mkdir(parents=True, exist_ok=True)  # type: ignore[attr-defined]
            if self._audit_inbound_fp is None:
                self._audit_inbound_fp = (self._audit_run_dir / "inbound.jsonl").open(  # type: ignore[attr-defined]
                    "a", encoding="utf-8"
                )
            if self._audit_outbound_fp is None:
                self._audit_outbound_fp = (self._audit_run_dir / "outbound.jsonl").open(  # type: ignore[attr-defined]
                    "a", encoding="utf-8"
                )
        except OSError as exc:
            self._audit_enabled = False
            logger.warning(
                "ACP audit disabled due to filesystem error run_dir={} error_type={} error={}",
                self._audit_run_dir,  # type: ignore[attr-defined]
                type(exc).__name__,
                exc,
            )
            self._close_observability()  # type: ignore[attr-defined]

    async def _append_audit_jsonl(self, *, fp: TextIO, record: dict[str, Any]) -> None:
        """串行写入 jsonl，避免并发会话下记录穿插。"""
        serialized = json.dumps(
            self._to_jsonable(record),  # type: ignore[attr-defined]
            ensure_ascii=False,
            separators=(",", ":"),
        )
        async with self._audit_lock:  # type: ignore[attr-defined]
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
                self._close_observability()  # type: ignore[attr-defined]

    async def _append_tool_audit_jsonl(self, *, tool_name: str, record: dict[str, Any]) -> None:
        """按工具维度拆分审计文件；同一工具在同次运行只打开一个句柄。"""
        self._ensure_audit_streams()
        if not self._audit_enabled:
            return
        safe_name = self._safe_filename(tool_name)  # type: ignore[attr-defined]
        fp = self._audit_tool_fps.get(safe_name)
        if fp is None:
            try:
                fp = (self._audit_tool_dir / f"{safe_name}.jsonl").open("a", encoding="utf-8")  # type: ignore[attr-defined]
                self._audit_tool_fps[safe_name] = fp
            except OSError as exc:
                self._audit_enabled = False
                logger.warning(
                    "ACP tool audit disabled due to filesystem error tool_dir={} tool={} error_type={} error={}",
                    self._audit_tool_dir,  # type: ignore[attr-defined]
                    safe_name,
                    type(exc).__name__,
                    exc,
                )
                self._close_observability()  # type: ignore[attr-defined]
                return
        await self._append_audit_jsonl(fp=fp, record=record)

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
                "runId": self._audit_run_id,  # type: ignore[attr-defined]
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
        payload = self._build_outbound_debug_payload(
            msg=msg, reason=reason, session_key=session_key
        )  # type: ignore[attr-defined]
        now = datetime.now(timezone.utc).isoformat()
        await self._append_audit_jsonl(
            fp=self._audit_outbound_fp,
            record={
                "event": "outbound",
                "recordedAt": now,
                "runId": self._audit_run_id,  # type: ignore[attr-defined]
                "payload": payload,
            },
        )
        # 中文注释：tool-hint 事件按“每个工具一个文件”落盘，便于逐工具全链路审计。
        if not bool((msg.metadata or {}).get("_tool_hint")):
            return
        hint_lines = [line for line in msg.content.splitlines() if line.strip()]
        for line in hint_lines:
            tool_name = self._tool_name_from_hint_line(line) or "unknown_tool"  # type: ignore[attr-defined]
            await self._append_tool_audit_jsonl(
                tool_name=tool_name,
                record={
                    "event": "tool_hint_outbound",
                    "recordedAt": now,
                    "runId": self._audit_run_id,  # type: ignore[attr-defined]
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
        session_key = self._session_id_to_session_key.get(session_id, "")  # type: ignore[attr-defined]
        await self._append_tool_audit_jsonl(
            tool_name=tool_name,
            record={
                "event": event,
                "recordedAt": datetime.now(timezone.utc).isoformat(),
                "runId": self._audit_run_id,  # type: ignore[attr-defined]
                "sessionId": session_id,
                "sessionKey": session_key,
                "payload": payload,
            },
        )
