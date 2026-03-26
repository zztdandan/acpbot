"""ACP progress tool hint 聚合能力。"""

from __future__ import annotations

import asyncio
import json
from typing import Any


class _ProgressToolHintMixin:
    """封装 tool hint 归一化、聚合与刷出逻辑。"""

    _tool_hint_buckets: dict[str, list[dict[str, Any]]]
    _tool_hint_idle_tasks: dict[str, asyncio.Task[Any]]
    _tool_event_seq: int
    _tool_hint_publish_mode: str
    _tool_hint_payload_mode: str
    _tool_hint_terminal_statuses: set[str]
    _tool_hint_idle_seconds: float
    _lock: asyncio.Lock

    async def _handle_tool_hint_locked(
        self,
        *,
        content: str,
        tool_event: dict[str, Any] | None,
    ) -> None:
        """处理单条 tool hint，按配置决定“直发”或“按 toolCallId 聚合”。

        注意：调用方已持有 self._lock。
        """
        event = self._normalize_tool_event(content=content, tool_event=tool_event)
        if self._tool_hint_publish_mode == "immediate":
            # 中文注释：默认模式不等待不合并，每条 tool 事件独立发送。
            payload = self._format_tool_events_payload([event])
            await self._publish(payload, True)  # type: ignore[attr-defined]
            return

        key = self._tool_bucket_key(event)
        bucket = self._tool_hint_buckets.setdefault(key, [])
        bucket.append(event)

        # 中文注释：终态集合必须走配置项，保持与历史可配置行为一致；
        # 若终态缺失，则由每个 toolCallId 的独立死手兜底。
        status = self._event_status(event)
        if status in self._tool_hint_terminal_statuses:
            await self._publish_tool_bucket_locked(key)
            return
        self._arm_tool_idle_timer_locked(key)

    def _arm_tool_idle_timer_locked(self, key: str) -> None:
        """重置指定 tool_call_id 的死手。"""
        task = self._tool_hint_idle_tasks.get(key)
        if task is not None:
            task.cancel()
        self._tool_hint_idle_tasks[key] = asyncio.create_task(self._tool_idle_flush_worker(key))

    async def _publish_tool_bucket_locked(self, key: str) -> None:
        """发布并清理指定 tool bucket。"""
        events = self._tool_hint_buckets.get(key, [])
        if not events:
            return
        payload = self._format_tool_events_payload(events)
        publish_task = asyncio.ensure_future(self._publish(payload, True))  # type: ignore[attr-defined]
        try:
            await asyncio.shield(publish_task)
        except asyncio.CancelledError:
            await publish_task
            raise
        self._tool_hint_buckets.pop(key, None)
        task = self._tool_hint_idle_tasks.pop(key, None)
        if task is not None:
            task.cancel()

    def _normalize_tool_event(
        self, *, content: str, tool_event: dict[str, Any] | None
    ) -> dict[str, Any]:
        """构造 tool 事件标准形态，确保最少含有 raw_hint 字段。"""
        event: dict[str, Any] = dict(tool_event or {})
        if "raw_hint" not in event:
            # 中文注释：raw_hint 保留 dispatcher 传来的原始文案（如 title/status），便于追查。
            event["raw_hint"] = content
        return event

    def _tool_bucket_key(self, event: dict[str, Any]) -> str:
        """提取 toolCallId 作为聚合 key；缺失时退化为独立事件 key。"""
        tool_call = event.get("tool_call")
        if isinstance(tool_call, dict):
            # 中文注释：兼容 snake/camel 两种字段名。
            raw_id = tool_call.get("toolCallId") or tool_call.get("tool_call_id")
            if isinstance(raw_id, str) and raw_id.strip():
                return raw_id.strip()
        self._tool_event_seq += 1
        return f"__no_tool_call_id_{self._tool_event_seq}"

    @staticmethod
    def _event_status(event: dict[str, Any]) -> str | None:
        """从工具事件中抽取状态字段。"""
        status = event.get("status")
        if isinstance(status, str) and status.strip():
            return status.strip()
        tool_call = event.get("tool_call")
        if isinstance(tool_call, dict):
            nested_status = tool_call.get("status")
            if isinstance(nested_status, str) and nested_status.strip():
                return nested_status.strip()
        return None

    def _format_tool_events_payload(self, events: list[dict[str, Any]]) -> str:
        """按配置格式编码 tool 事件序列。"""
        if self._tool_hint_payload_mode == "array":
            # 中文注释：第一种格式，JSON 数组直接作为消息体。
            return json.dumps(events, ensure_ascii=False, separators=(",", ":"))

        # 中文注释：第二种格式，优先把“终态状态”事件作为主体事件（默认 completed/failed）。
        anchor_index = len(events) - 1
        for idx in range(len(events) - 1, -1, -1):
            status = self._event_status(events[idx])
            if status in self._tool_hint_terminal_statuses:
                anchor_index = idx
                break
        anchor = events[anchor_index]
        # 中文注释：消息主体只保留 channel 最关心的核心字段；其余事件放 progress_info 供参考。
        payload: dict[str, Any] = {}
        payload["session_id"] = str(anchor.get("session_id") or "")
        payload["status"] = self._event_status(anchor) or "in_progress"
        payload["tool_call"] = anchor.get("tool_call") or {}
        payload["progress_info"] = [item for idx, item in enumerate(events) if idx != anchor_index]
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    async def _tool_idle_flush_worker(self, key: str) -> None:
        try:
            await asyncio.sleep(self._tool_hint_idle_seconds)
            async with self._lock:
                await self._publish_tool_bucket_locked(key)
        except asyncio.CancelledError:
            return
