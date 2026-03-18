"""ACP progress 聚合工具。

本模块只负责流式进度聚合，不关心业务分发与 ACP 协议细节。
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable, Coroutine, Literal


class _ProgressAccumulator:
    """progress/tool-hint 聚合器。

    设计目标：
    1) 文本 progress 继续保留短死手（默认 2s）以减少碎片；
    2) tool hint 支持“逐条直发”与“按 toolCallId 聚合”两种模式；
    3) tool hint 内容保持结构化 JSON，避免只保留 title/status 导致信息丢失。
    """

    def __init__(
        self,
        *,
        idle_seconds: float,
        tool_hint_publish_mode: Literal["immediate", "merge_by_tool_call_id"] = "immediate",
        tool_hint_idle_seconds: float = 5.0,
        tool_hint_payload_mode: Literal["array", "status_with_compact"] = "array",
        tool_hint_terminal_statuses: list[str] | tuple[str, ...] = ("completed", "failed"),
        publish: Callable[[str, bool], Coroutine[Any, Any, None]],
    ) -> None:
        # 中文注释：text 进度缓冲参数（原有行为：短死手防抖）。
        self._idle_seconds = idle_seconds
        # 中文注释：tool hint 的发布策略/死手/编码模式由配置决定。
        self._tool_hint_publish_mode = tool_hint_publish_mode
        self._tool_hint_idle_seconds = tool_hint_idle_seconds
        self._tool_hint_payload_mode = tool_hint_payload_mode
        # 中文注释：保留终态状态的可扩展接口，方便后续新增如 cancelled/timeout 等状态。
        self._tool_hint_terminal_statuses = {
            str(status).strip()
            for status in tool_hint_terminal_statuses
            if isinstance(status, str) and str(status).strip()
        } or {"completed", "failed"}
        self._publish = publish

        # 中文注释：文本缓冲区；只用于普通 progress，不用于 tool hint。
        self._text_parts: list[str] = []
        self._text_idle_task: asyncio.Task[Any] | None = None

        # 中文注释：tool hint 聚合桶。key=tool_call_id，value=该工具调用收到的事件序列。
        self._tool_hint_buckets: dict[str, list[dict[str, Any]]] = {}
        # 中文注释：每个 tool_call_id 独立死手任务，互不影响。
        self._tool_hint_idle_tasks: dict[str, asyncio.Task[Any]] = {}
        # 中文注释：无 toolCallId 的事件需要唯一 key，避免混入其他工具序列。
        self._tool_event_seq = 0

        # 中文注释：所有缓冲与发布都在单锁内串行，保证顺序和状态一致性。
        self._lock = asyncio.Lock()

    async def on_progress(
        self,
        content: str,
        *,
        tool_hint: bool = False,
        tool_event: dict[str, Any] | None = None,
    ) -> None:
        """收到新进度后，按配置选择 text/tool-hint 的不同发布路径。"""
        if not content:
            return

        async with self._lock:
            if not tool_hint:
                # 中文注释：文本事件进入 text 缓冲；若有 tool 聚合并不冲突，独立处理即可。
                self._append_text_locked(content=content)
                self._arm_text_idle_timer_locked()
                return

            # 中文注释：tool hint 到来时先 flush 文本，避免“工具事件插在文本片段中间”。
            await self._publish_text_locked()
            await self._handle_tool_hint_locked(content=content, tool_event=tool_event)

    async def flush(self) -> None:
        """立即 flush 当前缓存（text + 所有 tool bucket）。"""
        async with self._lock:
            await self._publish_text_locked()
            # 中文注释：显式 flush 需要把所有未到死手的工具事件也刷出去。
            for key in list(self._tool_hint_buckets.keys()):
                await self._publish_tool_bucket_locked(key)

    async def close(self) -> None:
        """结束本轮会话前，取消定时任务并 flush 遗留内容。"""
        text_idle_task: asyncio.Task[Any] | None = None
        tool_idle_tasks: list[asyncio.Task[Any]] = []
        async with self._lock:
            text_idle_task = self._text_idle_task
            self._text_idle_task = None
            tool_idle_tasks = list(self._tool_hint_idle_tasks.values())
            self._tool_hint_idle_tasks.clear()
        if text_idle_task is not None:
            text_idle_task.cancel()
            try:
                await text_idle_task
            except asyncio.CancelledError:
                pass
        for task in tool_idle_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await self.flush()

    def _append_text_locked(self, *, content: str) -> None:
        """向文本缓冲追加分片。"""
        self._text_parts.append(content)

    def _snapshot_text_locked(self) -> str | None:
        """提取文本缓冲快照；为空时返回 None。"""
        merged = "".join(self._text_parts).strip()
        return merged if merged else None

    def _clear_text_locked(self) -> None:
        """清理文本缓冲。"""
        self._text_parts.clear()

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
            await self._publish(payload, True)
            return

        key = self._tool_bucket_key(event)
        bucket = self._tool_hint_buckets.setdefault(key, [])
        bucket.append(event)

        # 中文注释：当收到 completed/failed 终态时立刻刷出，减少状态延迟；
        # 若终态缺失，则由每个 toolCallId 的独立死手兜底。
        status = self._event_status(event)
        if status in {"completed", "failed"}:
            await self._publish_tool_bucket_locked(key)
            return
        self._arm_tool_idle_timer_locked(key)

    def _arm_text_idle_timer_locked(self) -> None:
        """重置文本死手。"""
        if self._text_idle_task is not None:
            self._text_idle_task.cancel()
        self._text_idle_task = asyncio.create_task(self._text_idle_flush_worker())

    def _arm_tool_idle_timer_locked(self, key: str) -> None:
        """重置指定 tool_call_id 的死手。"""
        task = self._tool_hint_idle_tasks.get(key)
        if task is not None:
            task.cancel()
        self._tool_hint_idle_tasks[key] = asyncio.create_task(self._tool_idle_flush_worker(key))

    async def _publish_text_locked(self) -> None:
        payload = self._snapshot_text_locked()
        if payload is None:
            return
        # 中文注释：发布放在锁内串行执行，避免并发 on_progress 场景下顺序颠倒。
        # 中文注释：使用 shield 确保 close() 取消 idle 任务时不会中断正在进行的真实发布。
        publish_task = asyncio.ensure_future(self._publish(payload, False))
        try:
            await asyncio.shield(publish_task)
        except asyncio.CancelledError:
            await publish_task
            raise
        self._clear_text_locked()

    async def _publish_tool_bucket_locked(self, key: str) -> None:
        """发布并清理指定 tool bucket。"""
        events = self._tool_hint_buckets.get(key, [])
        if not events:
            return
        payload = self._format_tool_events_payload(events)
        publish_task = asyncio.ensure_future(self._publish(payload, True))
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

    async def _text_idle_flush_worker(self) -> None:
        try:
            await asyncio.sleep(self._idle_seconds)
            async with self._lock:
                await self._publish_text_locked()
                self._text_idle_task = None
        except asyncio.CancelledError:
            return

    async def _tool_idle_flush_worker(self, key: str) -> None:
        try:
            await asyncio.sleep(self._tool_hint_idle_seconds)
            async with self._lock:
                await self._publish_tool_bucket_locked(key)
        except asyncio.CancelledError:
            return
