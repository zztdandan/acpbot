"""ACP progress family 入口。

本文件只保留聚合器装配逻辑，文本与 tool-hint 分支在子模块中维护。
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Coroutine, Literal

from nanobot.acp.progress_text import _ProgressTextMixin
from nanobot.acp.progress_tool_hint import _ProgressToolHintMixin


class _ProgressAccumulator(_ProgressTextMixin, _ProgressToolHintMixin):
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
