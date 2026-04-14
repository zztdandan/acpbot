"""工具 handler：按 tool_call_id 粒度隔离工具开始与进度更新。"""

from __future__ import annotations

from typing import cast

from acp.schema import ToolCallProgress, ToolCallStart

from nanobot.acp.state.handlers.base import HandlerConsumeResult, StateUpdateHandler
from nanobot.acp.state.models import ACPBucketType, ACPUpdateType
from nanobot.acp.state.pools import ToolPool


class ToolUpdateHandler(StateUpdateHandler):
    """工具更新处理器：处理 `tool_call` 与 `tool_call_update` 两种工具事件。"""

    name = "tool_update"
    update_type = ACPUpdateType.TOOL_PROGRESS
    bucket_type = ACPBucketType.TOOL

    def match(self, update: object) -> bool:
        """匹配工具启动或工具进度更新。"""

        return isinstance(update, ToolCallStart | ToolCallProgress)

    def create_pool(self, *, bucket_key: str) -> ToolPool:
        """创建工具池；每个 tool_call_id 拥有独立实例。"""

        return ToolPool(bucket_key=bucket_key)

    def build_bucket_key(self, update: object) -> str:
        """使用 `tool:<tool_call_id>` 作为池键，避免不同工具共用同一池。"""

        tool_call_id = getattr(update, "tool_call_id", None) or getattr(update, "toolCallId", None)
        return f"tool:{tool_call_id or 'unknown'}"

    def consume(self, *, state_manager, update: object, pool) -> HandlerConsumeResult:
        """把工具事件转成可读文本，并把工具产出的媒体同步到请求结果。"""

        typed_update = cast(ToolCallStart | ToolCallProgress, update)
        message = self._render_tool_message(typed_update)
        pool.accept(message)
        for media_path in self._extract_tool_media_paths(typed_update, state_manager=state_manager):
            state_manager.append_media_path(media_path, source="tool")
        return HandlerConsumeResult(flush_results=[pool.flush()])

    @staticmethod
    def _render_tool_message(update: ToolCallStart | ToolCallProgress) -> str:
        """把 ACP 工具更新规整成统一可读文本；优先使用 title/message/status。"""

        if isinstance(update, ToolCallStart):
            tool_name = str(
                getattr(update, "tool_name", None) or getattr(update, "toolName", None) or "tool"
            ).strip()
            return f"Running tool: {tool_name or 'tool'}"
        message = str(getattr(update, "message", "") or "").strip()
        if message:
            return message
        status = getattr(update, "status", None)
        if status is not None:
            return str(getattr(status, "value", status) or "tool progress")
        title = str(getattr(update, "title", "") or "").strip()
        if title:
            return title
        return "tool progress"

    @staticmethod
    def _extract_tool_media_paths(
        update: ToolCallStart | ToolCallProgress,
        *,
        state_manager,
    ) -> list[str]:
        """从工具内容块中提取媒体路径；只采纳真正的内容附件，忽略 terminal/diff 等过程态。"""

        media_paths: list[str] = []
        for content in list(getattr(update, "content", None) or []):
            if getattr(content, "type", None) != "content":
                continue
            block = getattr(content, "content", None)
            path = state_manager.extract_media_path(block)
            if path and path not in media_paths:
                media_paths.append(path)
        return media_paths
