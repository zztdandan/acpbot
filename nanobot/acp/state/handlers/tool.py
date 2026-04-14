"""工具处理器：按 `tool_call_id` 粒度隔离工具开始与进度更新。"""

from __future__ import annotations

from typing import cast

from acp.schema import ToolCallProgress, ToolCallStart

from nanobot.acp.contracts import JSONMap, JSONValue
from nanobot.acp.state.handlers.base import HandlerConsumeResult, StateUpdateHandler
from nanobot.acp.state.models import ACPBucketType, ACPUpdateType
from nanobot.acp.state.pools import ToolPool
from nanobot.acp.state.pools.tool import ToolPoolPayload


class ToolUpdateHandler(StateUpdateHandler):
    """工具更新处理器：处理工具启动与工具进度两类事件。"""

    name = "tool_update"
    update_type = ACPUpdateType.TOOL_PROGRESS
    bucket_type = ACPBucketType.TOOL
    _MAX_STRING_BYTES = 10 * 1024
    _TRUNCATED_SUFFIX = "...(truncated)"

    def match(self, update: object) -> bool:
        """匹配工具启动或工具进度更新。"""

        return isinstance(update, ToolCallStart | ToolCallProgress)

    def create_pool(self, *, bucket_key: str) -> ToolPool:
        """创建工具池；每个 `tool_call_id` 拥有独立实例。"""

        return ToolPool(bucket_key=bucket_key)

    def build_bucket_key(self, update: object) -> str:
        """使用 `tool:<tool_call_id>` 作为池键，避免不同工具共用同一池。"""

        tool_call_id = getattr(update, "tool_call_id", None) or getattr(update, "toolCallId", None)
        return f"tool:{tool_call_id or 'unknown'}"

    def consume(self, *, state_manager, update: object, pool) -> HandlerConsumeResult:
        """把工具事件转成工具池载荷；完成/失败在 accept 后立即允许销毁池。"""

        del state_manager
        typed_update = cast(ToolCallStart | ToolCallProgress, update)
        typed_pool = cast(ToolPool, pool)
        payload = self._build_pool_payload(typed_update)
        typed_pool.accept(payload)
        return HandlerConsumeResult(immediate_finalize=payload.status in {"completed", "failed"})

    def on_deadhand(self, *, state_manager, pool) -> None:
        """工具池 300 秒死手到点后补写一条超时消息，并将该池转入终态。"""

        del state_manager
        cast(ToolPool, pool).mark_timeout()

    @classmethod
    def _build_pool_payload(cls, update: ToolCallStart | ToolCallProgress) -> ToolPoolPayload:
        """把 ACP 标准字段转换为池载荷，并在 handler 内完成 10KB 深度字符串裁剪。"""

        normalized = cls._truncate_json_strings(
            cast(JSONValue, update.model_dump(by_alias=True, exclude_none=True))
        )
        if not isinstance(normalized, dict):
            normalized = {}

        session_update = cls._read_string(normalized, "sessionUpdate") or "tool_call_update"
        tool_call_id = cls._read_string(normalized, "toolCallId") or "unknown"
        return ToolPoolPayload(
            session_update=session_update,
            tool_call_id=tool_call_id,
            title=cls._read_string(normalized, "title"),
            kind=cls._read_string(normalized, "kind"),
            status=cls._read_string(normalized, "status"),
            content=cls._read_list(normalized, "content"),
            locations=cls._read_list(normalized, "locations"),
            raw_input=normalized.get("rawInput"),
            raw_output=normalized.get("rawOutput"),
            field_meta=cls._read_map(normalized, "_meta"),
        )

    @classmethod
    def _truncate_json_strings(cls, value: JSONValue) -> JSONValue:
        """递归裁剪任意深度字符串，保证 UTF-8 字节长度不超过 10KB。"""

        if isinstance(value, str):
            return cls._truncate_string(value)
        if isinstance(value, list):
            return [cls._truncate_json_strings(item) for item in value]
        if isinstance(value, dict):
            return {
                str(key): cls._truncate_json_strings(cast(JSONValue, item))
                for key, item in value.items()
            }
        return value

    @classmethod
    def _truncate_string(cls, value: str) -> str:
        """按 UTF-8 字节长度裁剪字符串并追加标记；保证输出仍是合法 UTF-8 文本。"""

        raw_bytes = value.encode("utf-8")
        if len(raw_bytes) <= cls._MAX_STRING_BYTES:
            return value
        suffix_bytes = cls._TRUNCATED_SUFFIX.encode("utf-8")
        keep_bytes = max(0, cls._MAX_STRING_BYTES - len(suffix_bytes))
        trimmed = raw_bytes[:keep_bytes]
        while True:
            try:
                prefix = trimmed.decode("utf-8")
                break
            except UnicodeDecodeError as exc:
                trimmed = trimmed[: exc.start]
                if not trimmed:
                    prefix = ""
                    break
        return f"{prefix}{cls._TRUNCATED_SUFFIX}"

    @staticmethod
    def _read_string(payload: JSONMap, key: str) -> str | None:
        value = payload.get(key)
        return value if isinstance(value, str) else None

    @staticmethod
    def _read_map(payload: JSONMap, key: str) -> JSONMap | None:
        value = payload.get(key)
        return value if isinstance(value, dict) else None

    @staticmethod
    def _read_list(payload: JSONMap, key: str) -> list[JSONValue] | None:
        value = payload.get(key)
        return value if isinstance(value, list) else None
