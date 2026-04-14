"""工具池实现：按 `tool_call_id` 粒度保留单个工具的启动、进度与超时历史。"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import cast

from nanobot.acp.contracts import JSONMap, JSONValue
from nanobot.acp.state.models import ACPBucketType, ACPOutboundKind, FlushResult
from nanobot.acp.state.pools.base import ACPPoolBase


@dataclass(slots=True)
class ToolPoolPayload:
    """工具池输入载荷：与 ACP tool start/progress 共享结构保持一致。"""

    session_update: str
    tool_call_id: str
    title: str | None = None
    kind: str | None = None
    status: str | None = None
    content: list[JSONValue] | None = None
    locations: list[JSONValue] | None = None
    raw_input: JSONValue | None = None
    raw_output: JSONValue | None = None
    field_meta: JSONMap | None = None


class ToolPool(ACPPoolBase):
    """工具池：保存 tool start/progress 的结构化快照并生成统一提示文本。"""

    bucket_type = ACPBucketType.TOOL
    idle_timeout_seconds: float | None = 300.0

    def __init__(self, *, bucket_key: str) -> None:
        """建立工具池；每个 `tool:<tool_call_id>` 独立一份实例，死手时间固定为 300 秒。"""

        super().__init__(bucket_key=bucket_key)
        self._messages: list[str] = []
        self._events: list[ToolPoolPayload] = []
        self._snapshot: ToolPoolPayload | None = None
        self._status: str | None = None
        self._title: str | None = None
        self._tool_call_id: str = self._resolve_tool_call_id(bucket_key)
        self._dirty = False

    def _accept(self, payload: object) -> bool:
        """写入一条工具事件；保持结构化字段并维护 `title + status` 渲染文本。"""

        if not isinstance(payload, ToolPoolPayload):
            return False
        event_payload = self._clone_payload(payload)
        consumed = False
        if not self._events or self._events[-1] != event_payload:
            self._events.append(event_payload)
            self._dirty = True
            consumed = True

        self._merge_snapshot(payload)
        current_title = self._snapshot.title if self._snapshot is not None else payload.title
        current_status = self._snapshot.status if self._snapshot is not None else payload.status
        rendered = self._render_content(title=current_title, status=current_status)
        if not rendered:
            return consumed
        message = rendered.strip()
        if not self._messages or self._messages[-1] != message:
            self._messages.append(message)
            self._dirty = True
            consumed = True
        if self._snapshot is not None:
            self._status = self._snapshot.status
            self._title = self._snapshot.title
        if payload.status in {"completed", "failed", "timed_out"}:
            self.mark_terminal()
        return consumed

    def flush(self) -> FlushResult | None:
        """输出 `title + status` 文本，并在 metadata 中保留完整结构化事件与快照。"""

        if not self._dirty or not self._messages:
            return None
        self._dirty = False
        previous = list(self._messages[:-1])
        previous_values: list[JSONValue] = list(previous)
        metadata: JSONMap = {"tool_hint": True, "_tool_hint": True}
        if previous_values:
            metadata["previous"] = previous_values
        if self._status:
            metadata["status"] = self._status
        if self._events:
            metadata["tool_event"] = cast(JSONValue, self._build_event_map(self._events[-1]))
            if len(self._events) > 1:
                metadata["tool_events_previous"] = cast(
                    JSONValue,
                    [self._build_event_map(event) for event in self._events[:-1]],
                )
        if self._snapshot is not None:
            metadata["tool_snapshot"] = cast(JSONValue, self._build_event_map(self._snapshot))
        return FlushResult(
            kind=ACPOutboundKind.TOOL,
            content=self._messages[-1],
            metadata=metadata,
        )

    def mark_timeout(self) -> None:
        """把工具池切换到超时终态；死手触发时沿用正常 tool update 的结构。"""

        self._accept(
            ToolPoolPayload(
                session_update="tool_call_update",
                tool_call_id=self._tool_call_id,
                title=self._title,
                status="timed_out",
            )
        )

    @staticmethod
    def _resolve_tool_call_id(bucket_key: str) -> str:
        if bucket_key.startswith("tool:"):
            return bucket_key.split(":", 1)[1] or "unknown"
        return "unknown"

    @staticmethod
    def _render_content(*, title: str | None, status: str | None) -> str:
        """统一进度文本：正文以 `title` 为主，状态仅做轻量补充。"""

        clean_title = (title or "").strip()
        clean_status = (status or "").strip()
        if clean_title and clean_status:
            return f"{clean_title} [{clean_status}]"
        if clean_title:
            return clean_title
        if clean_status:
            return f"tool [{clean_status}]"
        return "tool progress"

    @staticmethod
    def _build_event_map(payload: ToolPoolPayload) -> JSONMap:
        """按 ACP alias 组织事件结构，便于下游无需再做字段名转换。"""

        event: JSONMap = {
            "sessionUpdate": payload.session_update,
            "toolCallId": payload.tool_call_id,
        }
        if payload.title is not None:
            event["title"] = payload.title
        if payload.kind is not None:
            event["kind"] = payload.kind
        if payload.status is not None:
            event["status"] = payload.status
        if payload.content is not None:
            event["content"] = payload.content
        if payload.locations is not None:
            event["locations"] = payload.locations
        if payload.raw_input is not None:
            event["rawInput"] = payload.raw_input
        if payload.raw_output is not None:
            event["rawOutput"] = payload.raw_output
        if payload.field_meta is not None:
            event["_meta"] = payload.field_meta
        return event

    @staticmethod
    def _clone_payload(payload: ToolPoolPayload) -> ToolPoolPayload:
        """复制载荷，避免外部对象引用穿透到池内部状态。"""

        return ToolPoolPayload(
            session_update=payload.session_update,
            tool_call_id=payload.tool_call_id,
            title=payload.title,
            kind=payload.kind,
            status=payload.status,
            content=copy.deepcopy(payload.content),
            locations=copy.deepcopy(payload.locations),
            raw_input=copy.deepcopy(payload.raw_input),
            raw_output=copy.deepcopy(payload.raw_output),
            field_meta=copy.deepcopy(payload.field_meta),
        )

    def _merge_snapshot(self, payload: ToolPoolPayload) -> None:
        """按 tool progress patch 语义维护 latest snapshot。"""

        if self._snapshot is None:
            self._snapshot = self._clone_payload(payload)
            return

        self._snapshot = ToolPoolPayload(
            session_update=payload.session_update,
            tool_call_id=payload.tool_call_id,
            title=payload.title if payload.title is not None else self._snapshot.title,
            kind=payload.kind if payload.kind is not None else self._snapshot.kind,
            status=payload.status if payload.status is not None else self._snapshot.status,
            content=copy.deepcopy(
                payload.content if payload.content is not None else self._snapshot.content
            ),
            locations=copy.deepcopy(
                payload.locations if payload.locations is not None else self._snapshot.locations
            ),
            raw_input=copy.deepcopy(
                payload.raw_input if payload.raw_input is not None else self._snapshot.raw_input
            ),
            raw_output=copy.deepcopy(
                payload.raw_output if payload.raw_output is not None else self._snapshot.raw_output
            ),
            field_meta=copy.deepcopy(
                payload.field_meta if payload.field_meta is not None else self._snapshot.field_meta
            ),
        )
