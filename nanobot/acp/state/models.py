from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol


class ACPUpdateType(str, Enum):
    """Normalized ACP update types for the new handler pipeline."""

    USER_MESSAGE_CHUNK = "user_message_chunk"
    AGENT_MESSAGE_CHUNK = "agent_message_chunk"
    AGENT_THOUGHT_CHUNK = "agent_thought_chunk"
    TOOL_CALL_START = "tool_call_start"
    TOOL_CALL_PROGRESS = "tool_call_progress"
    TOOL_CALL_UPDATE = "tool_call_update"
    AGENT_PLAN_UPDATE = "agent_plan_update"
    AVAILABLE_COMMANDS_UPDATE = "available_commands_update"
    CURRENT_MODE_UPDATE = "current_mode_update"
    CONFIG_OPTION_UPDATE = "config_option_update"
    SESSION_INFO_UPDATE = "session_info_update"
    USAGE_UPDATE = "usage_update"
    UNKNOWN = "unknown"


class ACPBucketType(str, Enum):
    """Pool categories owned by the session state manager."""

    NONE = "none"
    TEXT = "text"
    MEDIA = "media"
    TOOL = "tool"
    THOUGHT = "thought"
    PLAN = "plan"
    PERMISSION = "permission"


class ACPOutboundKind(str, Enum):
    """Outbound payload categories emitted after pool flush."""

    NONE = "none"
    TEXT = "text"
    MEDIA = "media"
    TOOL = "tool"
    THOUGHT = "thought"
    PLAN = "plan"
    PERMISSION = "permission"


@dataclass(slots=True)
class FlushResult:
    """Minimal flush payload contract shared by runtime pools."""

    outbound_kind: ACPOutboundKind
    payload: Any | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RequestScopeState:
    """Request-scope aggregate placeholders used by the future manager."""

    final_text: str = ""
    media_paths: list[str] = field(default_factory=list)


class ACPPool(Protocol):
    """Lifecycle contract shared by concrete ACP pools."""

    pool_id: str
    pool_type: ACPBucketType
    session_id: str

    async def accept(self, item: Any) -> None: ...

    async def flush(self, reason: str) -> FlushResult: ...

    async def close(self) -> None: ...

    def is_terminal(self) -> bool: ...
