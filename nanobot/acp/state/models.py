"""Core ACP state models.

The state package owns one request-scoped state manager at a time, so the models
here describe per-request facts rather than runtime-level global maps.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from nanobot.acp.contracts import JSONMap


class ACPUpdateType(str, Enum):
    """High-level update types recognized by the state router."""

    AGENT_MESSAGE_TEXT = "agent_message_text"
    AGENT_MESSAGE_MEDIA = "agent_message_media"
    TOOL_START = "tool_start"
    TOOL_PROGRESS = "tool_progress"
    PERMISSION_REQUEST = "permission_request"
    OTHER = "other"


class ACPBucketType(str, Enum):
    """Bucket types used for request-scoped pool indexing."""

    MESSAGE_TEXT = "message_text"
    MEDIA = "media"
    TOOL = "tool"
    PERMISSION = "permission"
    OTHER = "other"


class ACPOutboundKind(str, Enum):
    """Kinds of outbound fragments that can be mirrored as progress."""

    TEXT = "text"
    MEDIA = "media"
    TOOL = "tool"
    PERMISSION = "permission"


@dataclass(slots=True)
class FlushResult:
    """Progress flush payload produced by a router/pool pass."""

    kind: ACPOutboundKind
    content: str = ""
    media: list[str] = field(default_factory=list)
    metadata: JSONMap = field(default_factory=dict)


@dataclass(slots=True)
class RequestScopeState:
    """Request-scoped facts later materialized into the final outbound."""

    final_text: str = ""
    media_paths: list[str] = field(default_factory=list)
    final_metadata: JSONMap = field(default_factory=dict)
    partial_text: str = ""


class ACPPool(Protocol):
    """Shared pool contract used by state handlers and the router."""

    def accept(self, payload: object) -> None: ...

    def flush(self) -> FlushResult | None: ...

    def close(self) -> FlushResult | None: ...

    def is_terminal(self) -> bool: ...
