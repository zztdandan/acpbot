"""ACP runtime-level models shared across runtime, inbound, and state.

These models codify the owner boundaries described in the ACP redesign docs:
- runtime owns wait entries and stop results
- inbound owns direct input/context/request assembly
- process manager owns active request lifecycle and per-session queues
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Awaitable, Callable

from nanobot.acp.contracts import ACPArtifactMap, ACPChannelName, ACPDirectIdentity, JSONMap

from nanobot.bus.events import InboundMessage, OutboundMessage

if TYPE_CHECKING:
    from nanobot.acp.state.manager import SessionStateManager
    from nanobot.acp.state.router import ProgressRouter


ProgressCallback = Callable[..., Awaitable[None]]


class RequestStatus(str, Enum):
    """Lifecycle status for a process request owned by ProcessRuntimeManager."""

    QUEUED = "queued"
    STARTING = "starting"
    ACTIVE = "active"
    FINISHING = "finishing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(slots=True)
class RuntimeWaitEntry:
    """Runtime-owned await entry for one request_key."""

    request_key: str
    done_future: asyncio.Future[OutboundMessage]


@dataclass(slots=True)
class ProcessDirectInput:
    """Runtime-level normalized input for direct process entry."""

    content: str
    nanobot_side_session_key: str = ACPDirectIdentity.SESSION_KEY.value
    channel: str = ACPChannelName.CLI.value
    chat_id: str = ACPDirectIdentity.CHAT_ID.value
    sender_id: str | None = None
    media: list[str] = field(default_factory=list)
    metadata: JSONMap = field(default_factory=dict)
    on_progress: ProgressCallback | None = None


@dataclass(slots=True)
class ProcessRequest:
    """Inbound output handed to ProcessRuntimeManager for real execution."""

    request_key: str
    nanobot_side_session_key: str
    channel: str
    chat_id: str
    sender_id: str | None
    content: str
    media: list[str]
    metadata: JSONMap
    on_progress: ProgressCallback | None = None
    artifacts: dict[str, ACPArtifactMap | list[ACPArtifactMap]] = field(default_factory=dict)


@dataclass(slots=True)
class InboundContext:
    """Single context object flowing through inbound steps."""

    request_key: str
    nanobot_side_session_key: str
    raw_message: InboundMessage | None = None
    channel: str = ACPChannelName.CLI.value
    chat_id: str = ACPDirectIdentity.CHAT_ID.value
    sender_id: str | None = None
    content: str = ""
    media: list[str] = field(default_factory=list)
    metadata: JSONMap = field(default_factory=dict)
    progress_metadata: JSONMap = field(default_factory=dict)
    on_progress: ProgressCallback | None = None
    direct_response: OutboundMessage | None = None
    process_request: ProcessRequest | None = None
    artifacts: dict[str, ACPArtifactMap | list[ACPArtifactMap]] = field(default_factory=dict)


@dataclass(slots=True)
class ActiveProcessEntry:
    """Process-manager-owned active request entry."""

    request_key: str
    nanobot_side_session_key: str
    acp_side_session_id: str
    process_request: ProcessRequest
    state_manager: SessionStateManager
    progress_router: ProgressRouter
    status: RequestStatus = RequestStatus.STARTING

    async def close(self) -> None:
        """Close state-owned resources before runtime completion fires."""

        await self.state_manager.close()


@dataclass(slots=True)
class SessionQueueState:
    """Per-session serial queue owned by ProcessRuntimeManager."""

    nanobot_side_session_key: str
    queued_requests: deque[ProcessRequest] = field(default_factory=deque)
    draining_task: asyncio.Task[None] | None = None


@dataclass(slots=True)
class StopResult:
    """Structured result returned by runtime stop_session()."""

    nanobot_side_session_key: str
    active_cancel_requested: bool = False
    dropped_queued_count: int = 0
    acp_side_session_id: str | None = None

    @property
    def had_anything_to_stop(self) -> bool:
        return self.active_cancel_requested or self.dropped_queued_count > 0
