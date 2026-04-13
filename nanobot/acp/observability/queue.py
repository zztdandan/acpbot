"""ACP observability queue primitives."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from nanobot.acp.contracts import JSONMap, ObservabilityEventName, ObservabilityScopeName


@dataclass(slots=True)
class ObservabilityEvent:
    """Structured runtime/process/state event for debug and audit sinks."""

    scope: ObservabilityScopeName
    event: ObservabilityEventName
    request_key: str | None = None
    nanobot_side_session_key: str | None = None
    acp_side_session_id: str | None = None
    payload: JSONMap = field(default_factory=dict)


class ObservabilityQueue:
    """Small wrapper around an asyncio queue for observability events."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[ObservabilityEvent] = asyncio.Queue()

    async def push(self, event: ObservabilityEvent) -> None:
        await self._queue.put(event)

    async def consume(self) -> ObservabilityEvent:
        return await self._queue.get()
