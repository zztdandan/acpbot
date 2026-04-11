from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from nanobot.acp.state.models import ACPBucketType, ACPOutboundKind, FlushResult


@dataclass(slots=True)
class MessageTextPool:
    pool_id: str
    session_id: str
    pool_type: ACPBucketType = ACPBucketType.TEXT
    route_key: str = ""
    parts: list[str] = field(default_factory=list)
    idle_task: asyncio.Task[Any] | None = None
    _terminal: bool = False

    async def accept(self, item: Any) -> None:
        if item is not None:
            self.parts.append(str(item))

    async def flush(self, reason: str) -> FlushResult:
        text = "".join(self.parts)
        self.parts.clear()
        return FlushResult(
            outbound_kind=ACPOutboundKind.TEXT,
            payload=text,
            metadata={"reason": reason},
        )

    async def close(self) -> None:
        if self.idle_task is not None:
            self.idle_task.cancel()
            self.idle_task = None
        self._terminal = True

    def is_terminal(self) -> bool:
        return self._terminal


@dataclass(slots=True)
class MediaPool:
    pool_id: str
    session_id: str
    pool_type: ACPBucketType = ACPBucketType.MEDIA
    route_key: str = ""
    latest_payload: dict[str, Any] | None = None
    idle_task: asyncio.Task[Any] | None = None
    _terminal: bool = False

    async def accept(self, item: Any) -> None:
        self.latest_payload = dict(item or {})

    async def flush(self, reason: str) -> FlushResult:
        payload = dict(self.latest_payload or {})
        self.latest_payload = None
        return FlushResult(
            outbound_kind=ACPOutboundKind.MEDIA,
            payload=payload,
            metadata={"reason": reason},
        )

    async def close(self) -> None:
        if self.idle_task is not None:
            self.idle_task.cancel()
            self.idle_task = None
        self._terminal = True

    def is_terminal(self) -> bool:
        return self._terminal


@dataclass(slots=True)
class ToolPool:
    pool_id: str
    session_id: str
    pool_type: ACPBucketType = ACPBucketType.TOOL
    route_key: str = ""
    latest: dict[str, Any] | None = None
    history: list[dict[str, Any]] = field(default_factory=list)
    history_dropped: int = 0
    idle_task: asyncio.Task[Any] | None = None
    _terminal: bool = False

    async def accept(self, item: Any) -> None:
        payload = dict(item or {})
        self.latest = payload
        self.history.append(payload)

    async def flush(self, reason: str) -> FlushResult:
        return FlushResult(
            outbound_kind=ACPOutboundKind.TOOL,
            payload={
                "latest": dict(self.latest or {}),
                "history": list(self.history),
                "history_dropped": self.history_dropped,
            },
            metadata={"reason": reason},
        )

    async def close(self) -> None:
        if self.idle_task is not None:
            self.idle_task.cancel()
            self.idle_task = None
        self._terminal = True

    def is_terminal(self) -> bool:
        return self._terminal


@dataclass(slots=True)
class ThoughtPool:
    pool_id: str
    session_id: str
    pool_type: ACPBucketType = ACPBucketType.THOUGHT
    items: list[Any] = field(default_factory=list)
    _terminal: bool = False

    async def accept(self, item: Any) -> None:
        self.items.append(item)

    async def flush(self, reason: str) -> FlushResult:
        return FlushResult(
            outbound_kind=ACPOutboundKind.THOUGHT,
            payload=list(self.items),
            metadata={"reason": reason},
        )

    async def close(self) -> None:
        self._terminal = True

    def is_terminal(self) -> bool:
        return self._terminal


@dataclass(slots=True)
class PlanPool:
    pool_id: str
    session_id: str
    pool_type: ACPBucketType = ACPBucketType.PLAN
    items: list[Any] = field(default_factory=list)
    _terminal: bool = False

    async def accept(self, item: Any) -> None:
        self.items.append(item)

    async def flush(self, reason: str) -> FlushResult:
        return FlushResult(
            outbound_kind=ACPOutboundKind.PLAN,
            payload=list(self.items),
            metadata={"reason": reason},
        )

    async def close(self) -> None:
        self._terminal = True

    def is_terminal(self) -> bool:
        return self._terminal


@dataclass(slots=True)
class PermissionPool:
    pool_id: str
    session_id: str
    pool_type: ACPBucketType = ACPBucketType.PERMISSION
    items: list[Any] = field(default_factory=list)
    _terminal: bool = False

    async def accept(self, item: Any) -> None:
        self.items.append(item)

    async def flush(self, reason: str) -> FlushResult:
        return FlushResult(
            outbound_kind=ACPOutboundKind.PERMISSION,
            payload=list(self.items),
            metadata={"reason": reason},
        )

    async def close(self) -> None:
        self._terminal = True

    def is_terminal(self) -> bool:
        return self._terminal


@dataclass(slots=True)
class ConsumeOnlyPool:
    pool_id: str
    session_id: str
    pool_type: ACPBucketType = ACPBucketType.NONE
    accepted_items: list[Any] = field(default_factory=list)
    _terminal: bool = False

    async def accept(self, item: Any) -> None:
        self.accepted_items.append(item)
        self._terminal = True

    async def flush(self, reason: str) -> FlushResult:
        return FlushResult(
            outbound_kind=ACPOutboundKind.NONE,
            payload=None,
            metadata={"reason": reason},
        )

    async def close(self) -> None:
        self._terminal = True

    def is_terminal(self) -> bool:
        return self._terminal
