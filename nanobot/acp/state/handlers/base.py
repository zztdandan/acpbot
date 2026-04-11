from __future__ import annotations

from typing import Any, Protocol

from nanobot.acp.progress_event_types import ACPProgressEvent
from nanobot.acp.state.models import ACPBucketType, ACPOutboundKind, ACPUpdateType


class ACPSessionHandler(Protocol):
    """Minimal handler contract for registry-driven ACP dispatch."""

    name: str
    supported_update_types: frozenset[ACPUpdateType]
    bucket_type: ACPBucketType
    outbound_kind: ACPOutboundKind
    auto_dispatch: bool

    def match(self, event: ACPProgressEvent) -> bool: ...

    def build_bucket_key(self, event: ACPProgressEvent) -> str | None: ...

    async def consume(self, manager: Any, item: Any) -> Any: ...

    async def enqueue(self, manager: Any, item: Any) -> Any: ...

    async def flush(
        self,
        manager: Any,
        *,
        session_id: str,
        bucket_key: str,
        reason: str,
    ) -> Any: ...
