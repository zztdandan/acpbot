from __future__ import annotations

from typing import Any, cast

from nanobot.acp.progress_event_types import ACPProgressEvent
from nanobot.acp.state import ACPBucketType, ACPOutboundKind, ACPUpdateType, FlushResult
from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.pools import ConsumeOnlyPool


class UserMessageHandler:
    name = "user_message"
    supported_update_types = frozenset({ACPUpdateType.USER_MESSAGE_CHUNK})
    bucket_type = ACPBucketType.NONE
    outbound_kind = ACPOutboundKind.NONE
    auto_dispatch = True

    def match(self, event: ACPProgressEvent) -> bool:
        return event.update_type == ACPUpdateType.USER_MESSAGE_CHUNK

    def build_bucket_key(self, event: ACPProgressEvent) -> str | None:
        return event.route_key or event.session_id

    async def consume(self, manager: SessionStateManager, item: Any) -> None:
        del manager, item

    async def enqueue(
        self, manager: SessionStateManager, item: ACPProgressEvent
    ) -> ConsumeOnlyPool:
        bucket_key = self.build_bucket_key(item)
        if bucket_key is None:
            raise ValueError("User message handler requires a bucket key")
        pool = cast(
            ConsumeOnlyPool,
            manager.register_pool(
                ConsumeOnlyPool(
                    pool_id=f"consume:{item.session_id}:{bucket_key}",
                    session_id=item.session_id,
                ),
                bucket_key=bucket_key,
            ),
        )
        await pool.accept(item.raw_json or {})
        return pool

    async def flush(
        self,
        manager: SessionStateManager,
        *,
        session_id: str,
        bucket_key: str,
        reason: str,
    ) -> FlushResult:
        del manager, session_id, bucket_key
        return FlushResult(
            outbound_kind=self.outbound_kind, payload=None, metadata={"reason": reason}
        )
