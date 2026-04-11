from __future__ import annotations

from typing import Any
from typing import cast

from nanobot.acp.progress_event_types import ACPProgressEvent
from nanobot.acp.state import ACPBucketType, ACPOutboundKind, ACPUpdateType, FlushResult
from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.pools import MessageTextPool


class AgentMessageTextHandler:
    name = "agent_message_text"
    supported_update_types = frozenset({ACPUpdateType.AGENT_MESSAGE_CHUNK})
    bucket_type = ACPBucketType.TEXT
    outbound_kind = ACPOutboundKind.TEXT
    auto_dispatch = True

    def match(self, event: ACPProgressEvent) -> bool:
        text = ((event.raw_json or {}).get("content") or {}).get("text")
        return isinstance(text, str) and bool(text)

    def build_bucket_key(self, event: ACPProgressEvent) -> str | None:
        return event.route_key or event.session_id

    async def consume(self, manager: SessionStateManager, item: Any) -> None:
        del manager, item

    async def enqueue(
        self, manager: SessionStateManager, item: ACPProgressEvent
    ) -> MessageTextPool:
        request_scope_id = str(item.ext.get("request_scope_id") or item.session_id)
        bucket_key = self.build_bucket_key(item)
        if bucket_key is None:
            raise ValueError("Text handler requires a bucket key")
        pool = manager.get_pool_by_key(
            bucket_type=self.bucket_type,
            session_id=item.session_id,
            bucket_key=bucket_key,
        )
        if pool is None:
            pool = cast(
                MessageTextPool,
                manager.register_pool(
                    MessageTextPool(
                        pool_id=f"text:{item.session_id}:{bucket_key}",
                        session_id=item.session_id,
                        route_key=bucket_key,
                    ),
                    bucket_key=bucket_key,
                ),
            )
        else:
            pool = cast(MessageTextPool, pool)
        text = ((item.raw_json or {}).get("content") or {}).get("text")
        text_value = text if isinstance(text, str) else ""
        await pool.accept(text_value)
        manager.merge_request_text(request_scope_id, text_value)
        return pool

    async def flush(
        self,
        manager: SessionStateManager,
        *,
        session_id: str,
        bucket_key: str,
        reason: str,
    ) -> FlushResult:
        pool = manager.get_pool_by_key(
            bucket_type=self.bucket_type,
            session_id=session_id,
            bucket_key=bucket_key,
        )
        if pool is None:
            return FlushResult(
                outbound_kind=self.outbound_kind, payload="", metadata={"reason": reason}
            )
        return await pool.flush(reason)
