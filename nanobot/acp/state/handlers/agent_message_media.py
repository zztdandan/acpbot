from __future__ import annotations

from typing import Any
from typing import cast

from nanobot.acp.progress_event_types import ACPProgressEvent
from nanobot.acp.state import ACPBucketType, ACPOutboundKind, ACPUpdateType, FlushResult
from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.pools import MediaPool


class AgentMessageMediaHandler:
    name = "agent_message_media"
    supported_update_types = frozenset({ACPUpdateType.AGENT_MESSAGE_CHUNK})
    bucket_type = ACPBucketType.MEDIA
    outbound_kind = ACPOutboundKind.MEDIA
    auto_dispatch = True

    def match(self, event: ACPProgressEvent) -> bool:
        content_type = str(event.extracted.get("content_type") or "").strip().lower()
        return content_type in {"image", "resource", "resource_link", "embedded_resource"}

    def build_bucket_key(self, event: ACPProgressEvent) -> str | None:
        return event.route_key or event.session_id

    async def consume(self, manager: SessionStateManager, item: Any) -> None:
        del manager, item

    async def enqueue(self, manager: SessionStateManager, item: ACPProgressEvent) -> MediaPool:
        request_scope_id = str(item.ext.get("request_scope_id") or item.session_id)
        bucket_key = self.build_bucket_key(item)
        if bucket_key is None:
            raise ValueError("Media handler requires a bucket key")
        pool = manager.get_pool_by_key(
            bucket_type=self.bucket_type,
            session_id=item.session_id,
            bucket_key=bucket_key,
        )
        if pool is None:
            pool = cast(
                MediaPool,
                manager.register_pool(
                    MediaPool(
                        pool_id=f"media:{item.session_id}:{bucket_key}",
                        session_id=item.session_id,
                        route_key=bucket_key,
                    ),
                    bucket_key=bucket_key,
                ),
            )
        else:
            pool = cast(MediaPool, pool)
        await pool.accept(item.raw_json or {})
        media_path = item.ext.get("media_path")
        if isinstance(media_path, str) and media_path:
            manager.add_request_media(request_scope_id, media_path)
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
                outbound_kind=self.outbound_kind, payload={}, metadata={"reason": reason}
            )
        return await pool.flush(reason)
