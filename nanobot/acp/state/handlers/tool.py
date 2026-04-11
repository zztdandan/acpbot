from __future__ import annotations

from typing import Any, cast

from nanobot.acp.progress_event_types import ACPProgressEvent
from nanobot.acp.state import ACPBucketType, ACPOutboundKind, ACPUpdateType, FlushResult
from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.pools import ToolPool

HISTORY_LIMIT = 100


def append_history(history: list[dict[str, Any]], item: dict[str, Any]) -> int:
    history.append(item)
    dropped = 0
    while len(history) > HISTORY_LIMIT:
        history.pop(0)
        dropped += 1
    return dropped


def build_tool_payload(
    *, route_key: str, latest: dict[str, Any], history: list[dict[str, Any]], history_dropped: int
) -> dict[str, Any]:
    payload = dict(latest)
    payload.setdefault("tool_call_id", latest.get("tool_call_id") or route_key)
    payload["history"] = list(history)
    payload["history_dropped"] = history_dropped
    return payload


class ToolHandler:
    name = "tool"
    supported_update_types = frozenset(
        {
            ACPUpdateType.TOOL_CALL_START,
            ACPUpdateType.TOOL_CALL_PROGRESS,
            ACPUpdateType.TOOL_CALL_UPDATE,
        }
    )
    bucket_type = ACPBucketType.TOOL
    outbound_kind = ACPOutboundKind.TOOL
    auto_dispatch = True

    def match(self, event: ACPProgressEvent) -> bool:
        return event.update_type in self.supported_update_types

    def build_bucket_key(self, event: ACPProgressEvent) -> str | None:
        return event.route_key or event.session_id

    async def consume(self, manager: SessionStateManager, item: Any) -> None:
        del manager, item

    async def enqueue(self, manager: SessionStateManager, item: ACPProgressEvent) -> ToolPool:
        request_scope_id = str(item.ext.get("request_scope_id") or item.session_id)
        bucket_key = self.build_bucket_key(item)
        if bucket_key is None:
            raise ValueError("Tool handler requires a bucket key")
        pool = manager.get_pool_by_key(
            bucket_type=self.bucket_type,
            session_id=item.session_id,
            bucket_key=bucket_key,
        )
        if pool is None:
            pool = cast(
                ToolPool,
                manager.register_pool(
                    ToolPool(
                        pool_id=f"tool:{item.session_id}:{bucket_key}",
                        session_id=item.session_id,
                        route_key=bucket_key,
                    ),
                    bucket_key=bucket_key,
                ),
            )
        else:
            pool = cast(ToolPool, pool)

        payload = dict(item.raw_json or {})
        payload["update_type"] = (
            item.update_type.value
            if isinstance(item.update_type, ACPUpdateType)
            else str(item.update_type)
        )
        payload["received_at_ms"] = item.received_at_ms
        payload["session_id"] = item.session_id
        dropped = append_history(pool.history, payload)
        pool.history_dropped += dropped
        pool.latest = payload

        media_path = item.ext.get("tool_output_media_path")
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
        pool = cast(ToolPool, pool)
        return await pool.flush(reason)
