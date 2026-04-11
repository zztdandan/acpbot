from __future__ import annotations

from dataclasses import replace

from nanobot.acp.state.models import ACPBucketType, ACPPool, RequestScopeState


class SessionStateManager:
    """Own pool storage, indexes, and request-scope aggregation."""

    def __init__(self) -> None:
        self._pools: dict[str, ACPPool] = {}
        self._pool_index: dict[tuple[ACPBucketType, str, str], str] = {}
        self._pool_keys_by_id: dict[str, set[tuple[ACPBucketType, str, str]]] = {}
        self._request_state: dict[str, RequestScopeState] = {}

    def register_pool(self, pool: ACPPool, *, bucket_key: str) -> ACPPool:
        key = (pool.pool_type, pool.session_id, bucket_key)
        self._pools[pool.pool_id] = pool
        self._pool_index[key] = pool.pool_id
        self._pool_keys_by_id.setdefault(pool.pool_id, set()).add(key)
        return pool

    def get_pool(self, pool_id: str) -> ACPPool | None:
        return self._pools.get(pool_id)

    def get_pool_by_key(
        self,
        *,
        bucket_type: ACPBucketType,
        session_id: str,
        bucket_key: str,
    ) -> ACPPool | None:
        pool_id = self._pool_index.get((bucket_type, session_id, bucket_key))
        if pool_id is None:
            return None
        return self._pools.get(pool_id)

    def iter_pools_for_session(
        self,
        *,
        session_id: str,
        bucket_type: ACPBucketType | None = None,
    ) -> list[ACPPool]:
        pools: list[ACPPool] = []
        for (
            indexed_bucket_type,
            indexed_session_id,
            _bucket_key,
        ), pool_id in self._pool_index.items():
            if indexed_session_id != session_id:
                continue
            if bucket_type is not None and indexed_bucket_type != bucket_type:
                continue
            pool = self._pools.get(pool_id)
            if pool is not None:
                pools.append(pool)
        return pools

    async def destroy_pool(self, pool_id: str) -> None:
        pool = self._pools.pop(pool_id, None)
        if pool is None:
            return
        await pool.close()
        for key in self._pool_keys_by_id.pop(pool_id, set()):
            self._pool_index.pop(key, None)

    def get_request_state(self, request_id: str) -> RequestScopeState | None:
        state = self._request_state.get(request_id)
        if state is None:
            return None
        return replace(state)

    def merge_request_text(self, request_id: str, chunk: str) -> None:
        if not chunk:
            return
        state = self._request_state.setdefault(request_id, RequestScopeState())
        state.final_text += chunk

    def add_request_media(self, request_id: str, media_path: str) -> None:
        if not media_path:
            return
        state = self._request_state.setdefault(request_id, RequestScopeState())
        if media_path not in state.media_paths:
            state.media_paths.append(media_path)

    def finalize_request_scope(self, request_id: str) -> RequestScopeState:
        state = self._request_state.pop(request_id, RequestScopeState())
        return replace(state)
