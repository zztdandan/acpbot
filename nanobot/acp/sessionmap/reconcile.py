"""Reconciliation helpers for ACP binding truth and remote session listings."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, cast


def extract_acp_side_session_ids(payload: Any) -> set[str]:
    """Best-effort extraction of ACP-side session ids from list_sessions payloads."""

    ids: set[str] = set()

    def _walk(value: Any) -> None:
        if value is None:
            return
        if hasattr(value, "model_dump"):
            try:
                _walk(value.model_dump(by_alias=True, exclude_none=True))
                return
            except Exception:
                pass
        if isinstance(value, dict):
            for key in ("session_id", "sessionId", "id"):
                session_id = value.get(key)
                if isinstance(session_id, str) and session_id:
                    ids.add(session_id)
            for nested in value.values():
                _walk(nested)
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                _walk(item)
            return
        for key in ("session_id", "sessionId", "id"):
            session_id = getattr(value, key, None)
            if isinstance(session_id, str) and session_id:
                ids.add(session_id)
        for attr in ("sessions", "data", "items"):
            nested = getattr(value, attr, None)
            if nested is not None:
                _walk(nested)

    _walk(payload)
    return ids


async def fetch_acp_side_session_ids(conn: Any, *, cwd: str) -> tuple[set[str], bool]:
    """Fetch ACP session ids for reconciliation if the backend supports it."""

    list_sessions = cast(Callable[..., Awaitable[Any]] | None, getattr(conn, "list_sessions", None))
    if list_sessions is None:
        return set(), False

    async def _collect(cwd_arg: str | None) -> set[str]:
        collected: set[str] = set()
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            attempts: list[dict[str, str]] = []
            if cwd_arg is not None and cursor is not None:
                attempts.append({"cwd": cwd_arg, "cursor": cursor})
            if cwd_arg is not None:
                attempts.append({"cwd": cwd_arg})
            if cursor is not None:
                attempts.append({"cursor": cursor})
            attempts.append({})

            response: Any | None = None
            last_exc: Exception | None = None
            for kwargs in attempts:
                try:
                    response = await list_sessions(**kwargs)
                    break
                except TypeError as exc:
                    last_exc = exc
            if response is None:
                if last_exc is not None:
                    raise last_exc
                response = await list_sessions()
            collected.update(extract_acp_side_session_ids(response))

            next_cursor = getattr(response, "next_cursor", None)
            if next_cursor is None and isinstance(response, dict):
                next_cursor = response.get("nextCursor") or response.get("next_cursor")
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return collected

    merged: set[str] = set()
    any_success = False
    try:
        merged.update(await _collect(cwd))
        any_success = True
    except Exception:
        pass
    if not merged:
        try:
            merged.update(await _collect(None))
            any_success = True
        except Exception:
            pass
    return merged, any_success
