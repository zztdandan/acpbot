"""ACP existing-session restore helper.

This module keeps a single restore rule for nanobot/acp session recovery:
always try `resume_session` first, then fallback to `load_session`.
"""

from __future__ import annotations

from typing import Awaitable, Callable, cast

from nanobot.acp.contracts import ACPSessionPayload


async def restore_existing_session(
    conn: object,
    *,
    cwd: str,
    session_id: str,
) -> ACPSessionPayload | None:
    """Restore an existing ACP session with the fixed resume-then-load rule.

    Compatibility notes:
        - Some ACP backends expose `resume_session`
        - Some ACP backends expose `load_session`
        - When both exist, nanobot/acp always prefers `resume_session`

    Behavior:
        1. Try `resume_session` first because it is the lighter restore path
        2. If `resume_session` is missing or raises, fallback to `load_session`
        4. If both methods are missing, return None
        5. If both methods fail, re-raise the last error so the caller can log
           one failure outcome for the whole restore attempt
    """

    resume_session = cast(
        Callable[..., Awaitable[ACPSessionPayload]] | None,
        getattr(conn, "resume_session", None),
    )
    load_session = cast(
        Callable[..., Awaitable[ACPSessionPayload]] | None,
        getattr(conn, "load_session", None),
    )
    if resume_session is None and load_session is None:
        return None

    ordered_calls: list[Callable[..., Awaitable[ACPSessionPayload]] | None] = [
        resume_session,
        load_session,
    ]

    last_exc: Exception | None = None
    for method in ordered_calls:
        if method is None:
            continue
        try:
            return await method(cwd=cwd, session_id=session_id)
        except Exception as exc:
            last_exc = exc

    if last_exc is not None:
        raise last_exc
    return None
