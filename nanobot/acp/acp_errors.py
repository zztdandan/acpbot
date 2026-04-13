"""ACP error classification helpers."""

from __future__ import annotations


def _is_invalid_params_request_error(exc: Exception) -> bool:
    """Recognize ACP JSON-RPC invalid-params across SDK exception variants."""

    # python-sdk commonly surfaces RequestError with code=-32602. Avoid coupling to a
    # concrete exception class so this helper stays resilient to SDK internals.
    code = getattr(exc, "code", None)
    if code == -32602:
        return True
    return str(exc).strip().lower() == "invalid params"
