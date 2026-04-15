from __future__ import annotations

import os

import pytest


def _is_falsey(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"0", "false", "no", "off"}


def require_e2e_enabled() -> None:
    """Guardrail for expensive E2E runs.

    E2E tests are enabled by default in this repository. Set
    ``NANOBOT_ACP_E2E_ENABLED=0`` to explicitly skip them.
    """

    if _is_falsey(os.getenv("NANOBOT_ACP_E2E_ENABLED")):
        pytest.skip("ACP E2E disabled by NANOBOT_ACP_E2E_ENABLED=0")
