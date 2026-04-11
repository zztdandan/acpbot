from __future__ import annotations

import importlib.util
from pathlib import Path

from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.models import (
    ACPBucketType,
    ACPOutboundKind,
    ACPPool,
    ACPUpdateType,
    FlushResult,
    RequestScopeState,
)
from nanobot.acp.state.pools import (
    ConsumeOnlyPool,
    MediaPool,
    MessageTextPool,
    PermissionPool,
    PlanPool,
    ThoughtPool,
    ToolPool,
)


def _load_legacy_state_module() -> object:
    # Keep existing imports working while Task 1 introduces the package form.
    legacy_path = Path(__file__).resolve().parent.parent / "state.py"
    spec = importlib.util.spec_from_file_location("nanobot.acp._legacy_state", legacy_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load legacy ACP state module from {legacy_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_legacy_state = _load_legacy_state_module()

_ACPDispatchError = _legacy_state._ACPDispatchError
_SessionCapabilities = _legacy_state._SessionCapabilities
_StreamState = _legacy_state._StreamState

__all__ = [
    "ACPBucketType",
    "ACPOutboundKind",
    "ACPPool",
    "ACPUpdateType",
    "ConsumeOnlyPool",
    "FlushResult",
    "MediaPool",
    "MessageTextPool",
    "PermissionPool",
    "PlanPool",
    "RequestScopeState",
    "SessionStateManager",
    "ThoughtPool",
    "ToolPool",
    "_ACPDispatchError",
    "_SessionCapabilities",
    "_StreamState",
]
