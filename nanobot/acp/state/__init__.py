"""单请求状态聚合与进度发布层。"""

from __future__ import annotations

from nanobot.acp.sessionmap.internal.session_caps import _SessionCapabilities
from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.models import (
    ACPBucketType,
    ACPOutboundKind,
    ACPUpdateType,
    FlushResult,
    RequestScopeState,
)
from nanobot.acp.state.router import ProgressRouter


class _ACPDispatchError(RuntimeError):
    """负责本对象定义的职责边界与生命周期。"""

    def __init__(self, partial_response: str = "") -> None:
        """初始化当前对象并建立必要状态。"""
        super().__init__("ACP dispatch failed")
        self.partial_response = partial_response


__all__ = [
    "ACPBucketType",
    "ACPOutboundKind",
    "ACPUpdateType",
    "FlushResult",
    "ProgressRouter",
    "RequestScopeState",
    "SessionStateManager",
    "_ACPDispatchError",
    "_SessionCapabilities",
]
