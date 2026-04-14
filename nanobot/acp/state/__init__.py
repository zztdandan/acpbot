"""state 包导出：暴露请求级状态管理器、路由器与基础模型。"""

from __future__ import annotations

from nanobot.acp.sessionmap.internal.session_caps import _SessionCapabilities
from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.models import (
    ACPBucketType,
    ACPOutboundKind,
    ACPUpdateType,
    FlushResult,
    PoolKey,
    RequestScopeState,
)
from nanobot.acp.state.router import HandlerRegistry, ProgressRouter


class _ACPDispatchError(RuntimeError):
    """ACP dispatch 异常：在执行链路需要保留 partial fallback 时向上游传递信号。"""

    def __init__(self, partial_response: str = "") -> None:
        """记录 partial_response；供 ProcessRuntimeManager 在失败链路判断是否物化 partial final。"""
        super().__init__("ACP dispatch failed")
        self.partial_response = partial_response


__all__ = [
    "ACPBucketType",
    "ACPOutboundKind",
    "ACPUpdateType",
    "FlushResult",
    "HandlerRegistry",
    "PoolKey",
    "ProgressRouter",
    "RequestScopeState",
    "SessionStateManager",
    "_ACPDispatchError",
    "_SessionCapabilities",
]
