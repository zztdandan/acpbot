"""状态包导出：集中暴露请求级状态归属对象与基础模型。"""

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
    """ACP 分发异常：在执行失败但仍需保留部分回退结果时向上游传递信号。"""

    def __init__(self, partial_response: str = "") -> None:
        """记录部分响应文本；供流程管理器决定是否物化部分结果。"""
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
