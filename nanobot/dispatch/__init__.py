"""dispatch 包公开入口。

这里同时暴露 native 与 ACP dispatcher，
其中 ACP 真实实现位于 nanobot.acp。
"""

from nanobot.acp.dispatcher import ACPDispatcher
from nanobot.dispatch.native import NativeDispatcher

# 统一导出运行时可选 dispatcher 类型。
__all__ = ["ACPDispatcher", "NativeDispatcher"]
