"""ACP 运行时模块导出入口。"""

from nanobot.acp.dispatcher import ACPDispatcher

# 仅对外暴露 ACPDispatcher，内部实现细节不导出。
__all__ = ["ACPDispatcher"]
