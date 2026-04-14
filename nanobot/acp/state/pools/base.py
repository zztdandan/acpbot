"""池抽象基类：统一封装 accept/close/terminal 约束，避免各池重复实现生命周期细节。"""

from __future__ import annotations

from abc import ABC, abstractmethod

from nanobot.acp.state.models import ACPBucketType, FlushResult


class ACPPoolBase(ABC):
    """单请求池基类：为 state 内所有池提供统一生命周期与终态控制。"""

    bucket_type: ACPBucketType

    def __init__(self, *, bucket_key: str) -> None:
        """初始化池实例并记录索引键；同一请求内由 SessionStateManager 统一持有。"""

        self.bucket_key = bucket_key
        self._closed = False
        self._terminal = False

    def accept(self, payload: object) -> None:
        """接收一条输入；state close 后的池不再接受写入。"""

        if self._closed:
            return
        self._accept(payload)

    @abstractmethod
    def _accept(self, payload: object) -> None:
        """处理单条输入并更新池内事实；子类只关注自身语义。"""

    @abstractmethod
    def flush(self) -> FlushResult | None:
        """提取当前可镜像片段；返回 None 表示本次没有外部可见增量。"""

    def close(self) -> FlushResult | None:
        """关闭池并输出尾部增量；供 request close 链路做统一收尾。"""

        if self._closed:
            return None
        self._closed = True
        return self.flush()

    def is_terminal(self) -> bool:
        """返回池是否进入终态；终态池会被 manager 从索引中销毁。"""

        return self._terminal or self._closed

    def mark_terminal(self) -> None:
        """把池标记为终态；适用于 consume-only / other 这类一次性池。"""

        self._terminal = True
