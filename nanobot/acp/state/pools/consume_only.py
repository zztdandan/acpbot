"""一次性消费池实现：用于只记录事实、不镜像输出的更新类型。"""

from __future__ import annotations

from nanobot.acp.state.models import ACPBucketType, FlushResult
from nanobot.acp.state.pools.base import ACPPoolBase


class ConsumeOnlyPool(ACPPoolBase):
    """消费池：吞掉输入并在需要时立刻进入终态，适合静态事实类更新。"""

    bucket_type = ACPBucketType.CONSUME_ONLY

    def __init__(self, *, bucket_key: str, destroy_after_accept: bool = False) -> None:
        """建立消费池；可按需启用“接收后立即销毁”的一次性模式。"""

        super().__init__(bucket_key=bucket_key)
        self.destroy_after_accept = destroy_after_accept
        self.accept_count = 0
        self.last_payload: object | None = None

    def _accept(self, payload: object) -> None:
        """记录最后一次输入；仅做事实保留，不生成进度片段。"""

        self.accept_count += 1
        self.last_payload = payload
        if self.destroy_after_accept:
            self.mark_terminal()

    def flush(self) -> FlushResult | None:
        """消费池不会产生外部可见进度。"""

        return None


class OtherPool(ConsumeOnlyPool):
    """`other` 池：收口未识别更新，并在单次消费后立即转入销毁流程。"""

    bucket_type = ACPBucketType.OTHER

    def __init__(self, *, bucket_key: str) -> None:
        """建立 `other` 池；保证未知类型只短暂落池后即销毁。"""

        super().__init__(bucket_key=bucket_key, destroy_after_accept=True)
