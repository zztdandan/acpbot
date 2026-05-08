"""计划池实现：聚合计划更新并生成可读的计划摘要文本。"""

from __future__ import annotations

from nanobot.acp.state.models import ACPBucketType, ACPOutboundKind, FlushResult
from nanobot.acp.state.pools.base import ACPPoolBase


class PlanPool(ACPPoolBase):
    """计划池：保存当前计划的完整快照，并在变更时输出摘要。"""

    bucket_type = ACPBucketType.PLAN
    idle_timeout_seconds: float | None = 1.6

    def __init__(self, *, bucket_key: str) -> None:
        """建立计划池；一个请求内通常只需一个当前计划快照。"""

        super().__init__(bucket_key=bucket_key)
        self.summary = ""
        self._last_flushed_summary = ""

    def _accept(self, payload: object) -> bool:
        """接收计划摘要文本；最新摘要会覆盖旧内容。"""

        summary = str(payload or "").strip()
        if not summary:
            return False
        if summary == self.summary:
            return False
        self.summary = summary
        return True

    def flush(self) -> FlushResult | None:
        """在计划摘要变化时输出一条计划进度镜像。"""

        if not self.summary or self.summary == self._last_flushed_summary:
            return None
        self._last_flushed_summary = self.summary
        return FlushResult(
            kind=ACPOutboundKind.PLAN,
            content=self.summary,
            metadata={"plan": True},
        )
