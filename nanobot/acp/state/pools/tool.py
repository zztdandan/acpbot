"""工具池实现：按 tool_call_id 粒度隔离工具进度，避免多工具共用单池。"""

from __future__ import annotations

from nanobot.acp.state.models import ACPBucketType, ACPOutboundKind, FlushResult
from nanobot.acp.state.pools.base import ACPPoolBase


class ToolPool(ACPPoolBase):
    """工具池：记录单个工具调用的最新可见状态，并镜像为 tool progress。"""

    bucket_type = ACPBucketType.TOOL

    def __init__(self, *, bucket_key: str) -> None:
        """建立工具池；每个 `tool:<tool_call_id>` 独立一份实例。"""

        super().__init__(bucket_key=bucket_key)
        self.latest_message = ""
        self._dirty = False

    def _accept(self, payload: object) -> None:
        """更新工具最新状态文本；空字符串不会触发新输出。"""

        message = str(payload or "").strip()
        if not message:
            return
        self.latest_message = message
        self._dirty = True

    def flush(self) -> FlushResult | None:
        """在工具状态有变化时输出一条 tool 进度镜像。"""

        if not self._dirty or not self.latest_message:
            return None
        self._dirty = False
        return FlushResult(
            kind=ACPOutboundKind.TOOL,
            content=self.latest_message,
            metadata={"tool_hint": True, "_tool_hint": True},
        )
