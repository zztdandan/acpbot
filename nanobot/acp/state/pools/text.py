"""文本池实现：覆盖普通文本与思考文本两类流式拼接场景。"""

from __future__ import annotations

from nanobot.acp.contracts import JSONMap
from nanobot.acp.state.models import ACPBucketType, ACPOutboundKind, FlushResult
from nanobot.acp.state.pools.base import ACPPoolBase


class _TextStreamPool(ACPPoolBase):
    """文本流池基类：处理文本块去重、拼接与增量刷新。

    职责：
        - 兼容 ACP 文本流常见的整段覆盖、尾部重复与纯增量三种形态
        - 维护最近一次已刷新快照，避免重复镜像相同文本
    """

    bucket_type: ACPBucketType

    def __init__(
        self,
        *,
        bucket_key: str,
        outbound_kind: ACPOutboundKind,
        metadata: JSONMap | None = None,
    ) -> None:
        """建立文本池并绑定输出语义；不同文本族只需切换片段类型与元数据。"""

        super().__init__(bucket_key=bucket_key)
        self._outbound_kind = outbound_kind
        self._metadata = dict(metadata or {})
        self.text = ""
        self._last_flushed_text = ""

    def _accept(self, payload: object) -> bool:
        """吸收新的文本块；兼容整段覆盖、尾部重复与纯增量三种常见流式形态。"""

        chunk = str(payload or "")
        if not chunk:
            return False
        if not self.text:
            self.text = chunk
            return True
        if chunk.startswith(self.text):
            if chunk == self.text:
                return False
            self.text = chunk
            return True
        if self.text.endswith(chunk):
            return False
        self.text += chunk
        return True

    def flush(self) -> FlushResult | None:
        """返回尚未镜像的文本快照；无新增时保持静默。"""

        if not self.text or self.text == self._last_flushed_text:
            return None
        self._last_flushed_text = self.text
        return FlushResult(
            kind=self._outbound_kind,
            content=self.text,
            metadata=dict(self._metadata),
        )


class MessageTextPool(_TextStreamPool):
    """普通文本池：聚合消息正文文本并对外镜像为标准文本片段。"""

    bucket_type = ACPBucketType.MESSAGE_TEXT
    idle_timeout_seconds: float | None = 0.5

    def __init__(self, *, bucket_key: str) -> None:
        """建立普通文本池；适用于 `agent_message_chunk/text`。"""

        super().__init__(bucket_key=bucket_key, outbound_kind=ACPOutboundKind.TEXT)


class ThoughtPool(_TextStreamPool):
    """思考文本池：聚合思考文本，并通过元数据标记思考语义。"""

    bucket_type = ACPBucketType.THOUGHT
    idle_timeout_seconds: float | None = 0.8

    def __init__(self, *, bucket_key: str) -> None:
        """建立思考池；供思考处理器独立收口。"""

        super().__init__(
            bucket_key=bucket_key,
            outbound_kind=ACPOutboundKind.THOUGHT,
            metadata={"thought": True},
        )
