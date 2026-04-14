"""文本类池实现：覆盖普通文本与 thought 文本两类流式拼接场景。"""

from __future__ import annotations

from nanobot.acp.contracts import JSONMap
from nanobot.acp.state.models import ACPBucketType, ACPOutboundKind, FlushResult
from nanobot.acp.state.pools.base import ACPPoolBase


class _TextStreamPool(ACPPoolBase):
    """文本流池基类：处理 chunk 去重、拼接与增量 flush。"""

    bucket_type: ACPBucketType

    def __init__(
        self,
        *,
        bucket_key: str,
        outbound_kind: ACPOutboundKind,
        metadata: JSONMap | None = None,
    ) -> None:
        """建立文本池并绑定输出语义；不同文本族只需切换 kind 和 metadata。"""

        super().__init__(bucket_key=bucket_key)
        self._outbound_kind = outbound_kind
        self._metadata = dict(metadata or {})
        self.text = ""
        self._last_flushed_text = ""

    def _accept(self, payload: object) -> None:
        """吸收新的文本块；兼容整段覆盖、尾部重复与纯增量三种常见流式形态。"""

        chunk = str(payload or "")
        if not chunk:
            return
        if not self.text:
            self.text = chunk
            return
        if chunk.startswith(self.text):
            self.text = chunk
            return
        if self.text.endswith(chunk):
            return
        self.text += chunk

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
    """普通文本池：聚合 agent message 文本并对外镜像为标准 text 片段。"""

    bucket_type = ACPBucketType.MESSAGE_TEXT

    def __init__(self, *, bucket_key: str) -> None:
        """建立普通文本池；适用于 agent_message_chunk/text。"""

        super().__init__(bucket_key=bucket_key, outbound_kind=ACPOutboundKind.TEXT)


class ThoughtPool(_TextStreamPool):
    """思考文本池：聚合 agent_thought_chunk，并通过 metadata 标记 thought 语义。"""

    bucket_type = ACPBucketType.THOUGHT

    def __init__(self, *, bucket_key: str) -> None:
        """建立 thought 池；供 thought handler 独立收口。"""

        super().__init__(
            bucket_key=bucket_key,
            outbound_kind=ACPOutboundKind.THOUGHT,
            metadata={"thought": True},
        )
