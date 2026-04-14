"""thought handler：收口 agent_thought_chunk 到独立 thought 池。"""

from __future__ import annotations

from typing import cast

from acp.schema import AgentThoughtChunk, TextContentBlock

from nanobot.acp.state.handlers.base import HandlerConsumeResult, StateUpdateHandler
from nanobot.acp.state.models import ACPBucketType, ACPUpdateType
from nanobot.acp.state.pools import ThoughtPool


class AgentThoughtHandler(StateUpdateHandler):
    """agent thought 更新处理器：聚合思考文本并输出带 thought 标记的 progress。"""

    name = "agent_thought"
    update_type = ACPUpdateType.AGENT_THOUGHT
    bucket_type = ACPBucketType.THOUGHT

    def match(self, update: object) -> bool:
        """只匹配 thought 文本块。"""

        return isinstance(update, AgentThoughtChunk) and isinstance(
            update.content, TextContentBlock
        )

    def create_pool(self, *, bucket_key: str) -> ThoughtPool:
        """创建 thought 池；与普通文本池分开，避免语义混淆。"""

        return ThoughtPool(bucket_key=bucket_key)

    def build_bucket_key(self, update: object) -> str:
        """使用固定 thought 键；同一请求的思考流统一聚合。"""

        del update
        return "agent_thought"

    def consume(self, *, state_manager, update: object, pool) -> HandlerConsumeResult:
        """吸收 thought 文本，并把摘要写入 request final_metadata。"""

        typed_update = cast(AgentThoughtChunk, update)
        typed_pool = cast(ThoughtPool, pool)
        text = str(getattr(typed_update.content, "text", "") or "")
        typed_pool.accept(text)
        state_manager.update_named_metadata("latest_thought", typed_pool.text)
        return HandlerConsumeResult(flush_results=[typed_pool.flush()])
