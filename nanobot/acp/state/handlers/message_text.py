"""消息文本处理器：把普通文本流写入文本池，并把 final 提交延后到 flush。"""

from __future__ import annotations

from typing import cast

from acp.schema import AgentMessageChunk, TextContentBlock

from nanobot.acp.state.handlers.base import HandlerConsumeResult, StateUpdateHandler
from nanobot.acp.state.models import ACPBucketType, ACPUpdateType
from nanobot.acp.state.pools import MessageTextPool


class AgentMessageTextHandler(StateUpdateHandler):
    """消息文本更新处理器：收口 `agent_message_chunk/text` 到消息文本池。"""

    name = "agent_message_text"
    update_type = ACPUpdateType.AGENT_MESSAGE_TEXT
    bucket_type = ACPBucketType.MESSAGE_TEXT

    def match(self, update: object) -> bool:
        """只匹配文本内容块的 `AgentMessageChunk`。"""

        return isinstance(update, AgentMessageChunk) and isinstance(
            update.content, TextContentBlock
        )

    def create_pool(self, *, bucket_key: str) -> MessageTextPool:
        """创建普通文本池；同一请求内按固定文本流键复用。"""

        return MessageTextPool(bucket_key=bucket_key)

    def build_bucket_key(self, update: object) -> str:
        """使用固定文本流键；保持最终文本聚合语义稳定。"""

        del update
        return "agent_message_text"

    def consume(self, *, state_manager, update: object, pool) -> HandlerConsumeResult:
        """吸收文本块并刷新 partial 快照；最终文本只在 flush 时提交。"""

        typed_update = cast(AgentMessageChunk, update)
        typed_pool = cast(MessageTextPool, pool)
        text = str(getattr(typed_update.content, "text", "") or "")
        typed_pool.accept(text)
        state_manager.update_partial_text(typed_pool.text)
        return HandlerConsumeResult()

    def build_progress_outbound(self, *, state_manager, flush_result):
        """在文本池 flush 时统一提交 final 文本，再走标准 progress outbound 编制。"""

        state_manager.commit_final_text(flush_result.content)
        return super().build_progress_outbound(
            state_manager=state_manager,
            flush_result=flush_result,
        )
