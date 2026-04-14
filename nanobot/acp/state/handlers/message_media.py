"""消息媒体处理器：提取媒体路径并写入媒体池与请求级聚合结果。"""

from __future__ import annotations

from typing import cast

from acp.schema import (
    AgentMessageChunk,
    EmbeddedResourceContentBlock,
    ImageContentBlock,
    ResourceContentBlock,
)

from nanobot.acp.state.handlers.base import HandlerConsumeResult, StateUpdateHandler
from nanobot.acp.state.models import ACPBucketType, ACPUpdateType
from nanobot.acp.state.pools import MediaPool


class AgentMessageMediaHandler(StateUpdateHandler):
    """消息媒体更新处理器：收口媒体内容块并同步最终媒体列表。"""

    name = "agent_message_media"
    update_type = ACPUpdateType.AGENT_MESSAGE_MEDIA
    bucket_type = ACPBucketType.MEDIA

    def match(self, update: object) -> bool:
        """匹配媒体类 `AgentMessageChunk`。"""

        return isinstance(update, AgentMessageChunk) and isinstance(
            update.content,
            (ImageContentBlock, ResourceContentBlock, EmbeddedResourceContentBlock),
        )

    def create_pool(self, *, bucket_key: str) -> MediaPool:
        """创建媒体池；一个请求内的消息媒体默认共用一条媒体流。"""

        return MediaPool(bucket_key=bucket_key)

    def build_bucket_key(self, update: object) -> str:
        """使用固定媒体流键；便于统一管理请求级媒体输出。"""

        del update
        return "agent_message_media"

    def consume(self, *, state_manager, update: object, pool) -> HandlerConsumeResult:
        """提取媒体路径并写入池与最终媒体列表；缺少有效路径时保持静默。"""

        typed_update = cast(AgentMessageChunk, update)
        media_path = state_manager.extract_media_path(typed_update.content)
        if not media_path:
            return HandlerConsumeResult()
        pool.accept(media_path)
        state_manager.append_media_path(media_path, source="message")
        return HandlerConsumeResult(flush_results=[pool.flush()])
