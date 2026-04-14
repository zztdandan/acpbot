"""消息媒体处理器：把三类 ACP 媒体块统一落成 resource-link 风格的本地资源。"""

from __future__ import annotations

from typing import cast

from acp.schema import (
    AgentMessageChunk,
    EmbeddedResourceContentBlock,
    ImageContentBlock,
    ResourceContentBlock,
)

from nanobot.acp.state.handlers.base import HandlerConsumeResult, StateUpdateHandler
from nanobot.acp.state.handlers.media_resource import MediaResourceResolver
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
        """把消息媒体块归一化为本地 resource-link，并写入池与请求聚合结果。"""

        typed_update = cast(AgentMessageChunk, update)
        resource = self._media_resolver_for(state_manager).resolve(typed_update.content)
        if resource is None:
            # 解析失败时立即结束当前媒体池，避免创建后无 deadline 的空池残留。
            return HandlerConsumeResult(immediate_finalize=True)
        pool.accept(resource)
        state_manager.append_media_path(resource.local_path)
        return HandlerConsumeResult()

    def _media_resolver_for(self, state_manager) -> MediaResourceResolver:
        """为当前请求构造媒体解析器；落地目录由 handler 自己归属，不再挂在 manager。"""

        return MediaResourceResolver(landing_root=state_manager.media_landing_root("message"))
