"""进度出包辅助：把池刷新结果转换为 progress outbound 与 on_progress 镜像载荷。"""

from __future__ import annotations

from nanobot.acp.contracts import JSONMap
from nanobot.acp.state.models import ACPOutboundKind, FlushResult
from nanobot.bus.events import OutboundMessage


def build_progress_payload(result: FlushResult) -> tuple[str, JSONMap, list[str]]:
    """把 `FlushResult` 转成统一镜像载荷。"""

    metadata = dict(result.metadata)
    if result.kind == ACPOutboundKind.TOOL:
        metadata.setdefault("_tool_hint", True)
    return result.content, metadata, list(result.media)


def build_progress_outbound(*, channel: str, chat_id: str, result: FlushResult) -> OutboundMessage:
    """把一次 flush 结果编制成 progress outbound；bus 发布与 on_progress 镜像共享同一事实源。"""

    content, metadata, media = build_progress_payload(result)
    metadata.setdefault("_acp_progress", True)
    return OutboundMessage(
        channel=channel,
        chat_id=chat_id,
        content=content,
        media=media,
        metadata=metadata,
    )
