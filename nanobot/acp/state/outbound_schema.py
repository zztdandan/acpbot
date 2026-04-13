"""单请求状态聚合与进度发布层。"""

from __future__ import annotations

from nanobot.acp.contracts import JSONMap
from nanobot.acp.state.models import ACPOutboundKind, FlushResult


def build_progress_payload(result: FlushResult) -> tuple[str, JSONMap, list[str]]:
    """执行该方法定义的处理流程并返回结果。"""

    metadata = dict(result.metadata)
    if result.kind == ACPOutboundKind.TOOL:
        metadata.setdefault("_tool_hint", True)
    return result.content, metadata, list(result.media)
