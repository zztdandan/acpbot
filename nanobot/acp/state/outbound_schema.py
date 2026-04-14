"""进度出包辅助：把池刷新结果转换为 `on_progress` 兼容载荷。"""

from __future__ import annotations

from nanobot.acp.contracts import JSONMap
from nanobot.acp.state.models import ACPOutboundKind, FlushResult


def build_progress_payload(result: FlushResult) -> tuple[str, JSONMap, list[str]]:
    """把 `FlushResult` 转成统一进度载荷；供请求级镜像链路复用。

    处理流程：
        - 复制刷新结果里的 `metadata`，避免下游回调反向污染池内事实
        - 遇到工具片段时补齐 `_tool_hint` 兼容键，保持旧 `on_progress` 行为不回退
        - 返回 `(content, metadata, media)` 三元组，供不同下游按需消费
    """

    metadata = dict(result.metadata)
    if result.kind == ACPOutboundKind.TOOL:
        metadata.setdefault("_tool_hint", True)
    return result.content, metadata, list(result.media)
