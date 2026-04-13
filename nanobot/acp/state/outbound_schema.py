"""State-owned outbound schema builders.

These helpers replace the old root-level outbound_content_schema module so that
progress/final payload construction stays within the state owner boundary.
"""

from __future__ import annotations

from nanobot.acp.state.models import ACPOutboundKind, FlushResult


def build_progress_payload(result: FlushResult) -> tuple[str, dict[str, object], list[str]]:
    """Convert a FlushResult into `(content, metadata, media)` for progress sinks."""

    # 中文注释：progress 出包 schema 已经内迁到 state 内部，
    # 这保证“结构化 progress 如何对外表达”也由 state owner 统一定义。
    metadata = dict(result.metadata)
    if result.kind == ACPOutboundKind.TOOL:
        metadata.setdefault("_tool_hint", True)
    return result.content, metadata, list(result.media)
