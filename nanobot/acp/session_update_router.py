"""ACP session_update 路由器。

仅负责按 update 类型分发，不承载具体业务逻辑。
"""

from __future__ import annotations

from typing import Any

from nanobot.acp.session_update_media import _handle_media_chunk
from nanobot.acp.session_update_text import _handle_text_chunk
from nanobot.acp.session_update_tool import _handle_tool_progress, _handle_tool_start


async def _route_session_update(dispatcher: Any, session_id: str, state: Any, update: Any) -> None:
    """按 ACP update 类型进行分流。"""
    from acp.schema import (
        AgentMessageChunk,
        CurrentModeUpdate,
        EmbeddedResourceContentBlock,
        ImageContentBlock,
        ResourceContentBlock,
        TextContentBlock,
        ToolCallProgress,
        ToolCallStart,
    )

    if isinstance(update, AgentMessageChunk) and isinstance(update.content, TextContentBlock):
        await _handle_text_chunk(dispatcher, state, update)
        return

    if isinstance(update, AgentMessageChunk) and isinstance(
        update.content,
        (ImageContentBlock, ResourceContentBlock, EmbeddedResourceContentBlock),
    ):
        # 中文注释：这里保留 EmbeddedResourceContentBlock 分支，兼容 ACP 不同版本的资源块类型。
        _handle_media_chunk(dispatcher, session_id, state, update.content)
        return

    if isinstance(update, ToolCallStart):
        await _handle_tool_start(dispatcher, session_id, state, update)
        return

    if isinstance(update, ToolCallProgress):
        await _handle_tool_progress(dispatcher, session_id, state, update)
        return

    if isinstance(update, CurrentModeUpdate):
        caps = dispatcher._session_caps.get(session_id)
        if caps is not None:
            caps.current_agent = update.current_mode_id
        return
