"""ACP session_update 媒体族处理器。"""

from __future__ import annotations

from typing import Any


def _handle_media_chunk(dispatcher: Any, session_id: str, state: Any, content: Any) -> None:
    """处理非文本内容块：统一抽取并落盘为 filepath。"""
    # 中文注释：媒体抽取逻辑集中在 _extract_agent_media_path，handler 只负责流程编排。
    media_path = dispatcher._extract_agent_media_path(session_id, content)
    if media_path:
        state.add_media(media_path)
