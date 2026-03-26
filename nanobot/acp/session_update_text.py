"""ACP session_update 文本族处理器。"""

from __future__ import annotations

from typing import Any


async def _handle_text_chunk(dispatcher: Any, state: Any, update: Any) -> None:
    """处理文本分片：合并到 stream state，并把分片透传到 progress。"""
    # 中文注释：文本分片仍然先进入 merge_text 做去重/拼接，再触发 on_progress，保持历史行为。
    text = update.content.text
    state.merge_text(text)
    await dispatcher._emit_progress(state.on_progress, text)
