from __future__ import annotations

from typing import Any, cast

from acp.schema import ToolCallProgress

from nanobot.acp.contracts import JSONMap
from nanobot.acp.state.handlers.tool import ToolUpdateHandler
from nanobot.acp.state.pools.tool import ToolPool


def test_tool_handler_truncates_nested_strings_to_10kb() -> None:
    """handler 入池前必须裁剪任意深度字符串，且保留结构不变。"""

    oversized = "x" * (11 * 1024)
    update = ToolCallProgress.model_validate(
        {
            "sessionUpdate": "tool_call_update",
            "toolCallId": "tool-large-1",
            "title": "Process artifact",
            "status": "in_progress",
            "content": [
                {
                    "type": "content",
                    "content": {
                        "type": "text",
                        "text": oversized,
                    },
                }
            ],
            "rawInput": {"nested": {"deep": oversized}},
            "rawOutput": {"items": [{"text": oversized}]},
        }
    )

    handler = ToolUpdateHandler()
    pool = ToolPool(bucket_key="tool:tool-large-1")
    consume_result = handler.consume(state_manager=cast(Any, None), update=update, pool=pool)
    flush_result = pool.flush()

    assert consume_result.immediate_finalize is False
    assert flush_result is not None
    snapshot = cast(JSONMap, flush_result.metadata["tool_snapshot"])

    content = cast(list[object], snapshot["content"])
    first_content = cast(JSONMap, content[0])
    content_block = cast(JSONMap, first_content["content"])
    text_value = cast(str, content_block["text"])
    deep_input = cast(str, cast(JSONMap, cast(JSONMap, snapshot["rawInput"])["nested"])["deep"])
    deep_output = cast(
        str,
        cast(JSONMap, cast(list[object], cast(JSONMap, snapshot["rawOutput"])["items"])[0])["text"],
    )

    assert len(text_value.encode("utf-8")) <= 10 * 1024
    assert len(deep_input.encode("utf-8")) <= 10 * 1024
    assert len(deep_output.encode("utf-8")) <= 10 * 1024
    assert text_value.endswith("...(truncated)")
    assert deep_input.endswith("...(truncated)")
    assert deep_output.endswith("...(truncated)")


def test_tool_handler_completed_returns_immediate_finalize() -> None:
    """completed/failed 在 accept 后应立即允许路由层销毁池。"""

    update = ToolCallProgress.model_validate(
        {
            "sessionUpdate": "tool_call_update",
            "toolCallId": "tool-completed-1",
            "title": "Finalize report",
            "status": "completed",
        }
    )

    handler = ToolUpdateHandler()
    pool = ToolPool(bucket_key="tool:tool-completed-1")
    consume_result = handler.consume(state_manager=cast(Any, None), update=update, pool=pool)

    assert consume_result.immediate_finalize is True
