from __future__ import annotations

from typing import cast

from nanobot.acp.contracts import JSONMap
from nanobot.acp.state.pools.tool import ToolPool, ToolPoolPayload, ToolRuntimeStatus


def test_tool_pool_flushes_last_message_with_previous_history() -> None:
    """工具池 flush 时应输出最后一条消息，并把更早消息折叠进 `previous`。"""

    pool = ToolPool(bucket_key="tool:demo")
    pool.accept(
        ToolPoolPayload(
            session_update="tool_call",
            tool_call_id="demo",
            title="Read config",
            status=ToolRuntimeStatus.PENDING,
            raw_input={"path": "/tmp/config.json"},
        )
    )
    pool.accept(
        ToolPoolPayload(
            session_update="tool_call_update",
            tool_call_id="demo",
            title="Read config",
            status=ToolRuntimeStatus.IN_PROGRESS,
            raw_output={"lines": 10},
        )
    )
    pool.accept(
        ToolPoolPayload(
            session_update="tool_call_update",
            tool_call_id="demo",
            title="Read config",
            status=ToolRuntimeStatus.COMPLETED,
            raw_output={"ok": True},
        )
    )

    result = pool.flush()

    assert result is not None
    assert result.content == "[tool]Read config[completed]"
    assert result.metadata["previous"] == [
        "[tool]Read config[pending]",
        "[tool]Read config[in_progress]",
    ]
    assert result.metadata["status"] == "completed"
    assert result.metadata["tool_snapshot"] == {
        "sessionUpdate": "tool_call_update",
        "toolCallId": "demo",
        "title": "Read config",
        "status": "completed",
        "rawInput": {"path": "/tmp/config.json"},
        "rawOutput": {"ok": True},
    }
    assert "tool_event" not in result.metadata
    assert "tool_events_previous" not in result.metadata


def test_tool_pool_timeout_appends_timeout_message() -> None:
    """工具池死手到点时应补写超时消息，并把已有消息挪到 `previous`。"""

    pool = ToolPool(bucket_key="tool:demo")
    pool.accept(
        ToolPoolPayload(
            session_update="tool_call",
            tool_call_id="demo",
            title="Read config",
            status=ToolRuntimeStatus.PENDING,
        )
    )
    pool.accept(
        ToolPoolPayload(
            session_update="tool_call_update",
            tool_call_id="demo",
            title="Read config",
            status=ToolRuntimeStatus.IN_PROGRESS,
        )
    )

    pool.mark_timeout()
    result = pool.flush()

    assert result is not None
    assert result.content == "[tool]Read config[timeout]"
    assert result.metadata["previous"] == [
        "[tool]Read config[pending]",
        "[tool]Read config[in_progress]",
    ]
    assert result.metadata["status"] == "timeout"
    snapshot = cast(JSONMap, result.metadata["tool_snapshot"])
    assert snapshot["status"] == "timeout"
    assert "tool_event" not in result.metadata
    assert "tool_events_previous" not in result.metadata
