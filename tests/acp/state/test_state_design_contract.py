from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from acp.schema import AgentMessageChunk

from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.models import ACPBucketType, PoolKey


class _RecordingRuntime:
    """最小 runtime 替身：为 state/router 测试提供 publish 与 observability 桥。"""

    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.progress_outbounds: list[object] = []
        self.observability_events: list[object] = []

    async def push_observability(self, event) -> None:
        """记录 state 推送的结构化观测事件，便于后续断言。"""

        self.observability_events.append(event)

    def resolve_acp_workspace_path(self) -> Path:
        """返回测试工作目录；满足媒体 handler 的路径依赖。"""

        return self.workspace

    async def global_publish_progress_outbound(
        self,
        *,
        outbound,
        request_key=None,
        drop_if_inactive=False,
    ) -> None:
        """记录 progress outbound；测试替身只关注 state 是否成功把结果发往 runtime。"""

        del request_key, drop_if_inactive
        self.progress_outbounds.append(outbound)


def _build_state_manager(tmp_path: Path) -> SessionStateManager:
    """构造一个只依赖最小 runtime 替身的 request 级 state manager。"""

    runtime = _RecordingRuntime(tmp_path)
    return SessionStateManager(
        runtime=runtime,
        request_key="state-design-request",
        nanobot_side_session_key="state-design-session",
        acp_side_session_id="acp-session-id",
        channel="cli",
        chat_id="chat-id",
        on_progress=None,
    )


def test_materialize_final_outbound_wraps_final_tags(tmp_path: Path) -> None:
    """最终 outbound 必须统一包裹为三行 `<final>` 结构，确保多通道一致。"""

    state_manager = _build_state_manager(tmp_path)
    state_manager.request_scope.final_text = "hello final"

    outbound = state_manager.materialize_final_outbound(partial=False)

    assert outbound.content == "<final>\nhello final\n</final>"


def test_materialize_partial_outbound_keeps_raw_text(tmp_path: Path) -> None:
    """partial 回退场景保持原始文本，不注入 final 包裹。"""

    state_manager = _build_state_manager(tmp_path)
    state_manager.request_scope.partial_text = "partial snapshot"

    outbound = state_manager.materialize_final_outbound(partial=True)

    assert outbound.content == "partial snapshot"


@pytest.mark.asyncio
async def test_message_text_only_commits_final_text_on_flush(tmp_path: Path) -> None:
    """文本 chunk 进入池后只更新 partial，final text 必须等到 text pool flush 才提交。"""

    state_manager = _build_state_manager(tmp_path)
    update = AgentMessageChunk.model_validate(
        {
            "sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": "hello world"},
        }
    )

    await state_manager.consume_session_update(update)

    assert state_manager.request_scope.partial_text == "hello world"
    assert state_manager.request_scope.final_text == ""

    await state_manager.progress_router.flush_all()

    assert state_manager.request_scope.final_text == "hello world"


def test_append_final_text_uses_newline_between_segments(tmp_path: Path) -> None:
    """无重叠片段按段追加，段间固定一个换行。"""

    state_manager = _build_state_manager(tmp_path)

    state_manager.append_final_text("first segment")
    state_manager.append_final_text("second segment")

    assert state_manager.request_scope.final_text == "first segment\nsecond segment"


def test_append_final_text_keeps_only_snapshot_delta(tmp_path: Path) -> None:
    """当上游重复发送全量快照时，只并入新增尾段，避免全文重复。"""

    state_manager = _build_state_manager(tmp_path)

    state_manager.append_final_text("hello")
    state_manager.append_final_text("hello world")
    state_manager.append_final_text("hello world")

    assert state_manager.request_scope.final_text == "hello\nworld"


def test_append_final_text_merges_suffix_prefix_overlap(tmp_path: Path) -> None:
    """半重叠场景应根据后缀/前缀重叠长度仅追加真实新增部分。"""

    state_manager = _build_state_manager(tmp_path)

    state_manager.append_final_text("alpha beta")
    state_manager.append_final_text("beta gamma")

    assert state_manager.request_scope.final_text == "alpha beta\ngamma"


def test_append_final_text_ignores_tail_replay(tmp_path: Path) -> None:
    """旧尾段重放不应重复写入 final，避免跨 flush 回放污染。"""

    state_manager = _build_state_manager(tmp_path)

    state_manager.append_final_text("one\ntwo")
    state_manager.append_final_text("two")

    assert state_manager.request_scope.final_text == "one\ntwo"


def test_materialize_final_outbound_falls_back_to_partial_when_final_empty(tmp_path: Path) -> None:
    """final 区为空时，最终物化必须回退 partial，避免最终消息正文丢失。"""

    state_manager = _build_state_manager(tmp_path)
    state_manager.request_scope.partial_text = "fallback partial"

    outbound = state_manager.materialize_final_outbound(partial=False)

    assert outbound.content == "<final>\nfallback partial\n</final>"


@pytest.mark.asyncio
async def test_permission_reply_finalizes_permission_pool_via_router(tmp_path: Path) -> None:
    """permission request/reply 应由 router 协调，并在 reply 命中后立即摘除 permission pool。"""

    state_manager = _build_state_manager(tmp_path)
    router = state_manager.progress_router
    allow_once = SimpleNamespace(
        option_id="allow-once",
        kind=SimpleNamespace(value="allow_once"),
        label="Allow once",
    )
    cancelled = SimpleNamespace(
        option_id="cancelled",
        kind=SimpleNamespace(value="cancelled"),
        label="Cancel",
    )
    permission_key = PoolKey(bucket_type=ACPBucketType.PERMISSION, bucket_key="permission")

    request_task = asyncio.create_task(
        router.handle_permission_request(
            options=[allow_once, cancelled],
            tool_call=SimpleNamespace(toolCallId="call-perm-001"),
        )
    )
    await asyncio.sleep(0)

    assert router.has_pending_permission() is True
    assert state_manager.get_pool_entry(permission_key) is not None

    ack = await router.handle_permission_reply(reply_text="/permission call-perm-001:1")
    response = cast(Any, await request_task)

    assert ack.accepted is True
    assert ack.reason == "accepted"
    assert ack.permission_request_id == "call-perm-001"
    assert router.has_pending_permission() is False
    assert state_manager.get_pool_entry(permission_key) is None
    assert (
        response.model_dump(by_alias=True, exclude_none=True)["outcome"]["optionId"] == "allow-once"
    )


@pytest.mark.asyncio
async def test_permission_reply_with_mismatched_request_id_is_rejected(tmp_path: Path) -> None:
    """当 reply 携带 request_id 且不匹配时，router 必须拒绝并保持 waiter 挂起。"""

    state_manager = _build_state_manager(tmp_path)
    router = state_manager.progress_router
    allow_once = SimpleNamespace(
        option_id="allow-once",
        kind=SimpleNamespace(value="allow_once"),
        label="Allow once",
    )

    request_task = asyncio.create_task(
        router.handle_permission_request(
            options=[allow_once],
            tool_call=SimpleNamespace(toolCallId="call-perm-expected"),
        )
    )
    await asyncio.sleep(0)

    ack = await router.handle_permission_reply(reply_text="/permission call-perm-other:1")

    assert ack.accepted is False
    assert ack.reason == "invalid_reply"
    assert ack.permission_request_id == "call-perm-expected"
    assert router.has_pending_permission() is True

    # 用正确 request_id 收尾，避免挂起任务泄漏。
    await router.handle_permission_reply(reply_text="/permission call-perm-expected:1")
    await request_task
