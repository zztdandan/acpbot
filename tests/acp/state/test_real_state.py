from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator

import pytest
from acp.schema import SessionNotification

from nanobot.acp.state.manager import SessionStateManager
from nanobot.acp.state.outbound_schema import build_progress_payload
from nanobot.acp.state.router import ProgressRouter
from nanobot.config.loader import get_config_path, set_config_path
from tests.acp.sessionmap.helpers import (
    build_runtime,
    close_runtime_quietly,
    discover_real_session_seed,
    write_checked_in_sessionmap_fixture,
    write_runtime_config,
)

FIXTURE_PATH = Path(__file__).with_name("state_updates.real_fixture.json")
REAL_SESSION_PLACEHOLDER = "__REAL_SESSION_ID__"
BURST_DELAY_SECONDS = 0.005
IDLE_COLLECT_SECONDS = 0.05
COLLECT_TIMEOUT_SECONDS = 1.0


@dataclass(slots=True)
class ProgressEnvelope:
    """记录 state flush 到测试侧总线的原始负载。"""

    content: str
    metadata: dict[str, Any] = field(default_factory=dict)
    media: list[str] = field(default_factory=list)


class RecordingProgressRouter(ProgressRouter):
    """在保持真实 router 出包逻辑的同时，把结果镜像到测试 queue。"""

    def __init__(
        self, *, state_manager: SessionStateManager, queue: asyncio.Queue[ProgressEnvelope]
    ) -> None:
        super().__init__(
            state_manager=state_manager,
            permission_coordinator=state_manager.permission_coordinator,
        )
        self._queue = queue

    async def publish_progress_flush(self, *, flushed_handler, flush_result) -> None:  # type: ignore[override]
        if self._closed:
            return
        content, metadata, media = build_progress_payload(flush_result)
        await self._queue.put(
            ProgressEnvelope(content=content, metadata=dict(metadata), media=list(media))
        )
        outbound = flushed_handler.build_progress_outbound(
            state_manager=self._state_manager,
            flush_result=flush_result,
        )
        if outbound is not None:
            await self._state_manager.state_publish_progress_outbound(outbound=outbound)


@asynccontextmanager
async def open_real_state_manager(
    tmp_path: Path,
) -> AsyncIterator[
    tuple[SessionStateManager, RecordingProgressRouter, asyncio.Queue[ProgressEnvelope]]
]:
    """复用真实 sessionmap 测试链路，拿到已恢复真实 session 的 state manager。"""

    seed = await discover_real_session_seed()
    config_path = write_runtime_config(tmp_path)
    write_checked_in_sessionmap_fixture(config_path=config_path)

    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    try:
        await runtime.ensure_connection()
        ensured_session_id = await runtime.session_runtime_manager.ensure_ready_session(
            nanobot_side_session_key=seed.target_session_key
        )
        state_manager = SessionStateManager(
            runtime=runtime,
            request_key="state-real-request",
            nanobot_side_session_key=seed.target_session_key,
            acp_side_session_id=ensured_session_id,
            channel="websocket",
            chat_id="state-real-chat",
            on_progress=None,
        )
        queue: asyncio.Queue[ProgressEnvelope] = asyncio.Queue()
        router = RecordingProgressRouter(state_manager=state_manager, queue=queue)
        state_manager._progress_router = router
        yield state_manager, router, queue
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


def load_state_fixture() -> dict[str, Any]:
    """读取落地 fixture，确保测试数据不在代码里散落。"""

    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def materialize_notifications(
    *, scenario_name: str, session_id: str
) -> tuple[list[SessionNotification], dict[str, Any]]:
    """用真实 session id 替换占位符，并让 python-sdk 严格校验 payload。"""

    payload = load_state_fixture()
    scenario = payload["scenarios"][scenario_name]
    notifications: list[SessionNotification] = []
    for raw_notification in scenario["notifications"]:
        rendered = json.loads(
            json.dumps(raw_notification).replace(REAL_SESSION_PLACEHOLDER, session_id)
        )
        notifications.append(SessionNotification.model_validate(rendered))
    return notifications, scenario["expected"]


async def feed_notifications(
    *,
    state_manager: SessionStateManager,
    router: RecordingProgressRouter,
    notifications: list[SessionNotification],
) -> None:
    """模拟 burst updates：短间隔喂入，再由 state 自己聚合。"""

    for notification in notifications:
        assert notification.session_id == state_manager.acp_side_session_id
        await state_manager.consume_session_update(notification.update)
        await asyncio.sleep(BURST_DELAY_SECONDS)


async def collect_until_idle(
    queue: asyncio.Queue[ProgressEnvelope],
    *,
    idle_seconds: float = IDLE_COLLECT_SECONDS,
    timeout_seconds: float = COLLECT_TIMEOUT_SECONDS,
) -> list[ProgressEnvelope]:
    """等待一个很短的 idle 窗口，再返回本轮收集到的所有事件。"""

    collected: list[ProgressEnvelope] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError(f"Timed out collecting progress after {timeout_seconds} seconds")
        try:
            item = await asyncio.wait_for(queue.get(), timeout=min(idle_seconds, remaining))
        except asyncio.TimeoutError:
            return collected
        collected.append(item)


def assert_progress_text(events: list[ProgressEnvelope], expected_texts: list[str]) -> None:
    """只抽取文本刷新事件，避免媒体/工具事件干扰断言。"""

    actual = [
        event.content
        for event in events
        if event.content and not event.media and not event.metadata.get("_tool_hint")
    ]
    assert actual == expected_texts


def assert_progress_media(events: list[ProgressEnvelope], expected_media: list[list[str]]) -> None:
    """媒体事件必须按 flush 增量顺序完整到达。"""

    actual = [event.media for event in events if event.media]
    assert actual == expected_media


def assert_tool_hints(events: list[ProgressEnvelope], expected_hints: list[str]) -> None:
    """工具提示只看 `_tool_hint` 标记，避免把普通文本混进来。"""

    actual = [event.content for event in events if event.metadata.get("_tool_hint")]
    assert actual == expected_hints


@pytest.mark.asyncio
async def test_state_real_message_updates_assemble_text_and_media(tmp_path: Path) -> None:
    """消息类 update 应该在 burst 输入后正确拼接文本，并保留所有媒体路径。"""

    async with open_real_state_manager(tmp_path) as (state_manager, router, queue):
        notifications, expected = materialize_notifications(
            scenario_name="message_only",
            session_id=state_manager.acp_side_session_id,
        )

        await feed_notifications(
            state_manager=state_manager,
            router=router,
            notifications=notifications,
        )
        await router.flush_all()
        progress_events = await collect_until_idle(queue)
        final_outbound = state_manager.materialize_final_outbound()

        assert_progress_text(progress_events, expected["progressText"])
        assert_progress_media(progress_events, expected["progressMedia"])
        assert final_outbound.content == expected["final"]["content"]
        assert final_outbound.media == expected["final"]["media"]


@pytest.mark.asyncio
async def test_state_real_tool_updates_keep_tool_hints_and_attachment_media(tmp_path: Path) -> None:
    """tool start/progress 的提示流和附件都应该能被 state 聚合并最终保留。"""

    async with open_real_state_manager(tmp_path) as (state_manager, router, queue):
        notifications, expected = materialize_notifications(
            scenario_name="tool_only",
            session_id=state_manager.acp_side_session_id,
        )

        await feed_notifications(
            state_manager=state_manager,
            router=router,
            notifications=notifications,
        )
        await router.flush_all()
        progress_events = await collect_until_idle(queue)
        final_outbound = state_manager.materialize_final_outbound()

        assert_tool_hints(progress_events, expected["toolHints"])
        assert final_outbound.media == expected["toolMedia"]


@pytest.mark.asyncio
async def test_state_real_mixed_updates_match_split_case_union(tmp_path: Path) -> None:
    """混合输入时，state 的最终收敛应等价于消息流与 tool 流分别测试后的并集。"""

    async with open_real_state_manager(tmp_path) as (state_manager, router, queue):
        notifications, expected = materialize_notifications(
            scenario_name="mixed",
            session_id=state_manager.acp_side_session_id,
        )

        await feed_notifications(
            state_manager=state_manager,
            router=router,
            notifications=notifications,
        )
        await router.flush_all()
        progress_events = await collect_until_idle(queue)
        final_outbound = state_manager.materialize_final_outbound()

        assert_tool_hints(progress_events, expected["final"]["toolHints"])
        assert final_outbound.content == expected["final"]["content"]
        assert final_outbound.media == expected["final"]["media"]
