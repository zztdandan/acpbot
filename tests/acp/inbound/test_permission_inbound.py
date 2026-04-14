from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from uuid import uuid4

import pytest

from nanobot.config.loader import get_config_path, set_config_path
from tests.acp.sessionmap.helpers import (
    build_runtime,
    close_runtime_quietly,
    write_checked_in_sessionmap_fixture,
    write_runtime_config,
)


def _build_session_key(tag: str) -> tuple[str, str]:
    """为 permission 用例生成独立 websocket 会话键和 chat_id。"""

    chat_id = f"permission-{tag}-{uuid4().hex}"
    return f"websocket:{chat_id}", chat_id


@pytest.mark.asyncio
async def test_permission_reply_without_pending_returns_not_found(tmp_path: Path) -> None:
    """无 pending permission 时，`/permission <n>` 应该在 inbound 直返 not-found。"""

    config_path = write_runtime_config(tmp_path)
    write_checked_in_sessionmap_fixture(config_path=config_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key, chat_id = _build_session_key("not-found")
    try:
        outbound = await runtime.process_direct(
            "/permission 1",
            session_key=session_key,
            channel="websocket",
            chat_id=chat_id,
        )
        assert outbound.content == "No pending permission request."
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_permission_reply_with_pending_returns_fixed_ack_and_unblocks_request(
    tmp_path: Path,
) -> None:
    """真实 process_direct 场景下，permission reply 应命中 state waiter 并直返固定 ack。"""

    config_path = write_runtime_config(tmp_path)
    write_checked_in_sessionmap_fixture(config_path=config_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key, chat_id = _build_session_key("accepted")
    pending_task: asyncio.Task | None = None

    # 该提示刻意要求访问 workspace 外的目录，稳定触发一次 request_permission 回调。
    permission_prompt = (
        "Use tools to list only first-level entries directly under /home/base, "
        "then return the names as plain text."
    )

    try:
        pending_task = asyncio.create_task(
            runtime.process_direct(
                permission_prompt,
                session_key=session_key,
                channel="websocket",
                chat_id=chat_id,
            )
        )

        waiting_entry = None
        for _ in range(180):
            waiting_entry = runtime.process_runtime_manager.find_request_waiting_permission(
                nanobot_side_session_key=session_key
            )
            if waiting_entry is not None:
                break
            if pending_task.done():
                break
            await asyncio.sleep(0.2)

        if waiting_entry is None:
            if pending_task.done() and (exc := pending_task.exception()) is not None:
                raise exc
            pytest.skip("real backend did not surface a pending permission request in time")

        permission_outbound = await runtime.process_direct(
            "/permission 1",
            session_key=session_key,
            channel="websocket",
            chat_id=chat_id,
        )
        assert permission_outbound.content == "Permission accepted."

        final_outbound = await asyncio.wait_for(pending_task, timeout=90)
        assert isinstance(final_outbound.content, str)
        assert (
            runtime.process_runtime_manager.find_request_waiting_permission(
                nanobot_side_session_key=session_key
            )
            is None
        )
    finally:
        if pending_task is not None and not pending_task.done():
            pending_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pending_task
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)
