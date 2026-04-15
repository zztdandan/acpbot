from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from uuid import uuid4

import pytest

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.config.loader import get_config_path, set_config_path
from tests.acp.sessionmap.helpers import (
    HARNESS_ROOT,
    build_runtime,
    close_runtime_quietly,
    extract_json_object,
    write_checked_in_sessionmap_fixture,
    write_runtime_config,
)

FINAL_TIMEOUT_SECONDS = 180.0
POLL_TIMEOUT_SECONDS = 2.0


def _build_bus_session(tag: str) -> tuple[str, str]:
    """为总线 E2E 用例构造隔离 chat_id 与 sender_id，避免并发污染。"""

    chat_id = f"io-{tag}-{uuid4().hex}"
    sender_id = f"user-{uuid4().hex}"
    return chat_id, sender_id


def _is_final_payload(content: str) -> bool:
    """按约定识别 `<final>...</final>` 最终包裹。"""

    stripped = (content or "").strip()
    return stripped.startswith("<final>") and stripped.endswith("</final>")


def _unwrap_final_content(content: str) -> str:
    """去掉 final 包裹，提取内部正文给 JSON 解析。"""

    stripped = (content or "").strip()
    if stripped.startswith("<final>") and stripped.endswith("</final>"):
        return stripped[len("<final>") : -len("</final>")].strip()
    return stripped


async def _collect_outbound_until_final(
    *,
    runtime,
    timeout_seconds: float = FINAL_TIMEOUT_SECONDS,
) -> list[OutboundMessage]:
    """持续消费 outbound 总线，直到收到 final 包裹消息。"""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    collected: list[OutboundMessage] = []
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError(
                f"Timed out waiting for final outbound after {timeout_seconds} seconds"
            )
        try:
            message = await asyncio.wait_for(
                runtime.bus.consume_outbound(),
                timeout=min(POLL_TIMEOUT_SECONDS, remaining),
            )
        except asyncio.TimeoutError:
            # 轮询窗口无消息时继续等待，直到总超时。
            continue
        collected.append(message)
        if _is_final_payload(message.content):
            return collected


@pytest.mark.asyncio
async def test_runtime_bus_inbound_research_workspace_and_report_snapshot(tmp_path: Path) -> None:
    """启动 runtime+bus，用真实 inbound 消息驱动到 final，并校验目录调研回包。"""

    config_path = write_runtime_config(tmp_path)
    write_checked_in_sessionmap_fixture(config_path=config_path)
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    run_task: asyncio.Task[None] | None = None

    # 明确要求模型用工具扫描当前 workspace，并按 JSON 回答，方便测试稳定断言。
    research_prompt = (
        "Use tools to inspect the current workspace. "
        "Return ONLY a JSON object with keys: "
        "model_id (string), workspace_path (string), top_level_entries (array of names). "
        "top_level_entries must contain only first-level names directly under workspace_path. "
        "Sort top_level_entries in ascending order."
    )

    chat_id, sender_id = _build_bus_session("workspace-scan")
    inbound = InboundMessage(
        channel="websocket",
        sender_id=sender_id,
        chat_id=chat_id,
        content=research_prompt,
    )

    try:
        run_task = asyncio.create_task(runtime.run())
        await runtime.bus.publish_inbound(inbound)

        outbound_events = await _collect_outbound_until_final(runtime=runtime)
        final_message = outbound_events[-1]

        assert final_message.channel == inbound.channel
        assert final_message.chat_id == inbound.chat_id
        assert _is_final_payload(final_message.content)

        final_payload = extract_json_object(_unwrap_final_content(final_message.content))
        model_id = final_payload.get("model_id")
        workspace_path = final_payload.get("workspace_path")
        top_level_entries = final_payload.get("top_level_entries")

        assert isinstance(model_id, str) and model_id.startswith("RCode_OpenAI/")
        assert isinstance(workspace_path, str) and workspace_path.endswith("/nanobot-refactor")
        assert isinstance(top_level_entries, list) and top_level_entries
        assert all(isinstance(item, str) and item for item in top_level_entries)
        assert "nanobot" in top_level_entries

        # 至少要走过一次 outbound 消费，且最终消息必须是 final 包裹。
        assert len(outbound_events) >= 1
    finally:
        await close_runtime_quietly(runtime)
        if run_task is not None and not run_task.done():
            run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await run_task
        set_config_path(previous_config_path)
