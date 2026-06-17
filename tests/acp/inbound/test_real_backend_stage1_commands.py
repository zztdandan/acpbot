from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest

from nanobot.config.loader import get_config_path, set_config_path
from tests.acp.sessionmap.helpers import (
    OPENCODE_DEEPSEEK_FLASH_MODEL,
    OPENCODE_DEEPSEEK_PRO_MODEL,
    build_runtime,
    close_runtime_quietly,
    open_real_backend_connection,
    session_map_file_for,
    write_runtime_config,
)


def _build_session_key(tag: str) -> str:
    """为真实 backend case 构造唯一 websocket session key，避免互相污染。"""

    return f"websocket:stage1-real-{tag}-{uuid4().hex}"


def _parse_catalog_ids(content: str) -> list[str]:
    """从 /models 的文本中提取 catalog，保持和命令展示契约一致。"""

    ids: list[str] = []
    for line in content.splitlines():
        if line.startswith("* "):
            ids.append(line[2:].strip())
            continue
        if line.startswith("  "):
            ids.append(line[2:].strip())
    return [item for item in ids if item]


def _parse_current_model(content: str) -> str | None:
    """提取 /models 返回中的 Current model 字段。"""

    prefix = "Current model: "
    for line in content.splitlines():
        if line.startswith(prefix):
            value = line[len(prefix) :].strip()
            return value or None
    return None


async def _run_direct(runtime, *, session_key: str, chat_id: str, content: str) -> str:
    """统一走真实 runtime.process_direct，确保命中实际 ACP backend。"""

    outbound = await runtime.process_direct(
        content,
        session_key=session_key,
        channel="websocket",
        chat_id=chat_id,
    )
    return outbound.content


def _ok_probe() -> str:
    """要求后端仅返回确定性的 OK JSON，便于对真实响应做弱断言。"""

    return 'Reply with exactly one JSON object and no extra text: {"ok":"ok"}'


@pytest.mark.asyncio
async def test_real_backends_can_read_session_lists() -> None:
    """真实验证 opencode/hermes 都能通过 ACP 读取 session 列表。"""

    for backend in ("opencode", "hermes"):
        async with open_real_backend_connection(backend) as conn:
            response = await conn.list_sessions(cwd="/home/base/repo/harness/nanobot-refactor")
            payload = response.model_dump(mode="json", by_alias=True, exclude_none=True)
            assert isinstance(payload.get("sessions"), list), (
                f"{backend} list_sessions should return a sessions array, got {payload}"
            )


@pytest.mark.asyncio
async def test_opencode_real_stage1_session_map_message_and_deepseek_model_switches(
    tmp_path: Path,
) -> None:
    """真实验证 opencode 在 STAGE1 下可列模型、切 session map，并切换 deepseek-pro/flash。"""

    config_path = write_runtime_config(
        tmp_path,
        backend="opencode",
        default_model=OPENCODE_DEEPSEEK_PRO_MODEL,
    )
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key = _build_session_key("opencode")
    chat_id = session_key.split(":", maxsplit=1)[1]
    try:
        models_content = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content="/models",
        )
        available_models = _parse_catalog_ids(models_content)
        assert OPENCODE_DEEPSEEK_PRO_MODEL in available_models, models_content
        assert OPENCODE_DEEPSEEK_FLASH_MODEL in available_models, models_content

        first_session_id = runtime.sessionmap_binding_manager.resolve_session_id(session_key)
        assert isinstance(first_session_id, str) and first_session_id

        persisted_payload = json.loads(session_map_file_for(config_path).read_text(encoding="utf-8"))
        persisted_entry = next(
            entry
            for entry in persisted_payload.get("mappings", [])
            if entry.get("nanobotSideSessionKey") == session_key
        )
        assert persisted_entry["acpSideSessionId"] == first_session_id

        ok_content = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content=_ok_probe(),
        )
        assert '"ok":"ok"' in ok_content.replace(" ", "") or '"ok": "ok"' in ok_content

        switch_pro = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content=f"/set_model {OPENCODE_DEEPSEEK_PRO_MODEL}",
        )
        assert switch_pro == f"Model switched to: {OPENCODE_DEEPSEEK_PRO_MODEL}"

        verify_pro = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content="/models",
        )
        assert _parse_current_model(verify_pro) == OPENCODE_DEEPSEEK_PRO_MODEL

        switch_flash = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content=f"/set_model {OPENCODE_DEEPSEEK_FLASH_MODEL}",
        )
        assert switch_flash == f"Model switched to: {OPENCODE_DEEPSEEK_FLASH_MODEL}"

        verify_flash = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content="/models",
        )
        assert _parse_current_model(verify_flash) == OPENCODE_DEEPSEEK_FLASH_MODEL

        persisted_after = json.loads(session_map_file_for(config_path).read_text(encoding="utf-8"))
        persisted_entry_after = next(
            entry
            for entry in persisted_after.get("mappings", [])
            if entry.get("nanobotSideSessionKey") == session_key
        )
        assert persisted_entry_after["boundModel"] == OPENCODE_DEEPSEEK_FLASH_MODEL

        new_result = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content="/new",
        )
        assert new_result == "New session started."
        assert runtime.sessionmap_binding_manager.resolve_session_id(session_key) is None

        models_after_new = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content="/models",
        )
        recreated_session_id = runtime.sessionmap_binding_manager.resolve_session_id(session_key)
        assert isinstance(recreated_session_id, str) and recreated_session_id
        assert recreated_session_id != first_session_id
        assert OPENCODE_DEEPSEEK_PRO_MODEL in _parse_catalog_ids(models_after_new)
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_hermes_real_stage1_session_map_message_and_unknown_model_catalog(
    tmp_path: Path,
) -> None:
    """真实验证 Hermes 当前可跑 session/map/message，但 model catalog 仍为空。"""

    config_path = write_runtime_config(
        tmp_path,
        backend="hermes",
        default_model=OPENCODE_DEEPSEEK_PRO_MODEL,
    )
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    session_key = _build_session_key("hermes")
    chat_id = session_key.split(":", maxsplit=1)[1]
    try:
        models_content = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content="/models",
        )
        assert _parse_current_model(models_content) == OPENCODE_DEEPSEEK_PRO_MODEL
        assert _parse_catalog_ids(models_content) == [OPENCODE_DEEPSEEK_PRO_MODEL]

        first_session_id = runtime.sessionmap_binding_manager.resolve_session_id(session_key)
        assert isinstance(first_session_id, str) and first_session_id

        persisted_payload = json.loads(session_map_file_for(config_path).read_text(encoding="utf-8"))
        persisted_entry = next(
            entry
            for entry in persisted_payload.get("mappings", [])
            if entry.get("nanobotSideSessionKey") == session_key
        )
        assert persisted_entry["acpSideSessionId"] == first_session_id

        ok_content = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content=_ok_probe(),
        )
        assert '"ok":"ok"' in ok_content.replace(" ", "") or '"ok": "ok"' in ok_content

        switch_result = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content=f"/set_model {OPENCODE_DEEPSEEK_PRO_MODEL}",
        )
        assert switch_result == f"Model switched to: {OPENCODE_DEEPSEEK_PRO_MODEL}"

        verify_models = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content="/models",
        )
        assert _parse_current_model(verify_models) == OPENCODE_DEEPSEEK_PRO_MODEL
        assert _parse_catalog_ids(verify_models) == [OPENCODE_DEEPSEEK_PRO_MODEL]

        new_result = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content="/new",
        )
        assert new_result == "New session started."
        assert runtime.sessionmap_binding_manager.resolve_session_id(session_key) is None

        models_after_new = await _run_direct(
            runtime,
            session_key=session_key,
            chat_id=chat_id,
            content="/models",
        )
        recreated_session_id = runtime.sessionmap_binding_manager.resolve_session_id(session_key)
        assert isinstance(recreated_session_id, str) and recreated_session_id
        assert recreated_session_id != first_session_id
        assert _parse_current_model(models_after_new) == OPENCODE_DEEPSEEK_PRO_MODEL
        assert _parse_catalog_ids(models_after_new) == [OPENCODE_DEEPSEEK_PRO_MODEL]
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)
