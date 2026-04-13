from __future__ import annotations

import json
from pathlib import Path

import pytest

from nanobot.config.loader import get_config_path, set_config_path
from tests.acp.sessionmap.helpers import (
    STALE_SESSION_ID,
    build_runtime,
    close_runtime_quietly,
    discover_real_session_seed,
    load_or_resume_session,
    write_checked_in_sessionmap_fixture,
    write_runtime_config,
)


@pytest.mark.asyncio
async def test_runtime_startup_reconciles_sessionmap_against_real_session_list(
    tmp_path: Path,
) -> None:
    """验证启动大对账会读取落地 sessionmap 文件并清理 stale session。

    流程注解：
    1. 先把仓库里落地的 `session_map.real_fixture.json` 复制到本次测试 config-root。
    2. `runtime.ensure_connection()` 内部会直接调用 `binding_manager.load_persistent_truth()`。
    3. `load_persistent_truth()` 再进入 `_reconcile_entries()`，这里会调真实 ACP `list_sessions`。
    4. `_reconcile_entries()` 仅比较磁盘里的 `acpSideSessionId` 是否仍存在，不做 resume。
    5. 所以大对账结束后，binding truth 已清理 stale 数据，但 runtime ready 表仍应为空。
    """

    seed = await discover_real_session_seed()
    config_path = write_runtime_config(tmp_path)
    write_checked_in_sessionmap_fixture(config_path=config_path)

    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    try:
        await runtime.ensure_connection()

        remaining_entries = runtime.sessionmap_binding_manager.iter_entries()
        remaining_ids = {entry.acp_side_session_id for entry in remaining_entries}
        remaining_keys = {entry.nanobot_side_session_key for entry in remaining_entries}

        assert remaining_ids == set(seed.session_ids), (
            f"reconcile should keep exactly checked-in real sessions {seed.session_ids}, got {sorted(remaining_ids)}"
        )
        assert "nanobot-sessionmap-stale" not in remaining_keys, (
            "reconcile should remove the checked-in stale nanobot key from binding truth"
        )
        assert STALE_SESSION_ID not in remaining_ids, (
            f"reconcile should remove stale ACP session id {STALE_SESSION_ID} from binding truth"
        )
        for session_id in seed.session_ids:
            session_key = seed.key_by_session_id[session_id]
            assert (
                runtime.sessionmap_binding_manager.resolve_session_id(session_key) == session_id
            ), f"binding manager should preserve checked-in mapping {session_key} -> {session_id}"
            assert (
                runtime.session_runtime_manager.get_by_nanobot_side_session_key(session_key) is None
            ), (
                f"startup reconcile is list-only and must not register ready runtime entry for {session_key}"
            )
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_ensure_ready_session_replays_bound_model_and_agent_after_restore(
    tmp_path: Path,
) -> None:
    """明确 ensure 落地 fixture 中的目标映射，并验证恢复后立刻刷回 model/agent。

    本用例唯一被 ensure 的映射是：
    - nanobot key: `nanobot-sessionmap-target`
    - acp session id: `ses_279d9764affemAyT25zOD33zuU`

    校验方式：
    1. ensure 后检查 `SessionRuntimeManager` 已注册该 ready entry。
    2. 再次对真实 ACP session 做 `load_session/resume_session`。
    3. 直接断言 ACP 返回的 `current_model_id/current_mode_id` 已是 sessionmap 里保存的值。
    """

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
        assert ensured_session_id == seed.target_session_id, (
            "ensure_ready_session should recover the checked-in target mapping "
            f"{seed.target_session_key} -> {seed.target_session_id}, got {ensured_session_id}"
        )

        runtime_entry = runtime.session_runtime_manager.get_by_nanobot_side_session_key(
            seed.target_session_key
        )
        assert runtime_entry is not None, (
            f"runtime manager should register ready entry for {seed.target_session_key} after ensure"
        )
        assert runtime_entry.ready is True, (
            "ensured session should be marked ready in SessionRuntimeManager"
        )
        assert runtime_entry.acp_side_session_id == seed.target_session_id
        assert runtime_entry.capabilities.current_model == seed.target_model_id
        assert runtime_entry.capabilities.current_agent == seed.target_agent_id
        assert (
            runtime.session_runtime_manager.get_by_acp_side_session_id(seed.target_session_id)
            is runtime_entry
        ), (
            f"reverse runtime index should point {seed.target_session_id} back to the same runtime entry"
        )

        conn = runtime._acp_client_conn
        assert conn is not None
        refreshed_payload = await load_or_resume_session(conn, session_id=seed.target_session_id)
        refreshed_model_id = getattr(
            getattr(refreshed_payload, "models", None), "current_model_id", None
        )
        refreshed_agent_id = getattr(
            getattr(refreshed_payload, "modes", None), "current_mode_id", None
        )

        print(
            "ENSURE CHECK:",
            json.dumps(
                {
                    "nanobot_side_session_key": seed.target_session_key,
                    "acp_side_session_id": seed.target_session_id,
                    "expected_model": seed.target_model_id,
                    "actual_model": refreshed_model_id,
                    "expected_agent": seed.target_agent_id,
                    "actual_agent": refreshed_agent_id,
                },
                ensure_ascii=False,
            ),
        )

        assert refreshed_model_id == seed.target_model_id, (
            f"restored ACP session {seed.target_session_id} should be reset to model {seed.target_model_id}, got {refreshed_model_id}"
        )
        assert refreshed_agent_id == seed.target_agent_id, (
            f"restored ACP session {seed.target_session_id} should be reset to agent {seed.target_agent_id}, got {refreshed_agent_id}"
        )

        runtime.session_runtime_manager.update_caps_from_payload(
            acp_side_session_id=seed.target_session_id,
            payload=refreshed_payload,
        )
        capabilities = runtime.session_runtime_manager.get_session_capabilities(
            seed.target_session_id
        )
        assert capabilities is not None
        assert capabilities.current_model == seed.target_model_id
        assert capabilities.current_agent == seed.target_agent_id
        assert seed.target_model_id in capabilities.available_models
        assert seed.target_agent_id in capabilities.available_agents

        assert capabilities is not None
        prompt_metadata = capabilities.build_prompt_metadata()
        models_command = capabilities.render_models_command()
        agents_command = capabilities.render_agents_command()

        assert prompt_metadata["nanobot_session_model"] == seed.target_model_id
        assert prompt_metadata["nanobot_session_agent"] == seed.target_agent_id
        assert f"Current model: {seed.target_model_id}" in models_command
        assert seed.target_model_id in models_command
        assert f"Current agent: {seed.target_agent_id}" in agents_command
        assert seed.target_agent_id in agents_command
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)
