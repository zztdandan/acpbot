from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest

from nanobot.config.loader import get_config_path, set_config_path
from tests.acp.sessionmap.helpers import (
    STALE_SESSION_ID,
    build_runtime,
    close_runtime_quietly,
    discover_real_session_seed,
    extract_json_object,
    session_map_file_for,
    write_checked_in_sessionmap_fixture,
    write_runtime_config,
)


def _build_current_model_probe() -> str:
    """要求 ACP 仅返回当前模型 JSON，避免测试解析自然语言。"""

    return (
        "Do not use tools. Reply with exactly one JSON object and no extra text: "
        '{"current_model":"<exact model id used for this response>"}'
    )


def _extract_reported_model(outbound_content: str) -> str:
    """统一提取当前模型字段，便于对真实返回做弱校验。"""

    payload = extract_json_object(outbound_content)
    reported_model = payload.get("current_model")
    assert isinstance(reported_model, str) and reported_model, (
        f"ACP response should include a non-empty current_model field, got {payload}"
    )
    return reported_model


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
        assert runtime.sessionmap_binding_manager.is_bootstrapped() is True

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
async def test_ensure_ready_session_replays_bound_model_after_restore(
    tmp_path: Path,
) -> None:
    """明确 ensure 落地 fixture 中的目标映射，并验证恢复后立刻刷回 model。

    本用例唯一被 ensure 的映射是：
    - nanobot key: `nanobot-sessionmap-target`
    - acp session id: `ses_279d9764affemAyT25zOD33zuU`

    校验方式：
    1. ensure 后检查 `SessionRuntimeManager` 已注册该 ready entry。
    2. 启动期已完成一次 reconcile 后，本次 ensure 直接从 binding truth 查目标映射，不再重新做 load+reconcile。
    3. 不再通过 resume/load 回读验证，避免验证动作本身重建 ACP session 状态。
    4. 直接向当前 ready session 提问，并要求 ACP 仅返回规范 JSON，确认本轮实际使用 model。
    5. STAGE1 已删除 agent/mode 用户选择体系，因此这里只对 model 做 ACP 侧验证。
    """

    seed = await discover_real_session_seed()
    config_path = write_runtime_config(tmp_path)
    write_checked_in_sessionmap_fixture(config_path=config_path)

    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    try:
        await runtime.ensure_connection()
        assert runtime.sessionmap_binding_manager.is_bootstrapped() is True

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
        assert (
            runtime.session_runtime_manager.get_by_acp_side_session_id(seed.target_session_id)
            is runtime_entry
        ), (
            f"reverse runtime index should point {seed.target_session_id} back to the same runtime entry"
        )

        verification_outbound = await runtime.process_direct(
            _build_current_model_probe(),
            session_key=seed.target_session_key,
        )
        reported_model_id = _extract_reported_model(verification_outbound.content)

        print(
            "ENSURE CHECK:",
            json.dumps(
                {
                    "nanobot_side_session_key": seed.target_session_key,
                    "acp_side_session_id": seed.target_session_id,
                    "expected_model": seed.target_model_id,
                    "reported_model": reported_model_id,
                    "verification_response": verification_outbound.content,
                },
                ensure_ascii=False,
            ),
        )

        assert reported_model_id == seed.target_model_id, (
            f"restored ACP session {seed.target_session_id} should answer with model {seed.target_model_id}, got {reported_model_id}"
        )
        capabilities = runtime.session_runtime_manager.get_session_capabilities(
            seed.target_session_id
        )
        assert capabilities is not None
        assert capabilities.current_model == seed.target_model_id
        assert seed.target_model_id in capabilities.available_models
        assert capabilities is not None
        prompt_metadata = capabilities.build_prompt_metadata()
        models_command = capabilities.render_models_command()

        assert prompt_metadata["nanobot_session_model"] == seed.target_model_id
        assert "nanobot_session_agent" not in prompt_metadata
        assert f"Current model: {seed.target_model_id}" in models_command
        assert seed.target_model_id in models_command
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_process_direct_creates_real_session_for_new_websocket_key_and_persists_mapping(
    tmp_path: Path,
) -> None:
    """验证首次真实外部请求会 lazy bootstrap，并为新 websocket 会话创建真实绑定。"""

    seed = await discover_real_session_seed()
    config_path = write_runtime_config(tmp_path)
    write_checked_in_sessionmap_fixture(config_path=config_path)

    random_chat_id = f"sessionmap-{uuid4().hex}"
    session_key = f"websocket:{random_chat_id}"
    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    try:
        # 不显式 ensure_connection，直接走真实外部入口，验证 lazy bootstrap + 新建会话。
        assert runtime.sessionmap_binding_manager.is_bootstrapped() is False
        assert runtime.sessionmap_binding_manager.resolve_session_id(session_key) is None

        first_outbound = await runtime.process_direct(
            _build_current_model_probe(),
            session_key=session_key,
            channel="websocket",
            chat_id=random_chat_id,
        )
        reported_model_id = _extract_reported_model(first_outbound.content)
        assert "gpt" in reported_model_id.lower(), (
            f"newly created real session should report a GPT model, got {reported_model_id}"
        )

        runtime_entry = runtime.session_runtime_manager.get_by_nanobot_side_session_key(session_key)
        assert runtime.sessionmap_binding_manager.is_bootstrapped() is True
        assert runtime_entry is not None, (
            "first external request should create a ready runtime entry"
        )
        created_session_id = runtime_entry.acp_side_session_id
        assert created_session_id not in set(seed.session_ids), (
            "new websocket session key should create a fresh ACP session instead of reusing fixture sessions"
        )
        assert (
            runtime.sessionmap_binding_manager.resolve_session_id(session_key) == created_session_id
        )

        persisted_payload = json.loads(
            session_map_file_for(config_path).read_text(encoding="utf-8")
        )
        persisted_entry = next(
            (
                entry
                for entry in persisted_payload.get("mappings", [])
                if entry.get("nanobotSideSessionKey") == session_key
            ),
            None,
        )
        assert persisted_entry is not None, (
            "new real session binding should be persisted to session_map.json"
        )
        assert persisted_entry.get("acpSideSessionId") == created_session_id
        persisted_model_id = persisted_entry.get("boundModel")
        assert isinstance(persisted_model_id, str) and persisted_model_id, (
            f"new real session binding should persist a boundModel, got {persisted_entry}"
        )
        assert persisted_model_id == runtime.acp_config.default_model == reported_model_id, (
            "new real session should persist the model that was actually used for the first response"
        )

        second_outbound = await runtime.process_direct(
            _build_current_model_probe(),
            session_key=session_key,
            channel="websocket",
            chat_id=random_chat_id,
        )
        second_reported_model_id = _extract_reported_model(second_outbound.content)
        assert "gpt" in second_reported_model_id.lower(), (
            f"reused real session should still report a GPT model, got {second_reported_model_id}"
        )

        reused_entry = runtime.session_runtime_manager.get_by_nanobot_side_session_key(session_key)
        assert reused_entry is not None
        assert reused_entry.acp_side_session_id == created_session_id, (
            "second request for the same websocket key should reuse the session created by the first request"
        )
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)


@pytest.mark.asyncio
async def test_process_direct_restores_same_target_session_after_runtime_reset(
    tmp_path: Path,
) -> None:
    """验证 reset 只清空 runtime entry，不丢失 binding truth，后续请求恢复到同一真实 session。"""

    seed = await discover_real_session_seed()
    config_path = write_runtime_config(tmp_path)
    write_checked_in_sessionmap_fixture(config_path=config_path)

    previous_config_path = get_config_path()
    runtime = build_runtime(config_path)
    try:
        await runtime.ensure_connection()
        first_outbound = await runtime.process_direct(
            _build_current_model_probe(),
            session_key=seed.target_session_key,
        )
        first_reported_model_id = _extract_reported_model(first_outbound.content)
        assert "gpt" in first_reported_model_id.lower(), (
            f"restored target session should report a GPT model before reset, got {first_reported_model_id}"
        )

        first_entry = runtime.session_runtime_manager.get_by_nanobot_side_session_key(
            seed.target_session_key
        )
        assert first_entry is not None
        original_session_id = first_entry.acp_side_session_id
        assert original_session_id == seed.target_session_id

        await runtime.reset_connection()

        assert (
            runtime.session_runtime_manager.get_by_nanobot_side_session_key(seed.target_session_key)
            is None
        ), "reset should clear runtime-ready entries"
        assert runtime.sessionmap_binding_manager.is_bootstrapped() is False

        persisted_payload = json.loads(
            session_map_file_for(config_path).read_text(encoding="utf-8")
        )
        persisted_entry = next(
            (
                entry
                for entry in persisted_payload.get("mappings", [])
                if entry.get("nanobotSideSessionKey") == seed.target_session_key
            ),
            None,
        )
        assert persisted_entry is not None, "reset should not remove the persisted target binding"
        assert persisted_entry.get("acpSideSessionId") == original_session_id
        assert persisted_entry.get("boundModel") == seed.target_model_id, (
            "reset should preserve the persisted boundModel for the restored target session"
        )

        second_outbound = await runtime.process_direct(
            _build_current_model_probe(),
            session_key=seed.target_session_key,
        )
        second_reported_model_id = _extract_reported_model(second_outbound.content)
        assert "gpt" in second_reported_model_id.lower(), (
            f"restored target session should report a GPT model after reset, got {second_reported_model_id}"
        )

        second_entry = runtime.session_runtime_manager.get_by_nanobot_side_session_key(
            seed.target_session_key
        )
        assert runtime.sessionmap_binding_manager.is_bootstrapped() is True
        assert second_entry is not None
        assert second_entry.acp_side_session_id == original_session_id, (
            "post-reset request should recover the same persisted ACP session instead of creating a new one"
        )
        assert (
            runtime.sessionmap_binding_manager.resolve_session_id(seed.target_session_key)
            == original_session_id
        ), "post-reset process_direct should reload sessionmap truth and restore the target binding"
    finally:
        await close_runtime_quietly(runtime)
        set_config_path(previous_config_path)
