from __future__ import annotations

import json
from pathlib import Path

import pytest

from .helpers import has_final_message


@pytest.mark.asyncio
async def test_e2e_000_bootstrap_and_handshake_smoke(
    acp_e2e_harness,
    acp_e2e_session_key: str,
) -> None:
    """E2E-000: inbound 纯文本 -> outbound final，验证链路可跑通。"""

    await acp_e2e_harness.send_inbound(session_key=acp_e2e_session_key, content="hello")
    messages = await acp_e2e_harness.collect_outbound_until(
        stop_when=has_final_message,
        # 中文注释：build 模式恢复后可能先经历工具探索再产出 final，给足窗口避免把“仍在生成中”误判成失败。
        total_timeout=40.0,
    )

    assert messages, "Expected at least one outbound message"
    assert has_final_message(messages), "Expected final wrapper message in outbound stream"


@pytest.mark.asyncio
async def test_e2e_001_random_external_session_key_is_reusable(
    acp_e2e_harness,
    acp_e2e_session_key: str,
) -> None:
    """E2E-001: 同一外部 session key 连续两轮请求应可复用。"""

    await acp_e2e_harness.send_inbound(session_key=acp_e2e_session_key, content="first round")
    round1 = await acp_e2e_harness.collect_outbound_until(
        stop_when=has_final_message,
        total_timeout=15.0,
    )
    first_session_id = acp_e2e_harness.dispatcher._session_map.get(acp_e2e_session_key)  # noqa: SLF001

    await acp_e2e_harness.send_inbound(session_key=acp_e2e_session_key, content="second round")
    round2 = await acp_e2e_harness.collect_outbound_until(
        stop_when=has_final_message,
        total_timeout=15.0,
    )
    second_session_id = acp_e2e_harness.dispatcher._session_map.get(acp_e2e_session_key)  # noqa: SLF001

    assert round1 and round2
    assert has_final_message(round1)
    assert has_final_message(round2)
    assert first_session_id is not None
    assert second_session_id == first_session_id


@pytest.mark.asyncio
async def test_e2e_002_inbound_outbound_audit_logs_are_traceable(
    acp_e2e_harness,
    acp_e2e_session_key: str,
) -> None:
    """E2E-002: inbound/outbound JSONL 可按 session key 追踪。"""

    token = "ACP_E2E_AUDIT_TRACE"
    await acp_e2e_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content=f"Reply exactly: {token}",
    )
    _ = await acp_e2e_harness.collect_outbound_until(
        stop_when=has_final_message,
        total_timeout=15.0,
    )

    run_dir = Path(acp_e2e_harness.dispatcher._audit_run_dir)  # noqa: SLF001
    inbound_path = run_dir / "inbound.jsonl"
    outbound_path = run_dir / "outbound.jsonl"

    assert inbound_path.exists(), "Expected inbound audit log file"
    assert outbound_path.exists(), "Expected outbound audit log file"

    inbound_rows = [
        json.loads(line) for line in inbound_path.read_text(encoding="utf-8").splitlines()
    ]
    outbound_rows = [
        json.loads(line) for line in outbound_path.read_text(encoding="utf-8").splitlines()
    ]

    assert any(row.get("sessionKey") == acp_e2e_session_key for row in inbound_rows)
    assert any(
        isinstance(row.get("payload"), dict)
        and row["payload"].get("session_key") == acp_e2e_session_key
        for row in outbound_rows
    )


@pytest.mark.asyncio
async def test_e2e_003_recover_after_a_control_error(
    acp_e2e_harness,
    acp_e2e_session_key: str,
) -> None:
    """E2E-003: 先触发一次可控错误，再验证下一轮仍可正常响应。"""

    # 中文注释：先把模型切到无效值，模拟一次控制面错误；
    # 随后再切回默认模型，验证会话可恢复继续处理 prompt。
    await acp_e2e_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content="/set_model __acp_e2e_invalid_model__",
    )
    _ = await acp_e2e_harness.collect_outbound_until(
        stop_when=lambda messages: bool(messages),
        total_timeout=10.0,
    )

    default_model = acp_e2e_harness.dispatcher.acp_config.default_model
    await acp_e2e_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content=f"/set_model {default_model}",
    )
    _ = await acp_e2e_harness.collect_outbound_until(
        stop_when=lambda messages: bool(messages),
        total_timeout=10.0,
    )

    await acp_e2e_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content="recover check",
    )
    messages = await acp_e2e_harness.collect_outbound_until(
        stop_when=has_final_message,
        # 中文注释：recover 场景在 build mode 下可能先经历 glob/read，再产出 final，
        # 这里延长等待窗口避免把“仍在生成中”误判成失败。
        total_timeout=40.0,
    )

    assert messages
    assert has_final_message(messages)
