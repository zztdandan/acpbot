from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from .helpers import ensure_contains_any


async def _install_prompt_recorder(acp_e2e_harness) -> list[list[Any]]:
    """Patch conn.prompt to record the exact prompt blocks sent to ACP.

    中文注释：文件传输 E2E 的核心不是“模型回了什么文案”，而是
    dispatch 是否按约定把 media 变成 `resource_link` 下发给 ACP。
    因此这里直接记录 prompt payload，保证断言可解释、可复现。
    """

    await acp_e2e_harness.dispatcher._ensure_connection()  # noqa: SLF001
    conn = acp_e2e_harness.dispatcher._conn  # noqa: SLF001
    assert conn is not None

    records: list[list[Any]] = []
    original_prompt = conn.prompt

    async def _recording_prompt(*, prompt, session_id):
        records.append(list(prompt))
        return await original_prompt(prompt=prompt, session_id=session_id)

    conn.prompt = _recording_prompt  # type: ignore[method-assign]
    await asyncio.sleep(0)
    return records


def _block_types(blocks: list[Any]) -> list[str]:
    """Extract `type` fields from ACP content blocks."""

    types: list[str] = []
    for block in blocks:
        block_type = getattr(block, "type", None)
        if isinstance(block_type, str):
            types.append(block_type)
    return types


@pytest.mark.asyncio
async def test_e2e_ft_001_single_file_is_injected_as_resource_link(
    acp_e2e_harness,
    acp_e2e_session_key: str,
    tmp_path: Path,
) -> None:
    """E2E-FT-001: 单文件入站时，ACP prompt 应看到 resource_link。"""

    records = await _install_prompt_recorder(acp_e2e_harness)

    sample = tmp_path / "ft001.txt"
    sample.write_text("ACP_E2E_FILE_SINGLE_TOKEN\n", encoding="utf-8")

    await acp_e2e_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content="Read attached file and include token ACP_E2E_FILE_SINGLE_TOKEN in answer.",
        media=[str(sample)],
    )
    messages = await acp_e2e_harness.collect_outbound_until_idle(total_timeout=120)

    assert records, "Expected at least one prompt call to ACP"
    types = _block_types(records[-1])
    # 中文注释：这里明确锁定“入站附件统一 resource_link”目标契约；
    # 若当前实现仍走 embedded resource，此断言会失败，便于逐步调试修复。
    assert types.count("resource_link") == 1
    assert "resource" not in types
    assert ensure_contains_any(messages, ["ACP_E2E_FILE_SINGLE_TOKEN", "ft001.txt"])


@pytest.mark.asyncio
async def test_e2e_ft_002_multi_files_can_be_injected_in_one_prompt(
    acp_e2e_harness,
    acp_e2e_session_key: str,
    tmp_path: Path,
) -> None:
    """E2E-FT-002: 多文件允许同轮注入，且均为 resource_link。"""

    records = await _install_prompt_recorder(acp_e2e_harness)

    file_a = tmp_path / "ft002-a.txt"
    file_b = tmp_path / "ft002-b.txt"
    file_a.write_text("TOKEN_A\n", encoding="utf-8")
    file_b.write_text("TOKEN_B\n", encoding="utf-8")

    await acp_e2e_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content="Read both files, then output TOKEN_A and TOKEN_B.",
        media=[str(file_a), str(file_b)],
    )
    messages = await acp_e2e_harness.collect_outbound_until_idle(total_timeout=120)

    assert records, "Expected prompt payload for multi-file inbound"
    types = _block_types(records[-1])
    assert types.count("resource_link") == 2
    assert "resource" not in types
    assert ensure_contains_any(messages, ["TOKEN_A", "TOKEN_B", "ft002-a.txt", "ft002-b.txt"])


@pytest.mark.asyncio
async def test_e2e_ft_003_multi_file_response_accepts_multi_message_output(
    acp_e2e_harness,
    acp_e2e_session_key: str,
    tmp_path: Path,
) -> None:
    """E2E-FT-003: 多文件场景允许多条 outbound，只校验最小结果集合。"""

    alpha = tmp_path / "ft003-alpha.txt"
    beta = tmp_path / "ft003-beta.txt"
    alpha.write_text("ALPHA_CONTENT\n", encoding="utf-8")
    beta.write_text("BETA_CONTENT\n", encoding="utf-8")

    await acp_e2e_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content=(
            "Summarize each attachment separately. "
            "Your output may contain multiple messages, but must mention ALPHA_CONTENT and BETA_CONTENT."
        ),
        media=[str(alpha), str(beta)],
    )
    messages = await acp_e2e_harness.collect_outbound_until_idle(total_timeout=120)

    assert messages
    assert ensure_contains_any(messages, ["ALPHA_CONTENT", "BETA_CONTENT", "alpha", "beta"])


@pytest.mark.asyncio
async def test_e2e_ft_004_outbound_attachment_type_cases_match_expected_contract(
    acp_e2e_harness,
    acp_e2e_session_key: str,
    acp_e2e_case_dir: Path,
) -> None:
    """E2E-FT-004: 由用户准备 case，验证附件类型与契约是否匹配。

    约定：`NANOBOT_ACP_E2E_CASE_DIR/outbound_case_manifest.json` 提供测试清单。
    当前先做结构校验与运行入口，具体 case 内容由调试阶段逐步完善。
    """

    manifest = acp_e2e_case_dir / "outbound_case_manifest.json"
    if not manifest.exists():
        pytest.skip(f"Missing manifest file: {manifest}")

    data = json.loads(manifest.read_text(encoding="utf-8"))
    cases = data.get("cases") if isinstance(data, dict) else None
    if not isinstance(cases, list) or not cases:
        pytest.skip("No usable outbound type cases found in manifest")

    # 中文注释：当前阶段先保留最小入口，后续按你的真实 case 逐条落地断言。
    first = cases[0]
    prompt = first.get("prompt") if isinstance(first, dict) else None
    if not isinstance(prompt, str) or not prompt.strip():
        pytest.skip("Manifest first case does not provide a valid prompt")

    await acp_e2e_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content=prompt,
    )
    messages = await acp_e2e_harness.collect_outbound_until_idle(total_timeout=120)

    assert messages


@pytest.mark.asyncio
async def test_e2e_ft_005_outbound_paths_are_safe_and_traceable(
    acp_e2e_harness,
    acp_e2e_session_key: str,
) -> None:
    """E2E-FT-005: 出站落盘路径安全/可追踪（占位入口，后续逐条补断言）。"""

    # 中文注释：该项依赖具体附件 case（例如 resource_link/blob/image），
    # 当前先保留执行入口；你提供 case 后，我们再把路径规则断言补齐。
    pytest.skip(
        "Pending concrete outbound attachment cases; will assert safe naming and workspace-bound paths"
    )
