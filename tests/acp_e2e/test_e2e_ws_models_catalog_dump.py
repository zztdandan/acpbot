from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest


_MODEL_SWITCH_FIXTURE_PATH = (
    Path(__file__).resolve().parent / "fixtures" / "ws_model_switch_models.txt"
)
_CATALOG_ARTIFACT_PATH = Path(__file__).resolve().parent / "artifacts" / "ws_models_catalog.json"


def _parse_model_ids_from_models_command(content: str) -> list[str]:
    """从 /models 文本输出中提取模型 id。"""

    model_ids: list[str] = []
    for raw in content.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("Current model:") or line.startswith("Available models:"):
            continue
        if line.startswith("*"):
            candidate = line[1:].strip()
        else:
            candidate = line
        if candidate:
            model_ids.append(candidate)
    return model_ids


@pytest.mark.asyncio
async def test_e2e_ws_006_dump_models_catalog_and_compare_fixture(
    acp_e2e_harness,
) -> None:
    """E2E-WS-006: 用同一配置执行 /models，导出全量模型并对比切模清单。"""

    session_key = "websocket:acp-e2e-ws-models-catalog"
    chat_id = "acp-e2e-ws-models-catalog"

    await acp_e2e_harness.send_inbound(
        session_key=session_key,
        channel="ws",
        chat_id=chat_id,
        content="/models",
    )
    outputs = await acp_e2e_harness.collect_outbound_until(
        stop_when=lambda messages: bool(messages),
        total_timeout=25.0,
    )
    assert outputs, "Expected outbound content for /models"
    content = (outputs[-1].content or "").strip()
    assert content, "Expected non-empty /models output"

    catalog_models = _parse_model_ids_from_models_command(content)
    assert catalog_models, "Expected at least one model from /models output"

    fixture_models = [
        line.strip()
        for line in _MODEL_SWITCH_FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    catalog_set = set(catalog_models)
    fixture_set = set(fixture_models)

    payload = {
        "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
        "session_key": session_key,
        "chat_id": chat_id,
        "models_command_raw": content,
        "catalog_models": catalog_models,
        "fixture_models": fixture_models,
        "fixture_models_present_in_catalog": sorted(fixture_set & catalog_set),
        "fixture_models_missing_in_catalog": sorted(fixture_set - catalog_set),
    }
    _CATALOG_ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CATALOG_ARTIFACT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
