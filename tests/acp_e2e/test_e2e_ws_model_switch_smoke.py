from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from .helpers import has_final_message


_MODEL_FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "ws_model_switch_models.txt"
_QA_ARTIFACT_PATH = Path(__file__).resolve().parent / "artifacts" / "ws_model_switch_qa_pairs.jsonl"


def _load_switch_models() -> list[str]:
    """从固定清单读取 WS 多轮切模 smoke 的模型列表。"""

    rows = [line.strip() for line in _MODEL_FIXTURE_PATH.read_text(encoding="utf-8").splitlines()]
    return [row for row in rows if row]


def _extract_latest_final_answer(messages: list[object]) -> str:
    """提取最后一条 final 消息内容，供人工判读记录。"""

    final_payload = ""
    for item in messages:
        content = getattr(item, "content", "")
        if isinstance(content, str) and "<final>" in content:
            final_payload = content
    return final_payload


@pytest.mark.asyncio
async def test_e2e_ws_005_multi_chat_model_switch_smoke(
    acp_e2e_harness,
) -> None:
    """E2E-WS-005: 多 ws chat 交替切模并记录问答日志。"""

    models = _load_switch_models()
    assert models, "Expected non-empty ws model switch list"

    chat_ids = ["acp-e2e-ws-chat-a", "acp-e2e-ws-chat-b"]
    qa_rows: list[dict[str, object]] = []

    for round_index, requested_model in enumerate(models, start=1):
        for chat_id in chat_ids:
            session_key = f"websocket:{chat_id}"

            await acp_e2e_harness.send_inbound(
                session_key=session_key,
                channel="ws",
                chat_id=chat_id,
                content=f"/set_model {requested_model}",
            )
            set_outputs = await acp_e2e_harness.collect_outbound_until(
                stop_when=lambda messages: bool(messages),
                total_timeout=20.0,
            )
            assert set_outputs, (
                f"Expected set_model outbound for chat={chat_id} round={round_index}"
            )
            set_model_reply = (set_outputs[-1].content or "").strip()

            question = "请仅返回你当前使用的模型型号"
            await acp_e2e_harness.send_inbound(
                session_key=session_key,
                channel="ws",
                chat_id=chat_id,
                content=question,
            )
            messages = await acp_e2e_harness.collect_outbound_until(
                stop_when=has_final_message,
                total_timeout=45.0,
            )
            assert has_final_message(messages), (
                f"Expected websocket final for chat={chat_id} round={round_index} model={requested_model}"
            )

            qa_rows.append(
                {
                    "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
                    "chat_id": chat_id,
                    "round": round_index,
                    "requested_model": requested_model,
                    "set_model_reply": set_model_reply,
                    "question": question,
                    "final_answer": _extract_latest_final_answer(messages),
                    "session_key": session_key,
                    "acp_session_id": acp_e2e_harness.dispatcher._session_map.get(session_key),  # noqa: SLF001
                }
            )

    _QA_ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _QA_ARTIFACT_PATH.open("w", encoding="utf-8") as fh:
        for row in qa_rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    expected_rows = len(chat_ids) * len(models)
    assert len(qa_rows) == expected_rows
    assert _QA_ARTIFACT_PATH.exists(), "Expected ws model switch QA artifact file"
