from __future__ import annotations

import json
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, cast

import pytest
from acp import spawn_agent_process
from acp.schema import ClientCapabilities, Implementation

from nanobot.acp.runtime import ACPRuntime
from nanobot.acp.sessionmap.internal.session_restore import restore_existing_session
from nanobot.bus.queue import MessageBus
from nanobot.config.loader import load_config, set_config_path

HARNESS_ROOT = Path("/home/base/repo/harness/nanobot-refactor")
TESTS_ROOT = Path(__file__).resolve().parent
CONFIG_TEMPLATE_PATH = TESTS_ROOT / "config.real_backend.template.json"
SESSION_MAP_TEMPLATE_PATH = TESTS_ROOT / "session_map.real_fixture.json"
SESSION_MAP_RELATIVE_PATH = Path("acp") / "session_map.json"
TARGET_AGENT_ID = "build"
PREFERRED_RCODE_OPENAI_MODEL = "RCode_OpenAI/gpt-5.4"
STALE_SESSION_ID = "ses_sessionmap_stale_for_reconcile_check"
TARGET_SESSION_KEY = "nanobot-sessionmap-target"
TARGET_SESSION_ID = "ses_279d9764affemAyT25zOD33zuU"


@dataclass(slots=True)
class RealSessionSeed:
    """测试所需的真实 session 基线信息。"""

    session_ids: list[str]
    key_by_session_id: dict[str, str]
    target_session_id: str
    target_session_key: str
    target_model_id: str
    target_agent_id: str | None


class _ACPCallbackSink:
    """真后端测试只需占位 callback client。"""


@asynccontextmanager
async def open_real_backend_connection() -> AsyncIterator[Any]:
    """直接用 python-sdk 拉起 opencode ACP，获取真实 session 数据。"""

    connection_cm = spawn_agent_process(
        cast(Any, _ACPCallbackSink()),
        "opencode",
        "acp",
        "--print-logs",
        "--log-level",
        "WARN",
        cwd=str(HARNESS_ROOT),
    )
    pair = await connection_cm.__aenter__()
    try:
        conn, _ = pair
        conn = cast(Any, conn)
        await conn.initialize(
            protocol_version=1,
            client_capabilities=ClientCapabilities(),
            client_info=Implementation(name="nanobot-sessionmap-test", version="0"),
        )
        yield conn
    finally:
        # python-sdk 在关闭真实 opencode 会话时偶发抛 queue closed 噪音，这里只做清理兜底。
        with suppress(RuntimeError):
            await connection_cm.__aexit__(None, None, None)


def extract_session_id(raw: object) -> str | None:
    """兼容 pydantic 对象和 dict 结构，统一提取 session id。"""

    for name in ("session_id", "sessionId", "id"):
        value = getattr(raw, name, None)
        if isinstance(value, str) and value:
            return value
    if isinstance(raw, dict):
        for name in ("session_id", "sessionId", "id"):
            value = raw.get(name)
            if isinstance(value, str) and value:
                return value
    return None


def pick_rcode_openai_model(payload: object) -> str | None:
    """测试强约束：模型必须选 RCode_OpenAI 系。"""

    models = getattr(payload, "models", None)
    available = getattr(models, "available_models", None) or []
    collected: list[str] = []
    for entry in available:
        model_id = getattr(entry, "model_id", None) or getattr(entry, "modelId", None)
        if isinstance(model_id, str) and model_id.startswith("RCode_OpenAI/"):
            collected.append(model_id)
    if PREFERRED_RCODE_OPENAI_MODEL in collected:
        return PREFERRED_RCODE_OPENAI_MODEL
    return collected[0] if collected else None


async def resume_existing_session(conn: Any, *, session_id: str) -> Any:
    """测试与生产保持同一规则：永远先 resume，再 fallback load。"""

    payload = await restore_existing_session(conn, cwd=str(HARNESS_ROOT), session_id=session_id)
    if payload is None:
        raise RuntimeError("ACP backend does not expose load_session or resume_session")
    return payload


def extract_json_object(text: str) -> dict[str, Any]:
    """从 ACP 文本响应中提取单个 JSON 对象，兼容常见 fenced code block。"""

    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            stripped = "\n".join(lines[1:-1]).strip()
            if stripped.startswith("json"):
                stripped = stripped[4:].strip()

    # 兼容部分后端返回的标记包裹：
    # <final>
    # {...json...}
    # </final>
    if stripped.startswith("<final>") and stripped.endswith("</final>"):
        stripped = stripped[len("<final>") : -len("</final>")].strip()

    payload = json.loads(stripped)
    if not isinstance(payload, dict):
        raise ValueError("ACP response JSON must be an object")
    return payload


def load_checked_in_sessionmap_fixture() -> dict[str, Any]:
    """读取仓库里落地的 sessionmap fixture。"""

    return json.loads(SESSION_MAP_TEMPLATE_PATH.read_text(encoding="utf-8"))


async def discover_real_session_seed() -> RealSessionSeed:
    """以落地 fixture 为基线，确认这些真实 session 仍存在于 ACP backend。"""

    fixture_payload = load_checked_in_sessionmap_fixture()
    fixture_mappings = fixture_payload.get("mappings", [])
    fixture_entries = [
        entry for entry in fixture_mappings if entry.get("acpSideSessionId") != STALE_SESSION_ID
    ]
    fixture_session_ids = [entry["acpSideSessionId"] for entry in fixture_entries]
    key_by_session_id = {
        entry["acpSideSessionId"]: entry["nanobotSideSessionKey"] for entry in fixture_entries
    }

    if TARGET_SESSION_ID not in fixture_session_ids:
        pytest.skip("Checked-in sessionmap fixture does not contain the configured target session")

    async with open_real_backend_connection() as conn:
        # 说明：真实后端 list_sessions 在部分环境下只返回窗口化结果，
        # 仅凭列表缺失会把“可 restore 的老会话”误判为 stale。
        # 这里改为逐个探测 fixture session 是否可 restore，作为存在性真值来源。
        missing_fixture_sessions: list[str] = []
        for session_id in fixture_session_ids:
            try:
                await resume_existing_session(conn, session_id=session_id)
            except Exception:
                missing_fixture_sessions.append(session_id)

        if missing_fixture_sessions:
            pytest.skip(
                "Checked-in sessionmap fixture is stale; restore failed for real ACP sessions: "
                + ", ".join(missing_fixture_sessions)
            )

        target_payload = await resume_existing_session(conn, session_id=TARGET_SESSION_ID)
        target_model_id = pick_rcode_openai_model(target_payload)
        if target_model_id is None:
            pytest.skip("Real sessionmap tests require an available RCode_OpenAI model")

        modes = getattr(target_payload, "modes", None)
        available_modes = getattr(modes, "available_modes", None) or []
        available_mode_ids = {
            mode_id
            for entry in available_modes
            if isinstance((mode_id := getattr(entry, "id", None)), str) and mode_id
        }
        # 真实后端可用 mode 会随环境波动；这里只在存在 build 时记录，
        # 具体断言由各测试按需决定，避免让所有 real-session 用例一起被 skip。
        target_agent_id = TARGET_AGENT_ID if TARGET_AGENT_ID in available_mode_ids else None

        return RealSessionSeed(
            session_ids=fixture_session_ids,
            key_by_session_id=key_by_session_id,
            target_session_id=TARGET_SESSION_ID,
            target_session_key=TARGET_SESSION_KEY,
            target_model_id=target_model_id,
            target_agent_id=target_agent_id,
        )


def write_runtime_config(tmp_path: Path) -> Path:
    """复制最小测试配置；config-root 落到 tmp_path，避免污染真实 .nanobot。"""

    data = json.loads(CONFIG_TEMPLATE_PATH.read_text(encoding="utf-8"))
    config_path = tmp_path / "config.real_backend.json"
    config_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return config_path


def session_map_file_for(config_path: Path) -> Path:
    """与 binding manager 的 config-root 解析规则保持一致。"""

    return config_path.parent / SESSION_MAP_RELATIVE_PATH


def write_checked_in_sessionmap_fixture(*, config_path: Path) -> Path:
    """把仓库中落地的真实 sessionmap fixture 复制到本次测试的 config-root。"""

    session_map_file = session_map_file_for(config_path)
    session_map_file.parent.mkdir(parents=True, exist_ok=True)
    session_map_file.write_text(
        SESSION_MAP_TEMPLATE_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )
    return session_map_file


def build_runtime(config_path: Path) -> ACPRuntime:
    """用测试配置实例化 ACPRuntime，并把 data-dir 锁到临时 config-root。"""

    set_config_path(config_path)
    config = load_config(config_path)
    return ACPRuntime(
        bus=MessageBus(),
        workspace=HARNESS_ROOT,
        acp_config=config.dispatch.acp,
        channels_config=config.channels,
    )


async def close_runtime_quietly(runtime: ACPRuntime) -> None:
    """清理 runtime，并吞掉已知 python-sdk close 噪音。"""

    with suppress(RuntimeError):
        await runtime.close()
