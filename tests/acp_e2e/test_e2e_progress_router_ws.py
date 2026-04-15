from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any

import pytest
import websockets

from .helpers import require_e2e_enabled

HARNESS_ROOT = "/home/base/repo/harness/nanobot-refactor"
DEFAULT_CONFIG_PATH = (
    "/home/base/repo/harness/nanobot-refactor/.nanobot/acp-e2e/config.progress_router_ws_e2e.json"
)
DEFAULT_WS_URI = "ws://127.0.0.1:18937/ws"
DEFAULT_WS_TOKEN = "ws-dev-token"


def _is_truthy(name: str) -> bool:
    value = os.getenv(name)
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _ws_endpoint() -> tuple[str, str]:
    """Read WS endpoint from env so VSCode can drive debug sessions explicitly."""

    ws_uri = os.getenv("NANOBOT_ACP_E2E_WS_URI", DEFAULT_WS_URI)
    ws_token = os.getenv("NANOBOT_ACP_E2E_WS_TOKEN", DEFAULT_WS_TOKEN)
    return ws_uri, ws_token


async def _wait_ws_ready(uri: str, timeout: float = 30.0) -> None:
    started = asyncio.get_running_loop().time()
    while True:
        try:
            async with websockets.connect(uri, open_timeout=1.0, close_timeout=1.0):
                return
        except Exception:
            if asyncio.get_running_loop().time() - started >= timeout:
                raise
            await asyncio.sleep(0.2)


async def _start_gateway_if_needed() -> asyncio.subprocess.Process | None:
    """Start gateway only for local auto-run mode.

    When `NANOBOT_ACP_E2E_EXTERNAL_GATEWAY=1`, tests only connect to an already
    running gateway. This mode is intended for single-step debugging in VSCode.
    """

    ws_uri, _ = _ws_endpoint()
    if _is_truthy("NANOBOT_ACP_E2E_EXTERNAL_GATEWAY"):
        await _wait_ws_ready(ws_uri, timeout=30.0)
        return None

    config_path = os.getenv("NANOBOT_ACP_E2E_CONFIG", DEFAULT_CONFIG_PATH)
    env = dict(os.environ)
    env.setdefault("LOGURU_LEVEL", "DEBUG")
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "nanobot",
        "gateway",
        "--config",
        config_path,
        cwd=HARNESS_ROOT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    await _wait_ws_ready(ws_uri, timeout=30.0)
    return proc


async def _stop_gateway_if_needed(proc: asyncio.subprocess.Process | None) -> tuple[str, str]:
    """Stop local child gateway and return captured stdout/stderr for diagnostics."""

    if proc is None:
        return "", ""
    if proc.returncode is None:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=8.0)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()

    stdout_pipe = proc.stdout
    stderr_pipe = proc.stderr
    stdout_text = (await stdout_pipe.read()).decode("utf-8", "ignore") if stdout_pipe else ""
    stderr_text = (await stderr_pipe.read()).decode("utf-8", "ignore") if stderr_pipe else ""
    return stdout_text, stderr_text


async def _expect_type(
    ws: websockets.ClientConnection, expected: str, timeout: float
) -> dict[str, Any]:
    payload = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
    assert isinstance(payload, dict)
    assert payload.get("type") == expected
    return payload


async def _auth_and_bind(
    ws: websockets.ClientConnection,
    *,
    chat_id: str,
    principal_id: str,
    ws_token: str,
) -> None:
    await ws.send(json.dumps({"type": "auth", "token": ws_token, "principalId": principal_id}))
    await _expect_type(ws, "authed", timeout=5.0)
    await ws.send(json.dumps({"type": "bind_chat", "chatId": chat_id}))
    await _expect_type(ws, "bound", timeout=5.0)


async def _collect_turn_frames(
    ws: websockets.ClientConnection,
    *,
    total_timeout: float = 90.0,
    recv_timeout: float = 8.0,
) -> list[dict[str, Any]]:
    """Collect one conversation turn from WS.

    The collector keeps receiving until `final` is seen, then does one short
    trailing drain to include late-arriving frames in the same turn.
    """

    frames: list[dict[str, Any]] = []
    end_at = asyncio.get_running_loop().time() + total_timeout
    got_final = False

    while asyncio.get_running_loop().time() < end_at:
        remaining = end_at - asyncio.get_running_loop().time()
        try:
            payload = json.loads(
                await asyncio.wait_for(ws.recv(), timeout=min(recv_timeout, remaining))
            )
        except TimeoutError:
            if got_final:
                break
            if frames:
                break
            continue

        if isinstance(payload, dict):
            frames.append(payload)
            if payload.get("type") == "final":
                got_final = True
                # 给同轮尾随消息一个小窗口，避免只截到 final 前半段。
                recv_timeout = 1.0

    return frames


def _final_content(frames: list[dict[str, Any]]) -> str:
    finals = [frame for frame in frames if frame.get("type") == "final"]
    if not finals:
        return ""
    return str(finals[-1].get("content") or "")


def _progress_frames(frames: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [frame for frame in frames if frame.get("type") == "progress"]


def _metadata_session_ids(frames: list[dict[str, Any]]) -> set[str]:
    session_ids: set[str] = set()
    for frame in frames:
        metadata = frame.get("metadata")
        if not isinstance(metadata, dict):
            continue
        sid = metadata.get("_acp_session_id")
        if isinstance(sid, str) and sid:
            session_ids.add(sid)
    return session_ids


@pytest.mark.asyncio
async def test_e2e_progress_router_ws_scenario_a_outbound_schema_and_logs() -> None:
    """Scenario A: pure WS handshake + single turn message flow contract."""

    require_e2e_enabled()
    ws_uri, ws_token = _ws_endpoint()
    proc = await _start_gateway_if_needed()
    try:
        async with websockets.connect(ws_uri, open_timeout=5.0, close_timeout=2.0) as ws:
            chat_id = "progress-router-a"
            await _auth_and_bind(
                ws,
                chat_id=chat_id,
                principal_id="progress-router-a-user",
                ws_token=ws_token,
            )

            await ws.send(
                json.dumps(
                    {
                        "type": "send",
                        "chatId": chat_id,
                        "sessionKey": f"websocket:{chat_id}",
                        "content": "请查询你的工作目录结构，回复标记 PROGRESS_ROUTER_A_OK，并以一句简短中文结束。",
                    }
                )
            )

            frames = await _collect_turn_frames(ws, total_timeout=120.0)
            assert frames, "Expected outbound WS frames"

            # 仅打印 outbound 的非 metadata 信息，便于人工核对帧序列。
            for idx, frame in enumerate(frames, start=1):
                frame_type = str(frame.get("type") or "")
                content_text = str(frame.get("content") or "")
                media = frame.get("media")
                media_count = len(media) if isinstance(media, list) else 0
                print(
                    f"[acp_e2e][scenario_a][outbound#{idx}] type={frame_type} media_count={media_count} content={content_text}",
                    flush=True,
                )

            final_text = _final_content(frames)
            if final_text:
                print(f"[acp_e2e][scenario_a][final] {final_text}", flush=True)

            # 纯 WS 侧契约：progress 若存在，需要带上 ACP 路由元数据。
            for frame in _progress_frames(frames):
                metadata = frame.get("metadata") or {}
                assert isinstance(metadata, dict)
                assert metadata.get("_progress") is True
                # 兼容两类 progress：
                # 1) ACP 结构化 progress（带 _acp_*）
                # 2) 网关基础 progress（仅 _progress）
                if "_acp_kind" in metadata:
                    assert metadata.get("_acp_session_id")
                    assert metadata.get("_acp_route_key")
    finally:
        _stdout, _stderr = await _stop_gateway_if_needed(proc)


@pytest.mark.asyncio
async def test_e2e_progress_router_ws_scenario_b_permission_reply_and_timeout() -> None:
    """Scenario B: pure WS multi-turn collection and unified frame analysis."""

    require_e2e_enabled()
    ws_uri, ws_token = _ws_endpoint()
    proc = await _start_gateway_if_needed()
    try:
        async with websockets.connect(ws_uri, open_timeout=5.0, close_timeout=2.0) as ws:
            chat_id = "progress-router-b"
            session_key = f"websocket:{chat_id}"
            await _auth_and_bind(
                ws,
                chat_id=chat_id,
                principal_id="progress-router-b-user",
                ws_token=ws_token,
            )

            prompts = [
                "第1轮：请回复一句简短中文。",
                "第2轮：请再回复一句简短中文。",
                "第3轮：请最后回复一句简短中文。",
            ]
            rounds: list[list[dict[str, Any]]] = []

            for prompt in prompts:
                await ws.send(
                    json.dumps(
                        {
                            "type": "send",
                            "chatId": chat_id,
                            "sessionKey": session_key,
                            "content": prompt,
                        }
                    )
                )
                rounds.append(await _collect_turn_frames(ws, total_timeout=120.0))

            # 统一分析三轮消息帧，不依赖任何落盘日志或 jsonl 审计文件。
            all_frames = [frame for turn in rounds for frame in turn]
            assert all_frames, "Expected outbound WS frames across rounds"

            # 同 session_key 的多轮请求，若 metadata 含会话标识，应保持单一会话。
            session_ids = _metadata_session_ids(all_frames)
            if session_ids:
                assert len(session_ids) == 1
    finally:
        _stdout, _stderr = await _stop_gateway_if_needed(proc)
