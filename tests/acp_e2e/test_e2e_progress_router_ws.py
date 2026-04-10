from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import websockets

from .helpers import require_e2e_enabled

HARNESS_ROOT = Path("/home/base/repo/harness/nanobot-refactor")
ACP_E2E_ROOT = HARNESS_ROOT / ".nanobot" / "acp-e2e"
AUDIT_ROOT = ACP_E2E_ROOT / "acp-audit"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _build_progress_router_ws_config(*, permission_timeout_seconds: int) -> Path:
    """写入 progress_router 专用 WS E2E 配置（日志固定落到 .nanobot/acp-e2e）。"""

    source = HARNESS_ROOT / ".nanobot" / "config.json"
    data = json.loads(source.read_text(encoding="utf-8"))
    ws_cfg = ((data.get("channels") or {}).get("websocket") or {}).copy()
    ws_cfg["enabled"] = True
    ws_cfg["host"] = "127.0.0.1"

    ACP_E2E_ROOT.mkdir(parents=True, exist_ok=True)
    config = {
        "paths": {
            "root": str(ACP_E2E_ROOT),
        },
        "agents": {
            "defaults": {
                "workspace": str(HARNESS_ROOT),
            }
        },
        "dispatch": {
            "backend": "acp",
            "acp": {
                "command": ((data.get("dispatch") or {}).get("acp") or {}).get(
                    "command", "opencode"
                ),
                "args": ((data.get("dispatch") or {}).get("acp") or {}).get(
                    "args", ["acp", "--print-logs", "--log-level", "WARN"]
                ),
                "cwd": str(HARNESS_ROOT),
                "env": ((data.get("dispatch") or {}).get("acp") or {}).get("env", {}),
                "protocolVersion": 1,
                "permissionsPolicy": "trusted",
                "permissionTimeoutSeconds": permission_timeout_seconds,
                "progressTextIdleSeconds": 1.0,
                "progressTextMaxChars": 2048,
                "progressToolIdleSeconds": 300.0,
                "progressToolTerminalDelaySeconds": 1.5,
                "progressOtherIdleSeconds": 0.2,
                "progressMediaIdleSeconds": 0.2,
                "startupTimeoutSeconds": 30,
                "defaultModel": ((data.get("dispatch") or {}).get("acp") or {}).get(
                    "defaultModel", "openai/gpt-5.3-codex/medium"
                ),
                "defaultMode": "build",
            },
        },
        "gateway": {"heartbeat": {"enabled": False}},
        "channels": {
            "sendProgress": True,
            "sendToolHints": True,
            "sendFinal": True,
            "websocket": ws_cfg,
            # 中文注释：关闭外部渠道，避免 e2e 期间引入与本场景无关的网络噪声。
            "telegram": {"enabled": False},
            "dingtalk": {"enabled": False},
        },
    }
    path = ACP_E2E_ROOT / "config.progress_router_ws_e2e.json"
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


async def _wait_ws_ready(uri: str, timeout: float = 30.0) -> None:
    started = asyncio.get_running_loop().time()
    while True:
        try:
            async with websockets.connect(uri, open_timeout=1.0):
                return
        except Exception:
            if asyncio.get_running_loop().time() - started >= timeout:
                raise
            await asyncio.sleep(0.25)


async def _start_gateway(config_path: Path) -> asyncio.subprocess.Process:
    env = dict(os.environ)
    env["LOGURU_LEVEL"] = "DEBUG"
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "nanobot",
        "gateway",
        "--config",
        str(config_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    ws_cfg = (json.loads(config_path.read_text(encoding="utf-8")).get("channels") or {}).get(
        "websocket", {}
    )
    ws_path = str(ws_cfg.get("path", "/ws"))
    if not ws_path.startswith("/"):
        ws_path = f"/{ws_path}"
    ws_uri = f"ws://127.0.0.1:{int(ws_cfg.get('port', 18790))}{ws_path}"
    await _wait_ws_ready(ws_uri, timeout=30.0)
    return proc


async def _stop_gateway(proc: asyncio.subprocess.Process, *, suffix: str) -> str:
    if proc.returncode is None:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
    stdout_pipe = proc.stdout
    stderr_pipe = proc.stderr
    stdout_tail = (await stdout_pipe.read()).decode("utf-8", "ignore") if stdout_pipe else ""
    stderr_tail = (await stderr_pipe.read()).decode("utf-8", "ignore") if stderr_pipe else ""

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
    log_path = AUDIT_ROOT / f"progress_router_ws_{suffix}_{stamp}.stderr.log"
    AUDIT_ROOT.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        f"# STDOUT\n{stdout_tail}\n\n# STDERR\n{stderr_tail}\n",
        encoding="utf-8",
    )
    return stderr_tail


def _newest_run_dir(before: set[str]) -> Path:
    runs = sorted(
        (p for p in AUDIT_ROOT.glob("run-*") if p.is_dir()), key=lambda p: p.stat().st_mtime
    )
    for candidate in reversed(runs):
        if candidate.name not in before:
            return candidate
    raise AssertionError("No new acp-audit run directory found")


async def _wait_new_run_dir(before: set[str], *, timeout: float = 30.0) -> Path:
    started = asyncio.get_running_loop().time()
    while True:
        try:
            return _newest_run_dir(before)
        except AssertionError:
            if asyncio.get_running_loop().time() - started >= timeout:
                raise
            await asyncio.sleep(0.2)


async def _wait_outbound_reason(
    outbound_path: Path,
    *,
    reason: str,
    after_index: int,
    timeout: float = 120.0,
) -> tuple[int, dict[str, Any]]:
    """轮询 outbound.jsonl，等待指定 reason 出现并返回其行索引与 payload。"""

    started = asyncio.get_running_loop().time()
    while True:
        rows = _read_jsonl(outbound_path)
        for idx, row in enumerate(rows[after_index:], start=after_index):
            payload = row.get("payload")
            if isinstance(payload, dict) and payload.get("reason") == reason:
                return idx, payload
        if asyncio.get_running_loop().time() - started >= timeout:
            raise AssertionError(f"Timeout waiting outbound reason={reason}")
        await asyncio.sleep(0.2)


def _ws_uri_and_token(config_path: Path) -> tuple[str, str]:
    data = json.loads(config_path.read_text(encoding="utf-8"))
    ws_cfg = (data.get("channels") or {}).get("websocket") or {}
    ws_path = str(ws_cfg.get("path", "/ws"))
    if not ws_path.startswith("/"):
        ws_path = f"/{ws_path}"
    token = ((ws_cfg.get("auth") or {}).get("tokens") or ["ws-dev-token"])[0]
    return f"ws://127.0.0.1:{int(ws_cfg.get('port', 18790))}{ws_path}", str(token)


@pytest.mark.asyncio
async def test_e2e_progress_router_ws_scenario_a_outbound_schema_and_logs() -> None:
    """Scenario A: WS 请求项目目录与 git，校验 progress/final schema 与 acp_outbound_ready 日志对齐。"""

    require_e2e_enabled()
    config_path = _build_progress_router_ws_config(permission_timeout_seconds=90)
    before_runs = {p.name for p in AUDIT_ROOT.glob("run-*") if p.is_dir()}
    ws_uri, ws_token = _ws_uri_and_token(config_path)

    proc = await _start_gateway(config_path)
    try:
        async with websockets.connect(ws_uri, open_timeout=5.0, close_timeout=2.0) as ws:
            chat_id = "progress-router-a"
            await ws.send(
                json.dumps(
                    {"type": "auth", "token": ws_token, "principalId": "progress-router-a-user"}
                )
            )
            assert (
                json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0)).get("type") == "authed"
            )
            await ws.send(json.dumps({"type": "bind_chat", "chatId": chat_id}))
            assert json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0)).get("type") == "bound"

            await ws.send(
                json.dumps(
                    {
                        "type": "send",
                        "chatId": chat_id,
                        "sessionKey": f"websocket:{chat_id}",
                        "content": (
                            "请回报当前项目顶层目录和 git 状态；"
                            "最终答案请包含标记 PROGRESS_ROUTER_A_OK。"
                        ),
                    }
                )
            )

            frames: list[dict[str, Any]] = []
            deadline = asyncio.get_running_loop().time() + 120.0
            while asyncio.get_running_loop().time() < deadline:
                try:
                    payload = json.loads(await asyncio.wait_for(ws.recv(), timeout=10.0))
                except TimeoutError:
                    continue
                frames.append(payload)
                if payload.get("type") == "final":
                    break

            progress_frames = [f for f in frames if f.get("type") == "progress"]
            final_frames = [f for f in frames if f.get("type") == "final"]
            assert progress_frames, "Expected progress frames"
            assert final_frames, "Expected final frame"

            for frame in progress_frames:
                metadata = frame.get("metadata") or {}
                assert isinstance(metadata, dict)
                assert metadata.get("_progress") is True
                assert metadata.get("_acp_kind")
                assert metadata.get("_acp_session_id")
                assert metadata.get("_acp_route_key")
                assert "_acp_payload" in metadata

            final_content = str(final_frames[-1].get("content") or "")
            assert "<final>" in final_content
            assert "PROGRESS_ROUTER_A_OK" in final_content
    finally:
        stderr_text = await _stop_gateway(proc, suffix="scenario_a")

    run_dir = _newest_run_dir(before_runs)
    outbound_rows = _read_jsonl(run_dir / "outbound.jsonl")
    assert outbound_rows, "Expected outbound audit rows"

    payload_rows = [
        row.get("payload") for row in outbound_rows if isinstance(row.get("payload"), dict)
    ]
    reasons = [p.get("reason") for p in payload_rows if isinstance(p, dict)]
    assert "progress_text" in reasons or "progress_other" in reasons or "progress_tool" in reasons
    assert "final" in reasons
    # 中文注释：flush reason 必须可追踪，验证 metadata 中保留 _acp_flush_reason。
    assert any(
        isinstance((p or {}).get("metadata"), dict)
        and (p or {}).get("metadata", {}).get("_acp_flush_reason")
        for p in payload_rows
    )

    ready_count = stderr_text.count('"event":"acp_outbound_ready"')
    assert ready_count == len(payload_rows)


@pytest.mark.asyncio
async def test_e2e_progress_router_ws_scenario_b_permission_reply_and_timeout() -> None:
    """Scenario B: WS 权限回执覆盖 1/2 与 timeout，并校验 permission 相关 outbound 日志。"""

    require_e2e_enabled()
    config_path = _build_progress_router_ws_config(permission_timeout_seconds=30)
    before_runs = {p.name for p in AUDIT_ROOT.glob("run-*") if p.is_dir()}
    ws_uri, ws_token = _ws_uri_and_token(config_path)

    proc = await _start_gateway(config_path)
    try:
        outbound_path: Path | None = None
        cursor = 0

        async with websockets.connect(ws_uri, open_timeout=5.0, close_timeout=2.0) as ws:
            chat_id = "progress-router-b"
            await ws.send(
                json.dumps(
                    {"type": "auth", "token": ws_token, "principalId": "progress-router-b-user"}
                )
            )
            assert (
                json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0)).get("type") == "authed"
            )
            await ws.send(json.dumps({"type": "bind_chat", "chatId": chat_id}))
            assert json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0)).get("type") == "bound"

            prompt = (
                "请严格执行：必须使用 glob 工具访问 /home/base/repo；"
                "先查询一级目录，再查询二级目录；"
                "不要直接口头回答，必须走工具调用。"
            )

            async def _request_and_reply(
                token: str | None, *, request_session_key: str
            ) -> dict[str, Any]:
                await ws.send(
                    json.dumps(
                        {
                            "type": "send",
                            "chatId": chat_id,
                            "sessionKey": request_session_key,
                            "content": prompt,
                        }
                    )
                )

                nonlocal cursor
                nonlocal outbound_path
                if outbound_path is None:
                    run_dir = await _wait_new_run_dir(before_runs, timeout=60.0)
                    outbound_path = run_dir / "outbound.jsonl"
                assert outbound_path is not None
                req_idx, req_payload = await _wait_outbound_reason(
                    outbound_path,
                    reason="permission_request",
                    after_index=cursor,
                    timeout=120.0,
                )
                cursor = req_idx + 1
                metadata = req_payload.get("metadata") if isinstance(req_payload, dict) else {}
                request_id = str((metadata or {}).get("request_id") or "")
                assert request_id
                if token is not None:
                    await ws.send(
                        json.dumps(
                            {
                                "type": "send",
                                "chatId": chat_id,
                                "sessionKey": request_session_key,
                                "content": f"perm:{request_id} {token}",
                            }
                        )
                    )

                ack_reason = "permission_ack" if token is not None else "permission_timeout"
                ack_idx, ack_payload = await _wait_outbound_reason(
                    outbound_path,
                    reason=ack_reason,
                    after_index=cursor,
                    timeout=90.0,
                )
                cursor = ack_idx + 1
                return ack_payload

            ack_one = await _request_and_reply("1", request_session_key=f"websocket:{chat_id}:1")
            ack_one_md = ack_one.get("metadata") or {}
            assert isinstance(ack_one_md, dict)
            assert ack_one_md.get("decision_source") in {"text", "metadata"}

            # 中文注释：第三轮不回执，验证 30s timeout 路径会回传 permission/ack(timeout)。
            ack_timeout = await _request_and_reply(
                None,
                request_session_key=f"websocket:{chat_id}:2",
            )
            ack_timeout_md = ack_timeout.get("metadata") or {}
            assert isinstance(ack_timeout_md, dict)
            assert ack_timeout_md.get("decision_source") == "timeout"

            ack_two = await _request_and_reply("2", request_session_key=f"websocket:{chat_id}:3")
            ack_two_md = ack_two.get("metadata") or {}
            assert isinstance(ack_two_md, dict)
            assert ack_two_md.get("decision_source") in {"text", "metadata"}
    finally:
        stderr_text = await _stop_gateway(proc, suffix="scenario_b")

    run_dir = _newest_run_dir(before_runs)
    outbound_rows = _read_jsonl(run_dir / "outbound.jsonl")
    payload_rows = [
        row.get("payload") for row in outbound_rows if isinstance(row.get("payload"), dict)
    ]
    reasons = [p.get("reason") for p in payload_rows if isinstance(p, dict)]

    assert "permission_request" in reasons
    assert "permission_ack" in reasons
    assert "permission_timeout" in reasons

    # 同时在 debug 日志中校验三类 reason 均出现在 acp_outbound_ready 事件里。
    assert '"event":"acp_outbound_ready"' in stderr_text
    assert '"reason":"permission_request"' in stderr_text
    assert '"reason":"permission_ack"' in stderr_text
    assert '"reason":"permission_timeout"' in stderr_text
