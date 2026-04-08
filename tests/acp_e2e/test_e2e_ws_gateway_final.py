from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest
import websockets

from .helpers import require_e2e_enabled


async def _wait_ws_ready(uri: str, timeout: float = 20.0) -> None:
    """Wait until websocket endpoint is reachable before sending test frames."""

    started = asyncio.get_running_loop().time()
    while True:
        try:
            async with websockets.connect(uri, open_timeout=1.0):
                return
        except Exception:
            if asyncio.get_running_loop().time() - started >= timeout:
                raise
            await asyncio.sleep(0.25)


@pytest.mark.asyncio
async def test_e2e_ws_004_gateway_request_project_tree_gets_final() -> None:
    """E2E-WS-004: 从真实 WS 通道发起目录探测请求，并等待 final 响应。"""

    require_e2e_enabled()

    config_path = Path(
        os.getenv(
            "NANOBOT_ACP_E2E_WS_CONFIG",
            "/home/base/repo/harness/nanobot-refactor/.nanobot/config.json",
        )
    )
    if not config_path.exists():
        pytest.skip(f"Config file not found: {config_path}")

    config_data = json.loads(config_path.read_text(encoding="utf-8"))
    ws_cfg = (config_data.get("channels") or {}).get("websocket") or {}
    ws_port = int(ws_cfg.get("port", 18790))
    ws_path = str(ws_cfg.get("path", "/ws"))
    if not ws_path.startswith("/"):
        ws_path = f"/{ws_path}"

    ws_token = os.getenv("NANOBOT_ACP_E2E_WS_TOKEN", "ws-dev-token")
    ws_uri = f"ws://127.0.0.1:{ws_port}{ws_path}"

    # 中文注释：显式通过 --config 引用用户提供的配置文件，并仅用 --port 避免端口冲突。
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "nanobot",
        "gateway",
        "--config",
        str(config_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        await _wait_ws_ready(ws_uri, timeout=30.0)

        async with websockets.connect(ws_uri, open_timeout=5.0, close_timeout=2.0) as ws:
            chat_id = "acp-e2e-ws-chat"
            await ws.send(
                json.dumps(
                    {
                        "type": "auth",
                        "token": ws_token,
                        "principalId": "acp-e2e-ws-user",
                    }
                )
            )
            # 中文注释：等待服务端确认 authed，避免 auth 尚未完成就发送业务帧被网关拒绝。
            authed_frame = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
            assert authed_frame.get("type") == "authed"
            await ws.send(json.dumps({"type": "bind_chat", "chatId": chat_id}))
            # 中文注释：等待 bind 确认，确保后续 send 可以被正确路由到 chat 会话。
            bound_frame = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
            assert bound_frame.get("type") == "bound"
            await ws.send(
                json.dumps(
                    {
                        "type": "send",
                        "chatId": chat_id,
                        "sessionKey": f"websocket:{chat_id}",
                        "content": (
                            "请探测当前项目根目录结构，列出顶层目录与关键文件；"
                            "最终答案务必包含标记 ACP_WS_FINAL_OK。"
                        ),
                    }
                )
            )

            got_final = False
            final_payload: dict[str, object] = {}
            deadline = asyncio.get_running_loop().time() + 120.0
            while asyncio.get_running_loop().time() < deadline:
                try:
                    frame = await asyncio.wait_for(ws.recv(), timeout=5.0)
                except TimeoutError:
                    continue
                payload = json.loads(frame)
                if payload.get("type") == "final":
                    got_final = True
                    final_payload = payload
                    break

            assert got_final, "Expected a websocket final frame"
            content = str(final_payload.get("content") or "")
            assert "ACP_WS_FINAL_OK" in content
            assert any(keyword in content for keyword in ("nanobot", "python-sdk", "docs"))
    finally:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
