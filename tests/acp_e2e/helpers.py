from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shlex
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from uuid import uuid4

import pytest

from nanobot.acp.dispatcher import ACPDispatcher
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import ACPBackendConfig


FILE_TRANSPORT_DIRNAME = "file_transport"
PLUGIN_FILENAME = "acp-send-file.ts"


def e2e_enabled() -> bool:
    """Return whether ACP E2E suite is explicitly enabled.

    中文注释：真实 ACP E2E 依赖外部进程与模型调用，为避免 CI/本地误触发，
    默认关闭，需手动设置 NANOBOT_ACP_E2E=1 才执行。
    """

    return os.getenv("NANOBOT_ACP_E2E", "").strip() == "1"


def require_e2e_enabled() -> None:
    """Skip current test when ACP E2E switch is disabled."""

    if not e2e_enabled():
        pytest.skip("ACP E2E is disabled; set NANOBOT_ACP_E2E=1 to enable")


def build_session_key() -> str:
    """Build an external/random session key used by inbound messages.

    中文注释：该 key 会覆盖默认 channel:chat_id 映射，避免读到历史映射文件时
    误复用旧会话，保证每轮 E2E 调试具备最小隔离性。
    """

    raw = os.getenv("NANOBOT_ACP_E2E_SESSION_KEY", "").strip()
    if raw:
        return raw
    return f"acp-e2e:{uuid4().hex[:12]}"


def build_acp_config(
    *,
    permission_policy: str | None = None,
    honor_env_policy: bool = True,
) -> ACPBackendConfig:
    """Create ACP backend config from environment overrides.

    中文注释：这里把 command/args/model/mode 等抽成环境变量，
    方便在不同机器上对接不同 opencode 入口，不需要改测试代码。
    """

    command = os.getenv("NANOBOT_ACP_E2E_COMMAND", "opencode").strip() or "opencode"
    args_raw = os.getenv("NANOBOT_ACP_E2E_ARGS", "acp --print-logs --log-level WARN").strip()
    args = shlex.split(args_raw) if args_raw else []

    cfg = ACPBackendConfig(command=command, args=args)

    model = os.getenv("NANOBOT_ACP_E2E_MODEL", "RCode_OpenAI/gpt-5.4").strip()
    if model:
        cfg.default_model = model

    mode = os.getenv("NANOBOT_ACP_E2E_MODE", "build").strip()
    if mode:
        # 中文注释：real-chain FT 需要稳定可调用工具的 agent mode，默认钉住 build。
        cfg.default_mode = mode

    if permission_policy in {"strict", "trusted", "yolo"}:
        # 中文注释：FT default/trusted harness 需要固定策略，避免环境变量悄悄污染测试语义。
        cfg.permissions_policy = permission_policy  # type: ignore[assignment]
    elif honor_env_policy:
        policy = os.getenv("NANOBOT_ACP_E2E_PERMISSION_POLICY", "").strip()
        if policy in {"strict", "trusted", "yolo"}:
            cfg.permissions_policy = policy  # type: ignore[assignment]

    timeout_raw = os.getenv("NANOBOT_ACP_E2E_STARTUP_TIMEOUT", "").strip()
    if timeout_raw.isdigit():
        cfg.startup_timeout_seconds = max(5, int(timeout_raw))

    return cfg


def file_transport_fixture_source_dir() -> Path:
    """Return repository source directory for canonical FT sample files."""

    return Path(__file__).resolve().parent / "tranport_file_example"


def plugin_fixture_source_path() -> Path:
    """Return repository source path for canonical ACP send-file plugin fixture."""

    return Path(__file__).resolve().parent / "plugins" / PLUGIN_FILENAME


def stage_file_transport_workspace(workspace: Path) -> tuple[Path, Path]:
    """Copy FT sample files and plugin fixture into the ACP workspace."""

    fixture_dir = workspace / "fixtures" / FILE_TRANSPORT_DIRNAME
    plugin_path = workspace / ".opencode" / "plugin" / PLUGIN_FILENAME
    if fixture_dir.exists():
        shutil.rmtree(fixture_dir)
    shutil.copytree(file_transport_fixture_source_dir(), fixture_dir)
    plugin_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(plugin_fixture_source_path(), plugin_path)
    return fixture_dir, plugin_path


def staged_fixture_path(workspace: Path, filename: str) -> Path:
    """Build one staged FT sample path under the ACP workspace."""

    return workspace / "fixtures" / FILE_TRANSPORT_DIRNAME / filename


def read_jsonl_rows(path: Path) -> list[dict[str, object]]:
    """Read JSONL records from disk; missing files return an empty list."""

    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def tool_audit_path(harness: "ACPDispatcherHarness", tool_name: str) -> Path:
    """Return the per-tool audit JSONL path for the current harness run."""

    return Path(harness.dispatcher._audit_run_dir) / "tools" / f"{tool_name}.jsonl"  # noqa: SLF001


def read_tool_audit_rows(
    harness: "ACPDispatcherHarness", tool_name: str
) -> list[dict[str, object]]:
    """Read current run's per-tool audit rows."""

    return read_jsonl_rows(tool_audit_path(harness, tool_name))


@dataclass
class ACPDispatcherHarness:
    """Thin harness that drives ACPDispatcher via inbound/outbound queues.

    中文注释：该 harness 明确保持“测试端口=MessageBus inbound/outbound”，
    不直接调用内部 process 接口，从而更贴近 gateway 真实运行路径。
    """

    dispatcher: ACPDispatcher
    bus: MessageBus
    run_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start dispatcher main loop in background."""

        if self.run_task is not None:
            return
        self.run_task = asyncio.create_task(self.dispatcher.run())
        # 中文注释：让事件循环先推进一拍，尽早暴露启动阶段异常。
        await asyncio.sleep(0)
        if self.run_task.done():
            await self.run_task

    async def stop(self) -> None:
        """Stop dispatcher and close ACP connection resources."""

        self.dispatcher.stop()
        if self.run_task is not None:
            # 中文注释：run 循环内部 consume_inbound 有 1s timeout，给它留出退出窗口。
            try:
                await asyncio.wait_for(self.run_task, timeout=2.5)
            except asyncio.TimeoutError:
                self.run_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self.run_task
            self.run_task = None
        await self.dispatcher.close()

    async def send_inbound(
        self,
        *,
        session_key: str,
        content: str,
        media: Iterable[str] | None = None,
        channel: str = "acp_e2e",
        chat_id: str = "acp-e2e-chat",
        sender_id: str = "acp-e2e-user",
        metadata: dict[str, object] | None = None,
    ) -> None:
        """Publish one inbound message to dispatcher bus."""

        await self.bus.publish_inbound(
            InboundMessage(
                channel=channel,
                sender_id=sender_id,
                chat_id=chat_id,
                content=content,
                media=list(media or []),
                metadata=dict(metadata or {}),
                session_key_override=session_key,
            )
        )

    async def collect_outbound_until_idle(
        self,
        *,
        idle_seconds: float = 1.5,
        total_timeout: float = 90.0,
    ) -> list[OutboundMessage]:
        """Collect outbound messages until queue stays idle.

        中文注释：ACP 可能返回 progress + final 多条消息，
        所以这里不是“取第一条”，而是“取到一段空闲窗口”作为本轮结束信号。
        """

        started = time.monotonic()
        messages: list[OutboundMessage] = []
        while True:
            elapsed = time.monotonic() - started
            remain = total_timeout - elapsed
            if remain <= 0:
                break
            timeout = min(idle_seconds, remain)
            try:
                msg = await asyncio.wait_for(self.bus.consume_outbound(), timeout=timeout)
            except asyncio.TimeoutError:
                break
            messages.append(msg)
        return messages

    async def collect_outbound_until(
        self,
        *,
        stop_when,
        idle_after_match_seconds: float = 0.5,
        poll_seconds: float = 1.0,
        total_timeout: float = 90.0,
    ) -> list[OutboundMessage]:
        """Collect outbound messages until caller-defined completion is observed.

        中文注释：真实 ACP E2E 的首条 outbound 往往要数秒后才出现，
        仅靠短 idle 窗口容易把“还在生成中”误判成“本轮无输出”。
        这里改为先等到断言目标出现，再额外留一个短暂静默窗口收尾。
        """

        started = time.monotonic()
        matched_at: float | None = None
        messages: list[OutboundMessage] = []
        while True:
            now = time.monotonic()
            elapsed = now - started
            if elapsed >= total_timeout:
                break
            if matched_at is not None and now - matched_at >= idle_after_match_seconds:
                break

            timeout = min(poll_seconds, total_timeout - elapsed)
            try:
                msg = await asyncio.wait_for(self.bus.consume_outbound(), timeout=timeout)
            except asyncio.TimeoutError:
                continue

            messages.append(msg)
            if stop_when(messages):
                matched_at = time.monotonic()
        return messages


def first_non_empty_content(messages: list[OutboundMessage]) -> str:
    """Return first non-empty outbound content for simple assertions."""

    for msg in messages:
        content = (msg.content or "").strip()
        if content:
            return content
    return ""


def has_final_message(messages: list[OutboundMessage]) -> bool:
    """Check whether messages contain `<final>...</final>` wrapper."""

    return any("<final>" in (m.content or "") for m in messages)


def ensure_contains_any(messages: list[OutboundMessage], keywords: Iterable[str]) -> bool:
    """Return True when any outbound content contains one keyword."""

    joined = "\n".join((m.content or "") for m in messages)
    return any(k in joined for k in keywords)
