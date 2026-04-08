from __future__ import annotations

import shutil
from pathlib import Path
from typing import AsyncIterator

import pytest

from nanobot.acp.dispatcher import ACPDispatcher
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import ChannelsConfig

from .helpers import (
    ACPDispatcherHarness,
    build_acp_config,
    build_session_key,
    require_e2e_enabled,
    stage_file_transport_workspace,
)


@pytest.fixture
def acp_e2e_session_key() -> str:
    """Session key shared by E2E-000..003 in one test run.

    中文注释：用户要求通过环境变量可注入固定 key；若未设置则自动随机，
    以避免误用历史 session 映射。
    """

    return build_session_key()


@pytest.fixture
def acp_e2e_runtime_root(request: pytest.FixtureRequest) -> Path:
    """Place E2E runtime artifacts under repository for manual review."""

    # 中文注释：用户要求把 cwd/workspace 固定到仓库目录，便于人工审计对话与 session-map。
    root = Path(__file__).resolve().parent / "artifacts" / "runtime" / request.node.name
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    return root


@pytest.fixture
def acp_e2e_data_root(
    acp_e2e_runtime_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """Isolate ACP session-map and audit JSONL under repository runtime path."""

    root = acp_e2e_runtime_root / ".nanobot" / "acp-e2e"
    root.mkdir(parents=True, exist_ok=True)
    # 中文注释：dispatcher_state 通过 nanobot.acp.dispatcher.get_data_dir 读取会话映射路径。
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: root)
    # 中文注释：可观测性模块单独 import 了 get_data_dir，这里同步 patch，避免写到真实目录。
    monkeypatch.setattr("nanobot.acp.observability_tooling.get_data_dir", lambda: root)
    return root


@pytest.fixture
def acp_e2e_workspace(acp_e2e_runtime_root: Path) -> Path:
    """Provide dedicated ACP cwd for E2E suite under repository runtime path."""

    workspace = acp_e2e_runtime_root / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    stage_file_transport_workspace(workspace)
    return workspace


@pytest.fixture
def acp_e2e_fixture_dir(acp_e2e_workspace: Path) -> Path:
    """Return staged FT fixture directory inside ACP workspace."""

    return acp_e2e_workspace / "fixtures" / "file_transport"


@pytest.fixture
def acp_e2e_plugin_path(acp_e2e_workspace: Path) -> Path:
    """Return staged plugin path inside ACP workspace."""

    return acp_e2e_workspace / ".opencode" / "plugin" / "acp-send-file.ts"


@pytest.fixture
async def acp_e2e_harness(
    acp_e2e_data_root: Path,
    acp_e2e_workspace: Path,
) -> AsyncIterator[ACPDispatcherHarness]:
    """Create and run ACP dispatcher harness for real E2E tests."""

    del acp_e2e_data_root
    require_e2e_enabled()

    config = build_acp_config(permission_policy="strict", honor_env_policy=False)
    # 中文注释：命令不存在时直接 skip，避免测试输出误导性连接错误。
    if shutil.which(config.command) is None:
        pytest.skip(f"ACP command not found in PATH: {config.command}")

    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=acp_e2e_workspace,
        acp_config=config,
        channels_config=ChannelsConfig(send_final=True),
    )
    harness = ACPDispatcherHarness(dispatcher=dispatcher, bus=bus)
    await harness.start()
    try:
        yield harness
    finally:
        await harness.stop()


@pytest.fixture
async def acp_e2e_trusted_harness(
    acp_e2e_data_root: Path,
    acp_e2e_workspace: Path,
) -> AsyncIterator[ACPDispatcherHarness]:
    """Create a trusted ACP harness used only by outbound plugin FT tests."""

    del acp_e2e_data_root
    require_e2e_enabled()

    config = build_acp_config(permission_policy="trusted", honor_env_policy=False)
    if shutil.which(config.command) is None:
        pytest.skip(f"ACP command not found in PATH: {config.command}")

    bus = MessageBus()
    dispatcher = ACPDispatcher(
        bus=bus,
        workspace=acp_e2e_workspace,
        acp_config=config,
        channels_config=ChannelsConfig(send_final=True),
    )
    harness = ACPDispatcherHarness(dispatcher=dispatcher, bus=bus)
    await harness.start()
    try:
        yield harness
    finally:
        await harness.stop()
