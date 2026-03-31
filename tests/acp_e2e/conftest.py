from __future__ import annotations

import os
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
)


@pytest.fixture
def acp_e2e_session_key() -> str:
    """Session key shared by E2E-000..003 in one test run.

    中文注释：用户要求通过环境变量可注入固定 key；若未设置则自动随机，
    以避免误用历史 session 映射。
    """

    return build_session_key()


@pytest.fixture
def acp_e2e_data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolate ACP session-map and audit JSONL under tmp path."""

    root = tmp_path / ".nanobot" / "acp-e2e"
    root.mkdir(parents=True, exist_ok=True)
    # 中文注释：dispatcher_state 通过 nanobot.acp.dispatcher.get_data_dir 读取会话映射路径。
    monkeypatch.setattr("nanobot.acp.dispatcher.get_data_dir", lambda: root)
    # 中文注释：可观测性模块单独 import 了 get_data_dir，这里同步 patch，避免写到真实目录。
    monkeypatch.setattr("nanobot.acp.observability_tooling.get_data_dir", lambda: root)
    return root


@pytest.fixture
def acp_e2e_workspace(tmp_path: Path) -> Path:
    """Provide dedicated ACP cwd for E2E suite."""

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    return workspace


@pytest.fixture
async def acp_e2e_harness(
    acp_e2e_data_root: Path,
    acp_e2e_workspace: Path,
) -> AsyncIterator[ACPDispatcherHarness]:
    """Create and run ACP dispatcher harness for real E2E tests."""

    del acp_e2e_data_root
    require_e2e_enabled()

    config = build_acp_config()
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
def acp_e2e_case_dir() -> Path:
    """Directory for manually prepared E2E case files.

    中文注释：用户会手工准备出站附件类型 case；这里通过环境变量注入路径，
    测试代码不绑定仓库固定目录。
    """

    raw = os.getenv("NANOBOT_ACP_E2E_CASE_DIR", "").strip()
    if not raw:
        pytest.skip("NANOBOT_ACP_E2E_CASE_DIR is required for file-transport E2E cases")
    path = Path(raw).expanduser().resolve()
    if not path.exists() or not path.is_dir():
        pytest.skip(f"NANOBOT_ACP_E2E_CASE_DIR is invalid: {path}")
    return path
