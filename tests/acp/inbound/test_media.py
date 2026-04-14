from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from nanobot.acp.inbound.pipeline import build_media_prepare_step
from nanobot.acp.inbound.media import build_media_artifacts, normalize_inbound_media_paths
from nanobot.acp.runtime_models import InboundContext
from nanobot.bus.events import OutboundMessage


class _FakeRuntime:
    """最小 runtime 替身：仅提供 media_prepare 依赖的字段与消息构造方法。"""

    def __init__(self, *, workspace: Path) -> None:
        self.workspace = workspace

    @staticmethod
    def new_outbound_message(*, channel: str, chat_id: str, content: str) -> OutboundMessage:
        return OutboundMessage(channel=channel, chat_id=chat_id, content=content)


def test_normalize_inbound_media_paths_accepts_workspace_local_paths(tmp_path: Path) -> None:
    """验证 inbound 仅传文件路径时，workspace 内路径会被标准化保留。"""

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    file_path = workspace / "docs" / "a.txt"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text("hello", encoding="utf-8")

    relative_path = str(Path("docs") / "a.txt")
    absolute_path = str(file_path)
    file_uri_path = file_path.as_uri()

    normalized = normalize_inbound_media_paths(
        workspace=workspace,
        media=[relative_path, absolute_path, file_uri_path],
        nanobot_side_session_key="websocket:test-media-accept",
        channel="websocket",
    )

    assert normalized == [file_path.resolve(), file_path.resolve(), file_path.resolve()]


def test_normalize_inbound_media_paths_rejects_invalid_or_outside_paths(tmp_path: Path) -> None:
    """验证 workspace 外、缺失文件、非 file scheme 都会被过滤掉。"""

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    valid_file = workspace / "ok.txt"
    valid_file.write_text("ok", encoding="utf-8")

    outside_file = tmp_path / "outside.txt"
    outside_file.write_text("outside", encoding="utf-8")
    missing_file = workspace / "missing.txt"

    normalized = normalize_inbound_media_paths(
        workspace=workspace,
        media=[
            str(valid_file),
            str(outside_file),
            str(missing_file),
            "https://example.com/file.png",
        ],
        nanobot_side_session_key="websocket:test-media-reject",
        channel="websocket",
    )

    assert normalized == [valid_file.resolve()]


def test_build_media_artifacts_builds_metadata_for_workspace_files(tmp_path: Path) -> None:
    """验证 build_media_artifacts 会产出路径与文件元信息。"""

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    image_file = workspace / "images" / "sample.png"
    image_file.parent.mkdir(parents=True, exist_ok=True)
    image_file.write_bytes(b"PNG")

    artifacts = build_media_artifacts(
        workspace=workspace,
        media=[str(image_file)],
        nanobot_side_session_key="websocket:test-media-artifacts",
        channel="websocket",
    )

    assert len(artifacts) == 1
    artifact = artifacts[0]
    assert artifact["path"] == str(image_file.resolve())
    assert artifact["uri"] == image_file.resolve().as_uri()
    assert artifact["name"] == "sample.png"
    assert artifact["size"] == 3
    assert artifact["mime_type"] == "image/png"


def test_build_media_artifacts_returns_empty_list_when_all_paths_invalid(tmp_path: Path) -> None:
    """验证 inbound 只传无效路径时不会构造伪造 artifact。"""

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    artifacts = build_media_artifacts(
        workspace=workspace,
        media=["/definitely/not/in/workspace.txt", "http://example.com/nope"],
        nanobot_side_session_key="websocket:test-media-empty",
        channel="websocket",
    )

    assert artifacts == []


@pytest.mark.asyncio
async def test_media_prepare_returns_direct_response_when_all_media_invalid(tmp_path: Path) -> None:
    """media 全部非法时应在 inbound 直返错误，避免静默进入执行链。"""

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    runtime = _FakeRuntime(workspace=workspace)
    step = build_media_prepare_step(cast(Any, runtime))

    ctx = InboundContext(
        request_key="req-invalid",
        nanobot_side_session_key="websocket:media-all-invalid",
        channel="websocket",
        chat_id="media-all-invalid",
        content="test",
        media=["/definitely/outside/workspace.txt"],
    )
    await step(ctx)

    assert ctx.direct_response is not None
    assert (
        ctx.direct_response.content
        == "Inbound media validation failed. Only existing workspace-local files are allowed."
    )
    assert ctx.artifacts.get("media_artifacts") is None


@pytest.mark.asyncio
async def test_media_prepare_writes_warning_artifact_when_media_partially_valid(
    tmp_path: Path,
) -> None:
    """media 部分合法时应继续执行，并写入 warning artifact 给后续链路消费。"""

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    valid_file = workspace / "ok.txt"
    valid_file.write_text("ok", encoding="utf-8")

    runtime = _FakeRuntime(workspace=workspace)
    step = build_media_prepare_step(cast(Any, runtime))
    ctx = InboundContext(
        request_key="req-partial",
        nanobot_side_session_key="websocket:media-partial",
        channel="websocket",
        chat_id="media-partial",
        content="test",
        media=[str(valid_file), "/outside/invalid.txt"],
    )
    await step(ctx)

    assert ctx.direct_response is None
    artifacts = ctx.artifacts["media_artifacts"]
    assert isinstance(artifacts, list)
    assert len(artifacts) == 1
    warning = ctx.artifacts["media_validation_warning"]
    assert isinstance(warning, dict)
    assert warning["invalid_count"] == 1
    assert warning["accepted_count"] == 1
