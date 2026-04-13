"""Inbound media normalization for ACP runtime."""

from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from loguru import logger


def normalize_inbound_media_paths(
    *,
    workspace: Path,
    media: list[str],
    nanobot_side_session_key: str,
    channel: str,
) -> list[Path]:
    """Normalize inbound media to safe workspace-local file paths."""

    # 中文注释：inbound media 只允许引用 workspace 内已有文件，
    # 这样后续执行期 prompt blocks 才不会越权读到工作区外资产。
    workspace = workspace.resolve()
    normalized: list[Path] = []
    for raw_media in media:
        parsed = urlparse(raw_media)
        if parsed.scheme and parsed.scheme != "file":
            logger.error(
                "ACP inbound media rejected nanobot_side_session_key={} channel={} reason=unsupported_scheme media={}",
                nanobot_side_session_key,
                channel,
                raw_media,
            )
            continue
        candidate = (
            Path(unquote(parsed.path)).expanduser()
            if parsed.scheme == "file"
            else Path(raw_media).expanduser()
        )
        if not candidate.is_absolute():
            candidate = workspace / candidate
        try:
            resolved = candidate.resolve()
        except OSError:
            logger.error(
                "ACP inbound media rejected nanobot_side_session_key={} channel={} reason=resolve_failed media={}",
                nanobot_side_session_key,
                channel,
                raw_media,
            )
            continue
        if not resolved.is_relative_to(workspace) or not resolved.is_file():
            logger.error(
                "ACP inbound media rejected nanobot_side_session_key={} channel={} reason=outside_workspace_or_missing media={}",
                nanobot_side_session_key,
                channel,
                raw_media,
            )
            continue
        normalized.append(resolved)
    return normalized


def build_media_artifacts(
    *,
    workspace: Path,
    media: list[str],
    nanobot_side_session_key: str,
    channel: str,
) -> list[dict[str, Any]]:
    """Prepare safe media artifacts for the execution-stage prompt builder."""

    # 中文注释：这里产出的 artifact 只是执行期输入材料，
    # 不代表已经构造好了 ACP prompt block；真正 block 组装仍在执行期 helper 中完成。
    artifacts: list[dict[str, Any]] = []
    for path in normalize_inbound_media_paths(
        workspace=workspace,
        media=media,
        nanobot_side_session_key=nanobot_side_session_key,
        channel=channel,
    ):
        artifacts.append(
            {
                "path": str(path),
                "uri": path.as_uri(),
                "name": path.name,
                "mime_type": mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                "size": path.stat().st_size,
            }
        )
    return artifacts
