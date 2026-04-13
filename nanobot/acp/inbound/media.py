"""入站归一化与步骤编排层。"""

from __future__ import annotations

import mimetypes
from pathlib import Path
from urllib.parse import unquote, urlparse

from loguru import logger

from nanobot.acp.contracts import ACPArtifactList, ACPArtifactMap


def normalize_inbound_media_paths(
    *,
    workspace: Path,
    media: list[str],
    nanobot_side_session_key: str,
    channel: str,
) -> list[Path]:
    """执行该方法定义的处理流程并返回结果。"""

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
) -> ACPArtifactList:
    """执行该方法定义的处理流程并返回结果。"""

    artifacts: list[ACPArtifactMap] = []
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
