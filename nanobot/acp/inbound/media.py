"""入站媒体辅助：负责路径归一化、工作区约束校验与 artifact 构造。"""

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
    """归一化入站媒体路径；只保留工作区内真实存在的本地文件。

    处理流程：
        - 解析原始媒体字符串，拒绝非 `file` 协议的远程路径
        - 把相对路径补成 workspace 下绝对路径，再做 `resolve`
        - 过滤掉工作区外或不存在的文件，并记录拒绝原因
    """

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
    """构造入站媒体 artifact 列表；把合法文件转换为 ACP 可消费的结构化描述。

    处理流程：
        - 先复用路径归一化逻辑筛出合法本地文件
        - 为每个文件补齐 `path`、`uri`、`name`、`mime_type`、`size`
        - 返回可直接写入 `ProcessRequest.artifacts` 的列表
    """

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
