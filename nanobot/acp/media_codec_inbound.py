"""ACP inbound media 编码能力。"""

from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from loguru import logger


class _ACPInboundMediaCodecMixin:
    """封装 inbound media -> ACP `resource_link` blocks 逻辑。"""

    workspace: Path
    acp_config: Any

    @staticmethod
    def _acp_text_block(content: str) -> Any:
        raise NotImplementedError

    @staticmethod
    def _acp_resource_link_block(
        name: str,
        uri: str,
        *,
        mime_type: str | None,
        size: int | None,
    ) -> Any:
        raise NotImplementedError

    @staticmethod
    def _summarize_inbound_media(raw_media: object) -> str:
        """生成安全摘要，便于审计时定位被拒绝输入。"""

        if isinstance(raw_media, str):
            parsed = urlparse(raw_media)
            if parsed.scheme:
                return f"scheme={parsed.scheme} path={unquote(parsed.path)[:200]}"
            return f"path={raw_media[:200]}"
        return f"repr={type(raw_media).__name__}"

    def _log_inbound_media_rejection(
        self,
        *,
        raw_media: object,
        reason: str,
        session_key: str,
        channel: str,
    ) -> None:
        """记录 error 级别拒绝日志，保留审计所需上下文。"""

        logger.error(
            "ACP inbound media rejected rejected_media_type={} reason={} session_key={} channel={} summary={}",
            type(raw_media).__name__,
            reason,
            session_key,
            channel,
            self._summarize_inbound_media(raw_media),
        )

    def _resolve_inbound_media_dir(self) -> Path:
        """解析并约束 inbound materialize 目录始终位于 workspace 内。"""

        workspace = self.workspace.resolve()
        raw_dir = (
            str(getattr(self.acp_config, "inbound_media_dir", "") or "").strip()
            or "Download/channel-inbound/acp-dispatch"
        )
        candidate = Path(raw_dir).expanduser()
        if not candidate.is_absolute():
            candidate = workspace / candidate
        try:
            resolved = candidate.resolve()
        except OSError:
            resolved = (workspace / "Download" / "channel-inbound" / "acp-dispatch").resolve()
        if not resolved.is_relative_to(workspace):
            # 中文注释：materialize 目录语义必须绑定到 workspace，禁止穿透到外部目录。
            logger.warning(
                "ACP inbound media dir escaped workspace configured={} workspace={} fallback=default",
                raw_dir,
                workspace,
            )
            resolved = (workspace / "Download" / "channel-inbound" / "acp-dispatch").resolve()
        return resolved

    def _normalize_inbound_media_paths(
        self,
        media: list[str],
        *,
        session_key: str,
        channel: str,
    ) -> list[Path]:
        """把可安全识别的 inbound media 统一归一化为 workspace 内本地文件。"""

        workspace = self.workspace.resolve()
        normalized: list[Path] = []
        _ = self._resolve_inbound_media_dir()
        for raw_media in media:
            parsed = urlparse(raw_media)
            if parsed.scheme and parsed.scheme != "file":
                # 中文注释：当前 FT 仅接受本地文件路径/file:// URI，其他 scheme 一律拒绝。
                self._log_inbound_media_rejection(
                    raw_media=raw_media,
                    reason="unsupported_scheme",
                    session_key=session_key,
                    channel=channel,
                )
                continue

            if parsed.scheme == "file":
                candidate = Path(unquote(parsed.path)).expanduser()
            else:
                candidate = Path(raw_media).expanduser()
                if not candidate.is_absolute():
                    candidate = workspace / candidate

            try:
                resolved = candidate.resolve()
            except OSError:
                self._log_inbound_media_rejection(
                    raw_media=raw_media,
                    reason="resolve_failed",
                    session_key=session_key,
                    channel=channel,
                )
                continue

            if not resolved.is_relative_to(workspace):
                self._log_inbound_media_rejection(
                    raw_media=raw_media,
                    reason="outside_workspace",
                    session_key=session_key,
                    channel=channel,
                )
                continue
            if not resolved.is_file():
                self._log_inbound_media_rejection(
                    raw_media=raw_media,
                    reason="missing_file",
                    session_key=session_key,
                    channel=channel,
                )
                continue
            normalized.append(resolved)
        return normalized

    def _build_inbound_prompt_blocks(
        self,
        content: str,
        media: list[str],
        *,
        session_key: str,
        channel: str,
    ) -> list[Any]:
        """把 inbound 文本+附件规范化为 ACP prompt blocks。"""

        blocks: list[Any] = [self._acp_text_block(content)]
        for path in self._normalize_inbound_media_paths(
            media,
            session_key=session_key,
            channel=channel,
        ):
            mime_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            # 中文注释：FT 范围统一输出 resource_link，避免 inline resource/image 受模型能力差异影响。
            blocks.append(
                self._acp_resource_link_block(
                    name=path.name,
                    uri=path.as_uri(),
                    mime_type=mime_type,
                    size=path.stat().st_size,
                )
            )
        return blocks
