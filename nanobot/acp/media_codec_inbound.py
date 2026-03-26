"""ACP inbound media 编码能力。"""

from __future__ import annotations

import base64
import mimetypes
from pathlib import Path
from typing import Any

from loguru import logger


class _ACPInboundMediaCodecMixin:
    """封装 inbound media filepath -> ACP prompt blocks 逻辑。"""

    _INLINE_TEXT_BYTES_LIMIT = 512 * 1024
    _INLINE_BLOB_BYTES_LIMIT = 2 * 1024 * 1024

    @staticmethod
    def _is_text_like_mime(path: Path, mime_type: str | None) -> bool:
        """判断文件是否适合按文本内嵌到 ACP resource。"""
        if mime_type and (
            mime_type.startswith("text/")
            or mime_type
            in {
                "application/json",
                "application/xml",
                "application/x-yaml",
                "application/yaml",
                "application/javascript",
            }
        ):
            return True
        return path.suffix.lower() in {
            ".txt",
            ".md",
            ".markdown",
            ".json",
            ".jsonl",
            ".yaml",
            ".yml",
            ".xml",
            ".csv",
            ".py",
            ".js",
            ".ts",
            ".tsx",
            ".jsx",
            ".java",
            ".go",
            ".rs",
            ".c",
            ".cpp",
            ".h",
            ".hpp",
            ".sh",
            ".bash",
            ".zsh",
            ".toml",
            ".ini",
            ".cfg",
            ".conf",
            ".log",
        }

    def _build_inbound_prompt_blocks(self, content: str, media: list[str]) -> list[Any]:
        """把 inbound 文本+附件规范化为 ACP prompt blocks。"""
        blocks: list[Any] = [self._acp_text_block(content)]
        for raw_path in media:
            path = Path(str(raw_path)).expanduser()
            if not path.is_file():
                logger.warning("ACP inbound media skipped (not file): {}", raw_path)
                continue
            try:
                abs_path = path.resolve()
            except Exception as e:
                logger.warning("ACP inbound media resolve failed path={} err={}", raw_path, e)
                continue
            try:
                file_size = abs_path.stat().st_size
            except OSError as e:
                logger.warning("ACP inbound media stat failed path={} err={}", abs_path, e)
                continue

            mime_type = mimetypes.guess_type(abs_path.name)[0] or "application/octet-stream"
            file_uri = abs_path.as_uri()
            file_name = abs_path.name

            try:
                if mime_type.startswith("image/") and file_size <= self._INLINE_BLOB_BYTES_LIMIT:
                    # 中文注释：图片走 image block，兼容具备多模态能力的 ACP/OpenCode 模型。
                    b64 = base64.b64encode(abs_path.read_bytes()).decode("ascii")
                    blocks.append(
                        self._acp_image_block(
                            data=b64,
                            mime_type=mime_type,
                            uri=file_uri,
                        )
                    )
                    continue
                if (
                    self._is_text_like_mime(abs_path, mime_type)
                    and file_size <= self._INLINE_TEXT_BYTES_LIMIT
                ):
                    # 中文注释：文本文件优先内嵌为 resource(text)，减少 ACP 侧额外拉取文件步骤。
                    text_data = abs_path.read_text(encoding="utf-8", errors="replace")
                    resource = self._acp_embedded_text_resource(
                        uri=file_uri,
                        text=text_data,
                        mime_type=mime_type,
                    )
                    blocks.append(self._acp_resource_block(resource))
                    continue
                if file_size <= self._INLINE_BLOB_BYTES_LIMIT:
                    # 中文注释：小体积二进制可直接嵌入 blob，避免 file:// 访问权限差异导致失败。
                    b64 = base64.b64encode(abs_path.read_bytes()).decode("ascii")
                    resource = self._acp_embedded_blob_resource(
                        uri=file_uri,
                        blob=b64,
                        mime_type=mime_type,
                    )
                    blocks.append(self._acp_resource_block(resource))
                    continue
            except Exception as e:
                logger.warning(
                    "ACP inbound media inline conversion failed path={} mime={} err={}",
                    abs_path,
                    mime_type,
                    e,
                )

            # 中文注释：超大文件或内嵌失败时降级为 resource_link，避免整轮 prompt 失败。
            blocks.append(
                self._acp_resource_link_block(
                    name=file_name,
                    uri=file_uri,
                    mime_type=mime_type,
                    size=file_size,
                )
            )
        return blocks
