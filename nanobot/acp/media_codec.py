"""ACP dispatcher 的文件传输编解码辅助。

该模块只负责：
1) inbound media filepath -> ACP prompt blocks；
2) ACP session_update 文件块 -> 本地落盘 filepath。
"""

from __future__ import annotations

import base64
import binascii
import mimetypes
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from loguru import logger


class _ACPFileTransportMixin:
    """为 ACPDispatcher 提供文件传输相关能力（与调度主流程解耦）。"""

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

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        """移除危险字符，避免 ACP 附件落盘覆盖或目录穿越。"""
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", (name or "").strip())
        return safe or "attachment.bin"

    def _outbound_download_dir(self) -> Path:
        """ACP 出站附件统一落盘目录。"""
        download_dir = Path(self._resolved_acp_cwd()) / "Download" / "acp-outbound"
        download_dir.mkdir(parents=True, exist_ok=True)
        return download_dir

    def _write_outbound_file(self, *, data: bytes, filename: str) -> str:
        """把 ACP 回传附件落盘为可追踪文件名，并返回路径字符串。"""
        safe_name = self._sanitize_filename(filename)
        short_hash = f"{(hash(data) & 0xFFFFFFFF):08x}"
        stamped_name = f"{int(time.time() * 1000)}-{short_hash}-{safe_name}"
        path = self._outbound_download_dir() / stamped_name
        path.write_bytes(data)
        return str(path)

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

    def _extract_agent_media_path(self, session_id: str, content: Any) -> str | None:
        """把 ACP agent_message 的非文本 block 统一落盘并返回 filepath。"""
        del session_id
        content_type = str(getattr(content, "type", "") or "")
        if not content_type:
            return None

        try:
            if content_type == "resource_link":
                uri = str(getattr(content, "uri", "") or "")
                if not uri:
                    return None
                parsed = urlparse(uri)
                filename = self._sanitize_filename(
                    str(getattr(content, "name", "") or Path(unquote(parsed.path)).name or "resource.bin")
                )
                if parsed.scheme == "file":
                    source = Path(unquote(parsed.path))
                    if source.is_file():
                        return self._write_outbound_file(data=source.read_bytes(), filename=filename)
                # 中文注释：非 file:// 资源（如 http 链接）暂存为说明文件，保持 channel 仍消费 filepath。
                marker = f"resource_link: {uri}\n".encode("utf-8")
                return self._write_outbound_file(data=marker, filename=f"{filename}.url.txt")

            if content_type == "image":
                b64_data = str(getattr(content, "data", "") or "")
                if not b64_data:
                    return None
                mime_type = str(getattr(content, "mime_type", "") or "image/png")
                ext = mimetypes.guess_extension(mime_type) or ".bin"
                try:
                    raw = base64.b64decode(b64_data, validate=True)
                except (ValueError, binascii.Error):
                    return None
                return self._write_outbound_file(data=raw, filename=f"acp-image{ext}")

            if content_type == "resource":
                resource = getattr(content, "resource", None)
                if resource is None:
                    return None
                resource_uri = str(getattr(resource, "uri", "") or "resource://acp")
                mime_type = str(getattr(resource, "mime_type", "") or "application/octet-stream")
                parsed = urlparse(resource_uri)
                filename = self._sanitize_filename(Path(unquote(parsed.path)).name or "resource.bin")
                text_value = getattr(resource, "text", None)
                if isinstance(text_value, str):
                    ext = mimetypes.guess_extension(mime_type) or ".txt"
                    return self._write_outbound_file(
                        data=text_value.encode("utf-8"),
                        filename=f"{Path(filename).stem}{ext}",
                    )
                blob_value = getattr(resource, "blob", None)
                if isinstance(blob_value, str):
                    try:
                        raw = base64.b64decode(blob_value, validate=True)
                    except (ValueError, binascii.Error):
                        return None
                    ext = mimetypes.guess_extension(mime_type) or ".bin"
                    return self._write_outbound_file(
                        data=raw,
                        filename=f"{Path(filename).stem}{ext}",
                    )
        except OSError as e:
            logger.warning("ACP media file extraction failed type={} err={}", content_type, e)
            return None
        except Exception as e:
            logger.warning("ACP media decode failed type={} err={}", content_type, e)
            return None
        logger.debug("ACP session_update ignored non-media content type={}", content_type)
        return None
