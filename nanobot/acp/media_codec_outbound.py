"""ACP outbound media 解码能力。"""

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


class _ACPOutboundMediaCodecMixin:
    """封装 ACP session_update 非文本块 -> 本地 filepath 落盘逻辑。"""

    @staticmethod
    def _mapping_like(value: Any) -> dict[str, Any] | None:
        """把 dict / Pydantic model / 简单对象尽量视作映射读取。"""

        if isinstance(value, dict):
            return value
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            try:
                dumped = model_dump(by_alias=True, exclude_none=True)
            except TypeError:
                dumped = model_dump()
            if isinstance(dumped, dict):
                return dumped
        if hasattr(value, "__dict__"):
            dumped = dict(vars(value))
            if dumped:
                return dumped
        return None

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

    def _extract_tool_output_media_path(self, update: Any) -> str | None:
        """从工具完成事件的 rawOutput metadata 中兜底提取附件。"""

        raw_output = getattr(update, "raw_output", None) or getattr(update, "rawOutput", None)
        raw_output_map = self._mapping_like(raw_output)
        metadata = getattr(raw_output, "metadata", None)
        metadata_map = self._mapping_like(metadata)
        if metadata_map is None and raw_output_map is not None:
            metadata_map = self._mapping_like(raw_output_map.get("metadata"))
        if metadata_map is None:
            return None
        acp_send_file = self._mapping_like(metadata_map.get("acp_send_file"))
        if acp_send_file is None:
            return None
        source_raw = acp_send_file.get("file")
        if not isinstance(source_raw, str) or not source_raw.strip():
            return None
        source = Path(source_raw).expanduser()
        if not source.is_file():
            return None
        filename = acp_send_file.get("filename")
        if not isinstance(filename, str) or not filename.strip():
            filename = source.name
        try:
            # 中文注释：部分 ACP 后端不会把 plugin attachment 回放成 resource_link，
            # 这里只在工具完成态从 rawOutput metadata 做一次等价兜底，保持 channel 仍收到本地 filepath。
            return self._write_outbound_file(data=source.read_bytes(), filename=filename)
        except OSError as e:
            logger.warning("ACP tool media extraction failed file={} err={}", source, e)
            return None

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
                    str(
                        getattr(content, "name", "")
                        or Path(unquote(parsed.path)).name
                        or "resource.bin"
                    )
                )
                if parsed.scheme == "file":
                    source = Path(unquote(parsed.path))
                    if source.is_file():
                        return self._write_outbound_file(
                            data=source.read_bytes(), filename=filename
                        )
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
                filename = self._sanitize_filename(
                    Path(unquote(parsed.path)).name or "resource.bin"
                )
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
