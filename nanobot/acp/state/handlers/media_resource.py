"""媒体资源归一化：把 ACP 媒体块统一收敛成 resource-link 风格的本地落地结果。"""

from __future__ import annotations

import base64
import hashlib
import mimetypes
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

from acp.schema import EmbeddedResourceContentBlock, ImageContentBlock, ResourceContentBlock


@dataclass(frozen=True, slots=True)
class ResolvedMediaResource:
    """归一化媒体资源：无论原始块类型如何，最终都表达成 resource-link 风格的本地文件。"""

    name: str  # 资源名称；优先保留 ACP 原始名称。
    uri: str  # 归一化后的 file:// URI。
    local_path: str  # 供 OutboundMessage.media 使用的本地绝对路径。
    mime_type: str | None = None  # 资源 MIME 类型。
    title: str | None = None  # 资源标题；用于调试与后续 UI 扩展。


class MediaResourceResolver:
    """媒体资源解析器：把 image/resource/embedded-resource 块落地为本地 resource-link。"""

    def __init__(self, *, landing_root: Path) -> None:
        """绑定落地目录；所有需要写盘的嵌入式媒体都由 handler 在这里落地。"""

        self._landing_root = landing_root

    def resolve(self, block: object) -> ResolvedMediaResource | None:
        """解析一个 ACP 媒体块；命中本地路径直接复用，缺路径时落地写盘。"""

        if isinstance(block, ResourceContentBlock):
            return self._resolve_resource_link(block)
        if isinstance(block, ImageContentBlock):
            return self._resolve_image(block)
        if isinstance(block, EmbeddedResourceContentBlock):
            return self._resolve_embedded_resource(block)
        return None

    def _resolve_resource_link(self, block: ResourceContentBlock) -> ResolvedMediaResource | None:
        """解析 resource_link；若不是本地 file URI，则写出一个 `.url` 资源文件。"""

        uri = str(getattr(block, "uri", "") or "").strip()
        name = str(getattr(block, "name", "") or "resource")
        mime_type = self._normalize_mime(
            getattr(block, "mime_type", None) or getattr(block, "mimeType", None)
        )
        title = str(getattr(block, "title", "") or name)
        local_path = self._local_path_from_uri(uri)
        if local_path is None:
            local_path = self._write_bytes(
                payload=uri.encode("utf-8"),
                name=name,
                mime_type="text/uri-list",
                suffix=".url",
            )
            uri = Path(local_path).as_uri()
        return ResolvedMediaResource(
            name=name,
            uri=uri,
            local_path=local_path,
            mime_type=mime_type,
            title=title,
        )

    def _resolve_image(self, block: ImageContentBlock) -> ResolvedMediaResource | None:
        """解析 image 块；优先复用现成 file URI，否则把 base64 数据落地为本地文件。"""

        uri = str(getattr(block, "uri", "") or "").strip()
        mime_type = self._normalize_mime(
            getattr(block, "mime_type", None) or getattr(block, "mimeType", None)
        )
        local_path = self._local_path_from_uri(uri)
        if local_path is None:
            data = str(getattr(block, "data", "") or "")
            if not data:
                return None
            local_path = self._write_bytes(
                payload=base64.b64decode(data),
                name=str(getattr(block, "title", "") or "image"),
                mime_type=mime_type,
            )
            uri = Path(local_path).as_uri()
        return ResolvedMediaResource(
            name=Path(local_path).name,
            uri=uri or Path(local_path).as_uri(),
            local_path=local_path,
            mime_type=mime_type,
            title=str(getattr(block, "title", "") or Path(local_path).name),
        )

    def _resolve_embedded_resource(
        self,
        block: EmbeddedResourceContentBlock,
    ) -> ResolvedMediaResource | None:
        """解析 embedded resource；若未自带本地 file URI，则把文本或 blob 内容写入落地目录。"""

        resource = getattr(block, "resource", None)
        if resource is None:
            return None
        uri = str(getattr(resource, "uri", "") or "").strip()
        mime_type = self._normalize_mime(
            getattr(resource, "mime_type", None) or getattr(resource, "mimeType", None)
        )
        local_path = self._local_path_from_uri(uri)
        if local_path is None:
            text = getattr(resource, "text", None)
            blob = getattr(resource, "blob", None)
            payload: bytes | None = None
            if isinstance(text, str):
                payload = text.encode("utf-8")
            elif isinstance(blob, str):
                payload = base64.b64decode(blob)
            if payload is None:
                return None
            local_path = self._write_bytes(
                payload=payload,
                name=Path(urlparse(uri).path).name or "embedded-resource",
                mime_type=mime_type,
            )
            uri = Path(local_path).as_uri()
        return ResolvedMediaResource(
            name=Path(local_path).name,
            uri=uri or Path(local_path).as_uri(),
            local_path=local_path,
            mime_type=mime_type,
            title=Path(local_path).name,
        )

    @staticmethod
    def _normalize_mime(raw_mime: object) -> str | None:
        """归一化 MIME 文本；空值时返回 None。"""

        value = str(raw_mime or "").strip()
        return value or None

    @staticmethod
    def _local_path_from_uri(uri: str) -> str | None:
        """从 file URI 提取本地路径；非 file URI 一律返回 None，交由落地逻辑处理。"""

        if not uri:
            return None
        parsed = urlparse(uri)
        if parsed.scheme == "file":
            return str(Path(unquote(parsed.path)))
        if not parsed.scheme:
            return str(Path(uri))
        return None

    def _write_bytes(
        self,
        *,
        payload: bytes,
        name: str,
        mime_type: str | None,
        suffix: str | None = None,
    ) -> str:
        """把嵌入式媒体写到请求专属落地目录；文件名由内容摘要稳定生成，避免重复落地。"""

        self._landing_root.mkdir(parents=True, exist_ok=True)
        normalized_name = Path(name or "media").name or "media"
        chosen_suffix = suffix or Path(normalized_name).suffix or self._suffix_from_mime(mime_type)
        digest = hashlib.sha256(payload).hexdigest()[:16]
        target = (
            self._landing_root / f"{Path(normalized_name).stem or 'media'}-{digest}{chosen_suffix}"
        )
        if not target.exists():
            target.write_bytes(payload)
        return str(target)

    @staticmethod
    def _suffix_from_mime(mime_type: str | None) -> str:
        """按 MIME 推断文件扩展名；未知类型统一回退到 `.bin`。"""

        guessed = mimetypes.guess_extension(mime_type or "")
        return guessed or ".bin"
