"""Request-scoped pools used by SessionStateManager and ProgressRouter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from nanobot.acp.state.models import ACPOutboundKind, FlushResult


@dataclass(slots=True)
class MessageTextPool:
    """Accumulates agent text both for progress and final text aggregation."""

    text: str = ""
    _last_flushed_text: str = ""

    def accept(self, payload: Any) -> None:
        # 中文注释：文本 pool 要兼容 ACP 可能回放“全量文本”或“增量文本”两种形态，
        # 因此这里做去重拼接，避免 final/progress 都出现重复内容。
        chunk = str(payload or "")
        if not chunk:
            return
        if not self.text:
            self.text = chunk
            return
        if chunk.startswith(self.text):
            self.text = chunk
            return
        if self.text.endswith(chunk):
            return
        self.text += chunk

    def flush(self) -> FlushResult | None:
        # 中文注释：flush 只发布“自上次 flush 之后真正变化过”的内容，
        # 这样 state router 才能稳定地把它镜像成结构化 progress。
        if not self.text or self.text == self._last_flushed_text:
            return None
        self._last_flushed_text = self.text
        return FlushResult(kind=ACPOutboundKind.TEXT, content=self.text)

    def close(self) -> FlushResult | None:
        return self.flush()

    def is_terminal(self) -> bool:
        return False


@dataclass(slots=True)
class MediaPool:
    """Accumulates media paths while preserving order and uniqueness."""

    media_paths: list[str] = field(default_factory=list)
    _last_flushed_count: int = 0

    def accept(self, payload: Any) -> None:
        path = str(payload or "").strip()
        if path and path not in self.media_paths:
            self.media_paths.append(path)

    def flush(self) -> FlushResult | None:
        if len(self.media_paths) <= self._last_flushed_count:
            return None
        # 中文注释：media pool 以“新增资源列表”为 flush 单位，
        # 避免每次 progress 都重复回放全部历史附件。
        flushed = self.media_paths[self._last_flushed_count :]
        self._last_flushed_count = len(self.media_paths)
        return FlushResult(kind=ACPOutboundKind.MEDIA, media=list(flushed))

    def close(self) -> FlushResult | None:
        return self.flush()

    def is_terminal(self) -> bool:
        return False


@dataclass(slots=True)
class ToolPool:
    """Tracks the latest tool lifecycle message for progress mirroring."""

    latest_message: str = ""
    _dirty: bool = False

    def accept(self, payload: Any) -> None:
        message = str(payload or "").strip()
        if not message:
            return
        self.latest_message = message
        self._dirty = True

    def flush(self) -> FlushResult | None:
        if not self._dirty or not self.latest_message:
            return None
        # 中文注释：tool pool 在 flush 时补齐 tool_hint 元数据，
        # 让 state 统一出口仍能保留旧链路需要的 tool hint 语义。
        self._dirty = False
        return FlushResult(
            kind=ACPOutboundKind.TOOL,
            content=self.latest_message,
            metadata={"tool_hint": True, "_tool_hint": True},
        )

    def close(self) -> FlushResult | None:
        return self.flush()

    def is_terminal(self) -> bool:
        return False


@dataclass(slots=True)
class PermissionPool:
    """Tracks the latest permission prompt mirrored to the outside world."""

    prompt: str = ""
    _dirty: bool = False

    def accept(self, payload: Any) -> None:
        prompt = str(payload or "").strip()
        if not prompt:
            return
        self.prompt = prompt
        self._dirty = True

    def flush(self) -> FlushResult | None:
        if not self._dirty or not self.prompt:
            return None
        # 中文注释：permission prompt 也走统一 FlushResult，
        # 这样 permission progress 与 text/tool progress 共用同一条 state 出口。
        self._dirty = False
        return FlushResult(kind=ACPOutboundKind.PERMISSION, content=self.prompt)

    def close(self) -> FlushResult | None:
        return self.flush()

    def is_terminal(self) -> bool:
        return False
