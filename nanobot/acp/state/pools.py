"""Request-scoped pools used by SessionStateManager and ProgressRouter."""

from __future__ import annotations

from dataclasses import dataclass, field

from nanobot.acp.state.models import ACPOutboundKind, FlushResult


@dataclass(slots=True)
class MessageTextPool:
    """Accumulates agent text both for progress and final text aggregation."""

    text: str = ""
    _last_flushed_text: str = ""

    def accept(self, payload: object) -> None:
        # Text updates can arrive as full snapshots or incremental chunks, so this pool
        # deduplicates aggressively to avoid repeated progress and final content.
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
        # Only publish content that changed since the last flush so the state router can
        # mirror stable structured progress.
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

    def accept(self, payload: object) -> None:
        path = str(payload or "").strip()
        if path and path not in self.media_paths:
            self.media_paths.append(path)

    def flush(self) -> FlushResult | None:
        if len(self.media_paths) <= self._last_flushed_count:
            return None
        # Flush media as only the newly added slice so progress does not replay the full
        # attachment history every time.
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

    def accept(self, payload: object) -> None:
        message = str(payload or "").strip()
        if not message:
            return
        self.latest_message = message
        self._dirty = True

    def flush(self) -> FlushResult | None:
        if not self._dirty or not self.latest_message:
            return None
        # Reattach tool_hint metadata during flush so the unified state exit keeps the
        # legacy tool-hint semantics that downstream consumers still expect.
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

    def accept(self, payload: object) -> None:
        prompt = str(payload or "").strip()
        if not prompt:
            return
        self.prompt = prompt
        self._dirty = True

    def flush(self) -> FlushResult | None:
        if not self._dirty or not self.prompt:
            return None
        # Permission prompts also flow through the unified FlushResult path so permission,
        # text, and tool progress all share the same state-owned output boundary.
        self._dirty = False
        return FlushResult(kind=ACPOutboundKind.PERMISSION, content=self.prompt)

    def close(self) -> FlushResult | None:
        return self.flush()

    def is_terminal(self) -> bool:
        return False
