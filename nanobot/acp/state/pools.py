"""单请求状态聚合与进度发布层。"""

from __future__ import annotations

from dataclasses import dataclass, field

from nanobot.acp.state.models import ACPOutboundKind, FlushResult


@dataclass(slots=True)
class MessageTextPool:
    """负责同类事件聚合与刷新。"""

    text: str = ""
    _last_flushed_text: str = ""

    def accept(self, payload: object) -> None:
        """执行该方法定义的处理流程并返回结果。"""
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
        """执行该方法定义的处理流程并返回结果。"""
        if not self.text or self.text == self._last_flushed_text:
            return None
        self._last_flushed_text = self.text
        return FlushResult(kind=ACPOutboundKind.TEXT, content=self.text)

    def close(self) -> FlushResult | None:
        """关闭运行时并释放资源。"""
        return self.flush()

    def is_terminal(self) -> bool:
        """执行该方法定义的处理流程并返回结果。"""
        return False


@dataclass(slots=True)
class MediaPool:
    """负责同类事件聚合与刷新。"""

    media_paths: list[str] = field(default_factory=list)
    _last_flushed_count: int = 0

    def accept(self, payload: object) -> None:
        """执行该方法定义的处理流程并返回结果。"""
        path = str(payload or "").strip()
        if path and path not in self.media_paths:
            self.media_paths.append(path)

    def flush(self) -> FlushResult | None:
        """执行该方法定义的处理流程并返回结果。"""
        if len(self.media_paths) <= self._last_flushed_count:
            return None
        flushed = self.media_paths[self._last_flushed_count :]
        self._last_flushed_count = len(self.media_paths)
        return FlushResult(kind=ACPOutboundKind.MEDIA, media=list(flushed))

    def close(self) -> FlushResult | None:
        """关闭运行时并释放资源。"""
        return self.flush()

    def is_terminal(self) -> bool:
        """执行该方法定义的处理流程并返回结果。"""
        return False


@dataclass(slots=True)
class ToolPool:
    """负责同类事件聚合与刷新。"""

    latest_message: str = ""
    _dirty: bool = False

    def accept(self, payload: object) -> None:
        """执行该方法定义的处理流程并返回结果。"""
        message = str(payload or "").strip()
        if not message:
            return
        self.latest_message = message
        self._dirty = True

    def flush(self) -> FlushResult | None:
        """执行该方法定义的处理流程并返回结果。"""
        if not self._dirty or not self.latest_message:
            return None
        self._dirty = False
        return FlushResult(
            kind=ACPOutboundKind.TOOL,
            content=self.latest_message,
            metadata={"tool_hint": True, "_tool_hint": True},
        )

    def close(self) -> FlushResult | None:
        """关闭运行时并释放资源。"""
        return self.flush()

    def is_terminal(self) -> bool:
        """执行该方法定义的处理流程并返回结果。"""
        return False


@dataclass(slots=True)
class PermissionPool:
    """负责同类事件聚合与刷新。"""

    prompt: str = ""
    _dirty: bool = False

    def accept(self, payload: object) -> None:
        """执行该方法定义的处理流程并返回结果。"""
        prompt = str(payload or "").strip()
        if not prompt:
            return
        self.prompt = prompt
        self._dirty = True

    def flush(self) -> FlushResult | None:
        """执行该方法定义的处理流程并返回结果。"""
        if not self._dirty or not self.prompt:
            return None
        self._dirty = False
        return FlushResult(kind=ACPOutboundKind.PERMISSION, content=self.prompt)

    def close(self) -> FlushResult | None:
        """关闭运行时并释放资源。"""
        return self.flush()

    def is_terminal(self) -> bool:
        """执行该方法定义的处理流程并返回结果。"""
        return False
