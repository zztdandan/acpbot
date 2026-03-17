"""ACP 分发模块的内部状态对象。

本文件只放轻量状态类型，避免主分发器文件继续膨胀。
"""

from __future__ import annotations

from typing import Awaitable, Callable


class _StreamState:
    """单个 ACP session 的流式输出聚合状态。"""

    def __init__(self, on_progress: Callable[..., Awaitable[None]] | None = None):
        # text 保存当前会话的累积文本；on_progress 用于转发进度回调。
        self.text = ""
        self.on_progress = on_progress

    def merge_text(self, chunk: str) -> None:
        """合并模型输出分片，尽量去重避免重复拼接。"""
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

    def final(self) -> str:
        """返回清理后的最终文本。"""
        return self.text.strip()


class _ACPDispatchError(RuntimeError):
    """ACP 调用失败时抛出的内部异常，允许携带部分可用响应。"""

    def __init__(self, partial_response: str = "") -> None:
        super().__init__("ACP dispatch failed")
        self.partial_response = partial_response


class _SessionCapabilities:
    """缓存 ACP 返回的会话能力信息（模型/agent 列表与当前值）。"""

    def __init__(self) -> None:
        self.available_models: list[str] = []
        self.current_model: str | None = None
        self.available_agents: list[str] = []
        self.current_agent: str | None = None
