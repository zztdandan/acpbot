"""Placeholder base handler protocol for future state handler expansion."""

from __future__ import annotations

from typing import Any, Protocol


class StateHandler(Protocol):
    """Minimal handler protocol kept for future handler extraction."""

    def consume(self, payload: Any) -> None: ...
