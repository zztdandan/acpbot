"""Persistent storage helpers for session binding truth."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from nanobot.acp.sessionmap.models import SessionMapBindingEntry


def read_sessionmap_payload(map_file: Path) -> dict[str, Any]:
    """Read the on-disk sessionmap payload with strict schema validation."""

    if not map_file.exists():
        return {"version": 2, "mappings": []}
    payload = json.loads(map_file.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("session map payload must be object")
    if payload.get("version") != 2:
        raise ValueError("session map schema version must be 2")
    mappings = payload.get("mappings")
    if not isinstance(mappings, list):
        raise ValueError("session map mappings must be list")
    return payload


def write_sessionmap_payload(
    map_file: Path,
    *,
    current_cwd: str,
    entries: dict[str, SessionMapBindingEntry],
) -> None:
    """Persist one cwd-scoped session binding view while preserving others."""

    payload = (
        read_sessionmap_payload(map_file) if map_file.exists() else {"version": 2, "mappings": []}
    )
    preserved: list[dict[str, Any]] = []
    for raw in payload.get("mappings", []):
        cwd = raw.get("cwd")
        if cwd != current_cwd:
            preserved.append(raw)
    current = [entry.as_payload() for _, entry in sorted(entries.items())]
    out_payload = {"version": 2, "mappings": preserved + current}
    map_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = map_file.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(out_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp_path.replace(map_file)
