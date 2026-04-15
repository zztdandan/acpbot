"""审计事件持久化层（当前后端：本地 jsonl 文件）。"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

from nanobot.acp.observability.queue import ObservabilityEvent

LOCAL_FILE_AUDIT_BACKEND = "local_file"


def initialize_audit_backend(*, backend: str, base_dir: Path) -> Path | None:
    """初始化审计后端并返回运行目录；便于后续扩展数据库/远端后端。"""

    if backend != LOCAL_FILE_AUDIT_BACKEND:
        return None
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    run_dir = (base_dir / ".nanobot-logs" / f"{timestamp}-observability").resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _sanitize_path_segment(value: str) -> str:
    """把动态字段转换为可安全落盘的目录/文件名。"""

    sanitized = re.sub(r"[^0-9A-Za-z._-]", "_", value.strip())
    return sanitized or "-"


def _resolve_audit_file_path(event: ObservabilityEvent, *, run_dir: Path) -> Path:
    """根据 scope/event/session/request 规则构造 audit 文件路径。"""

    scope = _sanitize_path_segment(str(event.scope))
    event_name = _sanitize_path_segment(str(event.event))
    nanobot_session = _sanitize_path_segment(event.nanobot_side_session_key or "-")
    payload = event.payload if isinstance(event.payload, dict) else {}
    unique_name = payload.get("unikey") or event.request_key or "unknown"
    file_name = f"{_sanitize_path_segment(str(unique_name))}.jsonl"
    return run_dir / scope / event_name / nanobot_session / file_name


def write_audit_event(
    event: ObservabilityEvent,
    *,
    backend: str,
    run_dir: Path | None,
) -> None:
    """写入审计事件；当前 local_file 后端仅落 payload 到 jsonl。"""

    if backend != LOCAL_FILE_AUDIT_BACKEND or run_dir is None:
        return
    file_path = _resolve_audit_file_path(event, run_dir=run_dir)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    payload = event.payload if isinstance(event.payload, dict) else {}
    with file_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
