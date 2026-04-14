"""SessionMap 存储工具：JSON 文件的读取与原子写入。"""

from __future__ import annotations

import json
from pathlib import Path

from nanobot.acp.contracts import JSONMap
from nanobot.acp.sessionmap.models import SessionMapBindingEntry


def read_sessionmap_payload(map_file: Path) -> JSONMap:
    """从磁盘读取 session_map.json 并验证格式。

    处理流程：
        1. 文件不存在则返回空结构
        2. 解析 JSON 并验证：必须是 dict，version=2，mappings 是 list

    异常：
        ValueError: 格式不正确时抛出（不是 dict / version 不是 2 / mappings 不是 list）
    """

    # 文件不存在时返回空结构（首次使用）
    if not map_file.exists():
        return {"version": 2, "mappings": []}

    # 读取并解析 JSON
    payload = json.loads(map_file.read_text(encoding="utf-8"))

    # 验证格式：必须是 dict
    if not isinstance(payload, dict):
        raise ValueError("session map payload must be object")

    # 验证版本：必须是 v2（确保向前兼容）
    if payload.get("version") != 2:
        raise ValueError("session map schema version must be 2")

    # 验证 mappings：必须是 list
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
    """原子写入 session_map.json（保留其他 cwd 的绑定）。

    处理流程：
        1. 读取现有文件（存在时）
        2. 保留非当前 cwd 的绑定（工作区隔离）
        3. 合并：preserved + 当前 cwd 的新绑定
        4. 原子写入：先写 .tmp 文件再 rename
    """

    # 步骤 1: 读取现有文件（如果存在）
    payload = (
        read_sessionmap_payload(map_file) if map_file.exists() else {"version": 2, "mappings": []}
    )

    # 步骤 2: 保留其他 cwd 的绑定（工作区隔离）
    preserved: list[JSONMap] = []
    raw_mappings = payload.get("mappings")
    mappings = raw_mappings if isinstance(raw_mappings, list) else []
    for raw in mappings:
        if not isinstance(raw, dict):
            continue
        cwd = raw.get("cwd")
        # 只保留非当前 cwd 的绑定
        if cwd != current_cwd:
            preserved.append(raw)

    # 步骤 3: 序列化当前 cwd 的新绑定
    current = [entry.as_payload() for _, entry in sorted(entries.items())]

    # 步骤 4: 合并并写入
    out_payload = {"version": 2, "mappings": preserved + current}

    # 确保目录存在（mkdir -p）
    map_file.parent.mkdir(parents=True, exist_ok=True)

    # 步骤 5: 原子写入（temp + rename）
    tmp_path = map_file.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(out_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp_path.replace(map_file)  # POSIX rename 是原子操作
