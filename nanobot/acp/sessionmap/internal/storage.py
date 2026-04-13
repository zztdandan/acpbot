"""SessionMap 存储工具：JSON 文件的读取与原子写入。

本模块提供了 session_map.json 文件的读写操作，包括：
    1. read_sessionmap_payload: 读取并验证 JSON 文件
    2. write_sessionmap_payload: 原子写入 JSON 文件（先写临时文件再 rename）

存储格式（session_map.json）：
    {
      "version": 2,
      "mappings": [
        {
          "cwd": "/path/to/workspace",
          "nanobotSideSessionKey": "user123:chat456",
          "acpSideSessionId": "abc123",
          "updatedAt": "2026-04-13T10:00:00+08:00",
          "revision": 1
        }
      ]
    }

设计原则：
    - 原子写入：通过 temp + rename 避免写坏文件
    - 工作区隔离：写入时保留其他 cwd 的绑定
    - 版本控制：强制要求 schema version = 2
"""

from __future__ import annotations

import json
from pathlib import Path

from nanobot.acp.contracts import JSONMap
from nanobot.acp.sessionmap.models import SessionMapBindingEntry


def read_sessionmap_payload(map_file: Path) -> JSONMap:
    """从磁盘读取 session_map.json 并验证格式。

    参数：
        map_file: JSON 文件路径（如 ~/.local/share/nanobot/acp/session_map.json）

    返回：
        JSONMap: 解析后的字典（包含 version 和 mappings 字段）

    处理流程：
        1. 如果文件不存在：返回空结构（version=2, mappings=[]）
        2. 读取文件内容并解析为 JSON
        3. 验证必须是 dict 类型
        4. 验证 version 必须是 2
        5. 验证 mappings 必须是 list 类型
        6. 返回解析后的 payload

    异常：
        ValueError: 文件格式不正确时抛出
            - payload 不是 dict
            - version 不是 2
            - mappings 不是 list

    使用场景：
        - binding_manager._load_entries_from_disk 中调用
        - write_sessionmap_payload 中读取现有数据

    注意：
        此函数不解析 mappings 中的具体条目（由 _parse_entry 处理）。
        文件不存在时返回空结构（不抛出异常）。
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

    参数：
        map_file: JSON 文件路径
        current_cwd: 当前工作目录（用于工作区隔离）
        entries: 当前 cwd 的绑定条目字典
            key: nanobot_side_session_key
            value: SessionMapBindingEntry

    处理流程：
        1. 读取现有文件（如果存在）
        2. 从 mappings 中保留其他 cwd 的绑定（preserved）
        3. 将当前 cwd 的新绑定序列化为 payload
        4. 合并：preserved + current
        5. 原子写入：先写 .tmp 文件，再 rename

    原子写入策略：
        1. 写入 session_map.json.tmp
        2. 调用 tmp_path.replace(map_file)
        3. replace 是原子操作（POSIX rename）

    工作区隔离：
        - 只替换 current_cwd 的绑定
        - 保留其他 cwd 的绑定不变
        - 保证多工作区场景下的数据安全

    使用场景：
        - binding_manager.persist 中调用
        - bind_session / clear_binding 后持久化

    注意：
        此函数是同步的（阻塞 IO）。
        文件目录不存在时会自动创建（mkdir -p）。
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
