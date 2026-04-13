"""会话绑定管理器：nanobot 侧会话键 ↔ ACP 侧会话 ID 的持久化映射。

启动对账流程（load_persistent_truth）：
    1. 从磁盘加载持久化绑定
    2. 按 acp_side_session_id 去重（保留 revision 最高的）
    3. 调用 ACP 侧获取真实会话列表，过滤掉不存在的绑定
    4. 持久化合并结果

持久化格式见 session_map.json（version=2, mappings 数组）。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from nanobot.acp.sessionmap.internal.reconcile import fetch_acp_side_session_ids
from nanobot.acp.sessionmap.internal.storage import (
    read_sessionmap_payload,
    write_sessionmap_payload,
)
from nanobot.acp.sessionmap.models import SessionMapBindingEntry
from nanobot.config.paths import get_data_dir

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime


def _now_iso_with_tz() -> str:
    """当前时间 ISO 8601（带时区），用于 updated_at 字段。"""
    return datetime.now().astimezone().isoformat(timespec="seconds")


class SessionMapBindingManager:
    """会话绑定管理器：持久化绑定 CRUD + 启动对账。

    主要状态：
        _entries: nanobot_side_session_key → SessionMapBindingEntry
        _bootstrapped: 是否已完成启动对账
    """

    def __init__(self, owner: ACPRuntime) -> None:
        """初始化。磁盘数据在 load_persistent_truth 中加载，构造时不读取。"""
        self._owner = owner
        self._session_map_file = get_data_dir() / "acp" / "session_map.json"
        self._entries: dict[str, SessionMapBindingEntry] = {}
        self._bootstrapped = False

    def mark_unbootstrapped(self) -> None:
        """重置启动对账标志。不会清除 _entries，需后续调用 load_persistent_truth。"""
        self._bootstrapped = False

    def is_bootstrapped(self) -> bool:
        """返回 binding truth 是否已完成本 runtime 周期的 load + reconcile。"""

        return self._bootstrapped

    def _resolved_acp_cwd(self) -> str:
        """解析 ACP 工作目录：acp_config.cwd 优先，否则 owner.workspace，展开为绝对路径。"""
        cwd = (
            Path(self._owner.acp_config.cwd).expanduser()
            if self._owner.acp_config.cwd
            else self._owner.workspace
        )
        return str(cwd.resolve())

    @staticmethod
    def _parse_entry(raw: object) -> SessionMapBindingEntry:
        """从 JSON 字典解析绑定条目。验证必填字段（cwd/key/acp_id/updatedAt/revision）。

        异常：
            ValueError: 必填字段缺失或格式不对
        """
        if not isinstance(raw, dict):
            raise ValueError("session map entry is not object")

        cwd = raw.get("cwd")
        nanobot_side_session_key = raw.get("nanobotSideSessionKey")
        acp_side_session_id = raw.get("acpSideSessionId")
        updated_at = raw.get("updatedAt")
        revision = raw.get("revision")
        bound_model = raw.get("boundModel")
        bound_agent = raw.get("boundAgent")

        if not isinstance(cwd, str) or not cwd:
            raise ValueError("session map entry cwd is invalid")
        if not isinstance(nanobot_side_session_key, str) or not nanobot_side_session_key:
            raise ValueError("session map entry nanobotSideSessionKey is invalid")
        if not isinstance(acp_side_session_id, str) or not acp_side_session_id:
            raise ValueError("session map entry acpSideSessionId is invalid")
        if not isinstance(updated_at, str) or not updated_at:
            raise ValueError("session map entry updatedAt is invalid")
        if not isinstance(revision, int) or revision < 1:
            raise ValueError("session map entry revision is invalid")

        return SessionMapBindingEntry(
            cwd=cwd,
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
            bound_model=bound_model if isinstance(bound_model, str) and bound_model else None,
            bound_agent=bound_agent if isinstance(bound_agent, str) and bound_agent else None,
            updated_at=updated_at,
            revision=revision,
        )

    def _load_entries_from_disk(self) -> dict[str, SessionMapBindingEntry]:
        """读取磁盘 JSON，只返回当前 cwd 的条目（工作区隔离）。"""
        payload = read_sessionmap_payload(self._session_map_file)
        current_cwd = self._resolved_acp_cwd()
        loaded: dict[str, SessionMapBindingEntry] = {}
        raw_mappings = payload.get("mappings")
        mappings = raw_mappings if isinstance(raw_mappings, list) else []

        for raw in mappings:
            entry = self._parse_entry(raw)
            if entry.cwd == current_cwd:
                loaded[entry.nanobot_side_session_key] = entry

        return loaded

    def persist(self) -> None:
        """原子写磁盘：保留其他 cwd 绑定，替换当前 cwd 绑定。"""
        write_sessionmap_payload(
            self._session_map_file,
            current_cwd=self._resolved_acp_cwd(),
            entries=self._entries,
        )

    def resolve_session_id(self, nanobot_side_session_key: str) -> str | None:
        """按 nanobot_key 查找 acp_side_session_id，不存在返回 None。"""
        entry = self._entries.get(nanobot_side_session_key)
        return entry.acp_side_session_id if entry is not None else None

    def get_bound_selection(self, nanobot_side_session_key: str) -> tuple[str | None, str | None]:
        """返回 (bound_model, bound_agent)。"""
        entry = self._entries.get(nanobot_side_session_key)
        if entry is None:
            return None, None
        return entry.bound_model, entry.bound_agent

    def iter_entries(self) -> list[SessionMapBindingEntry]:
        """返回按 key 排序的条目列表（快照）。"""
        return [entry for _, entry in sorted(self._entries.items())]

    def clear_binding(self, nanobot_side_session_key: str) -> str | None:
        """删除绑定并 persist。返回被删的 acp_side_session_id 或 None。"""
        old = self._entries.pop(nanobot_side_session_key, None)
        self.persist()
        return old.acp_side_session_id if old is not None else None

    def bind_session(self, nanobot_side_session_key: str, acp_side_session_id: str) -> None:
        """建立或更新绑定并 persist。

        处理流程：
            1. 清理冲突：如果 acp_id 已绑到其他 nanobot_key，删除旧绑定
            2. 新建绑定（revision=1），或更新已有绑定（revision+1）
            3. acp_id 未变时幂等返回
        """
        current_cwd = self._resolved_acp_cwd()
        now = _now_iso_with_tz()

        # 清理冲突绑定（保证 acp_side_session_id 唯一性）
        for existing_key, entry in list(self._entries.items()):
            if (
                existing_key != nanobot_side_session_key
                and entry.acp_side_session_id == acp_side_session_id
            ):
                self._entries.pop(existing_key, None)

        old = self._entries.get(nanobot_side_session_key)
        if old is None:
            self._entries[nanobot_side_session_key] = SessionMapBindingEntry(
                cwd=current_cwd,
                nanobot_side_session_key=nanobot_side_session_key,
                acp_side_session_id=acp_side_session_id,
                bound_model=None,
                bound_agent=None,
                updated_at=now,
                revision=1,
            )
            self.persist()
            return

        # 幂等：acp_id 未变时直接返回
        if old.acp_side_session_id == acp_side_session_id:
            return

        old.acp_side_session_id = acp_side_session_id
        old.updated_at = now
        old.revision += 1
        self.persist()

    def update_bound_model(self, nanobot_side_session_key: str, model: str) -> None:
        """更新 bound_model，revision+1，persist。幂等（model 未变时跳过）。"""
        entry = self._entries.get(nanobot_side_session_key)
        if entry is None or entry.bound_model == model:
            return
        entry.bound_model = model
        entry.revision += 1
        entry.updated_at = _now_iso_with_tz()
        self.persist()

    def update_bound_agent(self, nanobot_side_session_key: str, agent: str) -> None:
        """更新 bound_agent，revision+1，persist。幂等（agent 未变时跳过）。"""
        entry = self._entries.get(nanobot_side_session_key)
        if entry is None or entry.bound_agent == agent:
            return
        entry.bound_agent = agent
        entry.revision += 1
        entry.updated_at = _now_iso_with_tz()
        self.persist()

    async def load_persistent_truth(self) -> None:
        """加载持久化绑定并执行启动对账。

        处理流程：
            1. 从磁盘加载条目
            2. 按 acp_side_session_id 去重（保留 revision 最高的）
            3. _reconcile_entries 清理 ACP 侧不存在的绑定
            4. 持久化清理后的真相
            5. 幂等（已 bootstrapped 时跳过）

        边界约束：
            此方法只负责 binding truth 的 load + reconcile。
            不负责激活已有会话，也不会创建 runtime ready entry。
        """
        if self._bootstrapped:
            return

        self._entries = self._load_entries_from_disk()

        # 去重：按 acp_side_session_id 分组，保留 revision 最高的
        deduped: dict[str, SessionMapBindingEntry] = {}
        by_acp_side_session_id: dict[str, SessionMapBindingEntry] = {}
        for entry in self.iter_entries():
            winner = by_acp_side_session_id.get(entry.acp_side_session_id)
            if winner is None or entry.revision >= winner.revision:
                by_acp_side_session_id[entry.acp_side_session_id] = entry

        for entry in by_acp_side_session_id.values():
            deduped[entry.nanobot_side_session_key] = entry

        self._entries = deduped
        await self._reconcile_entries()
        self._bootstrapped = True

    async def _reconcile_entries(self) -> None:
        """对账：清理 ACP 侧不存在的绑定。

        处理流程：
            1. fetch_acp_side_session_ids 获取 ACP 侧真实会话列表
            2. 删除不在列表中的 stale 条目，同步清理 session_runtime_manager 能力缓存
            3. persist 写磁盘
            4. 获取失败时保守跳过（不删任何绑定）
        """
        conn = self._owner._acp_client_conn
        if conn is None or not self._entries:
            return

        listed_ids, authoritative = await fetch_acp_side_session_ids(
            conn, cwd=self._resolved_acp_cwd()
        )

        if not authoritative or not listed_ids:
            return

        stale_keys = [
            key
            for key, entry in self._entries.items()
            if entry.acp_side_session_id not in listed_ids
        ]

        for key in stale_keys:
            stale = self._entries.pop(key, None)
            if stale is not None:
                self._owner.session_runtime_manager.drop_session_capabilities(
                    acp_side_session_id=stale.acp_side_session_id
                )

        if stale_keys:
            self.persist()
