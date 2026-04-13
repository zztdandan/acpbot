"""会话绑定真相与运行态映射层。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Awaitable, Callable, cast

from loguru import logger

from nanobot.acp.contracts import ACPSessionPayload
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
    """执行该方法定义的处理流程并返回结果。"""
    return datetime.now().astimezone().isoformat(timespec="seconds")


class SessionMapBindingManager:
    """负责对应领域状态与流程编排。"""

    def __init__(self, owner: ACPRuntime) -> None:
        """初始化当前对象并建立必要状态。"""
        self._owner = owner
        self._session_map_file = get_data_dir() / "acp" / "session_map.json"
        self._entries: dict[str, SessionMapBindingEntry] = {}
        self._bootstrapped = False

    def mark_unbootstrapped(self) -> None:
        """执行该方法定义的处理流程并返回结果。"""
        self._bootstrapped = False

    def _resolved_acp_cwd(self) -> str:
        """执行该方法定义的处理流程并返回结果。"""
        cwd = (
            Path(self._owner.acp_config.cwd).expanduser()
            if self._owner.acp_config.cwd
            else self._owner.workspace
        )
        return str(cwd.resolve())

    @staticmethod
    def _parse_entry(raw: object) -> SessionMapBindingEntry:
        """执行该方法定义的处理流程并返回结果。"""
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
        """执行该方法定义的处理流程并返回结果。"""
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
        """持久化当前绑定真相。"""
        write_sessionmap_payload(
            self._session_map_file,
            current_cwd=self._resolved_acp_cwd(),
            entries=self._entries,
        )

    def resolve_session_id(self, nanobot_side_session_key: str) -> str | None:
        """执行该方法定义的处理流程并返回结果。"""
        entry = self._entries.get(nanobot_side_session_key)
        return entry.acp_side_session_id if entry is not None else None

    def get_bound_selection(self, nanobot_side_session_key: str) -> tuple[str | None, str | None]:
        """执行该方法定义的处理流程并返回结果。"""
        entry = self._entries.get(nanobot_side_session_key)
        if entry is None:
            return None, None
        return entry.bound_model, entry.bound_agent

    def iter_entries(self) -> list[SessionMapBindingEntry]:
        """执行该方法定义的处理流程并返回结果。"""

        return [entry for _, entry in sorted(self._entries.items())]

    def clear_binding(self, nanobot_side_session_key: str) -> str | None:
        """清除指定会话的绑定真相。"""
        old = self._entries.pop(nanobot_side_session_key, None)
        self.persist()
        return old.acp_side_session_id if old is not None else None

    def bind_session(self, nanobot_side_session_key: str, acp_side_session_id: str) -> None:
        """建立业务会话与协议会话绑定。"""
        current_cwd = self._resolved_acp_cwd()
        now = _now_iso_with_tz()
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
        if old.acp_side_session_id == acp_side_session_id:
            return
        old.acp_side_session_id = acp_side_session_id
        old.updated_at = now
        old.revision += 1
        self.persist()

    def update_bound_model(self, nanobot_side_session_key: str, model: str) -> None:
        """更新绑定模型信息。"""
        entry = self._entries.get(nanobot_side_session_key)
        if entry is None or entry.bound_model == model:
            return
        entry.bound_model = model
        entry.revision += 1
        entry.updated_at = _now_iso_with_tz()
        self.persist()

    def update_bound_agent(self, nanobot_side_session_key: str, agent: str) -> None:
        """更新绑定代理信息。"""
        entry = self._entries.get(nanobot_side_session_key)
        if entry is None or entry.bound_agent == agent:
            return
        entry.bound_agent = agent
        entry.revision += 1
        entry.updated_at = _now_iso_with_tz()
        self.persist()

    async def activate_session(
        self, nanobot_side_session_key: str, acp_side_session_id: str
    ) -> tuple[bool, ACPSessionPayload | None]:
        """执行该方法定义的处理流程并返回结果。"""

        conn = self._owner._acp_client_conn
        if conn is None:
            raise RuntimeError("ACP connection is not available")

        cwd = self._resolved_acp_cwd()
        resume_session = cast(
            Callable[..., Awaitable[ACPSessionPayload]] | None,
            getattr(conn, "resume_session", None),
        )
        load_session = cast(
            Callable[..., Awaitable[ACPSessionPayload]] | None,
            getattr(conn, "load_session", None),
        )

        if resume_session is None and load_session is None:
            return True, None

        response: ACPSessionPayload | None = None
        if resume_session is not None:
            try:
                response = await resume_session(cwd=cwd, session_id=acp_side_session_id)
            except Exception as exc:
                logger.debug(
                    "ACP resume_session failed nanobot_side_session_key={} acp_side_session_id={} error_type={} error={}",
                    nanobot_side_session_key,
                    acp_side_session_id,
                    type(exc).__name__,
                    exc,
                )
        if response is None and load_session is not None:
            try:
                response = await load_session(cwd=cwd, session_id=acp_side_session_id)
            except Exception as exc:
                logger.warning(
                    "ACP existing session activation failed nanobot_side_session_key={} acp_side_session_id={} error_type={} error={}",
                    nanobot_side_session_key,
                    acp_side_session_id,
                    type(exc).__name__,
                    exc,
                )
                return False, None
        replay_ok = await self._replay_after_activation(
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
        )
        return replay_ok, response

    async def _replay_after_activation(
        self,
        *,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
    ) -> bool:
        """执行该方法定义的处理流程并返回结果。"""
        conn = self._owner._acp_client_conn
        if conn is None:
            raise RuntimeError("ACP connection is not available")

        entry = self._entries.get(nanobot_side_session_key)
        bound_model = entry.bound_model if entry is not None else None
        bound_agent = entry.bound_agent if entry is not None else None
        model = bound_model or self._owner.acp_config.default_model
        agent = bound_agent or self._owner.acp_config.default_mode
        set_session_model = cast(
            Callable[..., Awaitable[ACPSessionPayload]] | None,
            getattr(conn, "set_session_model", None),
        )
        set_session_mode = cast(
            Callable[..., Awaitable[ACPSessionPayload]] | None,
            getattr(conn, "set_session_mode", None),
        )

        try:
            if model and set_session_model is not None:
                await set_session_model(model_id=model, session_id=acp_side_session_id)
            if agent and set_session_mode is not None:
                await set_session_mode(mode_id=agent, session_id=acp_side_session_id)
            return True
        except Exception as exc:
            logger.warning(
                "ACP activation replay failed nanobot_side_session_key={} acp_side_session_id={} error_type={} error={}",
                nanobot_side_session_key,
                acp_side_session_id,
                type(exc).__name__,
                exc,
            )
            return False

    async def bootstrap(self) -> list[str]:
        """执行该方法定义的处理流程并返回结果。"""

        if self._bootstrapped:
            return []
        self._entries = self._load_entries_from_disk()
        await self._reconcile_entries()
        ok_keys: list[str] = []
        for nanobot_side_session_key, entry in sorted(self._entries.items()):
            activated, _ = await self.activate_session(
                nanobot_side_session_key,
                entry.acp_side_session_id,
            )
            if activated:
                ok_keys.append(nanobot_side_session_key)
        self._bootstrapped = True
        return ok_keys

    async def load_persistent_truth(self) -> None:
        """持久化当前绑定真相。"""

        if self._bootstrapped:
            return
        self._entries = self._load_entries_from_disk()
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
        """执行该方法定义的处理流程并返回结果。"""
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
