"""Session binding truth manager for ACP runtime."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, cast

from loguru import logger

from nanobot.acp.session_caps import _update_caps_from_session_payload
from nanobot.acp.sessionmap.models import SessionMapBindingEntry
from nanobot.acp.sessionmap.reconcile import fetch_acp_side_session_ids
from nanobot.acp.sessionmap.storage import read_sessionmap_payload, write_sessionmap_payload


def _now_iso_with_tz() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


class SessionMapBindingManager:
    """Owns persistent session binding truth and activation replay logic."""

    def __init__(self, owner: Any) -> None:
        self._owner = owner
        self._entries: dict[str, SessionMapBindingEntry] = {}
        self._bootstrapped = False

    def mark_unbootstrapped(self) -> None:
        self._bootstrapped = False

    def _resolved_acp_cwd(self) -> str:
        cwd = (
            Path(self._owner.acp_config.cwd).expanduser()
            if self._owner.acp_config.cwd
            else self._owner.workspace
        )
        return str(cwd.resolve())

    @staticmethod
    def _parse_entry(raw: Any) -> SessionMapBindingEntry:
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
        payload = read_sessionmap_payload(self._owner._session_map_file)
        current_cwd = self._resolved_acp_cwd()
        loaded: dict[str, SessionMapBindingEntry] = {}
        for raw in payload.get("mappings", []):
            entry = self._parse_entry(raw)
            if entry.cwd == current_cwd:
                loaded[entry.nanobot_side_session_key] = entry
        return loaded

    def persist(self) -> None:
        # 中文注释：binding manager 是跨 runtime 生命周期的真相源，
        # 所有会影响后续 replay 的选择与绑定变化都必须及时落盘。
        write_sessionmap_payload(
            self._owner._session_map_file,
            current_cwd=self._resolved_acp_cwd(),
            entries=self._entries,
        )

    def resolve_session_id(self, nanobot_side_session_key: str) -> str | None:
        entry = self._entries.get(nanobot_side_session_key)
        return entry.acp_side_session_id if entry is not None else None

    def get_bound_selection(self, nanobot_side_session_key: str) -> tuple[str | None, str | None]:
        entry = self._entries.get(nanobot_side_session_key)
        if entry is None:
            return None, None
        return entry.bound_model, entry.bound_agent

    def iter_entries(self) -> list[SessionMapBindingEntry]:
        """Return a stable snapshot of current binding truth entries."""

        return [entry for _, entry in sorted(self._entries.items())]

    def clear_binding(self, nanobot_side_session_key: str) -> str | None:
        old = self._entries.pop(nanobot_side_session_key, None)
        self.persist()
        return old.acp_side_session_id if old is not None else None

    def bind_session(self, nanobot_side_session_key: str, acp_side_session_id: str) -> None:
        current_cwd = self._resolved_acp_cwd()
        now = _now_iso_with_tz()
        # 中文注释：同一个 acp_side_session_id 只能被一个 nanobot 会话拥有，
        # 这里先清理反向冲突，维持 binding truth 的一对一语义。
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
        entry = self._entries.get(nanobot_side_session_key)
        if entry is None or entry.bound_model == model:
            return
        entry.bound_model = model
        entry.revision += 1
        entry.updated_at = _now_iso_with_tz()
        self.persist()

    def update_bound_agent(self, nanobot_side_session_key: str, agent: str) -> None:
        entry = self._entries.get(nanobot_side_session_key)
        if entry is None or entry.bound_agent == agent:
            return
        entry.bound_agent = agent
        entry.revision += 1
        entry.updated_at = _now_iso_with_tz()
        self.persist()

    async def activate_session(
        self, nanobot_side_session_key: str, acp_side_session_id: str
    ) -> bool:
        """Resume/load the ACP-side session and replay bound selection if possible."""

        # 中文注释：binding manager 只 owner 持久化真相与激活回放；
        # 激活成功后是否成为当前 runtime 的 ready session，则由 SessionRuntimeManager 决定。
        conn = self._owner._acp_client_conn
        if conn is None:
            raise RuntimeError("ACP connection is not available")

        cwd = self._resolved_acp_cwd()
        resume_session = cast(
            Callable[..., Awaitable[Any]] | None, getattr(conn, "resume_session", None)
        )
        load_session = cast(
            Callable[..., Awaitable[Any]] | None, getattr(conn, "load_session", None)
        )

        if resume_session is None and load_session is None:
            return True

        response: Any | None = None
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
                return False
        if response is not None:
            _update_caps_from_session_payload(
                self._owner._session_caps, acp_side_session_id, response
            )
        return await self._replay_after_activation(
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
        )

    async def _replay_after_activation(
        self,
        *,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
    ) -> bool:
        # 中文注释：resume/load 只恢复 ACP 侧会话实体；真正让用户感知一致的，
        # 是把 bound model / bound agent 再次回放到新连接周期里。
        conn = self._owner._acp_client_conn
        if conn is None:
            raise RuntimeError("ACP connection is not available")

        entry = self._entries.get(nanobot_side_session_key)
        bound_model = entry.bound_model if entry is not None else None
        bound_agent = entry.bound_agent if entry is not None else None
        model = bound_model or self._owner.acp_config.default_model
        agent = bound_agent or self._owner.acp_config.default_mode
        set_session_model = cast(
            Callable[..., Awaitable[Any]] | None, getattr(conn, "set_session_model", None)
        )
        set_session_mode = cast(
            Callable[..., Awaitable[Any]] | None, getattr(conn, "set_session_mode", None)
        )

        try:
            caps = self._owner._session_caps.setdefault(
                acp_side_session_id, self._owner.new_session_capabilities()
            )
            if model and set_session_model is not None:
                await set_session_model(model_id=model, session_id=acp_side_session_id)
                caps.current_model = model
            if agent and set_session_mode is not None:
                await set_session_mode(mode_id=agent, session_id=acp_side_session_id)
                caps.current_agent = agent
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
        """Load persisted bindings, reconcile them, and replay activation once."""

        if self._bootstrapped:
            return []
        self._entries = self._load_entries_from_disk()
        await self._reconcile_entries()
        ok_keys: list[str] = []
        for nanobot_side_session_key, entry in sorted(self._entries.items()):
            if await self.activate_session(nanobot_side_session_key, entry.acp_side_session_id):
                ok_keys.append(nanobot_side_session_key)
        self._bootstrapped = True
        return ok_keys

    async def load_persistent_truth(self) -> None:
        """Load and reconcile binding truth without creating runtime-ready entries."""

        if self._bootstrapped:
            return
        # 中文注释：这里故意只加载/对账真相，不自动激活 ready session；
        # 这样 runtime rebuild 后仍然遵守“下一条请求再懒 ensure”的设计原则。
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
                self._owner._session_caps.pop(stale.acp_side_session_id, None)
        if stale_keys:
            self.persist()
