"""ACP session_map manager：统一会话绑定状态与激活回放。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, cast

from loguru import logger

from nanobot.acp.session_caps import _update_caps_from_session_payload
from nanobot.acp.state import _SessionCapabilities


def _now_iso_with_tz() -> str:
    """返回带数值时区偏移的 ISO8601 时间。"""

    return datetime.now().astimezone().isoformat(timespec="seconds")


@dataclass
class _SessionMapBindingEntry:
    """session_map 单条绑定记录。"""

    cwd: str
    session_key: str
    session_id: str
    bound_model: str | None
    bound_agent: str | None
    updated_at: str
    revision: int

    def as_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "cwd": self.cwd,
            "nanobotSideSessionKey": self.session_key,
            "acpSideSessionId": self.session_id,
            "updatedAt": self.updated_at,
            "revision": self.revision,
        }
        if isinstance(self.bound_model, str) and self.bound_model:
            payload["boundModel"] = self.bound_model
        if isinstance(self.bound_agent, str) and self.bound_agent:
            payload["boundAgent"] = self.bound_agent
        return payload


class _SessionMapBindingManager:
    """session map 单一真相源（内存 + 磁盘 + 激活回放 + 对账日志）。"""

    def __init__(self, owner: Any) -> None:
        self._owner = owner
        self._entries: dict[str, _SessionMapBindingEntry] = {}
        self._bootstrapped = False

    def mark_unbootstrapped(self) -> None:
        """连接断开后重置 bootstrap 标记。"""

        self._bootstrapped = False

    def _resolved_acp_cwd(self) -> str:
        cwd = (
            Path(self._owner.acp_config.cwd).expanduser()
            if self._owner.acp_config.cwd
            else self._owner.workspace
        )
        return str(cwd.resolve())

    def _sync_runtime_views(self) -> None:
        """兼容旧调用方视图：_session_map/_session_desired 只读镜像由 manager 生成。"""

        self._owner._session_map = {
            key: entry.session_id for key, entry in sorted(self._entries.items())
        }
        mirrored_desired: dict[str, dict[str, str]] = {}
        for key, entry in sorted(self._entries.items()):
            row: dict[str, str] = {}
            if isinstance(entry.bound_model, str) and entry.bound_model:
                row["model"] = entry.bound_model
            if isinstance(entry.bound_agent, str) and entry.bound_agent:
                row["agent"] = entry.bound_agent
            if row:
                mirrored_desired[key] = row
        self._owner._session_desired = mirrored_desired

    @staticmethod
    def _parse_entry(raw: Any) -> _SessionMapBindingEntry:
        if not isinstance(raw, dict):
            raise ValueError("session map entry is not object")
        cwd = raw.get("cwd")
        session_key = raw.get("nanobotSideSessionKey")
        session_id = raw.get("acpSideSessionId")
        updated_at = raw.get("updatedAt")
        revision = raw.get("revision")
        # 中文注释：兼容旧字段 desiredModel/desiredAgent，读取时统一折叠到 bound*。
        bound_model = raw.get("boundModel") or raw.get("desiredModel")
        bound_agent = raw.get("boundAgent") or raw.get("desiredAgent")
        if not isinstance(cwd, str) or not cwd:
            raise ValueError("session map entry cwd is invalid")
        if not isinstance(session_key, str) or not session_key:
            raise ValueError("session map entry nanobotSideSessionKey is invalid")
        if not isinstance(session_id, str) or not session_id:
            raise ValueError("session map entry acpSideSessionId is invalid")
        normalized_updated_at = (
            updated_at if isinstance(updated_at, str) and updated_at else _now_iso_with_tz()
        )
        normalized_revision = revision if isinstance(revision, int) and revision >= 1 else 1
        normalized_model = bound_model if isinstance(bound_model, str) and bound_model else None
        normalized_agent = bound_agent if isinstance(bound_agent, str) and bound_agent else None
        return _SessionMapBindingEntry(
            cwd=cwd,
            session_key=session_key,
            session_id=session_id,
            bound_model=normalized_model,
            bound_agent=normalized_agent,
            updated_at=normalized_updated_at,
            revision=normalized_revision,
        )

    def _read_payload_strict(self) -> dict[str, Any]:
        map_file = self._owner._session_map_file
        if not map_file.exists():
            return {"version": 2, "mappings": []}
        try:
            payload = json.loads(map_file.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("session map read failed, fallback to empty payload error={}", exc)
            return {"version": 2, "mappings": []}
        if not isinstance(payload, dict):
            logger.warning("session map payload is not object, fallback to empty payload")
            return {"version": 2, "mappings": []}
        version = payload.get("version")
        if version not in {1, 2}:
            logger.warning("session map schema version unsupported: {}, fallback empty", version)
            return {"version": 2, "mappings": []}
        mappings = payload.get("mappings")
        if not isinstance(mappings, list):
            logger.warning("session map mappings is not list, fallback empty")
            return {"version": 2, "mappings": []}
        return {"version": 2, "mappings": mappings}

    def _load_entries_from_disk(self) -> dict[str, _SessionMapBindingEntry]:
        payload = self._read_payload_strict()
        current_cwd = self._resolved_acp_cwd()
        loaded: dict[str, _SessionMapBindingEntry] = {}
        for raw in payload.get("mappings", []):
            entry = self._parse_entry(raw)
            if entry.cwd != current_cwd:
                continue
            loaded[entry.session_key] = entry
        return loaded

    def _write_entries_to_disk(self) -> None:
        map_file = self._owner._session_map_file
        payload = (
            self._read_payload_strict() if map_file.exists() else {"version": 2, "mappings": []}
        )
        current_cwd = self._resolved_acp_cwd()
        preserved: list[dict[str, Any]] = []
        for raw in payload.get("mappings", []):
            entry = self._parse_entry(raw)
            if entry.cwd == current_cwd:
                continue
            preserved.append(entry.as_payload())
        current = [entry.as_payload() for _, entry in sorted(self._entries.items())]
        out_payload = {"version": 2, "mappings": preserved + current}
        map_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = map_file.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(out_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(map_file)

    def persist(self) -> None:
        """显式持久化当前绑定状态。"""

        self._write_entries_to_disk()
        self._sync_runtime_views()

    def resolve_session_id(self, session_key: str) -> str | None:
        entry = self._entries.get(session_key)
        if entry is None:
            return None
        return entry.session_id

    def get_bound_selection(self, session_key: str) -> tuple[str | None, str | None]:
        entry = self._entries.get(session_key)
        if entry is None:
            return None, None
        return entry.bound_model, entry.bound_agent

    def list_active_channel_pairs(self) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        for session_key, entry in sorted(self._entries.items()):
            if ":" not in session_key:
                continue
            prefix = session_key.split(":", 1)[0].strip().lower()
            if prefix in {"cli", "system", "cron", "heartbeat"}:
                continue
            pairs.append((session_key, entry.session_id))
        return pairs

    def clear_binding(self, session_key: str) -> str | None:
        """删除 session_key 对应映射并落盘。"""

        old = self._entries.pop(session_key, None)
        self.persist()
        return old.session_id if old is not None else None

    def bind_session(self, session_key: str, session_id: str) -> None:
        """建立/更新 session_key -> session_id 绑定。"""

        current_cwd = self._resolved_acp_cwd()
        now = _now_iso_with_tz()
        old = self._entries.get(session_key)
        if old is None:
            entry = _SessionMapBindingEntry(
                cwd=current_cwd,
                session_key=session_key,
                session_id=session_id,
                bound_model=None,
                bound_agent=None,
                updated_at=now,
                revision=1,
            )
            self._entries[session_key] = entry
            logger.info(
                "【会话映射变更】sessionKey={} oldSessionId={} newSessionId={} revision={} updatedAt={}",
                session_key,
                "-",
                session_id,
                entry.revision,
                entry.updated_at,
            )
            self.persist()
            return

        if old.session_id == session_id:
            return

        old_session_id = old.session_id
        old.session_id = session_id
        old.revision += 1
        old.updated_at = now
        logger.info(
            "【会话映射变更】sessionKey={} oldSessionId={} newSessionId={} revision={} updatedAt={}",
            session_key,
            old_session_id,
            session_id,
            old.revision,
            old.updated_at,
        )
        self.persist()

    def update_bound_model(self, session_key: str, model: str) -> None:
        """更新 boundModel；仅写盘和日志，不触发激活。"""

        entry = self._entries.get(session_key)
        if entry is None:
            return
        if entry.bound_model == model:
            return
        old = entry.bound_model or "-"
        entry.bound_model = model
        entry.revision += 1
        entry.updated_at = _now_iso_with_tz()
        logger.info(
            "【会话绑定更新】sessionKey={} 字段=boundModel old={} new={} revision={} updatedAt={}",
            session_key,
            old,
            model,
            entry.revision,
            entry.updated_at,
        )
        self.persist()

    def update_bound_agent(self, session_key: str, agent: str) -> None:
        """更新 boundAgent；仅写盘和日志，不触发激活。"""

        entry = self._entries.get(session_key)
        if entry is None:
            return
        if entry.bound_agent == agent:
            return
        old = entry.bound_agent or "-"
        entry.bound_agent = agent
        entry.revision += 1
        entry.updated_at = _now_iso_with_tz()
        logger.info(
            "【会话绑定更新】sessionKey={} 字段=boundAgent old={} new={} revision={} updatedAt={}",
            session_key,
            old,
            agent,
            entry.revision,
            entry.updated_at,
        )
        self.persist()

    async def _activate_and_replay(self, session_key: str, session_id: str) -> bool:
        conn = self._owner._conn
        if conn is None:
            raise RuntimeError("ACP connection is not available")

        cwd = self._resolved_acp_cwd()
        mcp_servers = self._owner._convert_mcp_servers()
        resume_session = cast(
            Callable[..., Awaitable[Any]] | None,
            getattr(conn, "resume_session", None),
        )
        load_session = cast(
            Callable[..., Awaitable[Any]] | None,
            getattr(conn, "load_session", None),
        )

        if resume_session is None and load_session is None:
            return True

        resume_exc: Exception | None = None
        if resume_session is not None:
            try:
                response = await resume_session(
                    cwd=cwd, session_id=session_id, mcp_servers=mcp_servers
                )
                _update_caps_from_session_payload(self._owner._session_caps, session_id, response)
                return await self._replay_after_activation(
                    session_key=session_key, session_id=session_id
                )
            except Exception as exc:
                resume_exc = exc
                logger.debug(
                    "ACP resume_session failed session_key={} session_id={} error_type={} error={}",
                    session_key,
                    session_id,
                    type(exc).__name__,
                    exc,
                )

        if load_session is not None:
            try:
                response = await load_session(
                    cwd=cwd, session_id=session_id, mcp_servers=mcp_servers
                )
                _update_caps_from_session_payload(self._owner._session_caps, session_id, response)
                return await self._replay_after_activation(
                    session_key=session_key, session_id=session_id
                )
            except Exception as load_exc:
                logger.warning(
                    "ACP existing session activation failed session_key={} session_id={} resume_error_type={} resume_error={} load_error_type={} load_error={}",
                    session_key,
                    session_id,
                    type(resume_exc).__name__ if resume_exc is not None else "n/a",
                    resume_exc if resume_exc is not None else "n/a",
                    type(load_exc).__name__,
                    load_exc,
                )
                return False

        logger.warning(
            "ACP existing session activation failed session_key={} session_id={} reason=resume_not_available resume_error_type={} resume_error={}",
            session_key,
            session_id,
            type(resume_exc).__name__ if resume_exc is not None else "n/a",
            resume_exc if resume_exc is not None else "n/a",
        )
        return False

    async def _replay_after_activation(self, *, session_key: str, session_id: str) -> bool:
        """每次激活后都回放 model+agent（bound 优先，缺失回退 default）。"""

        conn = self._owner._conn
        if conn is None:
            raise RuntimeError("ACP connection is not available")

        entry = self._entries.get(session_key)
        bound_model = entry.bound_model if entry is not None else None
        bound_agent = entry.bound_agent if entry is not None else None
        model = bound_model or self._owner.acp_config.default_model
        agent = bound_agent or self._owner.acp_config.default_mode
        model_source = "bound" if bound_model else "default"
        agent_source = "bound" if bound_agent else "default"

        set_session_model = cast(
            Callable[..., Awaitable[Any]] | None,
            getattr(conn, "set_session_model", None),
        )
        set_session_mode = cast(
            Callable[..., Awaitable[Any]] | None,
            getattr(conn, "set_session_mode", None),
        )
        caps = self._owner._session_caps.setdefault(session_id, _SessionCapabilities())

        try:
            if model:
                if set_session_model is None:
                    raise RuntimeError("set_session_model is unavailable")
                await set_session_model(model_id=model, session_id=session_id)
                caps.current_model = model
            if agent:
                if set_session_mode is None:
                    raise RuntimeError("set_session_mode is unavailable")
                await set_session_mode(mode_id=agent, session_id=session_id)
                caps.current_agent = agent
            logger.info(
                "【会话激活回放】sessionKey={} sessionId={} model={}(来源={}) agent={}(来源={}) 结果=成功",
                session_key,
                session_id,
                model or "-",
                model_source,
                agent or "-",
                agent_source,
            )
            return True
        except Exception as exc:
            logger.warning(
                "【会话激活回放】sessionKey={} sessionId={} model={}(来源={}) agent={}(来源={}) 结果=失败 error_type={} error={}",
                session_key,
                session_id,
                model or "-",
                model_source,
                agent or "-",
                agent_source,
                type(exc).__name__,
                exc,
            )
            return False

    async def activate_session(self, session_key: str, session_id: str) -> bool:
        """按 manager 统一策略激活已存在会话。"""

        return await self._activate_and_replay(session_key, session_id)

    @staticmethod
    def _extract_session_ids_from_payload(payload: Any) -> set[str]:
        ids: set[str] = set()

        def _walk(value: Any) -> None:
            if value is None:
                return
            if hasattr(value, "model_dump"):
                try:
                    _walk(value.model_dump(by_alias=True, exclude_none=True))
                    return
                except Exception:
                    pass
            if isinstance(value, dict):
                for key in ("session_id", "sessionId", "id"):
                    session_id = value.get(key)
                    if isinstance(session_id, str) and session_id:
                        ids.add(session_id)
                for nested in value.values():
                    _walk(nested)
                return
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    _walk(item)
                return
            for key in ("session_id", "sessionId", "id"):
                session_id = getattr(value, key, None)
                if isinstance(session_id, str) and session_id:
                    ids.add(session_id)
            for attr in ("sessions", "data", "items"):
                nested = getattr(value, attr, None)
                if nested is not None:
                    _walk(nested)

        _walk(payload)
        return ids

    async def _fetch_acp_side_session_ids(self) -> tuple[set[str], bool]:
        conn = self._owner._conn
        if conn is None:
            return set(), False
        list_sessions = cast(
            Callable[..., Awaitable[Any]] | None,
            getattr(conn, "list_sessions", None),
        )
        if list_sessions is None:
            return set(), False
        cwd = self._resolved_acp_cwd()

        async def _collect(cwd_arg: str | None) -> set[str]:
            collected: set[str] = set()
            cursor: str | None = None
            seen_cursors: set[str] = set()
            while True:
                attempts: list[dict[str, str]] = []
                if cwd_arg is not None and cursor is not None:
                    attempts.append({"cwd": cwd_arg, "cursor": cursor})
                if cwd_arg is not None:
                    attempts.append({"cwd": cwd_arg})
                if cursor is not None:
                    attempts.append({"cursor": cursor})
                attempts.append({})

                response: Any | None = None
                last_exc: Exception | None = None
                for kwargs in attempts:
                    try:
                        response = await list_sessions(**kwargs)
                        break
                    except TypeError as exc:
                        last_exc = exc
                        continue
                if response is None:
                    if last_exc is not None:
                        raise last_exc
                    response = await list_sessions()
                collected.update(self._extract_session_ids_from_payload(response))

                next_cursor = getattr(response, "next_cursor", None)
                if next_cursor is None and isinstance(response, dict):
                    next_cursor = response.get("nextCursor") or response.get("next_cursor")
                if not isinstance(next_cursor, str) or not next_cursor:
                    break
                if next_cursor in seen_cursors:
                    break
                seen_cursors.add(next_cursor)
                cursor = next_cursor
            return collected

        merged: set[str] = set()
        any_success = False
        try:
            merged.update(await _collect(cwd))
            any_success = True
        except Exception:
            logger.debug("ACP session list failed for cwd scope cwd={}", cwd)
        if not merged:
            try:
                merged.update(await _collect(None))
                any_success = True
            except Exception:
                logger.debug("ACP session list failed for global scope")
        return merged, any_success

    async def _reconcile_entries(self) -> None:
        if not self._entries:
            return
        listed_ids, authoritative = await self._fetch_acp_side_session_ids()
        if not authoritative or not listed_ids:
            return
        stale_keys = [
            key for key, entry in self._entries.items() if entry.session_id not in listed_ids
        ]
        for key in stale_keys:
            stale = self._entries.pop(key, None)
            if stale is not None:
                self._owner._session_caps.pop(stale.session_id, None)
        if stale_keys:
            self.persist()

    async def bootstrap(self) -> list[str]:
        """启动加载 + 对账 + 激活批次；返回激活成功的 session_key 列表。"""

        if self._bootstrapped:
            return []
        self._entries = self._load_entries_from_disk()
        self._sync_runtime_views()
        await self._reconcile_entries()

        ok_keys: list[str] = []
        fail = 0
        for session_key, entry in sorted(self._entries.items()):
            if await self.activate_session(session_key, entry.session_id):
                ok_keys.append(session_key)
            else:
                fail += 1
        logger.info("【启动对账】总数={} 成功={} 失败={}", len(self._entries), len(ok_keys), fail)
        self._bootstrapped = True
        return ok_keys
