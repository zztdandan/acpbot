"""ACP session 映射持久化与对账支持。

本模块通过 mixin 方式给 ACPDispatcher 提供：
1) session_map 的本地持久化；
2) 启动后与 ACP 侧会话列表对账；
3) 按活跃渠道会话广播 heartbeat。
"""

from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path
from typing import Any, Awaitable, Callable, cast

from loguru import logger

from nanobot.acp.state import _SessionCapabilities


class _SessionMapSupport:
    """为 ACPDispatcher 提供会话映射管理能力的混入类。"""

    _conn: Any = None
    _session_map: dict[str, str] = {}
    _session_map_file: Path = Path()
    _session_map_bootstrapped: bool = False
    _session_caps: dict[str, _SessionCapabilities] = {}
    acp_config: Any = None
    workspace: Path = Path()

    async def _ensure_connection(self) -> None:
        """由具体 Dispatcher 实现连接建立；mixin 只声明能力契约。"""
        raise NotImplementedError

    @staticmethod
    def _acp_text_block(content: str) -> Any:
        """按需导入 ACP 包，避免静态导入导致环境缺包时报错。"""
        return import_module("acp").text_block(content)

    def _resolved_acp_cwd(self) -> str:
        """解析 ACP 实际运行 cwd（优先 acp_config.cwd，其次 workspace）。"""
        cwd = Path(self.acp_config.cwd).expanduser() if self.acp_config.cwd else self.workspace
        return str(cwd.resolve())

    @staticmethod
    def _session_map_entry(
        *,
        cwd: str,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
    ) -> dict[str, str]:
        """统一持久化 entry 的字段结构。"""
        return {
            "cwd": cwd,
            "nanobotSideSessionKey": nanobot_side_session_key,
            "acpSideSessionId": acp_side_session_id,
        }

    def _load_session_map_from_disk(self) -> dict[str, str]:
        """只加载当前 cwd 的映射，避免不同实例互相污染。"""
        if not self._session_map_file.exists():
            return {}
        try:
            payload = json.loads(self._session_map_file.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("ACP session map load failed: {}", self._session_map_file)
            return {}

        current_cwd = self._resolved_acp_cwd()
        loaded: dict[str, str] = {}
        for raw in payload.get("mappings", []):
            if not isinstance(raw, dict):
                continue
            if raw.get("cwd") != current_cwd:
                continue
            nanobot_side_session_key = raw.get("nanobotSideSessionKey")
            acp_side_session_id = raw.get("acpSideSessionId")
            if isinstance(nanobot_side_session_key, str) and isinstance(acp_side_session_id, str):
                if nanobot_side_session_key and acp_side_session_id:
                    loaded[nanobot_side_session_key] = acp_side_session_id
        return loaded

    def _persist_session_map(self) -> None:
        """以原子替换方式回写当前 cwd 的映射。"""
        current_cwd = self._resolved_acp_cwd()
        preserved: list[dict[str, str]] = []
        if self._session_map_file.exists():
            try:
                payload = json.loads(self._session_map_file.read_text(encoding="utf-8"))
                for raw in payload.get("mappings", []):
                    if not isinstance(raw, dict):
                        continue
                    if raw.get("cwd") == current_cwd:
                        continue
                    if (
                        isinstance(raw.get("cwd"), str)
                        and isinstance(raw.get("nanobotSideSessionKey"), str)
                        and isinstance(raw.get("acpSideSessionId"), str)
                    ):
                        preserved.append(raw)
            except Exception:
                logger.exception(
                    "ACP session map read-before-write failed: {}", self._session_map_file
                )

        mappings = preserved + [
            self._session_map_entry(
                cwd=current_cwd,
                nanobot_side_session_key=nanobot_side_session_key,
                acp_side_session_id=acp_side_session_id,
            )
            for nanobot_side_session_key, acp_side_session_id in sorted(self._session_map.items())
        ]
        # 持久化采用 version 包裹，便于后续结构升级。
        data = {"version": 1, "mappings": mappings}
        self._session_map_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._session_map_file.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(self._session_map_file)

    @staticmethod
    def _extract_session_ids_from_payload(payload: Any) -> set[str]:
        """兼容多种 ACP 返回结构，递归提取 session_id/sessionId。"""
        ids: set[str] = set()

        def _walk(value: Any) -> None:
            if value is None:
                return
            if isinstance(value, dict):
                for key in ("session_id", "sessionId"):
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
            for key in ("session_id", "sessionId"):
                session_id = getattr(value, key, None)
                if isinstance(session_id, str) and session_id:
                    ids.add(session_id)

        _walk(payload)
        return ids

    async def _fetch_acp_side_session_ids(self) -> set[str]:
        """通过多种接口名兼容不同 ACP 后端的 session list 能力。"""
        if self._conn is None:
            return set()

        list_candidates = [
            ("list_sessions", {}),
            ("session_list", {}),
            ("listSessions", {}),
            ("sessionList", {}),
        ]
        for method_name, kwargs in list_candidates:
            method = cast(
                Callable[..., Awaitable[Any]] | None, getattr(self._conn, method_name, None)
            )
            if method is None:
                continue
            try:
                payload = await method(**kwargs)
                ids = self._extract_session_ids_from_payload(payload)
                if ids:
                    return ids
            except Exception:
                logger.debug("ACP list method {} failed", method_name)

        ext_method = cast(
            Callable[..., Awaitable[Any]] | None, getattr(self._conn, "ext_method", None)
        )
        if ext_method is not None:
            for rpc_name in ("session/list", "session.list"):
                try:
                    payload = await ext_method(rpc_name, {})
                    ids = self._extract_session_ids_from_payload(payload)
                    if ids:
                        return ids
                except Exception:
                    logger.debug("ACP ext session list failed: {}", rpc_name)

        return set()

    async def _acp_session_exists(self, acp_side_session_id: str) -> bool:
        """校验某 ACP session 是否仍存在（先 list，后 load 探测）。"""
        if self._conn is None:
            return False

        listed_ids = await self._fetch_acp_side_session_ids()
        if listed_ids:
            return acp_side_session_id in listed_ids

        load_candidates = [
            ("load_session", {"session_id": acp_side_session_id}),
            ("session_load", {"session_id": acp_side_session_id}),
            ("loadSession", {"sessionId": acp_side_session_id}),
            ("sessionLoad", {"sessionId": acp_side_session_id}),
        ]
        for method_name, kwargs in load_candidates:
            method = cast(
                Callable[..., Awaitable[Any]] | None, getattr(self._conn, method_name, None)
            )
            if method is None:
                continue
            try:
                await method(**kwargs)
                return True
            except Exception:
                continue

        ext_method = cast(
            Callable[..., Awaitable[Any]] | None, getattr(self._conn, "ext_method", None)
        )
        if ext_method is not None:
            for rpc_name, params in (
                ("session/load", {"sessionId": acp_side_session_id}),
                ("session.load", {"sessionId": acp_side_session_id}),
            ):
                try:
                    await ext_method(rpc_name, params)
                    return True
                except Exception:
                    continue

        return False

    async def _reconcile_session_map(self) -> None:
        """启动对账：删掉 nanobot 有但 ACP 不存在的映射。"""
        if not self._session_map:
            return
        listed_ids = await self._fetch_acp_side_session_ids()
        to_delete: list[str] = []
        for nanobot_side_session_key, acp_side_session_id in self._session_map.items():
            exists = (
                acp_side_session_id in listed_ids
                if listed_ids
                else await self._acp_session_exists(acp_side_session_id)
            )
            if not exists:
                to_delete.append(nanobot_side_session_key)
        for nanobot_side_session_key in to_delete:
            stale_session_id = self._session_map.pop(nanobot_side_session_key, None)
            if stale_session_id:
                self._session_caps.pop(stale_session_id, None)
        if to_delete:
            self._persist_session_map()

    async def _bootstrap_session_map(self) -> None:
        """完成启动期 map 加载+对账，且仅执行一次。"""
        if self._session_map_bootstrapped:
            return
        self._session_map = self._load_session_map_from_disk()
        await self._reconcile_session_map()
        self._persist_session_map()
        self._session_map_bootstrapped = True

    @staticmethod
    def _is_active_channel_session_key(nanobot_side_session_key: str) -> bool:
        """判断是否为渠道会话 key（过滤 cli/system/cron/heartbeat）。"""
        if ":" not in nanobot_side_session_key:
            return False
        prefix = nanobot_side_session_key.split(":", 1)[0].strip().lower()
        return prefix not in {"cli", "system", "cron", "heartbeat"}

    async def send_heartbeat_to_active_sessions(self, heartbeat_instruction: str) -> int:
        """向当前活跃渠道映射对应的 ACP 会话逐个发送 heartbeat 指令。"""
        await self._ensure_connection()
        if self._conn is None:
            raise RuntimeError("ACP connection is not available")
        active_pairs = [
            (nanobot_side_session_key, acp_side_session_id)
            for nanobot_side_session_key, acp_side_session_id in self._session_map.items()
            if self._is_active_channel_session_key(nanobot_side_session_key)
        ]
        delivered = 0
        for _, acp_side_session_id in active_pairs:
            try:
                await self._conn.prompt(
                    prompt=[self._acp_text_block(heartbeat_instruction)],
                    session_id=acp_side_session_id,
                )
                delivered += 1
            except Exception:
                logger.exception("ACP heartbeat prompt failed for session {}", acp_side_session_id)
        return delivered
