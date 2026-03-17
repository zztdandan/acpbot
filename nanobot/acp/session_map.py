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
            logger.debug(
                "ACP session map load skipped: file not found path={} cwd={}",
                self._session_map_file,
                self._resolved_acp_cwd(),
            )
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
        logger.debug(
            "ACP session map loaded path={} cwd={} loaded={} total_entries={}",
            self._session_map_file,
            current_cwd,
            len(loaded),
            len(payload.get("mappings", [])) if isinstance(payload, dict) else 0,
        )
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
        logger.debug(
            "ACP session map persisted path={} cwd={} active={} preserved_other_cwd={} total_written={}",
            self._session_map_file,
            current_cwd,
            len(self._session_map),
            len(preserved),
            len(mappings),
        )

    @staticmethod
    def _extract_session_ids_from_payload(payload: Any) -> set[str]:
        """兼容 pydantic/dict/object 结构，递归提取 session_id/sessionId/id。"""
        ids: set[str] = set()

        def _walk(value: Any) -> None:
            if value is None:
                return
            # 中文注释：python-sdk 返回常见为 Pydantic 模型，先 dump 成 dict 再递归。
            if hasattr(value, "model_dump"):
                try:
                    dumped = value.model_dump(by_alias=True, exclude_none=True)
                    _walk(dumped)
                    return
                except Exception:
                    # model_dump 失败时降级到后续分支处理，避免单点解析失败。
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
            # 中文注释：对象属性递归覆盖 sessions/data/items 等聚合字段。
            for attr in ("sessions", "data", "items"):
                nested = getattr(value, attr, None)
                if nested is not None:
                    _walk(nested)

        _walk(payload)
        return ids

    async def _fetch_acp_side_session_ids(self) -> tuple[set[str], bool]:
        """通过 list_sessions 拉取 ACP 侧 session（含分页）。"""
        if self._conn is None:
            return set(), False

        cwd = self._resolved_acp_cwd()
        list_sessions = cast(
            Callable[..., Awaitable[Any]] | None, getattr(self._conn, "list_sessions", None)
        )
        if list_sessions is None:
            logger.debug("ACP session list unavailable: list_sessions method missing")
            return set(), False

        async def _collect(cwd_arg: str | None) -> set[str]:
            # 中文注释：按 next_cursor 分页拉取，避免只拿到第一页导致误判“远端为空”。
            collected: set[str] = set()
            cursor: str | None = None
            seen_cursors: set[str] = set()
            page = 0

            async def _call_list_sessions(cursor_arg: str | None) -> Any:
                # 中文注释：兼容不同实现的函数签名（仅 cwd / 仅 cursor / 无参数）。
                attempts: list[dict[str, str]] = []
                if cwd_arg is not None and cursor_arg is not None:
                    attempts.append({"cwd": cwd_arg, "cursor": cursor_arg})
                if cwd_arg is not None:
                    attempts.append({"cwd": cwd_arg})
                if cursor_arg is not None:
                    attempts.append({"cursor": cursor_arg})
                attempts.append({})

                last_exc: Exception | None = None
                for kwargs in attempts:
                    try:
                        return await list_sessions(**kwargs)
                    except TypeError as exc:
                        # 中文注释：参数不兼容时继续尝试下一组 kwargs。
                        last_exc = exc
                        logger.debug(
                            "ACP session list call signature mismatch cwd_arg={} cursor_arg={} kwargs={} error={}",
                            cwd_arg,
                            cursor_arg,
                            kwargs,
                            exc,
                        )
                        continue

                if last_exc is not None:
                    raise last_exc
                return await list_sessions()

            while True:
                page += 1
                response = await _call_list_sessions(cursor)
                page_ids = self._extract_session_ids_from_payload(response)
                collected.update(page_ids)

                next_cursor = getattr(response, "next_cursor", None)
                if next_cursor is None and isinstance(response, dict):
                    next_cursor = response.get("nextCursor") or response.get("next_cursor")

                logger.debug(
                    "ACP session list page cwd_arg={} page={} cursor={} next_cursor={} page_ids_count={} page_ids={}",
                    cwd_arg,
                    page,
                    cursor,
                    next_cursor,
                    len(page_ids),
                    sorted(page_ids),
                )

                if not isinstance(next_cursor, str) or not next_cursor:
                    break
                if next_cursor in seen_cursors:
                    logger.warning(
                        "ACP session list pagination stopped due to repeated cursor cwd_arg={} repeated_cursor={}",
                        cwd_arg,
                        next_cursor,
                    )
                    break
                seen_cursors.add(next_cursor)
                cursor = next_cursor
            return collected

        merged: set[str] = set()
        any_success = False
        try:
            ids_with_cwd = await _collect(cwd)
            merged.update(ids_with_cwd)
            any_success = True
        except Exception as exc:
            logger.debug(
                "ACP session list failed scope=cwd cwd_arg={} error_type={} error={}",
                cwd,
                type(exc).__name__,
                exc,
            )

        # 中文注释：当 cwd 作用域结果为空时，再补一次全局拉取，便于排查目录过滤差异。
        if not merged:
            try:
                ids_global = await _collect(None)
                merged.update(ids_global)
                any_success = True
            except Exception as exc:
                logger.debug(
                    "ACP session list failed scope=global error_type={} error={}",
                    type(exc).__name__,
                    exc,
                )
        return merged, any_success

    async def _acp_session_exists(self, acp_side_session_id: str) -> bool:
        """校验某 ACP session 是否仍存在（先 list，后 load 探测）。"""
        if self._conn is None:
            return False

        listed_ids, listed_authoritative = await self._fetch_acp_side_session_ids()
        if listed_authoritative:
            return acp_side_session_id in listed_ids

        cwd = self._resolved_acp_cwd()
        load_candidates = [
            ("load_session", {"cwd": cwd, "session_id": acp_side_session_id}),
            ("load_session", {"session_id": acp_side_session_id}),
            ("session_load", {"cwd": cwd, "session_id": acp_side_session_id}),
            ("session_load", {"session_id": acp_side_session_id}),
            ("loadSession", {"cwd": cwd, "sessionId": acp_side_session_id}),
            ("loadSession", {"sessionId": acp_side_session_id}),
            ("sessionLoad", {"cwd": cwd, "sessionId": acp_side_session_id}),
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
                ("session/load", {"cwd": cwd, "sessionId": acp_side_session_id}),
                ("session.load", {"cwd": cwd, "sessionId": acp_side_session_id}),
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
        listed_ids, listed_authoritative = await self._fetch_acp_side_session_ids()
        logger.debug(
            "ACP reconcile start cwd={} local_mappings={} list_authoritative={} listed_ids={}",
            self._resolved_acp_cwd(),
            len(self._session_map),
            listed_authoritative,
            len(listed_ids),
        )
        if not listed_authoritative:
            # session/list 在 ACP 里是可选能力；当后端不支持或临时失败时，
            # 这里拿到空集合并不等价于“远端没有会话”。
            # 为避免误删本地映射导致会话续接丢失，采用保守策略：本轮不做删除。
            logger.warning(
                "ACP session reconciliation skipped: remote session list unavailable, keeping {} local mappings",
                len(self._session_map),
            )
            return
        if not listed_ids:
            # 即使 list 调用“成功”，空集合在部分 ACP 后端上仍可能是不可靠结果。
            # 为避免重启时误清空本地映射，空列表场景同样跳过删除。
            logger.warning(
                "ACP session reconciliation skipped: remote session list returned empty while local mappings={}, keep local map",
                len(self._session_map),
            )
            return
        to_delete: list[str] = []
        for nanobot_side_session_key, acp_side_session_id in self._session_map.items():
            exists = acp_side_session_id in listed_ids
            if not exists:
                to_delete.append(nanobot_side_session_key)
        for nanobot_side_session_key in to_delete:
            stale_session_id = self._session_map.pop(nanobot_side_session_key, None)
            if stale_session_id:
                self._session_caps.pop(stale_session_id, None)
        if to_delete:
            logger.info(
                "ACP session reconciliation removed stale mappings count={} remaining={}",
                len(to_delete),
                len(self._session_map),
            )
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
