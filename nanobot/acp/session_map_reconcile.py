"""ACP session_map 启动对账与远端会话探测能力。"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, cast

from loguru import logger


class _SessionMapReconcileMixin:
    """封装会话列表探测、分页解析与本地映射对账逻辑。"""

    _conn: Any
    _session_map: dict[str, str]
    _session_map_bootstrapped: bool
    _session_caps: dict[str, Any]

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
                    # 中文注释：model_dump 失败时降级到后续分支处理，避免单点解析失败。
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

        cwd = self._resolved_acp_cwd()  # type: ignore[attr-defined]
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

        cwd = self._resolved_acp_cwd()  # type: ignore[attr-defined]
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
            self._resolved_acp_cwd(),  # type: ignore[attr-defined]
            len(self._session_map),
            listed_authoritative,
            len(listed_ids),
        )
        if not listed_authoritative:
            # 中文注释：session/list 在 ACP 里是可选能力；当后端不支持或临时失败时，
            # 中文注释：这里拿到空集合并不等价于“远端没有会话”。
            # 中文注释：为避免误删本地映射导致会话续接丢失，采用保守策略：本轮不做删除。
            logger.warning(
                "ACP session reconciliation skipped: remote session list unavailable, keeping {} local mappings",
                len(self._session_map),
            )
            return
        if not listed_ids:
            # 中文注释：即使 list 调用“成功”，空集合在部分 ACP 后端上仍可能是不可靠结果。
            # 中文注释：为避免重启时误清空本地映射，空列表场景同样跳过删除。
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
            self._persist_session_map()  # type: ignore[attr-defined]

    async def _bootstrap_session_map(self) -> None:
        """完成启动期 map 加载+对账，且仅执行一次。"""
        if self._session_map_bootstrapped:
            return
        self._session_map = self._load_session_map_from_disk()  # type: ignore[attr-defined]
        await self._reconcile_session_map()
        self._persist_session_map()  # type: ignore[attr-defined]
        self._session_map_bootstrapped = True
