"""Runtime-ready session manager for ACP runtime lifecycle."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from loguru import logger

from nanobot.acp.session_caps import _update_caps_from_session_payload
from nanobot.acp.sessionmap.models import SessionRuntimeEntry


class SessionRuntimeManager:
    """Owns runtime-lifetime ready session entries and dual indexes."""

    def __init__(self, *, runtime: Any, binding_manager: Any) -> None:
        self._runtime = runtime
        self._binding_manager = binding_manager
        self._lock = asyncio.Lock()
        self._by_nanobot_side_session_key: dict[str, SessionRuntimeEntry] = {}
        self._by_acp_side_session_id: dict[str, SessionRuntimeEntry] = {}

    def get_by_nanobot_side_session_key(
        self,
        nanobot_side_session_key: str,
    ) -> SessionRuntimeEntry | None:
        return self._by_nanobot_side_session_key.get(nanobot_side_session_key)

    def get_by_acp_side_session_id(self, acp_side_session_id: str) -> SessionRuntimeEntry | None:
        return self._by_acp_side_session_id.get(acp_side_session_id)

    async def ensure_ready_session(
        self,
        *,
        nanobot_side_session_key: str,
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
    ) -> str:
        """Ensure there is a runtime-ready ACP session for the nanobot-side session."""

        async with self._lock:
            await self._binding_manager.load_persistent_truth()
            existing = self._by_nanobot_side_session_key.get(nanobot_side_session_key)
            if existing is not None and existing.ready:
                # 中文注释：ready entry 命中后不重新建 session，
                # 只增量应用本次请求的 preferred selection，保持当前 runtime 内的复用语义。
                await self._apply_runtime_selection(
                    acp_side_session_id=existing.acp_side_session_id,
                    nanobot_side_session_key=nanobot_side_session_key,
                    preferred_model=preferred_model,
                    preferred_agent=preferred_agent,
                )
                return existing.acp_side_session_id

            await self._runtime.ensure_connection()
            conn = self._runtime._acp_client_conn
            if conn is None:
                raise RuntimeError("ACP connection is not available")

            acp_side_session_id = self._binding_manager.resolve_session_id(nanobot_side_session_key)
            if acp_side_session_id:
                # 中文注释：有 binding truth 时优先尝试激活历史 ACP session；
                # 只有激活失败才会退化到新建 session。
                activated = await self._binding_manager.activate_session(
                    nanobot_side_session_key,
                    acp_side_session_id,
                )
                if activated:
                    await self._apply_runtime_selection(
                        acp_side_session_id=acp_side_session_id,
                        nanobot_side_session_key=nanobot_side_session_key,
                        preferred_model=preferred_model,
                        preferred_agent=preferred_agent,
                    )
                    return self._store_runtime_entry(
                        nanobot_side_session_key=nanobot_side_session_key,
                        acp_side_session_id=acp_side_session_id,
                    )
                self._drop_runtime_entry(nanobot_side_session_key=nanobot_side_session_key)

            cwd = (
                Path(self._runtime.acp_config.cwd).expanduser()
                if self._runtime.acp_config.cwd
                else self._runtime.workspace
            )
            # 中文注释：ACP 侧不再注入 nanobot 的 MCP server 配置；ACP session
            # 只按 cwd 创建，工具能力完全交由 ACP 对端自行决定。
            response = await conn.new_session(cwd=str(cwd.resolve()))
            # 中文注释：新建 session 成功后，马上把当前选择同时写到 ACP 会话、
            # binding truth 和 runtime entry 镜像，避免“当前有效、重连丢失”。
            acp_side_session_id = response.session_id
            _update_caps_from_session_payload(
                self._runtime._session_caps, acp_side_session_id, response
            )

            bound_model, bound_agent = self._binding_manager.get_bound_selection(
                nanobot_side_session_key
            )
            selected_model = (
                preferred_model or bound_model or self._runtime.acp_config.default_model
            )
            selected_agent = preferred_agent or bound_agent or self._runtime.acp_config.default_mode
            if selected_model and hasattr(conn, "set_session_model"):
                await conn.set_session_model(
                    model_id=selected_model, session_id=acp_side_session_id
                )
                self._runtime._session_caps[acp_side_session_id].current_model = selected_model
            if selected_agent and hasattr(conn, "set_session_mode"):
                await conn.set_session_mode(mode_id=selected_agent, session_id=acp_side_session_id)
                self._runtime._session_caps[acp_side_session_id].current_agent = selected_agent

            self._binding_manager.bind_session(nanobot_side_session_key, acp_side_session_id)
            if selected_model:
                self._binding_manager.update_bound_model(nanobot_side_session_key, selected_model)
            if selected_agent:
                self._binding_manager.update_bound_agent(nanobot_side_session_key, selected_agent)
            logger.info(
                "ACP ready session established nanobot_side_session_key={} acp_side_session_id={}",
                nanobot_side_session_key,
                acp_side_session_id,
            )
            return self._store_runtime_entry(
                nanobot_side_session_key=nanobot_side_session_key,
                acp_side_session_id=acp_side_session_id,
            )

    def rebuild(self) -> None:
        """Drop all runtime-owned ready session entries during runtime rebuild."""

        # 中文注释：rebuild 只清掉“这一代 runtime 里已经 ready 的 session”，
        # 持久化 binding truth 仍留在 binding manager；下一条请求再按 truth 重新 ensure。
        self._by_nanobot_side_session_key.clear()
        self._by_acp_side_session_id.clear()

    def drop_ready_session(self, *, nanobot_side_session_key: str) -> None:
        """Drop one ready session entry so the next request must re-ensure it."""

        self._drop_runtime_entry(nanobot_side_session_key=nanobot_side_session_key)

    async def bootstrap_ready_sessions(self) -> list[str]:
        """Load binding truth lazily; ready sessions are ensured on demand."""

        # 中文注释：这里故意不批量激活历史 session，
        # 只把跨 runtime 的 binding truth 载入内存，保持“ready session 懒建立”的设计边界。
        await self._binding_manager.load_persistent_truth()
        return []

    def update_runtime_selection(
        self,
        *,
        nanobot_side_session_key: str,
        bound_model: str | None = None,
        bound_agent: str | None = None,
    ) -> None:
        """Keep the mirrored ready-entry selection in sync with command writes."""

        entry = self._by_nanobot_side_session_key.get(nanobot_side_session_key)
        if entry is None:
            return
        if bound_model is not None:
            entry.bound_model = bound_model
        if bound_agent is not None:
            entry.bound_agent = bound_agent

    async def _apply_runtime_selection(
        self,
        *,
        acp_side_session_id: str,
        nanobot_side_session_key: str,
        preferred_model: str | None,
        preferred_agent: str | None,
    ) -> None:
        """Apply per-request preferred selection onto an existing ready session."""

        # 中文注释：preferred selection 的 owner 是“这次请求想怎么跑”，
        # 但一旦生效，就要同步刷新 binding truth，保证后续 reconnect/replay 仍沿用最新选择。
        conn = self._runtime._acp_client_conn
        if conn is None:
            return
        if preferred_model and hasattr(conn, "set_session_model"):
            await conn.set_session_model(model_id=preferred_model, session_id=acp_side_session_id)
            self._runtime._session_caps[acp_side_session_id].current_model = preferred_model
            self._binding_manager.update_bound_model(nanobot_side_session_key, preferred_model)
            self.update_runtime_selection(
                nanobot_side_session_key=nanobot_side_session_key,
                bound_model=preferred_model,
            )
        if preferred_agent and hasattr(conn, "set_session_mode"):
            await conn.set_session_mode(mode_id=preferred_agent, session_id=acp_side_session_id)
            self._runtime._session_caps[acp_side_session_id].current_agent = preferred_agent
            self._binding_manager.update_bound_agent(nanobot_side_session_key, preferred_agent)
            self.update_runtime_selection(
                nanobot_side_session_key=nanobot_side_session_key,
                bound_agent=preferred_agent,
            )

    def _store_runtime_entry(
        self,
        *,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
    ) -> str:
        bound_model, bound_agent = self._binding_manager.get_bound_selection(
            nanobot_side_session_key
        )
        entry = SessionRuntimeEntry(
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
            ready=True,
            bound_model=bound_model,
            bound_agent=bound_agent,
        )
        self._by_nanobot_side_session_key[nanobot_side_session_key] = entry
        self._by_acp_side_session_id[acp_side_session_id] = entry
        return acp_side_session_id

    def _drop_runtime_entry(self, *, nanobot_side_session_key: str) -> None:
        entry = self._by_nanobot_side_session_key.pop(nanobot_side_session_key, None)
        if entry is not None:
            self._by_acp_side_session_id.pop(entry.acp_side_session_id, None)
