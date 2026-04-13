"""运行时主入口与请求等待主链。"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from nanobot.acp.contracts import ACPSessionPayload, JSONMap
from nanobot.acp.sessionmap.internal.session_caps import (
    _build_prompt_metadata,
    _render_agents_command,
    _render_models_command,
    _update_caps_from_session_payload,
)
from nanobot.acp.sessionmap.models import SessionRuntimeEntry, _SessionCapabilities

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime
    from nanobot.acp.sessionmap.binding_manager import SessionMapBindingManager


class SessionRuntimeManager:
    """负责运行时主链路编排与资源生命周期。"""

    def __init__(
        self,
        *,
        runtime: ACPRuntime,
        binding_manager: SessionMapBindingManager,
    ) -> None:
        """初始化当前对象并建立必要状态。"""
        self._runtime = runtime
        self._binding_manager = binding_manager
        self._lock = asyncio.Lock()
        self._by_nanobot_side_session_key: dict[str, SessionRuntimeEntry] = {}
        self._by_acp_side_session_id: dict[str, SessionRuntimeEntry] = {}

    def get_by_nanobot_side_session_key(
        self,
        nanobot_side_session_key: str,
    ) -> SessionRuntimeEntry | None:
        """执行该方法定义的处理流程并返回结果。"""
        return self._by_nanobot_side_session_key.get(nanobot_side_session_key)

    def get_by_acp_side_session_id(self, acp_side_session_id: str) -> SessionRuntimeEntry | None:
        """执行该方法定义的处理流程并返回结果。"""
        return self._by_acp_side_session_id.get(acp_side_session_id)

    def ensure_session_capabilities(self, acp_side_session_id: str) -> _SessionCapabilities | None:
        """执行该方法定义的处理流程并返回结果。"""

        entry = self._by_acp_side_session_id.get(acp_side_session_id)
        return entry.capabilities if entry is not None else None

    def update_caps_from_payload(
        self,
        *,
        acp_side_session_id: str,
        payload: ACPSessionPayload,
    ) -> None:
        """执行该方法定义的处理流程并返回结果。"""

        entry = self._by_acp_side_session_id.get(acp_side_session_id)
        if entry is None:
            return
        _update_caps_from_session_payload(entry.capabilities, payload)

    def drop_session_capabilities(self, *, acp_side_session_id: str) -> None:
        """执行该方法定义的处理流程并返回结果。"""

        entry = self._by_acp_side_session_id.get(acp_side_session_id)
        if entry is not None:
            entry.capabilities = _SessionCapabilities()

    def set_current_model(self, *, acp_side_session_id: str, model_id: str) -> None:
        """执行该方法定义的处理流程并返回结果。"""

        caps = self.ensure_session_capabilities(acp_side_session_id)
        if caps is not None:
            caps.current_model = model_id

    def set_current_agent(self, *, acp_side_session_id: str, agent_id: str) -> None:
        """执行该方法定义的处理流程并返回结果。"""

        caps = self.ensure_session_capabilities(acp_side_session_id)
        if caps is not None:
            caps.current_agent = agent_id

    def build_prompt_metadata(self, *, acp_side_session_id: str) -> JSONMap:
        """执行该方法定义的处理流程并返回结果。"""

        return _build_prompt_metadata(self._caps_for(acp_side_session_id=acp_side_session_id))

    def render_models_command(self, *, acp_side_session_id: str) -> str:
        """执行该方法定义的处理流程并返回结果。"""

        return _render_models_command(self._caps_for(acp_side_session_id=acp_side_session_id))

    def render_agents_command(self, *, acp_side_session_id: str) -> str:
        """执行该方法定义的处理流程并返回结果。"""

        return _render_agents_command(self._caps_for(acp_side_session_id=acp_side_session_id))

    async def ensure_ready_session(
        self,
        *,
        nanobot_side_session_key: str,
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
    ) -> str:
        """确保当前会话存在可用协议会话。"""

        async with self._lock:
            await self._binding_manager.load_persistent_truth()
            existing = self._by_nanobot_side_session_key.get(nanobot_side_session_key)
            if existing is not None and existing.ready:
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
                activated, payload = await self._binding_manager.activate_session(
                    nanobot_side_session_key,
                    acp_side_session_id,
                )
                if activated:
                    self._store_runtime_entry(
                        nanobot_side_session_key=nanobot_side_session_key,
                        acp_side_session_id=acp_side_session_id,
                    )
                    if payload is not None:
                        self.update_caps_from_payload(
                            acp_side_session_id=acp_side_session_id,
                            payload=payload,
                        )
                    await self._apply_runtime_selection(
                        acp_side_session_id=acp_side_session_id,
                        nanobot_side_session_key=nanobot_side_session_key,
                        preferred_model=preferred_model,
                        preferred_agent=preferred_agent,
                    )
                    return acp_side_session_id
                self._drop_runtime_entry(nanobot_side_session_key=nanobot_side_session_key)

            cwd = (
                Path(self._runtime.acp_config.cwd).expanduser()
                if self._runtime.acp_config.cwd
                else self._runtime.workspace
            )
            response = await conn.new_session(cwd=str(cwd.resolve()))
            acp_side_session_id = response.session_id
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
                self.set_current_model(
                    acp_side_session_id=acp_side_session_id, model_id=selected_model
                )
            if selected_agent and hasattr(conn, "set_session_mode"):
                await conn.set_session_mode(mode_id=selected_agent, session_id=acp_side_session_id)
                self.set_current_agent(
                    acp_side_session_id=acp_side_session_id, agent_id=selected_agent
                )

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
            self._store_runtime_entry(
                nanobot_side_session_key=nanobot_side_session_key,
                acp_side_session_id=acp_side_session_id,
            )
            self.update_caps_from_payload(acp_side_session_id=acp_side_session_id, payload=response)
            if selected_model:
                self.set_current_model(
                    acp_side_session_id=acp_side_session_id, model_id=selected_model
                )
            if selected_agent:
                self.set_current_agent(
                    acp_side_session_id=acp_side_session_id, agent_id=selected_agent
                )
            return acp_side_session_id

    def rebuild(self) -> None:
        """重建当前运行态并清理旧代资源。"""

        self._by_nanobot_side_session_key.clear()
        self._by_acp_side_session_id.clear()

    def drop_ready_session(self, *, nanobot_side_session_key: str) -> None:
        """丢弃当前会话就绪条目。"""

        self._drop_runtime_entry(nanobot_side_session_key=nanobot_side_session_key)

    async def bootstrap_ready_sessions(self) -> list[str]:
        """执行该方法定义的处理流程并返回结果。"""

        await self._binding_manager.load_persistent_truth()
        return []

    def update_runtime_selection(
        self,
        *,
        nanobot_side_session_key: str,
        bound_model: str | None = None,
        bound_agent: str | None = None,
    ) -> None:
        """启动主循环并持续消费入站消息。"""

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
        """启动主循环并持续消费入站消息。"""

        conn = self._runtime._acp_client_conn
        if conn is None:
            return
        if preferred_model and hasattr(conn, "set_session_model"):
            await conn.set_session_model(model_id=preferred_model, session_id=acp_side_session_id)
            self.set_current_model(
                acp_side_session_id=acp_side_session_id, model_id=preferred_model
            )
            self._binding_manager.update_bound_model(nanobot_side_session_key, preferred_model)
            self.update_runtime_selection(
                nanobot_side_session_key=nanobot_side_session_key,
                bound_model=preferred_model,
            )
        if preferred_agent and hasattr(conn, "set_session_mode"):
            await conn.set_session_mode(mode_id=preferred_agent, session_id=acp_side_session_id)
            self.set_current_agent(
                acp_side_session_id=acp_side_session_id, agent_id=preferred_agent
            )
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
        """启动主循环并持续消费入站消息。"""
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
        """启动主循环并持续消费入站消息。"""
        entry = self._by_nanobot_side_session_key.pop(nanobot_side_session_key, None)
        if entry is not None:
            self._by_acp_side_session_id.pop(entry.acp_side_session_id, None)

    def _caps_for(self, *, acp_side_session_id: str) -> _SessionCapabilities | None:
        """执行该方法定义的处理流程并返回结果。"""

        entry = self._by_acp_side_session_id.get(acp_side_session_id)
        return entry.capabilities if entry is not None else None
