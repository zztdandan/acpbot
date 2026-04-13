"""Runtime-ready session manager for ACP runtime lifecycle."""

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
    """Owns runtime-lifetime ready session entries and dual indexes."""

    def __init__(
        self,
        *,
        runtime: ACPRuntime,
        binding_manager: SessionMapBindingManager,
    ) -> None:
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

    def ensure_session_capabilities(self, acp_side_session_id: str) -> _SessionCapabilities | None:
        """Return the runtime-owned capability cache for one ready ACP session."""

        entry = self._by_acp_side_session_id.get(acp_side_session_id)
        return entry.capabilities if entry is not None else None

    def update_caps_from_payload(
        self,
        *,
        acp_side_session_id: str,
        payload: ACPSessionPayload,
    ) -> None:
        """Mirror ACP payload capability facts into the runtime-owned session cache."""

        entry = self._by_acp_side_session_id.get(acp_side_session_id)
        if entry is None:
            return
        _update_caps_from_session_payload(entry.capabilities, payload)

    def drop_session_capabilities(self, *, acp_side_session_id: str) -> None:
        """Drop one session capability cache alongside binding/runtime invalidation."""

        entry = self._by_acp_side_session_id.get(acp_side_session_id)
        if entry is not None:
            entry.capabilities = _SessionCapabilities()

    def set_current_model(self, *, acp_side_session_id: str, model_id: str) -> None:
        """Keep the runtime capability mirror aligned with the latest model selection."""

        caps = self.ensure_session_capabilities(acp_side_session_id)
        if caps is not None:
            caps.current_model = model_id

    def set_current_agent(self, *, acp_side_session_id: str, agent_id: str) -> None:
        """Keep the runtime capability mirror aligned with the latest agent selection."""

        caps = self.ensure_session_capabilities(acp_side_session_id)
        if caps is not None:
            caps.current_agent = agent_id

    def build_prompt_metadata(self, *, acp_side_session_id: str) -> JSONMap:
        """Expose execution metadata without letting outer modules read session caps directly."""

        return _build_prompt_metadata(self._caps_for(acp_side_session_id=acp_side_session_id))

    def render_models_command(self, *, acp_side_session_id: str) -> str:
        """Render the model list through the runtime-owned capability mirror."""

        return _render_models_command(self._caps_for(acp_side_session_id=acp_side_session_id))

    def render_agents_command(self, *, acp_side_session_id: str) -> str:
        """Render the agent list through the runtime-owned capability mirror."""

        return _render_agents_command(self._caps_for(acp_side_session_id=acp_side_session_id))

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
                # Reusing a ready entry must not recreate the session. Only apply this
                # request's preferred selection delta so runtime reuse semantics stay intact.
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
                # When binding truth exists, try to activate the historical ACP session
                # first and fall back to new_session only if activation fails.
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
            # ACP no longer injects nanobot MCP server config into the remote side. New
            # sessions are created from cwd alone and tool capabilities stay remote-owned.
            response = await conn.new_session(cwd=str(cwd.resolve()))
            # As soon as new_session succeeds, mirror the current selection into the ACP
            # session, binding truth, and runtime entry so reconnects do not lose it.
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
        """Drop all runtime-owned ready session entries during runtime rebuild."""

        # Rebuild drops only ready sessions from the current runtime generation. Persistent
        # binding truth remains in the binding manager and is re-applied on the next ensure.
        self._by_nanobot_side_session_key.clear()
        self._by_acp_side_session_id.clear()

    def drop_ready_session(self, *, nanobot_side_session_key: str) -> None:
        """Drop one ready session entry so the next request must re-ensure it."""

        self._drop_runtime_entry(nanobot_side_session_key=nanobot_side_session_key)

    async def bootstrap_ready_sessions(self) -> list[str]:
        """Load binding truth lazily; ready sessions are ensured on demand."""

        # Deliberately avoid bulk-activating historical sessions here. Only load binding
        # truth into memory so ready sessions stay lazily established.
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

        # Preferred selection belongs to "how this request should run", but once applied
        # it must refresh binding truth so reconnect/replay keep the latest choice.
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

    def _caps_for(self, *, acp_side_session_id: str) -> _SessionCapabilities | None:
        """Return capability cache for one active session id if present."""

        entry = self._by_acp_side_session_id.get(acp_side_session_id)
        return entry.capabilities if entry is not None else None
