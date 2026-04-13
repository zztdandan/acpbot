"""运行时会话管理器：会话生命周期与能力编排（内存层）。

与 binding_manager（持久化层）协作，runtime_manager 负责运行时的内存状态。
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger

from nanobot.acp.contracts import ACPSessionPayload
from nanobot.acp.sessionmap.internal.session_caps import (
    _SessionCapabilities,
)
from nanobot.acp.sessionmap.internal.session_restore import restore_existing_session
from nanobot.acp.sessionmap.models import SessionRuntimeEntry

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime
    from nanobot.acp.sessionmap.binding_manager import SessionMapBindingManager


class SessionRuntimeManager:
    """运行时会话管理器：管理内存中的会话状态与能力缓存，通过 asyncio.Lock 保护并发。"""

    def __init__(
        self,
        *,
        runtime: ACPRuntime,
        binding_manager: SessionMapBindingManager,
    ) -> None:
        """初始化运行时会话管理器。"""
        self._runtime = runtime
        self._binding_manager = binding_manager
        self._lock = asyncio.Lock()
        self._by_nanobot_side_session_key: dict[str, SessionRuntimeEntry] = {}
        self._by_acp_side_session_id: dict[str, SessionRuntimeEntry] = {}

    def get_by_nanobot_side_session_key(
        self,
        nanobot_side_session_key: str,
    ) -> SessionRuntimeEntry | None:
        """按 nanobot_side_session_key 查找运行时会话条目，不存在返回 None。"""
        return self._by_nanobot_side_session_key.get(nanobot_side_session_key)

    def get_by_acp_side_session_id(self, acp_side_session_id: str) -> SessionRuntimeEntry | None:
        """按 acp_side_session_id 查找运行时会话条目，不存在返回 None。"""
        return self._by_acp_side_session_id.get(acp_side_session_id)

    def get_session_capabilities(self, acp_side_session_id: str) -> _SessionCapabilities | None:
        """获取会话的能力缓存对象，不存在返回 None。"""
        entry = self._by_acp_side_session_id.get(acp_side_session_id)
        return entry.capabilities if entry is not None else None

    def update_caps_from_payload(
        self,
        *,
        acp_side_session_id: str,
        payload: ACPSessionPayload,
    ) -> None:
        """从 ACP payload 更新能力缓存，条目不存在则跳过。"""
        caps = self.get_session_capabilities(acp_side_session_id)
        if caps is None:
            return
        caps.apply_session_payload(payload)

    def drop_session_capabilities(self, *, acp_side_session_id: str) -> None:
        """重置会话能力缓存为空（不删除条目本身）。"""
        entry = self._by_acp_side_session_id.get(acp_side_session_id)
        if entry is not None:
            entry.capabilities = _SessionCapabilities()

    async def ensure_ready_session(
        self,
        *,
        nanobot_side_session_key: str,
        preferred_model: str | None = None,
        preferred_agent: str | None = None,
    ) -> str:
        """确保会话就绪，返回可用的 ACP 侧会话 ID。

        处理流程：
            1. 建立 ACP 连接，不可用时抛出 RuntimeError
            2. 检查内存中是否有就绪会话（最快路径）
            3. 查询 binding truth 是否有映射（常规恢复）
            4. 若 binding 未 bootstrapped，补做 load + reconcile 再复查
            5. 恢复失败或无映射时走新会话兜底

        异常：
            RuntimeError: ACP 连接不可用
        """
        async with self._lock:
            # 先建立 ACP 连接，后续无论是恢复还是新建都会依赖同一个连接。
            await self._runtime.ensure_connection()
            conn = self._runtime._acp_client_conn
            if conn is None:
                raise RuntimeError("ACP connection is not available")

            # 运行时内存命中是最强热路径：命中后无需再触碰磁盘真相。
            existing = self._by_nanobot_side_session_key.get(nanobot_side_session_key)
            if existing is not None and existing.ready:
                await self._apply_session_selection(
                    acp_side_session_id=existing.acp_side_session_id,
                    nanobot_side_session_key=nanobot_side_session_key,
                    model_id=preferred_model,
                    agent_id=preferred_agent,
                )
                return existing.acp_side_session_id

            acp_side_session_id = self._binding_manager.resolve_session_id(nanobot_side_session_key)
            if acp_side_session_id is None and not self._binding_manager.is_bootstrapped():
                # 启动/重置后的第一次 ensure 才补做 load + reconcile；之后直接查内存真相。
                await self._binding_manager.load_persistent_truth()
                acp_side_session_id = self._binding_manager.resolve_session_id(
                    nanobot_side_session_key
                )

            if acp_side_session_id:
                activated, payload = await self._activate_existing_binding(
                    nanobot_side_session_key=nanobot_side_session_key,
                    acp_side_session_id=acp_side_session_id,
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
                    selected_model, selected_agent = self._resolve_selected_preferences(
                        nanobot_side_session_key=nanobot_side_session_key,
                        preferred_model=preferred_model,
                        preferred_agent=preferred_agent,
                    )
                    await self._apply_session_selection(
                        acp_side_session_id=acp_side_session_id,
                        nanobot_side_session_key=nanobot_side_session_key,
                        model_id=selected_model,
                        agent_id=selected_agent,
                    )
                    return acp_side_session_id
                self._drop_runtime_entry(nanobot_side_session_key=nanobot_side_session_key)

            return await self._create_ready_session(
                nanobot_side_session_key=nanobot_side_session_key,
                preferred_model=preferred_model,
                preferred_agent=preferred_agent,
                conn=conn,
            )

    def rebuild(self) -> None:
        """清空所有内存中的运行时会话条目（不影响持久化数据）。"""
        self._by_nanobot_side_session_key.clear()
        self._by_acp_side_session_id.clear()

    def drop_ready_session(self, *, nanobot_side_session_key: str) -> None:
        """从双向索引中删除指定会话的运行时条目（不清除持久化绑定）。"""
        self._drop_runtime_entry(nanobot_side_session_key=nanobot_side_session_key)

    async def _apply_session_selection(
        self,
        *,
        acp_side_session_id: str,
        nanobot_side_session_key: str,
        model_id: str | None,
        agent_id: str | None,
    ) -> None:
        """将 model/agent 选择统一刷到 ACP、持久化真相与本地能力缓存。

        处理流程：
            1. 若 ACP 连接不可用则静默返回
            2. 刷 model_id → ACP API + 能力缓存 + 持久化绑定
            3. 刷 agent_id → ACP API + 能力缓存 + 持久化绑定
        """
        conn = self._runtime._acp_client_conn
        if conn is None:
            return

        if model_id and hasattr(conn, "set_session_model"):
            await conn.set_session_model(model_id=model_id, session_id=acp_side_session_id)
            caps = self.get_session_capabilities(acp_side_session_id)
            if caps is not None:
                caps.remember_current_model(model_id)
            self._binding_manager.update_bound_model(nanobot_side_session_key, model_id)

        if agent_id and hasattr(conn, "set_session_mode"):
            await conn.set_session_mode(mode_id=agent_id, session_id=acp_side_session_id)
            caps = self.get_session_capabilities(acp_side_session_id)
            if caps is not None:
                caps.remember_current_agent(agent_id)
            self._binding_manager.update_bound_agent(nanobot_side_session_key, agent_id)

    async def _activate_existing_binding(
        self,
        *,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
    ) -> tuple[bool, ACPSessionPayload | None]:
        """按需把已有 binding 对应的 ACP session 接入当前 runtime。

        该方法属于 runtime owner：
            - 是否需要激活由 ensure_ready_session 决定
            - 激活动作依赖当前 ACP 连接
            - 激活结果直接服务于 runtime ready entry 建立

        兼容性说明：
            实际的 resume/load fallback 逻辑统一收口到
            sessionmap.internal.session_restore，避免多个调用点各自维护。
            其中顺序固定为 resume -> load，不允许再引入 load-first 分支。
        """

        conn = self._runtime._acp_client_conn
        if conn is None:
            raise RuntimeError("ACP connection is not available")

        cwd = (
            Path(self._runtime.acp_config.cwd).expanduser()
            if self._runtime.acp_config.cwd
            else self._runtime.workspace
        )
        resolved_cwd = str(cwd.resolve())

        if not hasattr(conn, "resume_session") and not hasattr(conn, "load_session"):
            return True, None

        try:
            response = await restore_existing_session(
                conn,
                cwd=resolved_cwd,
                session_id=acp_side_session_id,
            )
        except Exception as exc:
            logger.warning(
                "ACP existing session activation failed nanobot_side_session_key={} acp_side_session_id={} error_type={} error={}",
                nanobot_side_session_key,
                acp_side_session_id,
                type(exc).__name__,
                exc,
            )
            return False, None

        return True, response

    def _resolve_selected_preferences(
        self,
        *,
        nanobot_side_session_key: str,
        preferred_model: str | None,
        preferred_agent: str | None,
    ) -> tuple[str | None, str | None]:
        """解析 model/agent 选择优先级：preferred > bound > default。"""

        bound_model, bound_agent = self._binding_manager.get_bound_selection(
            nanobot_side_session_key
        )
        selected_model = preferred_model or bound_model or self._runtime.acp_config.default_model
        selected_agent = preferred_agent or bound_agent or self._runtime.acp_config.default_mode
        return selected_model, selected_agent

    async def _create_ready_session(
        self,
        *,
        nanobot_side_session_key: str,
        preferred_model: str | None,
        preferred_agent: str | None,
        conn: Any,
    ) -> str:
        """创建新 ACP 会话并完成 runtime entry / binding / selection 一次性收口。"""

        cwd = (
            Path(self._runtime.acp_config.cwd).expanduser()
            if self._runtime.acp_config.cwd
            else self._runtime.workspace
        )
        response = await conn.new_session(cwd=str(cwd.resolve()))
        acp_side_session_id = response.session_id
        self._store_runtime_entry(
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
        )
        self.update_caps_from_payload(acp_side_session_id=acp_side_session_id, payload=response)

        # 先建立 binding，再统一应用最终选择；这样后续读取的持久化真相与运行态一致。
        self._binding_manager.bind_session(nanobot_side_session_key, acp_side_session_id)
        selected_model, selected_agent = self._resolve_selected_preferences(
            nanobot_side_session_key=nanobot_side_session_key,
            preferred_model=preferred_model,
            preferred_agent=preferred_agent,
        )
        await self._apply_session_selection(
            acp_side_session_id=acp_side_session_id,
            nanobot_side_session_key=nanobot_side_session_key,
            model_id=selected_model,
            agent_id=selected_agent,
        )

        logger.info(
            "ACP ready session established nanobot_side_session_key={} acp_side_session_id={}",
            nanobot_side_session_key,
            acp_side_session_id,
        )
        return acp_side_session_id

    def _store_runtime_entry(
        self,
        *,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
    ) -> str:
        """创建 SessionRuntimeEntry 并写入双向索引。"""
        entry = SessionRuntimeEntry(
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
            ready=True,
        )
        # 双向索引：同一个 entry 对象被两个字典引用
        self._by_nanobot_side_session_key[nanobot_side_session_key] = entry
        self._by_acp_side_session_id[acp_side_session_id] = entry
        return acp_side_session_id

    def _drop_runtime_entry(self, *, nanobot_side_session_key: str) -> None:
        """从双向索引中删除条目，不存在则静默跳过。"""
        entry = self._by_nanobot_side_session_key.pop(nanobot_side_session_key, None)
        if entry is not None:
            self._by_acp_side_session_id.pop(entry.acp_side_session_id, None)
