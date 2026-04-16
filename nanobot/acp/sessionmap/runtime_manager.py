"""运行时会话管理器：会话生命周期与能力编排（内存层）。

与 binding_manager（持久化层）协作，runtime_manager 负责运行时的内存状态。
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from loguru import logger

from nanobot.acp.contracts import ACPSessionPayload
from nanobot.acp.runtime_models import SessionSelectionResult
from nanobot.acp.sessionmap.internal.session_caps import (
    _SessionCapabilities,
)
from nanobot.acp.sessionmap.internal.session_restore import restore_existing_session
from nanobot.acp.sessionmap.models import SessionRuntimeEntry

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime
    from nanobot.acp.sessionmap.binding_manager import SessionMapBindingManager


class SessionRuntimeManager:
    """运行时会话管理器：管理内存中的会话状态与能力缓存，通过 asyncio.Lock 保护并发。

    职责：
        - 维护运行时会话条目的双向索引（nanobot_side_session_key / acp_side_session_id）
        - 提供会话能力缓存查询与更新接口
        - 确保会话就绪（ensure_ready_session）：恢复现有会话或创建新会话
        - 统一刷写 model/agent 选择到 ACP、持久化真相与本地能力缓存

    生命周期：
        - 创建：ACPRuntime 初始化时实例化，传入 runtime 和 binding_manager 依赖
        - 重建：rebuild 方法清空所有内存条目（不影响持久化数据）
        - 销毁：ACPRuntime 销毁时一同销毁
    """

    def __init__(
        self,
        *,
        runtime: ACPRuntime,
        binding_manager: SessionMapBindingManager,
    ) -> None:
        """初始化运行时会话管理器（依赖注入 runtime 和 binding_manager）。"""
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
        """获取会话的能力缓存对象，不存在返回 None。

        使用示例：
            caps = runtime_manager.get_session_capabilities("abc123")
            if caps:
                print(f"当前模型: {caps.current_model}")
        """
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
            2. 内存命中检查：有就绪会话则直接返回（最快路径）
            3. 查询 binding truth：有映射则尝试激活现有会话
            4. binding 未 bootstrapped 时补做 load + reconcile 再复查
            5. 恢复失败或无映射时创建新会话（兜底）

        参数：
            nanobot_side_session_key: nanobot 侧会话标识（如 "user_id:chat_id" 或 "cli:direct"）
            preferred_model: 期望的模型 ID（如 "gpt-4"），None 表示使用会话当前模型
            preferred_agent: 期望的代理 ID（如 "code-assistant"），None 表示使用会话当前代理

        返回：
            str: ACP 侧会话 ID（可用于后续 ACP API 调用）

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
            1. 检查 ACP 连接可用性，不可用时静默返回
            2. 刷 model_id（如果提供）：
               - 调用 ACP set_session_model API
               - 更新本地能力缓存（remember_current_model）
               - 更新持久化绑定（update_bound_model）
            3. 刷 agent_id（如果提供）：
               - 调用 ACP set_session_mode API
               - 更新本地能力缓存（remember_current_agent）
               - 更新持久化绑定（update_bound_agent）

        参数：
            acp_side_session_id: ACP 侧会话 ID（目标会话）
            nanobot_side_session_key: nanobot 侧会话标识（用于更新持久化绑定）
            model_id: 模型 ID（如 "gpt-4"），None 表示不更新模型
            agent_id: 代理 ID（如 "code-assistant"），None 表示不更新代理
        """
        conn = self._runtime._acp_client_conn
        if conn is None:
            return

        caps = self.get_session_capabilities(acp_side_session_id)

        if model_id and hasattr(conn, "set_session_model"):
            if caps is not None and caps.available_models and model_id not in caps.available_models:
                logger.warning(
                    "Skip ACP set_session_model because model is not in current catalog acp_side_session_id={} model_id={}",
                    acp_side_session_id,
                    model_id,
                )
            else:
                try:
                    await conn.set_session_model(model_id=model_id, session_id=acp_side_session_id)
                except Exception as exc:
                    # 中文注释：任何 set 失败都按统一恢复策略执行：清空持久化选择并 resume。
                    await self._recover_after_set_failure_locked(
                        nanobot_side_session_key=nanobot_side_session_key,
                        acp_side_session_id=acp_side_session_id,
                        failed_kind="model",
                        failed_value=model_id,
                        error=exc,
                    )
                    return
                else:
                    if caps is not None:
                        caps.remember_current_model(model_id)
                    self._binding_manager.update_bound_model(nanobot_side_session_key, model_id)

        if agent_id and hasattr(conn, "set_session_mode"):
            if caps is not None and caps.available_agents and agent_id not in caps.available_agents:
                logger.warning(
                    "Skip ACP set_session_mode because agent is not in current catalog acp_side_session_id={} agent_id={}",
                    acp_side_session_id,
                    agent_id,
                )
                return
            try:
                await conn.set_session_mode(mode_id=agent_id, session_id=acp_side_session_id)
            except Exception as exc:
                await self._recover_after_set_failure_locked(
                    nanobot_side_session_key=nanobot_side_session_key,
                    acp_side_session_id=acp_side_session_id,
                    failed_kind="agent",
                    failed_value=agent_id,
                    error=exc,
                )
                return
            else:
                if caps is not None:
                    caps.remember_current_agent(agent_id)
                self._binding_manager.update_bound_agent(nanobot_side_session_key, agent_id)

    async def set_model_safe(
        self,
        *,
        nanobot_side_session_key: str,
        model_id: str,
    ) -> SessionSelectionResult:
        """安全设置会话模型：统一封装会话就绪、SDK 调用与失败恢复。"""

        await self._runtime.ensure_connection()
        acp_side_session_id = await self.ensure_ready_session(
            nanobot_side_session_key=nanobot_side_session_key,
        )
        return await self.apply_explicit_selection_safe(
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
            model_id=model_id,
        )

    async def set_agent_safe(
        self,
        *,
        nanobot_side_session_key: str,
        agent_id: str,
    ) -> SessionSelectionResult:
        """安全设置会话代理：统一封装会话就绪、SDK 调用与失败恢复。"""

        await self._runtime.ensure_connection()
        acp_side_session_id = await self.ensure_ready_session(
            nanobot_side_session_key=nanobot_side_session_key,
        )
        return await self.apply_explicit_selection_safe(
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
            agent_id=agent_id,
        )

    async def recover_after_set_failure(
        self,
        *,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
        failed_kind: str,
        failed_value: str,
        error: Exception,
    ) -> None:
        """公开恢复入口：命令路径 set 失败后统一清理持久化选择并尝试 resume。"""

        async with self._lock:
            await self._recover_after_set_failure_locked(
                nanobot_side_session_key=nanobot_side_session_key,
                acp_side_session_id=acp_side_session_id,
                failed_kind=failed_kind,
                failed_value=failed_value,
                error=error,
            )

    async def apply_explicit_selection_safe(
        self,
        *,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
        model_id: str | None = None,
        agent_id: str | None = None,
    ) -> SessionSelectionResult:
        """显式 set 模型/代理的安全入口。

        约束：
            - 一次只允许设置一个目标（model 或 agent）
            - 失败时统一走恢复逻辑（清空持久化选择 + resume）
            - 返回结构化结果，供上层路由统一构造用户可读回包
        """

        target_count = int(bool(model_id)) + int(bool(agent_id))
        if target_count != 1:
            return SessionSelectionResult(
                success=False,
                target="model" if model_id else "agent",
                value=model_id or agent_id or "",
                reason="exactly one selection target must be provided",
                acp_side_session_id=acp_side_session_id,
            )

        async with self._lock:
            conn = self._runtime._acp_client_conn
            if conn is None:
                return SessionSelectionResult(
                    success=False,
                    target="model" if model_id else "agent",
                    value=model_id or agent_id or "",
                    reason="ACP connection is not available",
                    acp_side_session_id=acp_side_session_id,
                )

            caps = self.get_session_capabilities(acp_side_session_id)

            if model_id is not None:
                if (
                    caps is not None
                    and caps.available_models
                    and model_id not in caps.available_models
                ):
                    # 中文注释：catalog 可用时严格校验，避免把无效 model 误报为切换成功。
                    return SessionSelectionResult(
                        success=False,
                        target="model",
                        value=model_id,
                        reason=f"invalid model id: {model_id}",
                        acp_side_session_id=acp_side_session_id,
                    )
                try:
                    await conn.set_session_model(model_id=model_id, session_id=acp_side_session_id)
                except Exception as exc:
                    await self._recover_after_set_failure_locked(
                        nanobot_side_session_key=nanobot_side_session_key,
                        acp_side_session_id=acp_side_session_id,
                        failed_kind="model",
                        failed_value=model_id,
                        error=exc,
                    )
                    return SessionSelectionResult(
                        success=False,
                        target="model",
                        value=model_id,
                        reason=str(exc),
                        acp_side_session_id=acp_side_session_id,
                    )

                if caps is not None:
                    caps.remember_current_model(model_id)
                self._binding_manager.update_bound_model(nanobot_side_session_key, model_id)
                return SessionSelectionResult(
                    success=True,
                    target="model",
                    value=model_id,
                    reason="ok",
                    acp_side_session_id=acp_side_session_id,
                )

            assert agent_id is not None
            if caps is not None and caps.available_agents and agent_id not in caps.available_agents:
                return SessionSelectionResult(
                    success=False,
                    target="agent",
                    value=agent_id,
                    reason=f"invalid agent id: {agent_id}",
                    acp_side_session_id=acp_side_session_id,
                )
            try:
                await conn.set_session_mode(mode_id=agent_id, session_id=acp_side_session_id)
            except Exception as exc:
                await self._recover_after_set_failure_locked(
                    nanobot_side_session_key=nanobot_side_session_key,
                    acp_side_session_id=acp_side_session_id,
                    failed_kind="agent",
                    failed_value=agent_id,
                    error=exc,
                )
                return SessionSelectionResult(
                    success=False,
                    target="agent",
                    value=agent_id,
                    reason=str(exc),
                    acp_side_session_id=acp_side_session_id,
                )

            if caps is not None:
                caps.remember_current_agent(agent_id)
            self._binding_manager.update_bound_agent(nanobot_side_session_key, agent_id)
            return SessionSelectionResult(
                success=True,
                target="agent",
                value=agent_id,
                reason="ok",
                acp_side_session_id=acp_side_session_id,
            )

    async def _recover_after_set_failure_locked(
        self,
        *,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
        failed_kind: str,
        failed_value: str,
        error: Exception,
    ) -> None:
        """set 失败恢复策略（调用方需已持有 _lock）。

        恢复动作：
            1) 清空 sessionmap 里的 bound_model/bound_agent，避免错误选择持续污染。
            2) 尝试对同一 ACP session 做 resume（含 load 回退）保持运行态可用。
        """

        logger.warning(
            "ACP set_{} failed; reset persisted selection and try resume nanobot_side_session_key={} acp_side_session_id={} value={} error_type={} error={}",
            failed_kind,
            nanobot_side_session_key,
            acp_side_session_id,
            failed_value,
            type(error).__name__,
            error,
        )

        self._binding_manager.clear_bound_selection(nanobot_side_session_key)
        try:
            activated, payload = await self._activate_existing_binding(
                nanobot_side_session_key=nanobot_side_session_key,
                acp_side_session_id=acp_side_session_id,
            )
        except Exception as resume_exc:
            logger.warning(
                "ACP resume after set failure raised nanobot_side_session_key={} acp_side_session_id={} error_type={} error={}",
                nanobot_side_session_key,
                acp_side_session_id,
                type(resume_exc).__name__,
                resume_exc,
            )
            return

        if activated and payload is not None:
            self.update_caps_from_payload(acp_side_session_id=acp_side_session_id, payload=payload)
            return

        logger.warning(
            "ACP resume after set failure did not activate session nanobot_side_session_key={} acp_side_session_id={}",
            nanobot_side_session_key,
            acp_side_session_id,
        )

    async def _activate_existing_binding(
        self,
        *,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
    ) -> tuple[bool, ACPSessionPayload | None]:
        """激活现有 binding 对应的 ACP session（接入当前 runtime）。

        职责：
            - 调用 restore_existing_session 恢复会话（resume -> load 回退）
            - 恢复成功返回 (True, payload)，失败返回 (False, None)
            - 属于 runtime owner 方法：激活动作依赖当前 ACP 连接

        参数：
            nanobot_side_session_key: nanobot 侧会话标识
            acp_side_session_id: ACP 侧会话 ID（要激活的目标会话）

        返回：
            tuple[bool, ACPSessionPayload | None]:
                - bool: 是否激活成功（True 表示成功，False 表示失败）
                - ACPSessionPayload | None: 恢复后的 session payload（成功时返回）

        兼容性说明：
            resume/load fallback 逻辑统一收口到 sessionmap.internal.session_restore，
            顺序固定为 resume -> load，不允许再引入 load-first 分支。
        """

        conn = self._runtime._acp_client_conn
        if conn is None:
            raise RuntimeError("ACP connection is not available")

        resolved_cwd = str(self._runtime.resolve_acp_workspace_path())

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

        response = await conn.new_session(cwd=str(self._runtime.resolve_acp_workspace_path()))
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
