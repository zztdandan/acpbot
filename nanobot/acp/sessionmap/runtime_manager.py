"""运行时会话管理器：会话生命周期与能力编排（内存层）。

与 binding_manager（持久化层）协作，runtime_manager 负责运行时的内存状态。
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from loguru import logger

from nanobot.acp.contracts import ACPSessionPayload
from nanobot.acp.runtime_models import SessionSelectionResult
from nanobot.acp.sessionmap.internal.model_switch import switch_model_compat
from nanobot.acp.sessionmap.internal.session_caps import (
    ModelSource,
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
        - 统一刷写 model 选择到 ACP、持久化真相与本地能力缓存

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
                    selected_model = self._resolve_selected_model(
                        nanobot_side_session_key=nanobot_side_session_key,
                        preferred_model=preferred_model,
                    )
                    await self._apply_session_selection(
                        acp_side_session_id=acp_side_session_id,
                        nanobot_side_session_key=nanobot_side_session_key,
                        model_id=selected_model,
                    )
                    return acp_side_session_id
                self._drop_runtime_entry(nanobot_side_session_key=nanobot_side_session_key)

            return await self._create_ready_session(
                nanobot_side_session_key=nanobot_side_session_key,
                preferred_model=preferred_model,
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
    ) -> None:
        """将 model 选择刷到 ACP、持久化真相与本地能力缓存。

        中文注释：本入口只在 resume/load/new 之后或显式 preferred_model 命中时调用。
        set 成功后只更新本地 capability/binding，不再 resume/load refresh，避免 backend
        把刚设置的 model 重置回默认值。
        """
        if not model_id:
            return
        conn = self._runtime._acp_client_conn
        if conn is None:
            return

        caps = self.get_session_capabilities(acp_side_session_id)
        result = await switch_model_compat(
            conn,
            session_id=acp_side_session_id,
            model_id=model_id,
            caps=caps,
        )
        if not result.success:
            if caps is not None and caps.model_source is ModelSource.UNKNOWN and caps.current_model == model_id:
                # 中文注释：unknown source 没有真实 catalog 时，若目标就是 runtime 配置的
                # default/current model，则把这次选择视为本地 no-op 成功：既不触碰 backend，
                # 也不再把用户暴露为失败。
                caps.materialize_unknown_default_model(model_id)
                self._binding_manager.update_bound_model(nanobot_side_session_key, model_id)
                return
            logger.warning(
                "Skip ACP model selection nanobot_side_session_key={} acp_side_session_id={} model_id={} reason={}",
                nanobot_side_session_key,
                acp_side_session_id,
                model_id,
                result.reason,
            )
            return

        if caps is not None:
            if result.response_payload is not None:
                caps.apply_session_payload(result.response_payload)
            caps.remember_current_model(model_id)
        self._binding_manager.update_bound_model(nanobot_side_session_key, model_id)

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
    ) -> SessionSelectionResult:
        """显式 set 模型的安全入口。

        中文注释：失败不再触发恢复 refresh；按 STAGE1 规则，model 操作层失败只返回原因，
        避免 resume/load/new 重置 session model。
        """
        if not model_id:
            return SessionSelectionResult(
                success=False,
                target="model",
                value="",
                reason="model id is required",
                acp_side_session_id=acp_side_session_id,
            )

        async with self._lock:
            conn = self._runtime._acp_client_conn
            if conn is None:
                return SessionSelectionResult(
                    success=False,
                    target="model",
                    value=model_id,
                    reason="ACP connection is not available",
                    acp_side_session_id=acp_side_session_id,
                )

            caps = self.get_session_capabilities(acp_side_session_id)
            result = await switch_model_compat(
                conn,
                session_id=acp_side_session_id,
                model_id=model_id,
                caps=caps,
            )
            if not result.success:
                if caps is not None and caps.model_source is ModelSource.UNKNOWN:
                    current_or_default = caps.current_model or self._runtime.acp_config.default_model
                    if model_id == current_or_default:
                        caps.materialize_unknown_default_model(model_id)
                        self._binding_manager.update_bound_model(nanobot_side_session_key, model_id)
                        return SessionSelectionResult(
                            success=True,
                            target="model",
                            value=model_id,
                            reason="ok",
                            acp_side_session_id=acp_side_session_id,
                        )
                    if caps.available_models and model_id not in caps.available_models:
                        return SessionSelectionResult(
                            success=False,
                            target="model",
                            value=model_id,
                            reason="invalid model id for current ACP backend catalog",
                            acp_side_session_id=acp_side_session_id,
                        )
                return SessionSelectionResult(
                    success=False,
                    target="model",
                    value=model_id,
                    reason=result.reason,
                    acp_side_session_id=acp_side_session_id,
                )

            if caps is not None:
                if result.response_payload is not None:
                    caps.apply_session_payload(result.response_payload)
                caps.remember_current_model(model_id)
            self._binding_manager.update_bound_model(nanobot_side_session_key, model_id)
            return SessionSelectionResult(
                success=True,
                target="model",
                value=model_id,
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
        """历史恢复入口：仅保留给非 model set 的旧调用方兜底。

        中文注释：显式 `/set_model` 已不再调用此恢复入口，因为 model 操作层禁止
        resume/load refresh。若未来还有执行链路外的 set 失败走到这里，只清 bound_model。
        """

        logger.warning(
            "ACP set_{} failed; reset persisted model selection and try resume nanobot_side_session_key={} acp_side_session_id={} value={} error_type={} error={}",
            failed_kind,
            nanobot_side_session_key,
            acp_side_session_id,
            failed_value,
            type(error).__name__,
            error,
        )

        self._binding_manager.clear_bound_model(nanobot_side_session_key)
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

    def _resolve_selected_model(
        self,
        *,
        nanobot_side_session_key: str,
        preferred_model: str | None,
    ) -> str | None:
        """解析 model 选择优先级：preferred > bound > default。"""
        bound_model = self._binding_manager.get_bound_model(nanobot_side_session_key)
        return preferred_model or bound_model or self._runtime.acp_config.default_model

    async def _create_ready_session(
        self,
        *,
        nanobot_side_session_key: str,
        preferred_model: str | None,
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
        selected_model = self._resolve_selected_model(
            nanobot_side_session_key=nanobot_side_session_key,
            preferred_model=preferred_model,
        )
        entry = self.get_by_acp_side_session_id(acp_side_session_id)
        if entry is not None and entry.capabilities.model_source is ModelSource.UNKNOWN and selected_model:
            entry.capabilities.materialize_unknown_default_model(selected_model)
        await self._apply_session_selection(
            acp_side_session_id=acp_side_session_id,
            nanobot_side_session_key=nanobot_side_session_key,
            model_id=selected_model,
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
