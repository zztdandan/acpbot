"""运行时会话管理器：会话生命周期与能力编排。

本模块负责运行时会话的生命周期管理，包括：
    1. 会话创建：确保会话存在并就绪
    2. 会话查询：通过 nanobot_key 或 acp_id 查找会话
    3. 能力管理：维护会话的模型/代理能力缓存
    4. 会话清理：关闭会话时清理运行时条目

与 binding_manager 的协作：
    - binding_manager: 持久化层（磁盘存储）
    - runtime_manager: 运行时层（内存状态）
    - ensure_ready_session 中会调用 binding_manager 的方法

线程安全：
    - 使用 asyncio.Lock 保护并发访问
    - 所有公共方法都是线程安全的

使用示例：
    runtime_manager = SessionRuntimeManager(runtime, binding_manager)
    acp_id = await runtime_manager.ensure_ready_session(
        nanobot_side_session_key="user123:chat456",
        preferred_model="gpt-4",
    )
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from nanobot.acp.contracts import ACPSessionPayload
from nanobot.acp.sessionmap.internal.session_caps import (
    _SessionCapabilities,
)
from nanobot.acp.sessionmap.models import SessionRuntimeEntry

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime
    from nanobot.acp.sessionmap.binding_manager import SessionMapBindingManager


class SessionRuntimeManager:
    """运行时会话管理器：管理内存中的会话状态和能力。

    核心职责：
        1. 会话生命周期：创建/查询/销毁运行时会话条目
        2. 能力缓存：维护每个会话的模型/代理能力信息
        3. 会话选择：应用用户的模型/代理偏好
        4. 并发控制：通过 asyncio.Lock 保护并发访问

    内部状态：
        _by_nanobot_side_session_key: 按 nanobot_key 索引的会话字典
        _by_acp_side_session_id: 按 acp_id 索引的会话字典（双向索引）

    使用场景：
        - process_direct 中确保会话就绪
        - dispatch_inbound 中查询会话能力
        - stop_session 中清理会话条目
    """

    def __init__(
        self,
        *,
        runtime: ACPRuntime,
        binding_manager: SessionMapBindingManager,
    ) -> None:
        """初始化运行时会话管理器。

        参数：
            runtime: ACPRuntime 实例（用于访问 ACP 连接）
            binding_manager: SessionMapBindingManager 实例（用于持久化操作）

        初始化状态：
            _runtime: 保存 runtime 引用（用于访问 acp_config 和 acp_client_conn）
            _binding_manager: 保存 binding_manager 引用（用于调用持久化方法）
            _lock: asyncio.Lock（保护并发访问）
            _by_nanobot_side_session_key: 空字典（按 nanobot_key 索引）
            _by_acp_side_session_id: 空字典（按 acp_id 索引）

        注意：
            此方法同步执行（不涉及 IO）。
            通常在 ACPRuntime 初始化时调用。
        """
        self._runtime = runtime
        self._binding_manager = binding_manager
        self._lock = asyncio.Lock()
        self._by_nanobot_side_session_key: dict[str, SessionRuntimeEntry] = {}
        self._by_acp_side_session_id: dict[str, SessionRuntimeEntry] = {}

    def get_by_nanobot_side_session_key(
        self,
        nanobot_side_session_key: str,
    ) -> SessionRuntimeEntry | None:
        """根据 nanobot 侧会话键查找运行时会话条目。

        参数：
            nanobot_side_session_key: 业务侧会话键（如 "user123:chat456"）

        返回：
            SessionRuntimeEntry | None: 会话条目（如果存在），否则 None

        使用场景：
            - dispatch_inbound 中查找会话
            - 测试代码中断言会话状态

        注意：
            此方法只查询内存中的 _by_nanobot_side_session_key。
            调用方需要确保会话已通过 ensure_ready_session 创建。
        """
        return self._by_nanobot_side_session_key.get(nanobot_side_session_key)

    def get_by_acp_side_session_id(self, acp_side_session_id: str) -> SessionRuntimeEntry | None:
        """根据 ACP 侧会话 ID 查找运行时会话条目。

        参数：
            acp_side_session_id: ACP 侧会话 ID（如 "abc123"）

        返回：
            SessionRuntimeEntry | None: 会话条目（如果存在），否则 None

        使用场景：
            - dispatch_inbound 中通过 acp_id 查找会话
            - 观测事件中上报会话状态

        注意：
            此方法只查询内存中的 _by_acp_side_session_id。
        """
        return self._by_acp_side_session_id.get(acp_side_session_id)

    def get_session_capabilities(self, acp_side_session_id: str) -> _SessionCapabilities | None:
        """获取会话的能力缓存对象。

        参数：
            acp_side_session_id: ACP 侧会话 ID

        返回：
            _SessionCapabilities | None: 能力缓存（如果会话存在），否则 None

        使用场景：
            - render_models_command 中获取可用模型列表
            - render_agents_command 中获取可用代理列表
            - build_prompt_metadata 中构建提示元数据

        注意：
            返回的是会话条目的 capabilities 引用（可修改）。
        """
        entry = self._by_acp_side_session_id.get(acp_side_session_id)
        return entry.capabilities if entry is not None else None

    def update_caps_from_payload(
        self,
        *,
        acp_side_session_id: str,
        payload: ACPSessionPayload,
    ) -> None:
        """从 ACP 会话负载更新能力缓存。

        参数：
            acp_side_session_id: ACP 侧会话 ID
            payload: ACP 会话负载（包含模型/代理信息）

        处理流程：
            1. 查找会话条目
            2. 如果条目不存在：直接返回
            3. 调用能力对象的 apply_session_payload 合并缓存

        使用场景：
            - ensure_ready_session 中创建会话后
            - activate_session 中激活会话后

        注意：
            此方法会修改会话条目的 capabilities（原地更新）。
        """
        caps = self.get_session_capabilities(acp_side_session_id)
        if caps is None:
            return
        caps.apply_session_payload(payload)

    def drop_session_capabilities(self, *, acp_side_session_id: str) -> None:
        """丢弃会话的能力缓存（重置为空）。

        参数：
            acp_side_session_id: ACP 侧会话 ID

        处理流程：
            1. 查找会话条目
            2. 如果条目存在：创建新的空 _SessionCapabilities

        使用场景：
            - _reconcile_entries 中清理 stale 会话
            - stop_session 中关闭会话

        注意：
            此方法不会删除会话条目，只重置 capabilities。
        """
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
        """确保会话就绪：返回可用的 ACP 侧会话 ID。

        参数：
            nanobot_side_session_key: 业务侧会话键（如 "user123:chat456"）
            preferred_model: 首选模型（如 "gpt-4"，可选）
            preferred_agent: 首选代理（如 "assistant"，可选）

        返回：
            str: ACP 侧会话 ID（已就绪可用）

        处理流程（3 层查找）：
            1. 检查内存中是否有就绪的会话条目（最快路径）
            2. 尝试激活 binding_manager 中的历史绑定（恢复路径）
            3. 创建新的 ACP 会话（兜底路径）

        详细步骤：
            步骤 1: 加载持久化真相（load_persistent_truth）
            步骤 2: 如果内存中存在且 ready=True，直接返回（快速路径）
            步骤 3: 确保 ACP 连接可用
            步骤 4: 查找已有绑定（resolve_session_id）
            步骤 5a: 如果有绑定，尝试激活（activate_session）
            步骤 5b: 如果激活成功，更新能力缓存并返回
            步骤 5c: 如果激活失败，清理并继续
            步骤 6: 创建新会话（conn.new_session）
            步骤 7: 应用模型/代理选择（优先级：preferred > bound > default）
            步骤 8: 创建绑定（bind_session + update_bound_model/agent）
            步骤 9: 存储运行时条目并返回

        模型选择优先级：
            preferred_model > bound_model > acp_config.default_model

        代理选择优先级：
            preferred_agent > bound_agent > acp_config.default_mode

        异常：
            RuntimeError: ACP 连接不可用时抛出

        使用场景：
            - process_direct 中发送请求前确保会话就绪
            - dispatch_inbound 中处理入站消息前

        注意：
            此方法是异步的（需要 await）。
            使用 _lock 保护并发访问（同一时间只处理一个请求）。
        """
        async with self._lock:
            # 步骤 1: 加载持久化真相
            await self._binding_manager.load_persistent_truth()

            # 步骤 2: 检查内存中是否有就绪的会话（快速路径）
            existing = self._by_nanobot_side_session_key.get(nanobot_side_session_key)
            if existing is not None and existing.ready:
                await self._apply_runtime_selection(
                    acp_side_session_id=existing.acp_side_session_id,
                    nanobot_side_session_key=nanobot_side_session_key,
                    preferred_model=preferred_model,
                    preferred_agent=preferred_agent,
                )
                return existing.acp_side_session_id

            # 步骤 3: 确保 ACP 连接可用
            await self._runtime.ensure_connection()
            conn = self._runtime._acp_client_conn
            if conn is None:
                raise RuntimeError("ACP connection is not available")

            # 步骤 4-5: 尝试激活已有绑定（恢复路径）
            acp_side_session_id = self._binding_manager.resolve_session_id(nanobot_side_session_key)
            if acp_side_session_id:
                activated, payload = await self._binding_manager.activate_session(
                    nanobot_side_session_key,
                    acp_side_session_id,
                )
                if activated:
                    # 激活成功：更新运行时状态并返回
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
                # 激活失败：清理运行时条目
                self._drop_runtime_entry(nanobot_side_session_key=nanobot_side_session_key)

            # 步骤 6: 创建新会话（兜底路径）
            cwd = (
                Path(self._runtime.acp_config.cwd).expanduser()
                if self._runtime.acp_config.cwd
                else self._runtime.workspace
            )
            response = await conn.new_session(cwd=str(cwd.resolve()))
            acp_side_session_id = response.session_id

            # 步骤 7: 计算模型/代理选择（优先级：preferred > bound > default）
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
                caps = self.get_session_capabilities(acp_side_session_id)
                if caps is not None:
                    caps.remember_current_model(selected_model)
            if selected_agent and hasattr(conn, "set_session_mode"):
                await conn.set_session_mode(mode_id=selected_agent, session_id=acp_side_session_id)
                caps = self.get_session_capabilities(acp_side_session_id)
                if caps is not None:
                    caps.remember_current_agent(selected_agent)

            # 步骤 8: 创建绑定并持久化
            self._binding_manager.bind_session(nanobot_side_session_key, acp_side_session_id)
            if selected_model:
                self._binding_manager.update_bound_model(nanobot_side_session_key, selected_model)
            if selected_agent:
                self._binding_manager.update_bound_agent(nanobot_side_session_key, selected_agent)

            # 步骤 9: 存储运行时条目并记录日志
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
                caps = self.get_session_capabilities(acp_side_session_id)
                if caps is not None:
                    caps.remember_current_model(selected_model)
            if selected_agent:
                caps = self.get_session_capabilities(acp_side_session_id)
                if caps is not None:
                    caps.remember_current_agent(selected_agent)
            return acp_side_session_id

    def rebuild(self) -> None:
        """重建运行时状态：清空所有内存中的会话条目。

        处理流程：
            1. 清空 _by_nanobot_side_session_key
            2. 清空 _by_acp_side_session_id

        使用场景：
            - ACPRuntime 重置连接后（需要重新创建运行时条目）
            - 测试代码中重置状态
            - close 方法中清理资源

        注意：
            此方法不会清除持久化数据（binding_manager 的数据不受影响）。
            调用方需要在 rebuild 后重新调用 ensure_ready_session。
        """
        self._by_nanobot_side_session_key.clear()
        self._by_acp_side_session_id.clear()

    def drop_ready_session(self, *, nanobot_side_session_key: str) -> None:
        """丢弃指定会话的运行时条目。

        参数：
            nanobot_side_session_key: 要丢弃的业务侧会话键

        处理流程：
            调用 _drop_runtime_entry 从双向索引中删除条目。

        使用场景：
            - drop_session_binding_and_runtime_entry 中清理运行时状态
            - stop_session 中关闭会话
            - 测试代码中重置特定会话

        注意：
            此方法不会清除持久化绑定（binding_manager.unbind 不在此调用）。
            只清除内存中的运行时条目。
        """
        self._drop_runtime_entry(nanobot_side_session_key=nanobot_side_session_key)

    async def bootstrap_ready_sessions(self) -> list[str]:
        """启动时预加载持久化真相。

        返回：
            list[str]: 空列表（当前实现不预激活会话）

        处理流程：
            1. 调用 binding_manager.load_persistent_truth 加载持久化绑定
            2. 返回空列表（延迟激活，不在此阶段创建会话）

        设计说明：
            当前实现采用延迟激活策略（ensure_ready_session 中按需激活）。
            不在此阶段批量激活所有会话（避免启动时间过长）。

        使用场景：
            - ACPRuntime 启动时调用
            - 测试代码中手动触发加载
        """
        await self._binding_manager.load_persistent_truth()
        return []

    def update_runtime_selection(
        self,
        *,
        nanobot_side_session_key: str,
        bound_model: str | None = None,
        bound_agent: str | None = None,
    ) -> None:
        """更新运行时会话的模型/代理选择（仅内存）。

        参数：
            nanobot_side_session_key: 业务侧会话键
            bound_model: 新的模型名称（如果为 None 则不更新）
            bound_agent: 新的代理名称（如果为 None 则不更新）

        处理流程：
            1. 查找运行时会话条目
            2. 如果条目不存在：直接返回
            3. 更新 bound_model（如果提供了新值）
            4. 更新 bound_agent（如果提供了新值）

        使用场景：
            - _apply_runtime_selection 中同步更新运行时状态
            - 用户切换模型/代理后更新内存中的绑定

        注意：
            此方法只更新内存中的运行时条目，不持久化。
            持久化由 binding_manager.update_bound_model/agent 处理。
        """
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
        """应用运行时模型/代理选择（调用 ACP API + 更新缓存 + 持久化）。

        参数：
            acp_side_session_id: ACP 侧会话 ID
            nanobot_side_session_key: 业务侧会话键
            preferred_model: 首选模型（如果为 None 则不切换模型）
            preferred_agent: 首选代理（如果为 None 则不切换代理）

        处理流程（以模型为例）：
            1. 检查 ACP 连接是否可用
            2. 如果 preferred_model 存在且 SDK 支持 set_session_model：
               a. 调用 ACP API 设置模型
               b. 更新运行时能力缓存（remember_current_model）
               c. 更新持久化绑定（update_bound_model）
               d. 更新运行时选择（update_runtime_selection）

        使用场景：
            - ensure_ready_session 中应用用户偏好
            - 恢复会话时重新应用之前的模型/代理选择

        注意：
            此方法是异步的（需要 await）。
            此方法是私有的（_ 前缀），不直接对外暴露。
            ACP 连接不可用时静默返回（不抛出异常）。
        """
        conn = self._runtime._acp_client_conn
        if conn is None:
            return

        # 应用模型选择
        if preferred_model and hasattr(conn, "set_session_model"):
            await conn.set_session_model(model_id=preferred_model, session_id=acp_side_session_id)
            caps = self.get_session_capabilities(acp_side_session_id)
            if caps is not None:
                caps.remember_current_model(preferred_model)
            self._binding_manager.update_bound_model(nanobot_side_session_key, preferred_model)
            self.update_runtime_selection(
                nanobot_side_session_key=nanobot_side_session_key,
                bound_model=preferred_model,
            )

        # 应用代理选择
        if preferred_agent and hasattr(conn, "set_session_mode"):
            await conn.set_session_mode(mode_id=preferred_agent, session_id=acp_side_session_id)
            caps = self.get_session_capabilities(acp_side_session_id)
            if caps is not None:
                caps.remember_current_agent(preferred_agent)
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
        """存储运行时会话条目到双向索引。

        参数：
            nanobot_side_session_key: 业务侧会话键
            acp_side_session_id: ACP 侧会话 ID

        返回：
            str: acp_side_session_id（方便链式调用）

        处理流程：
            1. 从 binding_manager 获取持久化绑定中的 model/agent
            2. 创建 SessionRuntimeEntry（ready=True）
            3. 存入 _by_nanobot_side_session_key（正向索引）
            4. 存入 _by_acp_side_session_id（反向索引）

        双向索引：
            - 正向：nanobot_key -> SessionRuntimeEntry
            - 反向：acp_id -> SessionRuntimeEntry
            - 同一个 entry 对象被两个字典引用（内存共享）

        使用场景：
            - ensure_ready_session 中创建新运行时条目
            - activate_session 成功后存储条目

        注意：
            此方法是私有的（_ 前缀），不直接对外暴露。
            如果 nanobot_key 或 acp_id 已存在，旧条目会被覆盖。
        """
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
        # 双向索引：同一个 entry 对象被两个字典引用
        self._by_nanobot_side_session_key[nanobot_side_session_key] = entry
        self._by_acp_side_session_id[acp_side_session_id] = entry
        return acp_side_session_id

    def _drop_runtime_entry(self, *, nanobot_side_session_key: str) -> None:
        """从双向索引中删除运行时会话条目。

        参数：
            nanobot_side_session_key: 要删除的业务侧会话键

        处理流程：
            1. 从 _by_nanobot_side_session_key 中 pop 条目
            2. 如果条目存在：从 _by_acp_side_session_id 中也删除
            3. 如果条目不存在：静默跳过（幂等）

        使用场景：
            - drop_ready_session 中清理特定会话
            - ensure_ready_session 中激活失败时清理
            - close/rebuild 中批量清理

        注意：
            此方法是私有的（_ 前缀），不直接对外暴露。
            此方法不会删除持久化绑定（binding_manager.clear_binding 不在此调用）。
        """
        entry = self._by_nanobot_side_session_key.pop(nanobot_side_session_key, None)
        if entry is not None:
            self._by_acp_side_session_id.pop(entry.acp_side_session_id, None)
