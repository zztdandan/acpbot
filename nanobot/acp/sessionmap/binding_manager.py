"""会话绑定管理器：持久化真相与启动大对账。

本模块负责维护 nanobot 侧会话键与 ACP 侧会话 ID 的持久化映射关系。

核心职责：
    1. 持久化存储：将会话绑定保存到磁盘（JSON 格式）
    2. 启动对账：运行时启动时从 ACP 侧拉取真相并合并
    3. 绑定管理：CRUD 操作（创建/查询/更新/删除）
    4. 并发控制：通过 revision 字段保证并发安全
    5. 工作区隔离：不同 cwd 的绑定相互隔离

持久化格式（session_map.json）：
    {
      "version": 2,
      "mappings": [
        {
          "cwd": "/path/to/workspace",
          "nanobotSideSessionKey": "user123:chat456",
          "acpSideSessionId": "abc123",
          "boundModel": "gpt-4",
          "boundAgent": "assistant",
          "updatedAt": "2026-04-13T10:00:00+08:00",
          "revision": 1
        }
      ]
    }

启动对账流程（load_persistent_truth）：
    1. 从磁盘加载持久化绑定
    2. 从 ACP 侧拉取当前活跃会话 ID
    3. 合并两者：保留磁盘绑定，过滤掉 ACP 不存在的会话
    4. 持久化合并结果

使用示例：
    binding_manager = SessionMapBindingManager(runtime)
    await binding_manager.load_persistent_truth()  # 启动时调用
    acp_id = binding_manager.resolve_session_id("user123:chat456")
    binding_manager.bind_session("user123:chat456", "abc123")
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Awaitable, Callable, cast

from loguru import logger

from nanobot.acp.contracts import ACPSessionPayload
from nanobot.acp.sessionmap.internal.reconcile import fetch_acp_side_session_ids
from nanobot.acp.sessionmap.internal.storage import (
    read_sessionmap_payload,
    write_sessionmap_payload,
)
from nanobot.acp.sessionmap.models import SessionMapBindingEntry
from nanobot.config.paths import get_data_dir

if TYPE_CHECKING:
    from nanobot.acp.runtime import ACPRuntime


def _now_iso_with_tz() -> str:
    """获取当前时间的 ISO 8601 格式字符串（带时区）。

    返回：
        格式：YYYY-MM-DDTHH:MM:SS+TZ:00
        示例：2026-04-13T10:00:00+08:00

    用途：
        用于 SessionMapBindingEntry.updated_at 字段（持久化时间戳）。

    设计说明：
        - 使用 astimezone() 确保带时区信息（避免歧义）
        - timespec="seconds" 精确到秒（不需要毫秒）
        - ISO 8601 格式便于跨平台解析和排序
    """
    return datetime.now().astimezone().isoformat(timespec="seconds")


class SessionMapBindingManager:
    """会话绑定管理器：持久化真相与启动大对账。

    核心职责：
        1. 持久化存储：将会话绑定保存到磁盘（JSON 格式）
        2. 启动对账：运行时启动时从 ACP 侧拉取真相并合并
        3. 绑定管理：CRUD 操作（创建/查询/更新/删除）
        4. 并发控制：通过 revision 字段保证并发安全
        5. 工作区隔离：不同 cwd 的绑定相互隔离

    状态字段：
        - _owner: ACPRuntime 实例（用于获取配置和 workspace）
        - _session_map_file: 持久化文件路径（~/.local/share/nanobot/acp/session_map.json）
        - _entries: 内存中的绑定字典（nanobot_key -> entry）
        - _bootstrapped: 是否已完成启动对账（防止重复加载）

    线程安全：
        - 所有公开方法都是同步的（无 async）
        - persist 操作是原子的（写临时文件后 rename）
        - 调用方需要确保在 asyncio 事件循环中调用

    使用示例：
        binding_manager = SessionMapBindingManager(runtime)
        await binding_manager.load_persistent_truth()
        acp_id = binding_manager.resolve_session_id("user123:chat456")
    """

    def __init__(self, owner: ACPRuntime) -> None:
        """初始化 SessionMapBindingManager。

        参数：
            owner: ACPRuntime 实例（用于获取配置和 workspace）

        初始化状态：
            - _session_map_file: 持久化文件路径（get_data_dir()/acp/session_map.json）
            - _entries: 空字典（启动时通过 load_persistent_truth 加载）
            - _bootstrapped: False（尚未完成启动对账）

        注意：
            构造时不加载磁盘数据。
            调用方需要显式调用 load_persistent_truth 完成启动对账。
        """
        self._owner = owner
        self._session_map_file = get_data_dir() / "acp" / "session_map.json"
        # 内存中的绑定字典：nanobot_side_session_key -> SessionMapBindingEntry
        self._entries: dict[str, SessionMapBindingEntry] = {}
        # 启动对账标志：防止重复加载
        self._bootstrapped = False

    def mark_unbootstrapped(self) -> None:
        """标记为未完成启动对账状态。

        使用场景：
            - 运行时重置连接后（需要重新对账）
            - 测试代码中强制重新加载
            - 调试时手动触发重新对账

        注意：
            此方法不会清除 _entries 中的数据。
            调用方需要在 mark_unbootstrapped 后显式调用 load_persistent_truth。
        """
        self._bootstrapped = False

    def _resolved_acp_cwd(self) -> str:
        """解析 ACP 工作目录的绝对路径。

        解析优先级：
            1. acp_config.cwd: 如果配置中显式指定了 cwd，使用该值
            2. owner.workspace: 否则使用 ACPRuntime 的 workspace

        处理步骤：
            1. 展开用户路径（expanduser，支持 ~）
            2. 解析为绝对路径（resolve，消除符号链接）
            3. 转换为字符串

        返回：
            绝对路径字符串（用于绑定条目的 cwd 字段）

        用途：
            - 工作区隔离：不同 cwd 的绑定相互独立
            - 启动对账：只保留当前 cwd 的绑定
            - 持久化：写入 JSON 时标识绑定所属工作区

        示例：
            # 配置了 cwd
            acp_config.cwd = "~/projects/mybot"
            -> 返回："/home/user/projects/mybot"

            # 未配置 cwd
            acp_config.cwd = None
            owner.workspace = Path("/workspace")
            -> 返回："/workspace"
        """
        cwd = (
            Path(self._owner.acp_config.cwd).expanduser()
            if self._owner.acp_config.cwd
            else self._owner.workspace
        )
        return str(cwd.resolve())

    @staticmethod
    def _parse_entry(raw: object) -> SessionMapBindingEntry:
        """从原始字典解析 SessionMapBindingEntry 对象。

        参数：
            raw: 原始字典对象（从 JSON 加载）

        返回：
            SessionMapBindingEntry 对象（已验证和标准化）

        验证规则：
            1. cwd: 必须是非空字符串（绑定所属工作区）
            2. nanobotSideSessionKey: 必须是非空字符串（业务侧会话键）
            3. acpSideSessionId: 必须是非空字符串（ACP 侧会话 ID）
            4. updatedAt: 必须是非空字符串（ISO 8601 时间戳）
            5. revision: 必须是正整数（版本号 >= 1）
            6. boundModel/boundAgent: 可选，如果存在必须是非空字符串

        异常：
            ValueError: 当任何必填字段缺失或格式不正确时抛出

        设计说明：
            - 使用静态方法（不依赖实例状态）
            - 严格验证所有字段（避免脏数据进入内存）
            - 可选字段为 None 时不抛出异常（兼容旧数据）

        示例：
            raw = {
                "cwd": "/workspace",
                "nanobotSideSessionKey": "user123:chat456",
                "acpSideSessionId": "abc123",
                "boundModel": "gpt-4",
                "updatedAt": "2026-04-13T10:00:00+08:00",
                "revision": 1
            }
            entry = SessionMapBindingManager._parse_entry(raw)
        """
        if not isinstance(raw, dict):
            raise ValueError("session map entry is not object")

        # 提取所有字段（可选字段允许不存在）
        cwd = raw.get("cwd")
        nanobot_side_session_key = raw.get("nanobotSideSessionKey")
        acp_side_session_id = raw.get("acpSideSessionId")
        updated_at = raw.get("updatedAt")
        revision = raw.get("revision")
        bound_model = raw.get("boundModel")
        bound_agent = raw.get("boundAgent")

        # 验证必填字段
        if not isinstance(cwd, str) or not cwd:
            raise ValueError("session map entry cwd is invalid")
        if not isinstance(nanobot_side_session_key, str) or not nanobot_side_session_key:
            raise ValueError("session map entry nanobotSideSessionKey is invalid")
        if not isinstance(acp_side_session_id, str) or not acp_side_session_id:
            raise ValueError("session map entry acpSideSessionId is invalid")
        if not isinstance(updated_at, str) or not updated_at:
            raise ValueError("session map entry updatedAt is invalid")
        if not isinstance(revision, int) or revision < 1:
            raise ValueError("session map entry revision is invalid")

        # 构造对象（可选字段标准化为 None）
        return SessionMapBindingEntry(
            cwd=cwd,
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
            bound_model=bound_model if isinstance(bound_model, str) and bound_model else None,
            bound_agent=bound_agent if isinstance(bound_agent, str) and bound_agent else None,
            updated_at=updated_at,
            revision=revision,
        )

    def _load_entries_from_disk(self) -> dict[str, SessionMapBindingEntry]:
        """从磁盘加载持久化绑定条目。

        返回：
            dict[str, SessionMapBindingEntry]: 当前 cwd 的所有绑定条目
                key: nanobot_side_session_key
                value: SessionMapBindingEntry 对象

        处理流程：
            1. 读取 JSON 文件（read_sessionmap_payload）
            2. 解析当前 cwd（_resolved_acp_cwd）
            3. 遍历 mappings 数组，解析每个条目
            4. 过滤：只保留 cwd 匹配的条目（工作区隔离）
            5. 返回字典（按 nanobot_side_session_key 索引）

        工作区隔离：
            不同 cwd 的绑定相互独立。
            例如：/workspace1 的绑定不会出现在 /workspace2 中。

        异常处理：
            - 如果文件不存在：read_sessionmap_payload 返回空结构
            - 如果格式错误：抛出 ValueError（调用方需要捕获）

        注意：
            此方法是私有的（_ 前缀），不直接对外暴露。
            调用方应该使用 load_persistent_truth（包含对账逻辑）。
        """
        payload = read_sessionmap_payload(self._session_map_file)
        current_cwd = self._resolved_acp_cwd()
        loaded: dict[str, SessionMapBindingEntry] = {}
        raw_mappings = payload.get("mappings")
        mappings = raw_mappings if isinstance(raw_mappings, list) else []

        for raw in mappings:
            entry = self._parse_entry(raw)
            # 工作区隔离：只保留当前 cwd 的绑定
            if entry.cwd == current_cwd:
                loaded[entry.nanobot_side_session_key] = entry

        return loaded

    def persist(self) -> None:
        """将当前内存中的绑定持久化到磁盘。

        调用时机：
            - bind_session 后（新增绑定）
            - clear_binding 后（删除绑定）
            - 手动触发持久化（调试/测试）

        持久化策略：
            1. 读取现有 JSON（保留其他 cwd 的绑定）
            2. 过滤：删除当前 cwd 的旧绑定
            3. 合并：追加当前 cwd 的新绑定
            4. 原子写入：先写临时文件，再 rename（避免损坏）

        工作区隔离：
            persist 只影响当前 cwd 的绑定。
            其他 cwd 的绑定会被保留（不会覆盖）。

        注意：
            此方法是同步的（阻塞 IO）。
            调用方需要确保在合适的时机调用（避免频繁 IO）。
        """
        write_sessionmap_payload(
            self._session_map_file,
            current_cwd=self._resolved_acp_cwd(),
            entries=self._entries,
        )

    def resolve_session_id(self, nanobot_side_session_key: str) -> str | None:
        """根据 nanobot 侧会话键解析 ACP 侧会话 ID。

        参数：
            nanobot_side_session_key: 业务侧会话键（如 "user123:chat456"）

        返回：
            str | None: ACP 侧会话 ID（如果存在），否则 None

        使用场景：
            - stop_session 中查找目标会话
            - dispatch_inbound 中上报观测事件
            - 日志记录（追踪会话来源）

        注意：
            此方法只查询内存中的 _entries。
            调用方需要确保已调用 load_persistent_truth 完成加载。

        示例：
            acp_id = binding_manager.resolve_session_id("user123:chat456")
            if acp_id:
                await runtime.stop_session(acp_side_session_id=acp_id)
        """
        entry = self._entries.get(nanobot_side_session_key)
        return entry.acp_side_session_id if entry is not None else None

    def get_bound_selection(self, nanobot_side_session_key: str) -> tuple[str | None, str | None]:
        """获取会话绑定的模型和代理偏好。

        参数：
            nanobot_side_session_key: 业务侧会话键

        返回：
            tuple[str | None, str | None]: (bound_model, bound_agent)
                - bound_model: 绑定的模型名称（如 "gpt-4"）
                - bound_agent: 绑定的代理名称（如 "assistant"）

        使用场景：
            - process_direct 中恢复会话偏好
            - 会话迁移时保留用户选择

        示例：
            model, agent = binding_manager.get_bound_selection("user123:chat456")
            if model:
                print(f"会话绑定模型：{model}")
        """
        entry = self._entries.get(nanobot_side_session_key)
        if entry is None:
            return None, None
        return entry.bound_model, entry.bound_agent

    def iter_entries(self) -> list[SessionMapBindingEntry]:
        """按序遍历所有绑定条目。

        返回：
            list[SessionMapBindingEntry]: 按 nanobot_side_session_key 排序的条目列表

        使用场景：
            - 调试时打印所有绑定
            - 批量操作（如批量删除）
            - 测试代码中断言绑定状态

        注意：
            返回的是列表（快照），不是迭代器。
            调用方可以安全修改返回列表（不影响内部状态）。
        """
        return [entry for _, entry in sorted(self._entries.items())]

    def clear_binding(self, nanobot_side_session_key: str) -> str | None:
        """清除指定会话的绑定并持久化。

        参数：
            nanobot_side_session_key: 要删除的业务侧会话键

        返回：
            str | None: 被删除的 ACP 侧会话 ID（如果存在），否则 None

        处理流程：
            1. 从 _entries 中删除条目
            2. 调用 persist 持久化到磁盘
            3. 返回被删除的 acp_side_session_id

        使用场景：
            - drop_session_binding_and_runtime_entry 中清理绑定
            - 用户主动解绑会话
            - 会话过期自动清理

        注意：
            此方法会立即持久化（每次调用都会写磁盘）。
            批量删除时建议先 pop 再统一 persist。

        示例：
            acp_id = binding_manager.clear_binding("user123:chat456")
            if acp_id:
                print(f"已删除绑定：{acp_id}")
        """
        old = self._entries.pop(nanobot_side_session_key, None)
        self.persist()
        return old.acp_side_session_id if old is not None else None

    def bind_session(self, nanobot_side_session_key: str, acp_side_session_id: str) -> None:
        """建立或更新业务会话与 ACP 会话的绑定。

        参数：
            nanobot_side_session_key: 业务侧会话键（如 "user123:chat456"）
            acp_side_session_id: ACP 侧会话 ID（如 "abc123"）

        处理流程：
            1. 解析当前 cwd（工作区标识）
            2. 获取当前时间戳（ISO 8601 格式）
            3. 清理冲突绑定：如果 acp_side_session_id 已绑定到其他 nanobot_key，删除旧绑定
               （保证 acp_side_session_id 唯一性）
            4. 如果已存在绑定：更新 revision 和 updated_at
            5. 如果不存在：创建新绑定（revision=1）
            6. 持久化到磁盘

        绑定更新策略：
            - 如果 nanobot_key 已存在：递增 revision，更新 updated_at
            - 如果 acp_id 被其他 nanobot_key 占用：删除旧绑定（一对一映射）
            - bound_model/bound_agent: 保留已有值（不覆盖）

        使用场景：
            - ensure_ready_session 中建立新会话绑定
            - 会话迁移时更新绑定
            - 用户手动绑定会话

        示例：
            binding_manager.bind_session("user123:chat456", "abc123")
            # 磁盘 session_map.json 会新增一条记录

        注意：
            此方法会立即持久化（每次调用都会写磁盘）。
            批量绑定时建议先修改 _entries 再统一 persist。
        """
        current_cwd = self._resolved_acp_cwd()
        now = _now_iso_with_tz()

        # 步骤 1: 清理冲突绑定（保证 acp_side_session_id 唯一性）
        for existing_key, entry in list(self._entries.items()):
            if (
                existing_key != nanobot_side_session_key
                and entry.acp_side_session_id == acp_side_session_id
            ):
                self._entries.pop(existing_key, None)
        # 步骤 2: 检查是否已存在绑定
        old = self._entries.get(nanobot_side_session_key)
        if old is None:
            # 步骤 3a: 创建新绑定（revision=1）
            self._entries[nanobot_side_session_key] = SessionMapBindingEntry(
                cwd=current_cwd,
                nanobot_side_session_key=nanobot_side_session_key,
                acp_side_session_id=acp_side_session_id,
                bound_model=None,
                bound_agent=None,
                updated_at=now,
                revision=1,
            )
            self.persist()
            return

        # 步骤 4: 如果 acp_id 未变化，直接返回（幂等）
        if old.acp_side_session_id == acp_side_session_id:
            return

        # 步骤 5: 更新现有绑定（递增 revision）
        old.acp_side_session_id = acp_side_session_id
        old.updated_at = now
        old.revision += 1
        self.persist()

    def update_bound_model(self, nanobot_side_session_key: str, model: str) -> None:
        """更新会话绑定的模型偏好。

        参数：
            nanobot_side_session_key: 业务侧会话键
            model: 模型名称（如 "gpt-4"）

        处理流程：
            1. 查找绑定条目
            2. 如果条目不存在或 model 未变化：直接返回（幂等）
            3. 更新 bound_model 字段
            4. 递增 revision（版本号 +1）
            5. 更新 updated_at（当前时间戳）
            6. 持久化到磁盘

        使用场景：
            - 用户通过 /model 命令切换模型
            - process_direct 中 preferred_model 变化
            - 会话迁移时恢复模型偏好

        注意：
            此方法会立即持久化（每次调用都会写磁盘）。
        """
        entry = self._entries.get(nanobot_side_session_key)
        if entry is None or entry.bound_model == model:
            return
        entry.bound_model = model
        entry.revision += 1
        entry.updated_at = _now_iso_with_tz()
        self.persist()

    def update_bound_agent(self, nanobot_side_session_key: str, agent: str) -> None:
        """更新会话绑定的代理偏好。

        参数：
            nanobot_side_session_key: 业务侧会话键
            agent: 代理名称（如 "assistant"）

        处理流程：
            1. 查找绑定条目
            2. 如果条目不存在或 agent 未变化：直接返回（幂等）
            3. 更新 bound_agent 字段
            4. 递增 revision（版本号 +1）
            5. 更新 updated_at（当前时间戳）
            6. 持久化到磁盘

        使用场景：
            - 用户通过 /agent 命令切换代理
            - process_direct 中 preferred_agent 变化
            - 会话迁移时恢复代理偏好

        注意：
            此方法会立即持久化（每次调用都会写磁盘）。
        """
        entry = self._entries.get(nanobot_side_session_key)
        if entry is None or entry.bound_agent == agent:
            return
        entry.bound_agent = agent
        entry.revision += 1
        entry.updated_at = _now_iso_with_tz()
        self.persist()

    async def activate_session(
        self, nanobot_side_session_key: str, acp_side_session_id: str
    ) -> tuple[bool, ACPSessionPayload | None]:
        """激活已存在的 ACP 会话并回放历史状态。

        参数：
            nanobot_side_session_key: 业务侧会话键（用于日志记录）
            acp_side_session_id: ACP 侧会话 ID（要激活的会话）

        返回：
            tuple[bool, ACPSessionPayload | None]:
                - bool: 激活是否成功（True=成功，False=失败）
                - ACPSessionPayload | None: 会话负载（如果成功激活）

        处理流程：
            1. 检查 ACP 连接是否可用
            2. 解析当前 cwd
            3. 尝试 resume_session（优先）：恢复活跃会话
            4. 如果 resume_session 不可用或失败，尝试 load_session：加载历史会话
            5. 如果都不可用：返回 (True, None)（表示无需激活）
            6. 如果成功获取会话：调用 _replay_after_activation 回放历史
            7. 返回回放结果

        激活策略：
            - resume_session: 优先尝试（适用于活跃会话）
            - load_session: 降级方案（适用于历史会话）
            - 两者都失败：返回 (False, None)（激活失败）

        异常处理：
            - resume_session 失败：记录 debug 日志，降级到 load_session
            - load_session 失败：记录 warning 日志，返回 (False, None)

        使用场景：
            - ensure_ready_session 中恢复历史会话
            - 重启后重新激活之前的会话

        注意：
            此方法是异步的（需要 await）。
            调用方需要处理返回的 bool 判断激活是否成功。
        """
        conn = self._owner._acp_client_conn
        if conn is None:
            raise RuntimeError("ACP connection is not available")

        cwd = self._resolved_acp_cwd()

        # 动态获取方法（兼容不同 SDK 版本）
        resume_session = cast(
            Callable[..., Awaitable[ACPSessionPayload]] | None,
            getattr(conn, "resume_session", None),
        )
        load_session = cast(
            Callable[..., Awaitable[ACPSessionPayload]] | None,
            getattr(conn, "load_session", None),
        )

        # 如果两个方法都不存在，表示无需激活
        if resume_session is None and load_session is None:
            return True, None

        response: ACPSessionPayload | None = None

        # 步骤 1: 尝试 resume_session（优先）
        if resume_session is not None:
            try:
                response = await resume_session(cwd=cwd, session_id=acp_side_session_id)
            except Exception as exc:
                logger.debug(
                    "ACP resume_session failed nanobot_side_session_key={} acp_side_session_id={} error_type={} error={}",
                    nanobot_side_session_key,
                    acp_side_session_id,
                    type(exc).__name__,
                    exc,
                )

        # 步骤 2: 如果 resume_session 失败或不存在，尝试 load_session（降级）
        if response is None and load_session is not None:
            try:
                response = await load_session(cwd=cwd, session_id=acp_side_session_id)
            except Exception as exc:
                logger.warning(
                    "ACP existing session activation failed nanobot_side_session_key={} acp_side_session_id={} error_type={} error={}",
                    nanobot_side_session_key,
                    acp_side_session_id,
                    type(exc).__name__,
                    exc,
                )
                return False, None

        # 步骤 3: 回放历史状态
        replay_ok = await self._replay_after_activation(
            nanobot_side_session_key=nanobot_side_session_key,
            acp_side_session_id=acp_side_session_id,
        )
        return replay_ok, response

    async def _replay_after_activation(
        self,
        *,
        nanobot_side_session_key: str,
        acp_side_session_id: str,
    ) -> bool:
        """在会话激活后回放绑定的模型和代理偏好。

        参数：
            nanobot_side_session_key: 业务侧会话键（用于查找绑定）
            acp_side_session_id: ACP 侧会话 ID（要回放的会话）

        返回：
            bool: 回放是否成功（True=成功，False=失败）

        处理流程：
            1. 检查 ACP 连接是否可用
            2. 查找绑定条目，获取 bound_model/bound_agent
            3. 如果绑定不存在，使用 acp_config 的默认值
            4. 动态获取 set_session_model/set_session_mode 方法
            5. 调用 set_session_model（如果 model 存在且方法可用）
            6. 调用 set_session_mode（如果 agent 存在且方法可用）
            7. 返回 True（成功）或 False（异常）

        回放策略：
            - 优先使用绑定值（bound_model/bound_agent）
            - 降级到默认值（acp_config.default_model/default_mode）
            - 如果 SDK 不支持对应方法，静默跳过

        异常处理：
            任何异常都会被捕获并记录 warning 日志，返回 False。

        使用场景：
            - activate_session 成功后调用
            - 恢复用户之前的模型/代理选择

        注意：
            此方法是私有的（_ 前缀），不直接对外暴露。
            调用方应该使用 activate_session。
        """
        conn = self._owner._acp_client_conn
        if conn is None:
            raise RuntimeError("ACP connection is not available")

        # 获取绑定偏好
        entry = self._entries.get(nanobot_side_session_key)
        bound_model = entry.bound_model if entry is not None else None
        bound_agent = entry.bound_agent if entry is not None else None

        # 降级到默认配置
        model = bound_model or self._owner.acp_config.default_model
        agent = bound_agent or self._owner.acp_config.default_mode

        # 动态获取方法（兼容不同 SDK 版本）
        set_session_model = cast(
            Callable[..., Awaitable[ACPSessionPayload]] | None,
            getattr(conn, "set_session_model", None),
        )
        set_session_mode = cast(
            Callable[..., Awaitable[ACPSessionPayload]] | None,
            getattr(conn, "set_session_mode", None),
        )

        try:
            # 回放模型偏好
            if model and set_session_model is not None:
                await set_session_model(model_id=model, session_id=acp_side_session_id)

            # 回放代理偏好
            if agent and set_session_mode is not None:
                await set_session_mode(mode_id=agent, session_id=acp_side_session_id)

            return True
        except Exception as exc:
            logger.warning(
                "ACP activation replay failed nanobot_side_session_key={} acp_side_session_id={} error_type={} error={}",
                nanobot_side_session_key,
                acp_side_session_id,
                type(exc).__name__,
                exc,
            )
            return False

    async def bootstrap(self) -> list[str]:
        """启动时激活所有已绑定的会话。

        返回：
            list[str]: 成功激活的 nanobot_side_session_key 列表

        处理流程：
            1. 检查是否已启动（_bootstrapped 标志）
            2. 如果已启动：直接返回空列表（幂等）
            3. 从磁盘加载绑定条目
            4. 执行对账（_reconcile_entries，清理无效绑定）
            5. 遍历所有条目，调用 activate_session 激活
            6. 收集成功激活的 key
            7. 标记为已启动
            8. 返回成功列表

        使用场景：
            - ACPRuntime 启动时自动调用
            - 测试代码中手动触发

        注意：
            此方法是异步的（需要 await）。
            只应在启动时调用一次（后续调用直接返回空列表）。
        """
        if self._bootstrapped:
            return []

        # 步骤 1: 从磁盘加载
        self._entries = self._load_entries_from_disk()

        # 步骤 2: 执行对账（清理无效绑定）
        await self._reconcile_entries()

        # 步骤 3: 激活所有会话
        ok_keys: list[str] = []
        for nanobot_side_session_key, entry in sorted(self._entries.items()):
            activated, _ = await self.activate_session(
                nanobot_side_session_key,
                entry.acp_side_session_id,
            )
            if activated:
                ok_keys.append(nanobot_side_session_key)

        # 步骤 4: 标记为已启动
        self._bootstrapped = True
        return ok_keys

    async def load_persistent_truth(self) -> None:
        """加载持久化真相并执行启动对账。

        处理流程：
            1. 检查是否已启动（_bootstrapped 标志）
            2. 如果已启动：直接返回（幂等）
            3. 从磁盘加载绑定条目
            4. 去重：如果同一 acp_side_session_id 有多个条目，保留 revision 最高的
            5. 更新 _entries 为去重后的结果
            6. 标记为已启动

        对账策略：
            - 按 revision 排序：revision 高的覆盖 revision 低的
            - 保证一对一映射：一个 acp_id 只对应一个 nanobot_key

        使用场景：
            - ACPRuntime 启动时调用
            - 重置连接后重新加载

        注意：
            此方法是异步的（需要 await）。
            只应在启动时调用一次（后续调用直接返回）。
        """
        if self._bootstrapped:
            return

        # 步骤 1: 从磁盘加载
        self._entries = self._load_entries_from_disk()

        # 步骤 2: 去重（按 acp_side_session_id 分组，保留 revision 最高的）
        deduped: dict[str, SessionMapBindingEntry] = {}
        by_acp_side_session_id: dict[str, SessionMapBindingEntry] = {}
        for entry in self.iter_entries():
            winner = by_acp_side_session_id.get(entry.acp_side_session_id)
            if winner is None or entry.revision >= winner.revision:
                by_acp_side_session_id[entry.acp_side_session_id] = entry

        # 步骤 3: 重建索引（按 nanobot_side_session_key）
        for entry in by_acp_side_session_id.values():
            deduped[entry.nanobot_side_session_key] = entry

        # 步骤 4: 更新内存状态
        self._entries = deduped

        # 步骤 5: 执行对账（清理 ACP 侧不存在的绑定）
        await self._reconcile_entries()

        # 步骤 6: 标记为已启动
        self._bootstrapped = True

    async def _reconcile_entries(self) -> None:
        """对账：清理 ACP 侧不存在的绑定条目。

        处理流程：
            1. 检查 ACP 连接是否可用
            2. 如果无连接或无条目：直接返回
            3. 调用 fetch_acp_side_session_ids 获取 ACP 侧真实会话列表
            4. 如果获取失败或无数据：直接返回（保守策略）
            5. 找出 stale_keys：绑定中的 acp_id 不在 ACP 侧列表中
            6. 删除 stale 条目
            7. 清理 session_runtime_manager 中的能力缓存

        对账策略：
            - 以 ACP 侧为准：如果 acp_id 在 ACP 侧不存在，删除绑定
            - 保守清理：只对账当前 cwd 的绑定
            - 能力缓存同步：删除绑定时同步清理能力缓存

        使用场景：
            - bootstrap 中调用（启动时对账）
            - load_persistent_truth 中调用（重新加载时对账）

        注意：
            此方法是私有的（_ 前缀），不直接对外暴露。
            对账失败时不会抛出异常（静默跳过）。
        """
        conn = self._owner._acp_client_conn
        if conn is None or not self._entries:
            return

        # 步骤 1: 获取 ACP 侧真实会话列表
        listed_ids, authoritative = await fetch_acp_side_session_ids(
            conn, cwd=self._resolved_acp_cwd()
        )

        # 步骤 2: 如果获取失败或无数据，保守跳过
        if not authoritative or not listed_ids:
            return

        # 步骤 3: 找出 stale_keys（ACP 侧不存在的绑定）
        stale_keys = [
            key
            for key, entry in self._entries.items()
            if entry.acp_side_session_id not in listed_ids
        ]

        # 步骤 4: 删除 stale 条目并清理能力缓存
        for key in stale_keys:
            stale = self._entries.pop(key, None)
            if stale is not None:
                # 同步清理 session_runtime_manager 中的能力缓存
                self._owner.session_runtime_manager.drop_session_capabilities(
                    acp_side_session_id=stale.acp_side_session_id
                )

        # 步骤 5: 如果有清理，持久化到磁盘
        if stale_keys:
            self.persist()
