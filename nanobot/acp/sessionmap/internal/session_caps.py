"""会话能力缓存：模型/代理目录与当前选择。

核心职责：
    提供单会话的模型与代理能力缓存，支持从 ACP payload 解析、本地记忆更新、
    导出多种视图（prompt metadata、/models 命令、/agents 命令）。

使用场景：
    - SessionRuntimeEntry 存储能力缓存，用于快速查询会话状态
    - ACPRuntime 在会话激活后从 ACP payload 解析能力信息
    - Command handler 响应 /models、/agents 命令时导出文本视图
    - Provider 注入 prompt metadata 时导出当前模型/代理选择

设计约束：
    - 本对象不直接访问 ACP，所有 ACP 数据由 SessionRuntimeManager 传入
    - 字段命名兼容不同 ACP SDK 版本（snake_case / camelCase）
    - 支持部分数据缺失（available_models 为空时仍能正常工作）
"""

from __future__ import annotations

from nanobot.acp.contracts import ACPSessionPayload, JSONMap


class _SessionCapabilities:
    """单会话模型与代理能力缓存（本地内存层）。

    核心职责：
        作为 SessionRuntimeEntry 的能力组件，存储会话的可用模型/代理列表与当前选择。
        支持从 ACP payload 解析更新、本地记忆更新、导出多种视图用于不同场景。

    使用场景：
        - 会话激活时：从 ACP session payload 解析模型/代理目录与当前选择
        - 模型切换时：本地缓存更新（不触发 ACP IO，避免频繁网络调用）
        - prompt 注入时：导出 metadata 视图（nanobot_session_model / nanobot_session_agent）
        - 命令响应时：导出文本视图（/models：显示所有可用模型与当前选择状态）
                        （/agents：显示所有可用代理与当前选择状态）

    属性说明：
        available_models: 当前会话可用的模型 ID 列表（如 ["gpt-4", "gpt-3.5-turbo"]）
                         来源：ACP session payload 的 models.availableModels
                         更新时机：apply_session_payload 解析 payload 时更新
        current_model: 当前激活的模型 ID（如 "gpt-4"），None 表示未选择
                      来源：ACP session payload 的 models.currentModelId
                      更新时机：apply_session_payload 或 remember_current_model
        available_agents: 当前会话可用的代理 ID 列表（如 ["code-assistant", "default"]）
                          来源：ACP session payload 的 modes.availableModes
                          更新时机：apply_session_payload 解析 payload 时更新
        current_agent: 当前激活的代理 ID（如 "code-assistant"），None 表示未选择
                      来源：ACP session payload 的 modes.currentModeId
                      更新时机：apply_session_payload 或 remember_current_agent

    生命周期：
        - 创建：SessionRuntimeEntry 创建时实例化（initial state: all empty/None）
        - 更新：apply_session_payload 解析 ACP payload、remember_current_model/agent 本地更新
        - 销毁：SessionRuntimeEntry 删除时一同销毁（无外部引用）

    注意事项：
        - 本对象不直接访问 ACP，所有 ACP 数据由 SessionRuntimeManager 传入
        - current_model/agent 可能不存在（available 列表为空或 payload 缺失）
        - render_* 方法用于命令响应，build_prompt_metadata 用于 prompt 注入
    """

    def __init__(self) -> None:
        """初始化空的能力缓存（无模型/代理信息）。"""
        self.available_models: list[str] = []
        self.current_model: str | None = None
        self.available_agents: list[str] = []
        self.current_agent: str | None = None

    def apply_session_payload(self, payload: ACPSessionPayload) -> None:
        """从 ACP session payload 解析并合并进当前能力缓存。

        处理流程：
            1. 解析 models 部分：
               - 提取 current_model_id / currentModelId（兼容不同 SDK 字段名）
               - 提取 available_models / availableModels 列表
               - 过滤无效值（空/非字符串），更新 self.current_model 与 self.available_models
            2. 解析 modes 部分：
               - 提取 current_mode_id / currentModeId（代理在 ACP 中称为 "mode"）
               - 提取 available_modes / availableModes 列表
               - 过滤无效值，更新 self.current_agent 与 self.available_agents
            3. 部分数据缺失时的处理策略：
               - payload 不含 models/modes 时跳过对应解析
               - available 列表为空时仍保留空列表（不触发异常）

        参数：
            payload: ACP session payload，可能包含 models/modes 子结构
                     来源：ACR runtime manager 调用 ACP API 后的响应
                     类型：dict 或 Pydantic 模型（通过 _pick 兼容）

        使用示例：
            caps = _SessionCapabilities()
            payload = await conn.list_sessions()  # or conn.get_session()
            caps.apply_session_payload(payload)
            print(f"当前模型: {caps.current_model}")

        注意事项：
            - 如果 payload 格式不匹配（字段名变化），解析静默跳过（保底逻辑）
            - 不覆盖已有的 current_model/agent（仅当 payload 中有值时才更新）
            - available 列表会完全替换（不是增量追加）
        """

        models = _pick(payload, "models")
        if models is not None:
            current = _pick(models, "current_model_id", "currentModelId")
            if isinstance(current, str) and current:
                self.current_model = current

            available = _pick(models, "available_models", "availableModels") or []
            parsed_models: list[str] = []
            for entry in available:
                model_id = _pick(entry, "model_id", "modelId")
                if isinstance(model_id, str) and model_id:
                    parsed_models.append(model_id)
            if parsed_models:
                self.available_models = parsed_models

        modes = _pick(payload, "modes")
        if modes is not None:
            current = _pick(modes, "current_mode_id", "currentModeId")
            if isinstance(current, str) and current:
                self.current_agent = current

            available = _pick(modes, "available_modes", "availableModes") or []
            parsed_agents: list[str] = []
            for entry in available:
                mode_id = _pick(entry, "id")
                if isinstance(mode_id, str) and mode_id:
                    parsed_agents.append(mode_id)
            if parsed_agents:
                self.available_agents = parsed_agents

    def remember_current_model(self, model_id: str) -> None:
        """本地缓存当前模型选择（不触发任何 ACP IO）。

        处理流程：
            1. 直接更新 self.current_model 为传入的 model_id
            2. 不调用 ACP API、不做任何网络请求

        参数：
            model_id: 要缓存的模型 ID（如 "gpt-4"）
                      来源：SessionRuntimeManager 调用 ACP API 成功后传入
                      约束：必须是字符串（非空），否则静默忽略

        使用场景：
            用户显式发起 /model gpt-4 命令，SessionRuntimeManager 先调用 ACP API
            成功后调用本方法更新本地缓存，下次查询时无需再访问 ACP

        注意事项：
            - 本方法只更新本地缓存，不验证 model_id 是否在 available_models 中
            - 如果 model_id 无效，后续 ACP 调用会失败（由调用方处理）
            - 与 update_caps_from_payload 配合使用：apply 更新 available 列表，
              remember 更新当前选择，两个方法互补
        """

        self.current_model = model_id

    def remember_current_agent(self, agent_id: str) -> None:
        """本地缓存当前代理选择（不触发任何 ACP IO）。

        处理流程：
            1. 直接更新 self.current_agent 为传入的 agent_id
            2. 不调用 ACP API、不做任何网络请求

        参数：
            agent_id: 要缓存的代理 ID（如 "code-assistant"）
                      来源：SessionRuntimeManager 调用 ACP API 成功后传入
                      约束：必须是字符串（非空），否则静默忽略

        使用场景：
            用户显式发起 /agent code-assistant 命令，SessionRuntimeManager 先调用
            ACP API 成功后调用本方法更新本地缓存

        注意事项：
            - 本方法只更新本地缓存，不验证 agent_id 是否在 available_agents 中
            - 与 remember_current_model 配对使用（模型切换与代理切换逻辑一致）
        """

        self.current_agent = agent_id

    def build_prompt_metadata(self) -> JSONMap:
        """导出 prompt metadata 视图（用于 provider 注入）。

        处理流程：
            1. 创建空字典，根据 current_model/current_agent 是否存在决定是否添加字段
            2. 存在时添加 nanobot_session_model / nanobot_session_agent 字段
            3. 返回包含非 None 字段的字典

        返回：
            JSONMap: prompt metadata 字典，格式：
                   - nanobot_session_model: 当前模型 ID（仅当 self.current_model 非空时包含）
                   - nanobot_session_agent: 当前代理 ID（仅当 self.current_agent 非空时包含）
                   不存在时返回空字典 {}

        使用示例：
            caps = _SessionCapabilities()
            caps.current_model = "gpt-4"
            caps.current_agent = "code-assistant"
            metadata = caps.build_prompt_metadata()
            # metadata = {"nanobot_session_model": "gpt-4", "nanobot_session_agent": "code-assistant"}

        注意事项：
            - 导出的是当前选择，不包含 available 列表（避免 prompt 过大）
            - 字段名使用 nanobot 前缀（避免与 provider 其他字段冲突）
            - 返回的字典可直接注入到 prompt 的 metadata 参数中
        """

        prompt_meta: JSONMap = {}
        if isinstance(self.current_model, str) and self.current_model:
            prompt_meta["nanobot_session_model"] = self.current_model
        if isinstance(self.current_agent, str) and self.current_agent:
            prompt_meta["nanobot_session_agent"] = self.current_agent
        return prompt_meta

    def render_models_command(self) -> str:
        """导出 `/models` 命令的文本响应。

        处理流程：
            1. 检查 available_models 是否为空，空时返回提示信息
            2. 遍历 available_models，为每个 model_id 生成一行文本
               - 当前激活的模型前缀 "* "，其他模型前缀 "  "
            3. 拼接 header（Current model）与列表（Available models）

        返回：
            str: 格式化文本，格式：
                ```
                Current model: gpt-4
                Available models:
                * gpt-4
                  gpt-3.5-turbo
                ```
                或（当 available_models 为空时）：
                ```
                No model catalog returned by current ACP backend for this session.
                ```

        使用示例：
            caps = _SessionCapabilities()
            caps.available_models = ["gpt-4", "gpt-3.5-turbo"]
            caps.current_model = "gpt-4"
            print(caps.render_models_command())

        注意事项：
            - available_models 为空时不报错，返回友好提示信息
            - current_model 为 None 时 header 显示 "unknown"
            - 每个模型一行，使用前缀区分当前选择（"* " vs "  "）
        """

        if not self.available_models:
            return "No model catalog returned by current ACP backend for this session."

        lines = []
        for model_id in self.available_models:
            prefix = "* " if self.current_model == model_id else "  "
            lines.append(f"{prefix}{model_id}")

        header = (
            f"Current model: {self.current_model}"
            if self.current_model
            else "Current model: unknown"
        )
        return "\n".join([header, "Available models:", *lines])

    def render_agents_command(self) -> str:
        """导出 `/agents` 命令的文本响应。

        处理流程：
            1. 检查 available_agents 是否为空，空时返回提示信息
            2. 遍历 available_agents，为每个 agent_id 生成一行文本
               - 当前激活的代理前缀 "* "，其他代理前缀 "  "
            3. 拼接 header（Current agent）与列表（Available agents）

        返回：
            str: 格式化文本，格式：
                ```
                Current agent: code-assistant
                Available agents:
                * code-assistant
                  default
                ```
                或（当 available_agents 为空时）：
                ```
                No agent/mode catalog returned by current ACP backend for this session.
                ```

        使用示例：
            caps = _SessionCapabilities()
            caps.available_agents = ["code-assistant", "default"]
            caps.current_agent = "code-assistant"
            print(caps.render_agents_command())

        注意事项：
            - available_agents 为空时不报错，返回友好提示信息
            - current_agent 为 None 时 header 显示 "unknown"
            - 本方法与 render_models_command 逻辑对称（仅在数据源不同）
        """

        if not self.available_agents:
            return "No agent/mode catalog returned by current ACP backend for this session."

        lines = []
        for agent_id in self.available_agents:
            prefix = "* " if self.current_agent == agent_id else "  "
            lines.append(f"{prefix}{agent_id}")

        header = (
            f"Current agent: {self.current_agent}"
            if self.current_agent
            else "Current agent: unknown"
        )
        return "\n".join([header, "Available agents:", *lines])


def build_session_capabilities_from_payload(payload: ACPSessionPayload) -> _SessionCapabilities:
    """从 ACP payload 创建新的能力缓存对象（工厂函数）。

    处理流程：
        1. 创建空的 _SessionCapabilities 实例
        2. 调用 apply_session_payload 解析 payload 填充数据
        3. 返回填充后的实例

    参数：
        payload: ACP session payload，包含 models/modes 子结构
                 来源：ACR runtime manager 调用 ACP API 后的响应
                 类型：dict 或 Pydantic 模型（通过 _pick 兼容）

    返回：
        _SessionCapabilities: 填充完毕的能力缓存对象
                            包含 available_models/current_model/available_agents/current_agent

    使用示例：
        payload = await conn.get_session("abc123")
        caps = build_session_capabilities_from_payload(payload)
        print(f"可用模型: {caps.available_models}")

    注意事项：
        - 本方法是工厂函数，用于一次性创建并填充能力缓存
        - 等价于手动创建实例 + 调用 apply_session_payload
        - 适用于从 ACP payload 直接构建能力缓存的场景（如会话激活时）
    """

    caps = _SessionCapabilities()
    caps.apply_session_payload(payload)
    return caps


def _pick(obj: ACPSessionPayload, *names: str) -> ACPSessionPayload:
    """按候选字段名顺序获取属性值（兼容不同 SDK 版本的字段命名）。

    处理流程：
        1. 遍历候选字段名列表（按优先级排序）
        2. 对象包含该字段时立即返回值
        3. 所有候选字段都不存在时返回 None

    参数：
        obj: 要查询的对象（dict 或 Pydantic 模型）
        *names: 候选字段名列表（按优先级排序）
                典型用法：_pick(models, "current_model_id", "currentModelId")
                         先尝试 snake_case，再尝试 camelCase（兼容不同 SDK 版本）

    返回：
        ACPSessionPayload: 找到的属性值，所有候选字段都不存在时返回 None

    使用示例：
        # 兼容不同 SDK 版本的字段命名
        current = _pick(models, "current_model_id", "currentModelId")
        # 如果 models.current_model_id 存在则返回该值
        # 如果不存在但 models.currentModelId 存在则返回该值
        # 如果都不存在则返回 None

        # 用于安全访问嵌套字段
        models = _pick(payload, "models")  # 避免 payload 不含 models 时抛出异常
        if models is not None:
            current = _pick(models, "currentModelId")

    注意事项：
        - 本方法是 ACP payload 解析的通用工具，支持字段名版本兼容
        - 返回值可能是任意类型（由字段本身的类型决定）
        - 不做类型转换（如 str -> int），调用方需要自行处理
    """

    # 先处理 dict：ACP payload 在不同链路下可能就是纯 JSON 字典。
    if isinstance(obj, dict):
        for name in names:
            if name in obj:
                return obj[name]
        return None

    # 再处理对象属性：兼容 Pydantic 模型或 SDK 自定义对象。
    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    return None
