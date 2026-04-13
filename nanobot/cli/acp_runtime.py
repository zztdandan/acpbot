"""ACP 感知 CLI 运行时辅助模块。

本模块封装了与上游 nanobot 不同的 CLI 核心逻辑，主要职责包括：
1. 调度后端覆盖解析：支持通过命令行强制指定 dispatch 后端（native/acp）
2. ACP 配置覆盖解析：支持内联 JSON 字符串或 JSON 文件路径两种形式
3. 运行时调度器构造：为 gateway/agent 命令构建对应的 dispatcher 实例

使用场景：
- CLI 启动时通过 --dispatcher 标志选择后端实现
- 通过 --acp-config 动态覆盖 ACP 后端配置（调试/热更新）
- 在创建运行时前统一应用配置覆盖
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from nanobot.bus.queue import MessageBus
from nanobot.config.schema import Config
from nanobot.cron.service import CronService
from nanobot.providers.base import LLMProvider
from nanobot.session.manager import SessionManager

# =============================================================================
# 进程级 CLI 覆盖状态（全局变量）
# =============================================================================
# 设计说明：
#   这两个变量存储在模块级别，用于在单次进程生命周期内保持 CLI 参数覆盖状态。
#   每次 CLI 命令执行前应调用 reset_dispatch_overrides() 确保状态干净。
#   避免多次 CLI 调用之间的状态污染。
_CLI_DISPATCHER_OVERRIDE: str | None = None
_CLI_ACP_CONFIG_OVERRIDE: str | None = None


def reset_dispatch_overrides() -> None:
    """重置进程级 CLI 调度覆盖状态。

    核心职责：
        清除全局变量中存储的 dispatcher 和 acp_config 覆盖值。

    调用时机：
        - 每次 CLI 命令执行前（确保每次调用从干净状态开始）
        - 单元测试 setUp 阶段（避免测试间状态泄漏）

    设计原因：
        显式提供此函数而非依赖模块重新导入，是因为 Python 模块在单次进程中只加载一次。
        如果不显式清除，前一次 CLI 调用的覆盖值会污染后续调用。
    """
    global _CLI_DISPATCHER_OVERRIDE, _CLI_ACP_CONFIG_OVERRIDE
    _CLI_DISPATCHER_OVERRIDE = None
    _CLI_ACP_CONFIG_OVERRIDE = None


def set_dispatch_overrides(*, dispatcher: str | None, acp_config: str | None) -> None:
    """设置 CLI 调度覆盖参数（从全局 CLI 标志读取）。

    参数：
        dispatcher: 调度后端选择，可选值："native" | "acp"，不区分大小写
        acp_config: ACP 配置覆盖，可以是：
            - 内联 JSON 字符串：'{"heartbeat_interval": 30}'
            - JSON 文件路径：'/path/to/acp_config.json'

    处理逻辑：
        1. dispatcher 参数会被标准化为小写并去除首尾空格
        2. acp_config 参数原样存储，延迟到 apply_dispatch_overrides 时解析

    注意：
        此函数只存储原始值，不做验证或解析。验证在 apply_dispatch_overrides 中进行。
    """
    global _CLI_DISPATCHER_OVERRIDE, _CLI_ACP_CONFIG_OVERRIDE
    if dispatcher:
        _CLI_DISPATCHER_OVERRIDE = dispatcher.strip().lower()
    if acp_config:
        _CLI_ACP_CONFIG_OVERRIDE = acp_config


def _parse_acp_override(raw: str) -> dict[str, object]:
    """解析 ACP 配置覆盖字符串。

    支持两种输入格式（按优先级）：
        1. JSON 文件路径：如果 raw 是有效文件路径，读取并解析文件内容
        2. 内联 JSON：直接将 raw 作为 JSON 字符串解析

    参数：
        raw: 配置覆盖字符串（文件路径或内联 JSON）

    返回：
        解析后的字典对象，可直接用于 config.dispatch.acp.model_copy(update=...)

    异常：
        json.JSONDecodeError: 当 raw 既不是有效文件路径也不是有效 JSON 时抛出
        FileNotFoundError: 理论上不会发生（已用 exists() 检查），但 Path 操作失败时可能抛出

    示例：
        >>> _parse_acp_override('{"heartbeat_interval": 30}')
        {'heartbeat_interval': 30}
        >>> _parse_acp_override('/tmp/acp.json')  # 假设文件内容为 {"timeout": 60}
        {'timeout': 60}
    """
    candidate = Path(raw).expanduser()
    if candidate.exists() and candidate.is_file():
        # 优先尝试作为文件路径处理（支持 ~ 展开）
        return json.loads(candidate.read_text(encoding="utf-8"))
    # 回退为内联 JSON 解析
    return json.loads(raw)


def apply_dispatch_overrides(
    config: Config,
    *,
    dispatcher: str | None = None,
    acp_config: str | None = None,
) -> Config:
    """将 CLI 调度覆盖参数应用到已加载的配置对象。

    参数优先级（从高到低）：
        1. 函数直接参数（dispatcher/acp_config）
        2. 全局 CLI 覆盖状态（_CLI_DISPATCHER_OVERRIDE/_CLI_ACP_CONFIG_OVERRIDE）
        3. Config 对象原始值（无覆盖时保持原样）

    参数：
        config: 已加载的 nanobot 配置对象（包含 dispatch.backend 和 dispatch.acp）
        dispatcher: 可选，强制指定调度后端（"native" | "acp"）
        acp_config: 可选，ACP 配置覆盖（内联 JSON 或文件路径）

    返回：
        应用覆盖后的 Config 对象（注意：会原地修改传入的 config 对象）

    异常：
        ValueError: 当 dispatcher 参数不是 "native" 或 "acp" 时抛出
        json.JSONDecodeError: 当 acp_config 解析失败时抛出（由 _parse_acp_override 传播）

    关键流程：
        1. 解析 dispatcher 选择（参数 > 全局 > 默认）
        2. 验证 dispatcher 合法性（必须是 native/acp）
        3. 更新 config.dispatch.backend
        4. 解析 acp_config 覆盖（如果提供）
        5. 使用 model_copy(update=...) 原地更新 config.dispatch.acp

    设计说明：
        - 使用 model_copy(update=...) 而非直接赋值，是为了保持 Pydantic 模型的验证和不变量
        - 原地修改 config 对象是为了避免在 CLI 启动流程中传递多个配置版本
    """
    # 步骤 1: 解析 dispatcher 选择（支持三层优先级）
    dispatch_choice = (dispatcher or _CLI_DISPATCHER_OVERRIDE or "").strip().lower()
    if dispatch_choice:
        # 步骤 2: 验证 dispatcher 合法性
        if dispatch_choice not in {"native", "acp"}:
            raise ValueError("--dispatcher must be 'native' or 'acp'")
        # 步骤 3: 更新配置中的后端选择
        config.dispatch.backend = dispatch_choice  # type: ignore[assignment]

    # 步骤 4: 解析 acp_config 覆盖（如果提供）
    acp_raw = acp_config or _CLI_ACP_CONFIG_OVERRIDE
    if acp_raw:
        # 步骤 5: 解析 JSON 并原地更新 config.dispatch.acp
        overrides = _parse_acp_override(acp_raw)
        config.dispatch.acp = config.dispatch.acp.model_copy(update=overrides)

    return config


def dispatch_requires_provider(config: Config) -> bool:
    """判断当前选择的调度后端是否需要 LLM Provider 实例。

    后端类型与 Provider 需求：
        - native: 需要 Provider（NativeDispatcher 依赖 AgentLoop，AgentLoop 需要 LLM）
        - acp: 不需要 Provider（ACPDispatcher 直接委托给 ACP 协议层）

    参数：
        config: 已应用覆盖后的 Config 对象

    返回：
        True: 如果后端是 "native"（需要 Provider）
        False: 如果后端是 "acp"（不需要 Provider）

    使用场景：
        CLI 启动时根据此函数决定是否初始化 Provider，避免不必要的资源开销。
        例如：ACP 模式下可以跳过 OpenAI/Anthropic 等 Provider 的初始化。
    """
    return config.dispatch.backend == "native"


def create_dispatch_runtime(
    *,
    bus: MessageBus,
    config: Config,
    provider: LLMProvider | None,
    cron_service: CronService | None,
    session_manager: SessionManager | None,
) -> Any:
    """根据调度后端设置创建运行时调度器实例。

    核心职责：
        根据 config.dispatch.backend 的选择，构造对应的 Dispatcher 实例：
        - "acp": 创建 ACPDispatcher（ACP 协议层调度器）
        - "native": 创建 NativeDispatcher（原生 AgentLoop 调度器）

    参数：
        bus: 消息总线实例（用于 inbound/outbound 消息路由）
        config: 已应用覆盖后的 Config 对象（决定 backend 选择）
        provider: LLM Provider 实例（仅 native 后端需要）
        cron_service: 定时任务服务（仅 native 后端需要）
        session_manager: 会话管理器（仅 native 后端需要）

    返回：
        ACPDispatcher | NativeDispatcher 实例（类型标注为 Any 以避免循环导入）

    异常：
        RuntimeError: 当选择 native 后端但未提供 provider 时抛出

    关键设计决策：
        1. 延迟导入：在函数内部 import ACPDispatcher/NativeDispatcher，避免循环导入
           （dispatch 模块可能依赖 cli 模块，形成循环）
        2. 参数条件性：ACP 后端不需要 provider/cron_service/session_manager，
           这些参数仅在 native 模式下使用
        3. 类型擦除：返回类型标注为 Any，避免在类型检查阶段强制解析循环依赖

    ACPDispatcher 构造参数说明：
        - bus: 消息总线（ACP inbound/outbound 路由）
        - workspace: 工作区路径（ACP agent 进程 cwd）
        - acp_config: ACP 后端配置（心跳、权限策略等）
        - channels_config: 频道配置（用于 channel 路由）

    NativeDispatcher 构造参数说明：
        - bus: 消息总线
        - provider: LLM Provider（OpenAI/Anthropic 等）
        - config: 完整配置（包含 agent/skills/providers 等）
        - cron_service: 定时任务服务（用于 cron 触发）
        - session_manager: 会话状态管理（用于多会话隔离）

    使用示例：
        # ACP 模式
        dispatcher = create_dispatch_runtime(
            bus=bus, config=config, provider=None, cron_service=None, session_manager=None
        )

        # Native 模式
        dispatcher = create_dispatch_runtime(
            bus=bus, config=config, provider=provider,
            cron_service=cron_service, session_manager=session_manager
        )
    """
    # 延迟导入：避免循环依赖（dispatch 模块可能引用 cli 模块）
    from nanobot.dispatch import ACPDispatcher, NativeDispatcher

    if config.dispatch.backend == "acp":
        # ACP 模式：创建 ACPDispatcher，不依赖 LLM Provider
        return ACPDispatcher(
            bus=bus,
            workspace=config.workspace_path,
            acp_config=config.dispatch.acp,
            channels_config=config.channels,
        )

    # Native 模式：必须提供 LLM Provider
    if provider is None:
        raise RuntimeError("Native backend requires a configured provider")

    return NativeDispatcher(
        bus=bus,
        provider=provider,
        config=config,
        cron_service=cron_service,
        session_manager=session_manager,
    )
