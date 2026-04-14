"""静态事实处理器：处理已知但不需要独立进度镜像的更新类型。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from acp.schema import (
    AvailableCommandsUpdate,
    ConfigOptionUpdate,
    CurrentModeUpdate,
    SessionInfoUpdate,
    UsageUpdate,
    UserMessageChunk,
)

from nanobot.acp.state.handlers.base import (
    HandlerConsumeResult,
    StateUpdateHandler,
    sanitize_json_value,
)
from nanobot.acp.state.models import ACPBucketType, ACPUpdateType, PoolKey
from nanobot.acp.state.pools import ConsumeOnlyPool

if TYPE_CHECKING:
    from nanobot.acp.state.manager import SessionStateManager


@dataclass(frozen=True, slots=True)
class ConsumeOnlySpec:
    """静态事实规则：声明某种更新的匹配类、池键与最终元数据键。

    职责：
        - 把“某类更新应如何落池、写入哪个元数据键”集中配置化
        - 让静态事实处理器复用同一套逻辑，避免为静态类型重复写类
    """

    update_cls: type[object]
    update_type: ACPUpdateType
    bucket_key: str
    metadata_key: str
    destroy_after_accept: bool = False


class ConsumeOnlyUpdateHandler(StateUpdateHandler):
    """通用静态事实处理器：统一收口使用量、配置、会话信息等静态回流。"""

    bucket_type = ACPBucketType.CONSUME_ONLY

    def __init__(self, spec: ConsumeOnlySpec) -> None:
        """绑定一条静态事实规则；一条规则对应一种已知更新类型。"""

        self.spec = spec
        self.name = spec.metadata_key
        self.update_type = spec.update_type

    def match(self, update: object) -> bool:
        """按规则声明的 ACP 模型类做精准匹配。"""

        return isinstance(update, self.spec.update_cls)

    def create_pool(self, *, bucket_key: str) -> ConsumeOnlyPool:
        """创建消费池；是否一次性销毁由规则控制。"""

        return ConsumeOnlyPool(
            bucket_key=bucket_key,
            destroy_after_accept=self.spec.destroy_after_accept,
        )

    def build_bucket_key(self, update: object) -> str:
        """返回规则声明的固定桶键；同类静态更新复用一个池。"""

        del update
        return self.spec.bucket_key

    def consume(
        self,
        *,
        state_manager: SessionStateManager,
        update: object,
        pool,
    ) -> HandlerConsumeResult:
        """记录静态更新事实；需要立刻销毁时由路由器统一收口。

        处理流程：
            - 先把更新规整成可序列化载荷
            - 写入消费池，保留最后一次静态事实
            - 同步写入请求级最终元数据，并在终态时声明销毁池键
        """

        payload = sanitize_json_value(update)
        pool.accept(payload)
        state_manager.update_named_metadata(self.spec.metadata_key, payload)
        destroy_pool_keys: list[PoolKey] = []
        if pool.is_terminal():
            destroy_pool_keys.append(self.build_pool_key(update))
        return HandlerConsumeResult(destroy_pool_keys=destroy_pool_keys)


def build_consume_only_handlers() -> list[ConsumeOnlyUpdateHandler]:
    """构造全部静态事实处理器；避免管理器中出现大段类型分支。"""

    specs = [
        ConsumeOnlySpec(
            update_cls=SessionInfoUpdate,
            update_type=ACPUpdateType.SESSION_INFO,
            bucket_key="session_info",
            metadata_key="session_info",
        ),
        ConsumeOnlySpec(
            update_cls=CurrentModeUpdate,
            update_type=ACPUpdateType.CURRENT_MODE,
            bucket_key="current_mode",
            metadata_key="current_mode",
        ),
        ConsumeOnlySpec(
            update_cls=UsageUpdate,
            update_type=ACPUpdateType.USAGE,
            bucket_key="usage",
            metadata_key="usage",
        ),
        ConsumeOnlySpec(
            update_cls=AvailableCommandsUpdate,
            update_type=ACPUpdateType.AVAILABLE_COMMANDS,
            bucket_key="available_commands",
            metadata_key="available_commands",
        ),
        ConsumeOnlySpec(
            update_cls=ConfigOptionUpdate,
            update_type=ACPUpdateType.CONFIG_OPTION,
            bucket_key="config_option",
            metadata_key="config_option",
        ),
        ConsumeOnlySpec(
            update_cls=UserMessageChunk,
            update_type=ACPUpdateType.USER_MESSAGE,
            bucket_key="user_message",
            metadata_key="last_user_message",
            destroy_after_accept=True,
        ),
    ]
    return [ConsumeOnlyUpdateHandler(spec) for spec in specs]
