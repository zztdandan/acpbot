"""consume-only handler：处理已知但不需要独立 progress 的静态更新类型。"""

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
    """consume-only 规则描述：声明某种 update 的匹配类、池键与 final_metadata 键。"""

    update_cls: type[object]
    update_type: ACPUpdateType
    bucket_key: str
    metadata_key: str
    destroy_after_accept: bool = False


class ConsumeOnlyUpdateHandler(StateUpdateHandler):
    """通用消费型更新处理器：统一收口 usage/config/session_info 等静态回流。"""

    bucket_type = ACPBucketType.CONSUME_ONLY

    def __init__(self, spec: ConsumeOnlySpec) -> None:
        """绑定一条 consume-only 规则；一个规则对应一种已知 update 类型。"""

        self.spec = spec
        self.name = spec.metadata_key
        self.update_type = spec.update_type

    def match(self, update: object) -> bool:
        """按规则声明的 ACP schema 类做精准匹配。"""

        return isinstance(update, self.spec.update_cls)

    def create_pool(self, *, bucket_key: str) -> ConsumeOnlyPool:
        """创建消费池；是否一次性销毁由 spec 控制。"""

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
        """记录静态更新事实；需要立刻销毁时由 router 收口 destroy。"""

        payload = sanitize_json_value(update)
        pool.accept(payload)
        state_manager.update_named_metadata(self.spec.metadata_key, payload)
        destroy_pool_keys: list[PoolKey] = []
        if pool.is_terminal():
            destroy_pool_keys.append(self.build_pool_key(update))
        return HandlerConsumeResult(destroy_pool_keys=destroy_pool_keys)


def build_consume_only_handlers() -> list[ConsumeOnlyUpdateHandler]:
    """构造所有已知 consume-only handler；避免 manager 中出现大段 if/else。"""

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
