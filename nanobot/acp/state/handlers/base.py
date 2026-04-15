"""处理器抽象与公共工具：统一更新匹配、池创建、flush 出包与载荷归一化约束。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast

from nanobot.acp.contracts import (
    ACP_META_KIND,
    ACP_META_PROGRESS,
    ACP_META_RENDER_AS,
    JSONMap,
    JSONValue,
)
from nanobot.acp.state.models import (
    ACPBucketType,
    ACPOutboundKind,
    ACPPool,
    ACPUpdateType,
    FlushResult,
    PoolKey,
)
from nanobot.bus.events import OutboundMessage

if TYPE_CHECKING:
    from nanobot.acp.state.manager import SessionStateManager


@dataclass(slots=True)
class HandlerConsumeResult:
    """处理器消费结果：只描述结构性结论，不直接触发 flush/publish。"""

    immediate_finalize: bool = False


class StateUpdateHandler(ABC):
    """状态更新处理器基类：约束单类更新如何映射到池与请求级聚合。"""

    name: str
    update_type: ACPUpdateType
    bucket_type: ACPBucketType

    @abstractmethod
    def match(self, update: object) -> bool:
        """判断当前处理器是否负责该更新。"""

    @abstractmethod
    def create_pool(self, *, bucket_key: str) -> ACPPool:
        """创建该处理器对应的池实例。"""

    @abstractmethod
    def build_bucket_key(self, update: object) -> str:
        """为当前更新计算池索引键。"""

    @abstractmethod
    def consume(
        self,
        *,
        state_manager: SessionStateManager,
        update: object,
        pool: ACPPool,
    ) -> HandlerConsumeResult:
        """消费更新并写入池或 request-scope 聚合；内部保持 `consume -> accept` 主链。"""

    def build_pool_key(self, update: object) -> PoolKey:
        return PoolKey(bucket_type=self.bucket_type, bucket_key=self.build_bucket_key(update))

    def build_progress_outbound(
        self,
        *,
        state_manager: SessionStateManager,
        flush_result: FlushResult,
    ) -> OutboundMessage | None:
        """把一次 flush 结果编制成 progress outbound；默认走 state 公共出包规则。"""

        metadata = dict(flush_result.metadata)
        kind_value = flush_result.kind.value
        # 统一在出包层补齐语义字段，避免各个 pool/handler 分散维护。
        # 允许 pool 在 flush metadata 中显式指定 kind/render_as；这里仅做兜底补齐。
        metadata.setdefault(ACP_META_KIND, kind_value)
        metadata.setdefault(ACP_META_RENDER_AS, kind_value)
        if flush_result.kind == ACPOutboundKind.TOOL:
            # 使用统一 tool_hint约定字段，用于在各种场合下标注这个消息是工具
            metadata.setdefault("_tool_hint", True)
        # 统一使用历史约定字段 `_progress`，让各 channel 正确识别 progress 帧。
        metadata.setdefault(ACP_META_PROGRESS, True)
        return OutboundMessage(
            channel=state_manager.channel,
            chat_id=state_manager.chat_id,
            content=flush_result.content,
            media=list(flush_result.media),
            metadata=metadata,
        )

    def on_flush_result(
        self,
        *,
        state_manager: SessionStateManager,
        flush_result: FlushResult,
    ) -> None:
        """处理一次 flush 产物的 request-scope 副作用；默认不做额外处理。"""

        del state_manager, flush_result

    def on_deadhand(self, *, state_manager: SessionStateManager, pool: ACPPool) -> None:
        """死手触发回调；默认无需额外动作，权限类处理器可按需覆盖。"""

        del state_manager, pool

    def do_deadhand_operate_and_build_progress_outbound(
        self,
        *,
        state_manager: SessionStateManager,
        pool: ACPPool,
    ) -> OutboundMessage | None:
        """执行死手处理并构造 outbound；默认兼容旧 `on_deadhand + flush + build` 主链。"""

        self.on_deadhand(state_manager=state_manager, pool=pool)
        flush_result = pool.flush()
        if flush_result is None:
            return None
        self.on_flush_result(state_manager=state_manager, flush_result=flush_result)
        return self.build_progress_outbound(
            state_manager=state_manager,
            flush_result=flush_result,
        )


class _ModelDumpCapable(Protocol):
    """支持 `model_dump` 的对象协议：用于状态域安全读取 Pydantic 模型。"""

    def model_dump(self, *, by_alias: bool, exclude_none: bool) -> dict[str, object]: ...


def sanitize_json_value(value: object) -> JSONValue:
    """把任意载荷规整为 `JSONValue`；供 final metadata 与调试痕迹复用。"""

    if value is None or isinstance(value, str | int | float | bool):
        return value
    if hasattr(value, "model_dump"):
        dumped = cast(_ModelDumpCapable, value).model_dump(by_alias=True, exclude_none=True)
        return sanitize_json_value(dumped)
    if isinstance(value, dict):
        return {str(key): sanitize_json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set):
        return [sanitize_json_value(item) for item in value]
    return str(value)


def sanitize_json_map(value: object) -> JSONMap:
    """把任意对象规整为 `JSONMap`；非映射对象会包成 `{"value": ...}`。"""

    normalized = sanitize_json_value(value)
    if isinstance(normalized, dict):
        return normalized
    return {"value": normalized}
