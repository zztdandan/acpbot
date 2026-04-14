"""运行时共享模型：请求/响应/入站/执行/队列/停止等核心数据结构。

核心类：
    - RuntimeWaitEntry — 异步等待链条目（request_key → Future）
    - ProcessDirectInput — CLI/直接入口的规范化输入
    - ProcessRequest — 入站层交给执行管理器的标准请求
    - InboundContext — 入站流水线统一上下文（步骤间共享状态）
    - ActiveProcessEntry — 活跃执行请求条目（含状态管理器和进度路由器）
    - SessionQueueState — 单会话串行队列状态
    - StopResult — /stop 命令的结构化结果
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Awaitable, Callable

from nanobot.acp.contracts import ACPArtifactMap, ACPChannelName, ACPDirectIdentity, JSONMap
from nanobot.bus.events import InboundMessage, OutboundMessage

if TYPE_CHECKING:
    from nanobot.acp.state.manager import SessionStateManager
    from nanobot.acp.state.router import ProgressRouter


ProgressCallback = Callable[..., Awaitable[None]]


class RequestStatus(str, Enum):
    """请求在执行管理中的生命周期状态（queued → starting → active → finishing → completed/failed/cancelled）。"""

    QUEUED = "queued"
    STARTING = "starting"
    ACTIVE = "active"
    FINISHING = "finishing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(slots=True)
class RuntimeWaitEntry:
    """运行时等待条目：request_key 到异步 Future 的映射，用于 await 最终响应。"""

    request_key: str
    # 请求唯一键，用于匹配完成通知。
    done_future: asyncio.Future[OutboundMessage]
    # 最终结果等待对象，由运行时统一写入。


@dataclass(slots=True)
class ProcessDirectInput:
    """CLI/直接入口的规范化输入：文本内容、通道标识、媒体路径和进度回调。"""

    content: str
    # 文本输入内容。
    nanobot_side_session_key: str = ACPDirectIdentity.SESSION_KEY.value
    # 业务侧会话主键。
    channel: str = ACPChannelName.CLI.value
    # 输入通道标识。
    chat_id: str = ACPDirectIdentity.CHAT_ID.value
    # 通道会话标识。
    sender_id: str | None = None
    # 发送方标识，可为空。
    media: list[str] = field(default_factory=list)
    # 输入媒体路径集合。
    metadata: JSONMap = field(default_factory=dict)
    # 附带元数据。
    on_progress: ProgressCallback | None = None
    # 兼容进度回调。


@dataclass(slots=True)
class ProcessRequest:
    """入站层交给执行管理器的标准请求：包含完整的执行上下文（会话、通道、内容、媒体、进度回调）。"""

    request_key: str
    # 请求唯一键。
    nanobot_side_session_key: str
    # 业务侧会话主键。
    channel: str
    # 通道标识。
    chat_id: str
    # 通道会话标识。
    sender_id: str | None
    # 发送方标识。
    content: str
    # 标准化文本输入。
    media: list[str]
    # 标准化媒体列表。
    metadata: JSONMap
    # 执行上下文元数据。
    on_progress: ProgressCallback | None = None
    # 进度镜像回调。
    artifacts: dict[str, ACPArtifactMap | list[ACPArtifactMap]] = field(default_factory=dict)
    # 入站中间产物。


@dataclass(slots=True)
class InboundContext:
    """入站流水线统一上下文：流水线各步骤（校验/路由/直返/执行）间的共享状态。"""

    request_key: str
    # 请求唯一键。
    nanobot_side_session_key: str
    # 业务侧会话主键。
    raw_message: InboundMessage | None = None
    # 原始总线消息，直接入口可为空。
    channel: str = ACPChannelName.CLI.value
    # 标准化通道名。
    chat_id: str = ACPDirectIdentity.CHAT_ID.value
    # 标准化会话标识。
    sender_id: str | None = None
    # 发送者标识。
    content: str = ""
    # 标准化文本内容。
    media: list[str] = field(default_factory=list)
    # 标准化媒体路径。
    metadata: JSONMap = field(default_factory=dict)
    # 入站元数据。
    progress_metadata: JSONMap = field(default_factory=dict)
    # 进度镜像附加元数据。
    on_progress: ProgressCallback | None = None
    # 请求级进度回调。
    direct_response: OutboundMessage | None = None
    # 命中直返时的出站消息。
    outbound_messages: list[OutboundMessage] = field(default_factory=list)
    # TODO: 接入 inbound -> outbound 回发链路后，统一消费该缓冲池并逐条回发。
    process_request: ProcessRequest | None = None
    # 进入真实执行时的标准请求。
    artifacts: dict[str, ACPArtifactMap | list[ACPArtifactMap]] = field(default_factory=dict)
    # 步骤间共享中间产物。


@dataclass(slots=True)
class ActiveProcessEntry:
    """活跃执行请求条目：持有完整的运行态资源（请求、状态管理器、进度路由器、生命周期）。"""

    request_key: str
    # 请求唯一键。
    nanobot_side_session_key: str
    # 业务侧会话主键。
    acp_side_session_id: str
    # 协议侧会话标识。
    process_request: ProcessRequest
    # 标准化执行请求。
    state_manager: SessionStateManager
    # 单轮状态管理器。
    progress_router: ProgressRouter
    # 结构化进度路由器。
    status: RequestStatus = RequestStatus.STARTING
    # 当前生命周期状态。

    async def close(self) -> None:
        """关闭该请求持有的状态资源（state_manager + progress_router）。"""

        await self.state_manager.close()


@dataclass(slots=True)
class SessionQueueState:
    """单会话串行队列状态：维护排队请求列表与当前 draining_task 协程句柄。"""

    nanobot_side_session_key: str
    # 队列所属业务侧会话。
    queued_requests: deque[ProcessRequest] = field(default_factory=deque)
    # 待执行请求队列。
    draining_task: asyncio.Task[None] | None = None
    # 队列消费任务句柄。


@dataclass(slots=True)
class StopResult:
    """停止会话后的结构化结果：包含取消指令状态与被丢弃的排队请求数。"""

    nanobot_side_session_key: str
    # 被停止的业务侧会话主键。
    active_cancel_requested: bool = False
    # 是否请求取消活跃执行。
    dropped_queued_count: int = 0
    # 被丢弃的排队请求数量。
    acp_side_session_id: str | None = None
    # 对应协议侧会话标识。

    @property
    def had_anything_to_stop(self) -> bool:
        """判断停止动作是否命中活跃或排队请求（active_cancel_requested or dropped_queued_count > 0）。"""
        return self.active_cancel_requested or self.dropped_queued_count > 0
