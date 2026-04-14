"""运行时回调桥接：把 ACP SDK 回调入口转发回运行时 owner（ACPRuntime）。

职责：
    - 定义 _RuntimeCallbackOwner 协议（运行时必须实现的回调接口）
    - 实现 _NanobotACPClient 适配器（将 SDK 回调转发到 runtime owner）
    - 占位实现 nanobot 不支持的 ACP 能力（文件读写、终端管理等）

设计约束：
    回调不直接在 SDK 层分叉业务逻辑，统一收口到 ACPRuntime 的 handle_* 方法。
"""

from __future__ import annotations

from typing import Protocol

from nanobot.acp.contracts import (
    ACPCallbackUpdate,
    ACPPermissionOption,
    ACPToolCall,
    JSONMap,
)


class _RuntimeCallbackOwner(Protocol):
    """运行时回调归属方协议（ACPRuntime 必须实现的方法签名）。

    职责：
        - handle_permission_request: 处理 ACP 权限请求，返回用户选择的响应载荷
        - handle_session_update: 处理 ACP 会话更新回调，写入对应请求状态
    """

    async def handle_permission_request(
        self,
        *,
        acp_side_session_id: str,
        options: list[ACPPermissionOption],
        tool_call: ACPToolCall,
    ) -> object:
        """处理权限请求：接收 ACP 发来的权限选项列表，返回用户选择结果（selected/cancelled）。"""
        ...

    async def handle_session_update(
        self, *, acp_side_session_id: str, update: ACPCallbackUpdate
    ) -> None:
        """处理会话更新：接收 ACP 推送的流式文本/工具调用等更新，写入对应请求状态。"""
        ...


class _NanobotACPClient:
    """协议回调客户端适配器：将 ACP SDK 回调转发到 ACPRuntime。

    职责：
        - 作为 ACP SDK 的回调服务端（接收 permission/session_update 等回调）
        - 将回调参数规范化后转发到 runtime owner 的 handle_* 方法
        - 占位实现 nanobot 不支持的 ACP 能力（文件读写、终端管理）

    生命周期：
        - 创建：ensure_connection 时实例化，绑定到当前 ACPRuntime
        - 销毁：连接重置时随 connection_cm 一同释放
    """

    def __init__(self, runtime: _RuntimeCallbackOwner) -> None:
        """绑定运行时回调归属方（依赖注入）。"""
        self.runtime = runtime

    async def request_permission(self, options, session_id, tool_call, **kwargs):
        """转发权限请求到 runtime.handle_permission_request（ACP SDK 回调入口）。"""
        del kwargs
        return await self.runtime.handle_permission_request(
            acp_side_session_id=session_id,
            options=options,
            tool_call=tool_call,
        )

    async def session_update(self, session_id, update, **kwargs) -> None:
        """转发会话更新到 runtime.handle_session_update（ACP SDK 回调入口）。"""
        del kwargs
        await self.runtime.handle_session_update(acp_side_session_id=session_id, update=update)

    async def write_text_file(self, content, path, session_id, **kwargs):
        """占位：nanobot 不接管 ACP 写文件操作（由 ACP Agent 自行处理）。"""
        del content, path, session_id, kwargs
        return None

    async def read_text_file(self, path, session_id, limit=None, line=None, **kwargs):
        """占位：nanobot 不提供读文件能力（ACP 协议 capability 未声明）。"""
        del path, session_id, limit, line, kwargs
        raise NotImplementedError

    async def create_terminal(
        self,
        command,
        session_id,
        args=None,
        cwd=None,
        env=None,
        output_byte_limit=None,
        **kwargs,
    ):
        """占位：nanobot 不提供终端创建能力（ACP 协议 capability 未声明）。"""
        del command, session_id, args, cwd, env, output_byte_limit, kwargs
        raise NotImplementedError

    async def terminal_output(self, session_id, terminal_id, **kwargs):
        """占位：nanobot 不提供终端输出读取能力。"""
        del session_id, terminal_id, kwargs
        raise NotImplementedError

    async def release_terminal(self, session_id, terminal_id, **kwargs):
        """占位：nanobot 不持有终端资源（无需释放）。"""
        del session_id, terminal_id, kwargs
        return None

    async def wait_for_terminal_exit(self, session_id, terminal_id, **kwargs):
        """占位：nanobot 不等待终端退出。"""
        del session_id, terminal_id, kwargs
        raise NotImplementedError

    async def kill_terminal(self, session_id, terminal_id, **kwargs):
        """占位：nanobot 不负责终端销毁。"""
        del session_id, terminal_id, kwargs
        return None

    async def ext_method(self, method: str, params: JSONMap) -> JSONMap:
        """扩展方法兜底：未识别的扩展方法统一返回空对象。"""
        del method, params
        return {}

    async def ext_notification(self, method: str, params: JSONMap) -> None:
        """扩展通知兜底：未识别的扩展通知静默吞没。"""
        del method, params

    def on_connect(self, conn) -> None:
        """连接建立回调：当前无需额外处理（连接就绪事件由 lifecycle 层上报）。"""
        del conn
