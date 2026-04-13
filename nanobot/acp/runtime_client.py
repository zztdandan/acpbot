"""运行时回调桥接实现。

该模块负责把协议回调入口转发回运行时 owner，避免回调在 SDK 层直接分叉业务逻辑。
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
    """运行时回调归属方协议。"""

    # 处理权限请求并返回协议要求的响应载荷。
    async def handle_permission_request(
        self,
        *,
        acp_side_session_id: str,
        options: list[ACPPermissionOption],
        tool_call: ACPToolCall,
    ) -> object:
        """处理权限请求并返回协议响应对象。"""
        ...

    # 处理会话更新回调并写入对应请求状态。
    async def handle_session_update(
        self, *, acp_side_session_id: str, update: ACPCallbackUpdate
    ) -> None:
        """处理会话更新并写入对应请求状态。"""
        ...


class _NanobotACPClient:
    """协议回调客户端适配器。"""

    def __init__(self, runtime: _RuntimeCallbackOwner) -> None:
        """绑定运行时回调归属方。"""
        self.runtime = runtime

    async def request_permission(self, options, session_id, tool_call, **kwargs):
        """转发权限请求到运行时状态机。"""
        del kwargs
        return await self.runtime.handle_permission_request(
            acp_side_session_id=session_id,
            options=options,
            tool_call=tool_call,
        )

    async def session_update(self, session_id, update, **kwargs) -> None:
        """转发会话更新事件到运行时。"""
        del kwargs
        await self.runtime.handle_session_update(acp_side_session_id=session_id, update=update)

    async def write_text_file(self, content, path, session_id, **kwargs):
        """占位实现，当前后端不接管写文件。"""
        del content, path, session_id, kwargs
        return None

    async def read_text_file(self, path, session_id, limit=None, line=None, **kwargs):
        """占位实现，当前后端不提供读文件能力。"""
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
        """占位实现，当前后端不提供终端创建能力。"""
        del command, session_id, args, cwd, env, output_byte_limit, kwargs
        raise NotImplementedError

    async def terminal_output(self, session_id, terminal_id, **kwargs):
        """占位实现，当前后端不提供终端输出读取能力。"""
        del session_id, terminal_id, kwargs
        raise NotImplementedError

    async def release_terminal(self, session_id, terminal_id, **kwargs):
        """占位实现，当前后端不持有终端资源。"""
        del session_id, terminal_id, kwargs
        return None

    async def wait_for_terminal_exit(self, session_id, terminal_id, **kwargs):
        """占位实现，当前后端不等待终端退出。"""
        del session_id, terminal_id, kwargs
        raise NotImplementedError

    async def kill_terminal(self, session_id, terminal_id, **kwargs):
        """占位实现，当前后端不负责终端销毁。"""
        del session_id, terminal_id, kwargs
        return None

    async def ext_method(self, method: str, params: JSONMap) -> JSONMap:
        """扩展方法兜底，默认返回空对象。"""
        del method, params
        return {}

    async def ext_notification(self, method: str, params: JSONMap) -> None:
        """扩展通知兜底，当前仅做吞吐。"""
        del method, params

    def on_connect(self, conn) -> None:
        """连接建立回调，当前无需额外处理。"""
        del conn
