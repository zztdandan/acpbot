"""ACP client 回调适配层。

该类把 ACP SDK 的回调协议方法转接到 ACPDispatcher，
从而把连接层事件统一汇入 nanobot 运行时逻辑。
"""

from __future__ import annotations

from typing import Any


class _NanobotACPClient:
    """ACP SDK 侧 client 回调实现。"""

    def __init__(self, dispatcher: Any):
        # dispatcher 由外层注入，避免此层感知具体业务细节。
        self.dispatcher = dispatcher

    async def request_permission(self, options, session_id, tool_call, **kwargs):
        # 权限决策统一交由 dispatcher，根据配置策略返回 allow/cancel。
        del session_id, tool_call, kwargs
        return await self.dispatcher._permission_response(options)

    async def session_update(self, session_id, update, **kwargs) -> None:
        # ACP 的增量事件（文本/tool 状态）交给 dispatcher 聚合处理。
        del kwargs
        await self.dispatcher._handle_session_update(session_id, update)

    async def write_text_file(self, content, path, session_id, **kwargs):
        # 当前 nanobot ACP 路径不通过该回调执行文件写入，显式 no-op。
        del content, path, session_id, kwargs
        return None

    async def read_text_file(self, path, session_id, limit=None, line=None, **kwargs):
        # 未接入 read_text_file 协议回调，保持 NotImplemented 便于尽早暴露调用。
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
        # 终端能力目前由 ACP agent 自身处理，nanobot 侧不代理创建。
        del command, session_id, args, cwd, env, output_byte_limit, kwargs
        raise NotImplementedError

    async def terminal_output(self, session_id, terminal_id, **kwargs):
        # 同上：未接入 terminal output 回调。
        del session_id, terminal_id, kwargs
        raise NotImplementedError

    async def release_terminal(self, session_id, terminal_id, **kwargs):
        # 保持兼容返回，避免上层因为未实现而崩溃。
        del session_id, terminal_id, kwargs
        return None

    async def wait_for_terminal_exit(self, session_id, terminal_id, **kwargs):
        # 同上：未接入 terminal wait 回调。
        del session_id, terminal_id, kwargs
        raise NotImplementedError

    async def kill_terminal(self, session_id, terminal_id, **kwargs):
        # 保持幂等 no-op。
        del session_id, terminal_id, kwargs
        return None

    async def ext_method(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        # 扩展方法当前不由 nanobot 处理，返回空对象保持协议兼容。
        del method, params
        return {}

    async def ext_notification(self, method: str, params: dict[str, Any]) -> None:
        # 扩展通知默认忽略，后续若接入可在此扩展。
        del method, params

    def on_connect(self, conn) -> None:
        # 当前无需额外 on_connect 行为，保留钩子位。
        del conn
