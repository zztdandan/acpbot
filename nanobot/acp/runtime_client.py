"""ACP client callback bridge owned by runtime."""

from __future__ import annotations

from typing import Protocol

from nanobot.acp.contracts import (
    ACPCallbackUpdate,
    ACPPermissionOption,
    ACPToolCall,
    JSONMap,
)


class _RuntimeCallbackOwner(Protocol):
    async def handle_permission_request(
        self,
        *,
        acp_side_session_id: str,
        options: list[ACPPermissionOption],
        tool_call: ACPToolCall,
    ) -> object: ...

    async def handle_session_update(
        self, *, acp_side_session_id: str, update: ACPCallbackUpdate
    ) -> None: ...


class _NanobotACPClient:
    """ACP SDK callback surface bridged back into ACPRuntime owners."""

    def __init__(self, runtime: _RuntimeCallbackOwner) -> None:
        self.runtime = runtime

    async def request_permission(self, options, session_id, tool_call, **kwargs):
        del kwargs
        return await self.runtime.handle_permission_request(
            acp_side_session_id=session_id,
            options=options,
            tool_call=tool_call,
        )

    async def session_update(self, session_id, update, **kwargs) -> None:
        del kwargs
        await self.runtime.handle_session_update(acp_side_session_id=session_id, update=update)

    async def write_text_file(self, content, path, session_id, **kwargs):
        del content, path, session_id, kwargs
        return None

    async def read_text_file(self, path, session_id, limit=None, line=None, **kwargs):
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
        del command, session_id, args, cwd, env, output_byte_limit, kwargs
        raise NotImplementedError

    async def terminal_output(self, session_id, terminal_id, **kwargs):
        del session_id, terminal_id, kwargs
        raise NotImplementedError

    async def release_terminal(self, session_id, terminal_id, **kwargs):
        del session_id, terminal_id, kwargs
        return None

    async def wait_for_terminal_exit(self, session_id, terminal_id, **kwargs):
        del session_id, terminal_id, kwargs
        raise NotImplementedError

    async def kill_terminal(self, session_id, terminal_id, **kwargs):
        del session_id, terminal_id, kwargs
        return None

    async def ext_method(self, method: str, params: JSONMap) -> JSONMap:
        del method, params
        return {}

    async def ext_notification(self, method: str, params: JSONMap) -> None:
        del method, params

    def on_connect(self, conn) -> None:
        del conn
