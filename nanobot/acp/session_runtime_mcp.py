"""ACP session runtime 的 MCP 配置转换辅助。"""

from __future__ import annotations

from typing import Any

from nanobot.config.schema import MCPServerConfig


def _convert_mcp_servers(mcp_servers: dict[str, MCPServerConfig]) -> list[Any]:
    """把 nanobot MCP 配置转换为 ACP schema。"""
    from acp.schema import EnvVariable, HttpHeader, HttpMcpServer, McpServerStdio, SseMcpServer

    converted = []
    for name, cfg in mcp_servers.items():
        if cfg.command:
            # stdio MCP
            converted.append(
                McpServerStdio(
                    name=name,
                    command=cfg.command,
                    args=cfg.args,
                    env=[EnvVariable(name=k, value=v) for k, v in cfg.env.items()],
                    field_meta={"toolTimeout": cfg.tool_timeout},
                )
            )
            continue
        if cfg.url:
            # http/sse MCP
            headers = [HttpHeader(name=k, value=v) for k, v in cfg.headers.items()]
            if cfg.url.startswith("http://") or cfg.url.startswith("https://"):
                converted.append(
                    HttpMcpServer(
                        type="http",
                        name=name,
                        url=cfg.url,
                        headers=headers,
                        field_meta={"toolTimeout": cfg.tool_timeout},
                    )
                )
            else:
                converted.append(
                    SseMcpServer(
                        type="sse",
                        name=name,
                        url=cfg.url,
                        headers=headers,
                        field_meta={"toolTimeout": cfg.tool_timeout},
                    )
                )
    return converted
