"""ACP 延迟导入工厂。

集中管理 ACP schema/block 的懒加载，避免各模块重复 import_module("acp")。
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


def _acp_spawn_agent_process() -> Any:
    """延迟导入 ACP 进程启动函数。"""
    return import_module("acp").spawn_agent_process


def _acp_text_block(content: str) -> Any:
    """延迟导入 ACP text_block。"""
    return import_module("acp").text_block(content)


def _acp_image_block(data: str, mime_type: str, *, uri: str | None = None) -> Any:
    """延迟导入 ACP image_block。"""
    return import_module("acp").image_block(data=data, mime_type=mime_type, uri=uri)


def _acp_resource_link_block(
    name: str,
    uri: str,
    *,
    mime_type: str | None,
    size: int | None,
) -> Any:
    """延迟导入 ACP resource_link_block。"""
    return import_module("acp").resource_link_block(
        name=name,
        uri=uri,
        mime_type=mime_type,
        size=size,
        title=name,
    )


def _acp_embedded_text_resource(uri: str, text: str, *, mime_type: str | None) -> Any:
    """延迟导入 ACP embedded_text_resource。"""
    return import_module("acp").embedded_text_resource(uri=uri, text=text, mime_type=mime_type)


def _acp_embedded_blob_resource(uri: str, blob: str, *, mime_type: str | None) -> Any:
    """延迟导入 ACP embedded_blob_resource。"""
    return import_module("acp").embedded_blob_resource(uri=uri, blob=blob, mime_type=mime_type)


def _acp_resource_block(resource: Any) -> Any:
    """延迟导入 ACP resource_block。"""
    return import_module("acp").resource_block(resource=resource)
