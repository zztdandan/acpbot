"""该模块承接重构后的职责边界。"""

from __future__ import annotations

from importlib import import_module

from nanobot.acp.contracts import ACPFactoryValue


def _acp_spawn_agent_process() -> ACPFactoryValue:
    """执行该方法定义的处理流程并返回结果。"""
    return import_module("acp").spawn_agent_process


def _acp_text_block(content: str) -> ACPFactoryValue:
    """执行该方法定义的处理流程并返回结果。"""
    return import_module("acp").text_block(content)


def _acp_image_block(data: str, mime_type: str, *, uri: str | None = None) -> ACPFactoryValue:
    """执行该方法定义的处理流程并返回结果。"""
    return import_module("acp").image_block(data=data, mime_type=mime_type, uri=uri)


def _acp_resource_link_block(
    name: str,
    uri: str,
    *,
    mime_type: str | None,
    size: int | None,
) -> ACPFactoryValue:
    """执行该方法定义的处理流程并返回结果。"""
    return import_module("acp").resource_link_block(
        name=name,
        uri=uri,
        mime_type=mime_type,
        size=size,
        title=name,
    )


def _acp_embedded_text_resource(uri: str, text: str, *, mime_type: str | None) -> ACPFactoryValue:
    """执行该方法定义的处理流程并返回结果。"""
    return import_module("acp").embedded_text_resource(uri=uri, text=text, mime_type=mime_type)


def _acp_embedded_blob_resource(uri: str, blob: str, *, mime_type: str | None) -> ACPFactoryValue:
    """执行该方法定义的处理流程并返回结果。"""
    return import_module("acp").embedded_blob_resource(uri=uri, blob=blob, mime_type=mime_type)


def _acp_resource_block(resource: ACPFactoryValue) -> ACPFactoryValue:
    """执行该方法定义的处理流程并返回结果。"""
    return import_module("acp").resource_block(resource=resource)
