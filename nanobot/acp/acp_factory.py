"""ACP 工厂适配层：延迟导入 SDK 构造器并统一提供轻量包装函数。"""

from __future__ import annotations

from importlib import import_module

from nanobot.acp.contracts import ACPFactoryValue


def _acp_spawn_agent_process() -> ACPFactoryValue:
    """返回 ACP SDK 的 `spawn_agent_process` 工厂；用于运行时延迟创建连接。"""
    return import_module("acp").spawn_agent_process


def _acp_text_block(content: str) -> ACPFactoryValue:
    """构造文本块；用于 prompt 主文本的标准化封装。"""
    return import_module("acp").text_block(content)


def _acp_image_block(data: str, mime_type: str, *, uri: str | None = None) -> ACPFactoryValue:
    """构造图片块；用于把二进制或 base64 图片内容注入 prompt。"""
    return import_module("acp").image_block(data=data, mime_type=mime_type, uri=uri)


def _acp_resource_link_block(
    name: str,
    uri: str,
    *,
    mime_type: str | None,
    size: int | None,
) -> ACPFactoryValue:
    """构造资源链接块；用于把本地媒体文件以可下载引用传给 ACP。"""
    return import_module("acp").resource_link_block(
        name=name,
        uri=uri,
        mime_type=mime_type,
        size=size,
        title=name,
    )


def _acp_embedded_text_resource(uri: str, text: str, *, mime_type: str | None) -> ACPFactoryValue:
    """构造内嵌文本资源；用于把短文本以资源对象形式附加到消息。"""
    return import_module("acp").embedded_text_resource(uri=uri, text=text, mime_type=mime_type)


def _acp_embedded_blob_resource(uri: str, blob: str, *, mime_type: str | None) -> ACPFactoryValue:
    """构造内嵌二进制资源；用于把 blob 载荷封装为 ACP 资源对象。"""
    return import_module("acp").embedded_blob_resource(uri=uri, blob=blob, mime_type=mime_type)


def _acp_resource_block(resource: ACPFactoryValue) -> ACPFactoryValue:
    """构造资源块；用于把已创建资源对象包装到 prompt block。"""
    return import_module("acp").resource_block(resource=resource)
