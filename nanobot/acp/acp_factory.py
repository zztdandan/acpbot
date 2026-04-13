"""ACP lazy-import factory helpers.

These wrappers centralize ACP schema/block lazy imports so individual modules do
not repeat `import_module("acp")` or hard-import the SDK too early.
"""

from __future__ import annotations

from importlib import import_module

from nanobot.acp.contracts import ACPFactoryValue


def _acp_spawn_agent_process() -> ACPFactoryValue:
    """Lazy-import the ACP process spawn factory."""
    return import_module("acp").spawn_agent_process


def _acp_text_block(content: str) -> ACPFactoryValue:
    """Lazy-import ACP `text_block`."""
    return import_module("acp").text_block(content)


def _acp_image_block(data: str, mime_type: str, *, uri: str | None = None) -> ACPFactoryValue:
    """Lazy-import ACP `image_block`."""
    return import_module("acp").image_block(data=data, mime_type=mime_type, uri=uri)


def _acp_resource_link_block(
    name: str,
    uri: str,
    *,
    mime_type: str | None,
    size: int | None,
) -> ACPFactoryValue:
    """Lazy-import ACP `resource_link_block`."""
    return import_module("acp").resource_link_block(
        name=name,
        uri=uri,
        mime_type=mime_type,
        size=size,
        title=name,
    )


def _acp_embedded_text_resource(uri: str, text: str, *, mime_type: str | None) -> ACPFactoryValue:
    """Lazy-import ACP `embedded_text_resource`."""
    return import_module("acp").embedded_text_resource(uri=uri, text=text, mime_type=mime_type)


def _acp_embedded_blob_resource(uri: str, blob: str, *, mime_type: str | None) -> ACPFactoryValue:
    """Lazy-import ACP `embedded_blob_resource`."""
    return import_module("acp").embedded_blob_resource(uri=uri, blob=blob, mime_type=mime_type)


def _acp_resource_block(resource: ACPFactoryValue) -> ACPFactoryValue:
    """Lazy-import ACP `resource_block`."""
    return import_module("acp").resource_block(resource=resource)
