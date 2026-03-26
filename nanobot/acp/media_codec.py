"""ACP media codec family 入口。"""

from __future__ import annotations

from nanobot.acp.media_codec_inbound import _ACPInboundMediaCodecMixin
from nanobot.acp.media_codec_outbound import _ACPOutboundMediaCodecMixin


class _ACPFileTransportMixin(_ACPInboundMediaCodecMixin, _ACPOutboundMediaCodecMixin):
    """为 ACPDispatcher 提供文件传输相关能力（与调度主流程解耦）。"""

    _INLINE_TEXT_BYTES_LIMIT = 512 * 1024
    _INLINE_BLOB_BYTES_LIMIT = 2 * 1024 * 1024
