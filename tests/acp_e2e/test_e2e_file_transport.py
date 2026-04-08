from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import pytest

from .helpers import ensure_contains_any, read_tool_audit_rows, staged_fixture_path


async def _install_prompt_recorder(acp_e2e_harness) -> list[list[Any]]:
    """Patch conn.prompt to record the exact prompt blocks sent to ACP."""

    await acp_e2e_harness.dispatcher._ensure_connection()  # noqa: SLF001
    conn = acp_e2e_harness.dispatcher._conn  # noqa: SLF001
    assert conn is not None

    records: list[list[Any]] = []
    original_prompt = conn.prompt

    async def _recording_prompt(*, prompt, session_id, **kwargs):
        records.append(list(prompt))
        return await original_prompt(prompt=prompt, session_id=session_id, **kwargs)

    conn.prompt = _recording_prompt  # type: ignore[method-assign]
    await asyncio.sleep(0)
    return records


def _block_types(blocks: list[Any]) -> list[str]:
    return [str(getattr(block, "type", "")) for block in blocks]


def _resource_link_blocks(blocks: list[Any]) -> list[Any]:
    return [block for block in blocks if getattr(block, "type", None) == "resource_link"]


def _resource_link_path(block: Any) -> Path:
    parsed = urlparse(str(getattr(block, "uri", "") or ""))
    return Path(unquote(parsed.path))


def _latest_prompt_blocks(records: list[list[Any]]) -> list[Any]:
    assert records, "Expected at least one prompt call to ACP"
    return records[-1]


def _outbound_media_paths(messages: list[Any]) -> list[Path]:
    paths: list[Path] = []
    for msg in messages:
        for media_path in list(msg.media or []):
            paths.append(Path(media_path))
    return paths


def _send_file_prompt(*, relative_path: str, marker: str) -> str:
    return (
        "Use the acp_send_file tool exactly once. "
        f'Set filePath="{relative_path}" and filename="{marker}". '
        "Do not describe the file before calling the tool."
    )


@pytest.mark.asyncio
async def test_acp_e2e_workspace_contains_plugin_and_file_transport_fixtures(
    acp_e2e_workspace: Path,
    acp_e2e_fixture_dir: Path,
    acp_e2e_plugin_path: Path,
) -> None:
    assert (
        acp_e2e_workspace.joinpath(".opencode", "plugin", "acp-send-file.ts") == acp_e2e_plugin_path
    )
    assert acp_e2e_plugin_path.is_file()
    assert acp_e2e_fixture_dir.joinpath("doctor.txt").is_file()
    assert acp_e2e_fixture_dir.joinpath("Embeding_16.png").is_file()
    assert acp_e2e_fixture_dir.joinpath("6a489f05-0270-44e9-98b1-68df708c7c4f_hd.mp4").is_file()


@pytest.mark.asyncio
async def test_acp_e2e_plugin_fixture_is_loadable_by_outbound_smoke(
    acp_e2e_trusted_harness,
    acp_e2e_session_key: str,
) -> None:
    marker = f"ft-plugin-smoke-{acp_e2e_session_key}"
    before = read_tool_audit_rows(acp_e2e_trusted_harness, "acp_send_file")

    await acp_e2e_trusted_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content=_send_file_prompt(
            relative_path="fixtures/file_transport/doctor.txt", marker=marker
        ),
    )
    _ = await acp_e2e_trusted_harness.collect_outbound_until(
        stop_when=lambda messages: bool(messages),
        total_timeout=120.0,
    )

    after = read_tool_audit_rows(acp_e2e_trusted_harness, "acp_send_file")
    new_rows = after[len(before) :]

    assert any(
        row.get("event") == "tool_start"
        and row.get("tool_name") == "acp_send_file"
        and row.get("sessionKey") == acp_e2e_session_key
        for row in new_rows
    )
    assert any(marker in str(row) for row in new_rows)


@pytest.mark.asyncio
async def test_e2e_ft_001_doctor_txt_is_injected_as_resource_link(
    acp_e2e_harness,
    acp_e2e_session_key: str,
    acp_e2e_fixture_dir: Path,
) -> None:
    records = await _install_prompt_recorder(acp_e2e_harness)
    doctor = acp_e2e_fixture_dir / "doctor.txt"

    await acp_e2e_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content=(
            "Read the attached text file, repeat the exact sentence from the file, "
            "and mention doctor.txt in your answer."
        ),
        media=[str(doctor)],
    )
    messages = await acp_e2e_harness.collect_outbound_until(
        stop_when=lambda rows: ensure_contains_any(
            rows, ["i am a doctor,my name is zzt", "doctor.txt"]
        ),
        total_timeout=120,
    )

    blocks = _latest_prompt_blocks(records)
    resource_links = _resource_link_blocks(blocks)
    assert _block_types(blocks) == ["text", "resource_link"]
    assert len(resource_links) == 1
    assert getattr(resource_links[0], "name", None) == "doctor.txt"
    assert _resource_link_path(resource_links[0]) == doctor.resolve()
    assert ensure_contains_any(messages, ["i am a doctor,my name is zzt", "doctor.txt"])


@pytest.mark.asyncio
async def test_e2e_ft_002_png_and_mp4_are_injected_as_resource_links(
    acp_e2e_harness,
    acp_e2e_session_key: str,
    acp_e2e_fixture_dir: Path,
) -> None:
    records = await _install_prompt_recorder(acp_e2e_harness)
    png = acp_e2e_fixture_dir / "Embeding_16.png"
    mp4 = acp_e2e_fixture_dir / "6a489f05-0270-44e9-98b1-68df708c7c4f_hd.mp4"

    await acp_e2e_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content=(
            "For each attachment, mention the file type and the file address/basename. "
            "You must mention Embeding_16.png and 6a489f05-0270-44e9-98b1-68df708c7c4f_hd.mp4."
        ),
        media=[str(png), str(mp4)],
    )
    messages = await acp_e2e_harness.collect_outbound_until(
        stop_when=lambda rows: ensure_contains_any(rows, [png.name, mp4.name]),
        total_timeout=120,
    )

    blocks = _latest_prompt_blocks(records)
    resource_links = _resource_link_blocks(blocks)
    assert _block_types(blocks) == ["text", "resource_link", "resource_link"]
    assert [getattr(block, "name", None) for block in resource_links] == [png.name, mp4.name]
    assert [_resource_link_path(block) for block in resource_links] == [
        png.resolve(),
        mp4.resolve(),
    ]
    assert ensure_contains_any(messages, [png.name, mp4.name])


@pytest.mark.asyncio
async def test_e2e_ft_003_three_staged_files_share_one_prompt(
    acp_e2e_harness,
    acp_e2e_session_key: str,
    acp_e2e_fixture_dir: Path,
) -> None:
    records = await _install_prompt_recorder(acp_e2e_harness)
    doctor = acp_e2e_fixture_dir / "doctor.txt"
    png = acp_e2e_fixture_dir / "Embeding_16.png"
    mp4 = acp_e2e_fixture_dir / "6a489f05-0270-44e9-98b1-68df708c7c4f_hd.mp4"

    await acp_e2e_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content=(
            "Process all three attachments in one round. Repeat the text file sentence, "
            "and mention both non-text basenames in the response."
        ),
        media=[str(doctor), str(png), str(mp4)],
    )
    messages = await acp_e2e_harness.collect_outbound_until(
        stop_when=lambda rows: ensure_contains_any(
            rows, ["i am a doctor,my name is zzt", png.name, mp4.name]
        ),
        total_timeout=120,
    )

    blocks = _latest_prompt_blocks(records)
    resource_links = _resource_link_blocks(blocks)
    assert _block_types(blocks) == ["text", "resource_link", "resource_link", "resource_link"]
    assert [getattr(block, "name", None) for block in resource_links] == [
        doctor.name,
        png.name,
        mp4.name,
    ]
    assert [_resource_link_path(block) for block in resource_links] == [
        doctor.resolve(),
        png.resolve(),
        mp4.resolve(),
    ]
    assert ensure_contains_any(messages, ["i am a doctor,my name is zzt", png.name, mp4.name])


@pytest.mark.asyncio
async def test_acp_e2e_trusted_harness_overrides_permission_policy_only_for_outbound(
    acp_e2e_harness,
    acp_e2e_trusted_harness,
) -> None:
    assert acp_e2e_harness.dispatcher.acp_config.permissions_policy == "strict"
    assert acp_e2e_trusted_harness.dispatcher.acp_config.permissions_policy == "trusted"


@pytest.mark.asyncio
async def test_acp_send_file_is_denied_under_default_strict_harness(
    acp_e2e_harness,
    acp_e2e_session_key: str,
) -> None:
    marker = f"ft-strict-{acp_e2e_session_key}"
    before = read_tool_audit_rows(acp_e2e_harness, "acp_send_file")

    await acp_e2e_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content=_send_file_prompt(
            relative_path="fixtures/file_transport/doctor.txt", marker=marker
        ),
    )
    messages = await acp_e2e_harness.collect_outbound_until_idle(total_timeout=120.0)
    after = read_tool_audit_rows(acp_e2e_harness, "acp_send_file")

    assert _outbound_media_paths(messages) == []
    assert len(after) >= len(before)


@pytest.mark.asyncio
async def test_e2e_ft_004_acp_send_file_emits_outbound_media_path(
    acp_e2e_trusted_harness,
    acp_e2e_session_key: str,
) -> None:
    marker = f"ft-outbound-{acp_e2e_session_key}.txt"
    before = read_tool_audit_rows(acp_e2e_trusted_harness, "acp_send_file")

    await acp_e2e_trusted_harness.send_inbound(
        session_key=acp_e2e_session_key,
        content=_send_file_prompt(
            relative_path="fixtures/file_transport/doctor.txt", marker=marker
        ),
    )
    messages = await acp_e2e_trusted_harness.collect_outbound_until(
        stop_when=lambda rows: bool(_outbound_media_paths(rows)),
        total_timeout=120.0,
    )

    media_paths = _outbound_media_paths(messages)
    after = read_tool_audit_rows(acp_e2e_trusted_harness, "acp_send_file")
    new_rows = after[len(before) :]

    assert media_paths
    assert all(path.is_file() for path in media_paths)
    assert all(
        path.is_relative_to(
            acp_e2e_trusted_harness.dispatcher.workspace / "Download" / "acp-outbound"
        )
        for path in media_paths
    )
    assert any(
        row.get("event") == "tool_start"
        and row.get("tool_name") == "acp_send_file"
        and row.get("sessionKey") == acp_e2e_session_key
        for row in new_rows
    )
    assert any(marker in str(row) for row in new_rows)


@pytest.mark.asyncio
async def test_e2e_ft_005_each_sample_file_lands_under_download_acp_outbound(
    acp_e2e_trusted_harness,
    acp_e2e_session_key: str,
    acp_e2e_workspace: Path,
) -> None:
    outbound_root = acp_e2e_workspace / "Download" / "acp-outbound"
    samples = [
        "doctor.txt",
        "Embeding_16.png",
        "6a489f05-0270-44e9-98b1-68df708c7c4f_hd.mp4",
    ]

    for index, sample in enumerate(samples, start=1):
        marker = f"ft-outbound-{index}-{acp_e2e_session_key}-{sample}"
        before = read_tool_audit_rows(acp_e2e_trusted_harness, "acp_send_file")
        await acp_e2e_trusted_harness.send_inbound(
            session_key=acp_e2e_session_key,
            content=_send_file_prompt(
                relative_path=str(
                    staged_fixture_path(acp_e2e_workspace, sample).relative_to(acp_e2e_workspace)
                ),
                marker=marker,
            ),
        )
        messages = await acp_e2e_trusted_harness.collect_outbound_until(
            stop_when=lambda rows: bool(_outbound_media_paths(rows)),
            total_timeout=120.0,
        )
        media_paths = _outbound_media_paths(messages)
        after = read_tool_audit_rows(acp_e2e_trusted_harness, "acp_send_file")
        new_rows = after[len(before) :]

        assert media_paths
        assert all(path.is_file() for path in media_paths)
        assert all(path.is_relative_to(outbound_root) for path in media_paths)
        assert any(
            row.get("event") == "tool_start"
            and row.get("tool_name") == "acp_send_file"
            and row.get("sessionKey") == acp_e2e_session_key
            for row in new_rows
        )
        assert any(marker in str(row) for row in new_rows)
