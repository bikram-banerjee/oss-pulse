from __future__ import annotations

import gzip
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from orchestration.gh_archive import GitHubEvent, iter_gzip_lines, write_event_partition
from orchestration.storage import LocalLakeStorage

FIXTURE = Path(__file__).parent / "fixtures" / "gh_events.jsonl"


def test_github_event_validation_serializes_raw_payload() -> None:
    event = GitHubEvent.model_validate(
        {
            "id": "123",
            "type": "WatchEvent",
            "actor": {"login": "octocat"},
            "repo": {"name": "octocat/project"},
            "created_at": "2025-01-01T00:00:00Z",
            "payload": {"action": "started"},
        }
    )

    assert event.payload == '{"action":"started"}'
    assert event.created_at.isoformat() == "2025-01-01T00:00:00+00:00"


def test_github_event_rejects_missing_required_field() -> None:
    with pytest.raises(ValidationError):
        GitHubEvent.model_validate(
            {
                "id": "123",
                "type": "WatchEvent",
                "actor": {"login": "octocat"},
                "created_at": "2025-01-01T00:00:00Z",
                "payload": "{}",
            }
        )


def test_parquet_writer_filters_and_quarantines_fixture_rows(tmp_path: Path) -> None:
    lake = LocalLakeStorage(tmp_path / "lake")
    lines = (line for line in FIXTURE.read_bytes().splitlines() if line)

    counts = write_event_partition(
        lines,
        lake,
        "raw/gh_events/dt=2025-01-01/hour=00/part.parquet",
        "quarantine/gh_events/dt=2025-01-01/hour=00/part.parquet",
    )

    valid_path = (
        tmp_path
        / "lake"
        / "raw"
        / "gh_events"
        / "dt=2025-01-01"
        / "hour=00"
        / "part.parquet"
    )
    quarantine_path = (
        tmp_path
        / "lake"
        / "quarantine"
        / "gh_events"
        / "dt=2025-01-01"
        / "hour=00"
        / "part.parquet"
    )
    valid_table = pq.read_table(valid_path)
    quarantine_table = pq.read_table(quarantine_path)

    assert counts == (5, 2, 2)
    assert valid_table.num_rows == 2
    assert valid_table.column("type").to_pylist() == ["WatchEvent", "PushEvent"]
    assert valid_table.schema.field("payload").type == pa.string()
    assert quarantine_table.num_rows == 2
    assert quarantine_table.column("error_reason").to_pylist()
    assert pq.ParquetFile(valid_path).metadata.row_group(0).column(0).compression == "ZSTD"


def test_gzip_line_reader_streams_decompressed_jsonl() -> None:
    content = b'{"id":"1"}\n{"id":"2"}\n'
    compressed = gzip.compress(content)
    chunks = (compressed[index : index + 3] for index in range(0, len(compressed), 3))
    downloaded: list[int] = []

    lines = list(iter_gzip_lines(chunks, downloaded.append))

    assert lines == [b'{"id":"1"}', b'{"id":"2"}']
    assert sum(downloaded) == len(compressed)
