"""Create a tiny deterministic Parquet lake from committed CI fixtures."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from orchestration.gh_archive import write_event_partition
from orchestration.hn_algolia import write_story_partition
from orchestration.repo_metadata import METADATA_SCHEMA
from orchestration.storage import LocalLakeStorage

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
PARTITION_DATE = "2025-01-01"


def _read_json_lines(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def main() -> None:
    storage = LocalLakeStorage(os.getenv("LAKE_ROOT", str(ROOT / "data" / "lake")))
    gh_lines = (line for line in (FIXTURES / "gh_events.jsonl").read_bytes().splitlines() if line)
    write_event_partition(
        gh_lines,
        storage,
        f"raw/gh_events/dt={PARTITION_DATE}/hour=00/part.parquet",
        f"quarantine/gh_events/dt={PARTITION_DATE}/hour=00/part.parquet",
    )

    stories = _read_json_lines(FIXTURES / "hn_stories.jsonl")
    write_story_partition(
        stories,
        storage,
        f"raw/hn_stories/dt={PARTITION_DATE}/part.parquet",
        f"quarantine/hn_stories/dt={PARTITION_DATE}/part.parquet",
        datetime(2025, 1, 1, 13, tzinfo=UTC),
    )

    metadata_rows = _read_json_lines(FIXTURES / "repo_metadata.jsonl")
    for row in metadata_rows:
        row["fetched_at"] = datetime.fromisoformat(row["fetched_at"].replace("Z", "+00:00"))
    metadata_table = pa.Table.from_pylist(metadata_rows, schema=METADATA_SCHEMA)
    metadata_path = (
        storage.root / "raw" / "repo_metadata" / f"dt={PARTITION_DATE}"
    )
    metadata_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(metadata_table, metadata_path / "part.parquet", compression="zstd")


if __name__ == "__main__":
    main()
