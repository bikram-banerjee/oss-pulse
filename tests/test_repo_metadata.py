from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import respx

from orchestration.repo_metadata import GITHUB_API_URL, fetch_repo_metadata
from orchestration.storage import LocalLakeStorage


@respx.mock
def test_repository_metadata_is_cached_between_fetches(tmp_path: Path) -> None:
    storage = LocalLakeStorage(tmp_path / "lake")
    event_path = "raw/gh_events/dt=2026-11-08/hour=00/part.parquet"
    event_table = pa.Table.from_pylist(
        [
            {"repo": json.dumps({"name": "octo/most-active"})},
            {"repo": json.dumps({"name": "octo/most-active"})},
            {"repo": json.dumps({"name": "octo/next"})},
        ],
        schema=pa.schema([pa.field("repo", pa.string())]),
    )
    event_bytes = BytesIO()
    pq.write_table(event_table, event_bytes, compression="zstd")
    with storage.atomic_writer(event_path) as output:
        output.write(event_bytes.getvalue())

    route = respx.get(GITHUB_API_URL.format(repo_full_name="octo/most-active")).mock(
        return_value=httpx.Response(
            200,
            json={
                "language": "Python",
                "description": "A test repository",
                "topics": ["analytics", "data"],
            },
        )
    )
    now = datetime(2026, 11, 9, tzinfo=UTC)
    with httpx.Client() as client:
        first_result = fetch_repo_metadata("2026-11-08", storage, client, top_n=1, now=now)
        second_result = fetch_repo_metadata(
            "2026-11-08",
            storage,
            client,
            top_n=1,
            now=now + timedelta(hours=1),
        )

    assert first_result == second_result
    assert first_result[0]["repo_full_name"] == "octo/most-active"
    assert first_result[0]["language"] == "Python"
    assert first_result[0]["topics"] == ["analytics", "data"]
    assert route.call_count == 1
