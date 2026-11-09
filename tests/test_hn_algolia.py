from __future__ import annotations

from pathlib import Path

import httpx
import pyarrow.parquet as pq
import respx

from orchestration.hn_algolia import ALGOLIA_URL, USER_AGENT, ingest_hn_partition
from orchestration.storage import LocalLakeStorage


@respx.mock
def test_ingest_hn_partition_paginates_and_writes_parquet(tmp_path: Path) -> None:
    page_zero = respx.get(ALGOLIA_URL).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "nbPages": 2,
                    "hits": [
                        {
                            "objectID": "story-1",
                            "title": "First story",
                            "url": "https://example.com/1",
                            "author": "alice",
                            "points": 10,
                            "num_comments": 3,
                            "created_at": "2025-01-01T00:15:00Z",
                        }
                    ],
                },
            ),
            httpx.Response(
                200,
                json={
                    "nbPages": 2,
                    "hits": [
                        {
                            "objectID": "story-2",
                            "title": "Second story",
                            "url": None,
                            "author": "bob",
                            "points": 4,
                            "num_comments": 0,
                            "created_at": "2025-01-01T23:59:59Z",
                        },
                        {"objectID": "invalid-story", "title": "Missing required fields"},
                    ],
                },
            ),
        ]
    )
    storage = LocalLakeStorage(tmp_path / "lake")
    client = httpx.Client(headers={"User-Agent": USER_AGENT})

    with client:
        counts = ingest_hn_partition("2025-01-01", storage, client)

    expected_bytes = sum(len(call.response.content) for call in page_zero.calls)
    assert counts == (3, 2, 1, expected_bytes)
    assert page_zero.call_count == 2
    for call in page_zero.calls:
        assert call.request.headers["User-Agent"] == USER_AGENT
        assert call.request.url.params["tags"] == "story"
        assert call.request.url.params["numericFilters"] == (
            "created_at_i>=1735689600,created_at_i<1735776000"
        )
        assert call.request.url.params["hitsPerPage"] == "1000"

    valid_path = tmp_path / "lake" / "raw" / "hn_stories" / "dt=2025-01-01" / "part.parquet"
    quarantine_path = (
        tmp_path
        / "lake"
        / "quarantine"
        / "hn_stories"
        / "dt=2025-01-01"
        / "part.parquet"
    )
    stories = pq.read_table(valid_path)
    quarantined = pq.read_table(quarantine_path)

    assert stories.num_rows == 2
    assert stories.column("object_id").to_pylist() == ["story-1", "story-2"]
    assert stories.column("fetched_at").to_pylist()
    assert quarantined.num_rows == 1
    assert quarantined.column("error_reason").to_pylist()[0]
    assert pq.ParquetFile(valid_path).metadata.row_group(0).column(0).compression == "ZSTD"


@respx.mock
def test_ingest_hn_partition_retries_rate_limit(tmp_path: Path) -> None:
    route = respx.get(ALGOLIA_URL).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "0"}),
            httpx.Response(200, json={"nbPages": 0, "hits": []}),
        ]
    )
    storage = LocalLakeStorage(tmp_path / "lake")
    client = httpx.Client(headers={"User-Agent": USER_AGENT})

    with client:
        counts = ingest_hn_partition("2025-01-01", storage, client)

    assert counts == (0, 0, 0, len(route.calls[1].response.content))
    assert route.call_count == 2
