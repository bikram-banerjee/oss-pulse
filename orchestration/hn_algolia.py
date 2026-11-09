import json
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime, time, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetExecutionContext, DailyPartitionsDefinition, MetadataValue, Output, asset
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt, wait_exponential

from orchestration.storage import LakeStorage, create_lake_storage

ALGOLIA_URL = "https://hn.algolia.com/api/v1/search_by_date"
USER_AGENT = "oss-pulse/0.1 (+https://github.com/oss-pulse; public-data ingestion)"
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2025-01-01")
PAGE_SIZE = 1_000

STORY_SCHEMA = pa.schema(
    [
        pa.field("object_id", pa.string(), nullable=False),
        pa.field("title", pa.string()),
        pa.field("url", pa.string()),
        pa.field("author", pa.string(), nullable=False),
        pa.field("points", pa.int64(), nullable=False),
        pa.field("num_comments", pa.int64(), nullable=False),
        pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)
QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("raw_json", pa.string(), nullable=False),
        pa.field("error_reason", pa.string(), nullable=False),
    ]
)


class HackerNewsStory(BaseModel):
    model_config = ConfigDict(extra="ignore")

    object_id: str
    title: str | None = None
    url: str | None = None
    author: str
    points: int
    num_comments: int
    created_at: datetime
    fetched_at: datetime

    @field_validator("created_at", "fetched_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include a timezone")
        return value.astimezone(UTC)


class RetryableAlgoliaError(Exception):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


_exponential_wait = wait_exponential(multiplier=1, min=1, max=30)


def _wait_before_retry(retry_state: RetryCallState) -> float:
    error = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(error, RetryableAlgoliaError) and error.retry_after is not None:
        return error.retry_after
    return _exponential_wait(retry_state)


def _is_retryable(error: BaseException) -> bool:
    if isinstance(error, (httpx.TransportError, RetryableAlgoliaError)):
        return True
    return isinstance(error, httpx.HTTPStatusError) and (
        error.response.status_code in {408, 429} or error.response.status_code >= 500
    )


def _story_input(hit: Any, fetched_at: datetime) -> dict[str, Any]:
    if not isinstance(hit, dict):
        raise ValueError("Algolia hit must be a JSON object")
    return {
        "object_id": hit.get("objectID"),
        "title": hit.get("title"),
        "url": hit.get("url"),
        "author": hit.get("author"),
        "points": hit.get("points"),
        "num_comments": hit.get("num_comments"),
        "created_at": hit.get("created_at"),
        "fetched_at": fetched_at,
    }


def write_story_partition(
    hits: Iterable[Any],
    storage: LakeStorage,
    valid_path: str,
    quarantine_path: str,
    fetched_at: datetime,
) -> tuple[int, int, int]:
    rows_in = rows_valid = rows_quarantined = 0
    with storage.atomic_writer(valid_path) as valid_output:
        with storage.atomic_writer(quarantine_path) as quarantine_output:
            with (
                pq.ParquetWriter(valid_output, STORY_SCHEMA, compression="zstd") as valid_writer,
                pq.ParquetWriter(
                    quarantine_output,
                    QUARANTINE_SCHEMA,
                    compression="zstd",
                ) as quarantine_writer,
            ):
                valid_batch: list[dict[str, Any]] = []
                quarantine_batch: list[dict[str, str]] = []
                for hit in hits:
                    rows_in += 1
                    try:
                        story = HackerNewsStory.model_validate(_story_input(hit, fetched_at))
                    except (ValidationError, ValueError, TypeError, AttributeError) as error:
                        quarantine_batch.append(
                            {
                                "raw_json": json.dumps(
                                    hit,
                                    separators=(",", ":"),
                                    ensure_ascii=False,
                                ),
                                "error_reason": str(error),
                            }
                        )
                        rows_quarantined += 1
                    else:
                        valid_batch.append(story.model_dump())
                        rows_valid += 1

                    if len(valid_batch) >= PAGE_SIZE:
                        valid_writer.write_table(
                            pa.Table.from_pylist(valid_batch, schema=STORY_SCHEMA)
                        )
                        valid_batch.clear()
                    if len(quarantine_batch) >= PAGE_SIZE:
                        quarantine_writer.write_table(
                            pa.Table.from_pylist(quarantine_batch, schema=QUARANTINE_SCHEMA)
                        )
                        quarantine_batch.clear()

                if valid_batch:
                    valid_writer.write_table(pa.Table.from_pylist(valid_batch, schema=STORY_SCHEMA))
                if quarantine_batch:
                    quarantine_writer.write_table(
                        pa.Table.from_pylist(quarantine_batch, schema=QUARANTINE_SCHEMA)
                    )
    return rows_in, rows_valid, rows_quarantined


def _get_hits(
    client: httpx.Client,
    partition_date: date,
    bytes_counter: list[int],
) -> Iterator[dict[str, Any]]:
    start = int(datetime.combine(partition_date, time.min, tzinfo=UTC).timestamp())
    end = int(
        (datetime.combine(partition_date, time.min, tzinfo=UTC) + timedelta(days=1)).timestamp()
    )
    page = 0
    total_pages: int | None = None

    while total_pages is None or page < total_pages:
        response = client.get(
            ALGOLIA_URL,
            params={
                "tags": "story",
                "numericFilters": f"created_at_i>={start},created_at_i<{end}",
                "page": page,
                "hitsPerPage": PAGE_SIZE,
            },
        )
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            detail = "Algolia rate limited the request"
            retry_delay: float | None = None
            if retry_after:
                detail += f" (Retry-After: {retry_after})"
                try:
                    retry_delay = max(0.0, float(retry_after))
                except ValueError:
                    try:
                        retry_time = parsedate_to_datetime(retry_after)
                    except (TypeError, ValueError):
                        retry_time = None
                    if retry_time is not None:
                        if retry_time.tzinfo is None:
                            retry_time = retry_time.replace(tzinfo=UTC)
                        retry_delay = max(
                            0.0,
                            (retry_time - datetime.now(UTC)).total_seconds(),
                        )
            raise RetryableAlgoliaError(detail, retry_delay)
        if response.status_code in {408} or response.status_code >= 500:
            raise RetryableAlgoliaError(f"Algolia returned HTTP {response.status_code}")
        response.raise_for_status()
        bytes_counter[0] += len(response.content)
        body = response.json()
        hits = body["hits"]
        total_pages = body["nbPages"]
        yield from hits
        page += 1


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(5),
    wait=_wait_before_retry,
    reraise=True,
)
def ingest_hn_partition(
    partition_key: str,
    storage: LakeStorage,
    client: httpx.Client,
) -> tuple[int, int, int, int]:
    partition_date = date.fromisoformat(partition_key)
    fetched_at = datetime.now(UTC)
    byte_count = [0]
    valid_path = f"raw/hn_stories/dt={partition_key}/part.parquet"
    quarantine_path = f"quarantine/hn_stories/dt={partition_key}/part.parquet"
    rows_in, rows_valid, rows_quarantined = write_story_partition(
        _get_hits(client, partition_date, byte_count),
        storage,
        valid_path,
        quarantine_path,
        fetched_at,
    )
    return rows_in, rows_valid, rows_quarantined, byte_count[0]


@asset(partitions_def=DAILY_PARTITIONS, name="raw_hn_stories")
def raw_hn_stories(context: AssetExecutionContext) -> Output[None]:
    storage = create_lake_storage()
    with httpx.Client(
        timeout=30.0,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    ) as client:
        rows_in, rows_valid, rows_quarantined, bytes_downloaded = ingest_hn_partition(
            context.partition_key,
            storage,
            client,
        )
    return Output(
        value=None,
        metadata={
            "rows_in": MetadataValue.int(rows_in),
            "rows_valid": MetadataValue.int(rows_valid),
            "rows_quarantined": MetadataValue.int(rows_quarantined),
            "bytes_downloaded": MetadataValue.int(bytes_downloaded),
        },
    )
