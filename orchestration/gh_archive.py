import json
import zlib
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    AssetExecutionContext,
    HourlyPartitionsDefinition,
    MetadataValue,
    Output,
    asset,
)
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from orchestration.storage import LakeStorage, create_lake_storage

GH_ARCHIVE_URL = "https://data.gharchive.org/{date}-{hour}.json.gz"
EVENT_TYPES = frozenset(
    {
        "WatchEvent",
        "ForkEvent",
        "PushEvent",
        "IssuesEvent",
        "PullRequestEvent",
        "ReleaseEvent",
    }
)
HOURLY_PARTITIONS = HourlyPartitionsDefinition(
    start_date="2025-01-01-00",
    fmt="%Y-%m-%d-%H",
)

VALID_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("type", pa.string(), nullable=False),
        pa.field("actor", pa.string(), nullable=False),
        pa.field("repo", pa.string(), nullable=False),
        pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("payload", pa.string(), nullable=False),
    ]
)
QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("raw_json", pa.string(), nullable=False),
        pa.field("error_reason", pa.string(), nullable=False),
    ]
)


class GitHubEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    type: str
    actor: dict[str, Any]
    repo: dict[str, Any]
    created_at: datetime
    payload: str

    @model_validator(mode="before")
    @classmethod
    def serialize_payload(cls, value: Any) -> Any:
        if not isinstance(value, dict) or "payload" not in value:
            return value
        normalized = value.copy()
        if not isinstance(normalized["payload"], str):
            normalized["payload"] = json.dumps(
                normalized["payload"],
                separators=(",", ":"),
                ensure_ascii=False,
            )
        return normalized

    @field_validator("created_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must include a timezone")
        return value.astimezone(UTC)


class RetryableHTTPStatusError(Exception):
    pass


def is_retryable_http_error(error: BaseException) -> bool:
    if isinstance(error, (httpx.TransportError, RetryableHTTPStatusError)):
        return True
    return isinstance(error, httpx.HTTPStatusError) and (
        error.response.status_code in {408, 429} or error.response.status_code >= 500
    )


def iter_gzip_lines(chunks: Iterable[bytes], on_bytes: Any) -> Iterator[bytes]:
    decompressor = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
    pending = b""
    for chunk in chunks:
        on_bytes(len(chunk))
        pending += decompressor.decompress(chunk)
        while b"\n" in pending:
            line, pending = pending.split(b"\n", 1)
            if line.strip():
                yield line.rstrip(b"\r")
    pending += decompressor.flush()
    if not decompressor.eof:
        raise OSError("GH Archive gzip stream ended before its trailer")
    if pending.strip():
        yield pending.rstrip(b"\r")


def _arrow_event(event: GitHubEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "type": event.type,
        "actor": json.dumps(event.actor, separators=(",", ":"), ensure_ascii=False),
        "repo": json.dumps(event.repo, separators=(",", ":"), ensure_ascii=False),
        "created_at": event.created_at,
        "payload": event.payload,
    }


def write_event_partition(
    lines: Iterable[bytes],
    storage: LakeStorage,
    valid_path: str,
    quarantine_path: str,
) -> tuple[int, int, int]:
    rows_in = rows_valid = rows_quarantined = 0
    with storage.atomic_writer(valid_path) as valid_output:
        with storage.atomic_writer(quarantine_path) as quarantine_output:
            with (
                pq.ParquetWriter(valid_output, VALID_SCHEMA, compression="zstd") as valid_writer,
                pq.ParquetWriter(
                    quarantine_output,
                    QUARANTINE_SCHEMA,
                    compression="zstd",
                ) as quarantine_writer,
            ):
                valid_batch: list[dict[str, Any]] = []
                quarantine_batch: list[dict[str, str]] = []
                for line in lines:
                    rows_in += 1
                    try:
                        raw_json = line.decode("utf-8")
                        event_data = json.loads(raw_json)
                        if (
                            isinstance(event_data, dict)
                            and event_data.get("type") not in EVENT_TYPES
                        ):
                            continue
                        event = GitHubEvent.model_validate(event_data)
                    except (
                        UnicodeDecodeError,
                        json.JSONDecodeError,
                        ValidationError,
                        ValueError,
                    ) as error:
                        quarantine_batch.append(
                            {
                                "raw_json": line.decode("utf-8", errors="replace"),
                                "error_reason": str(error),
                            }
                        )
                        rows_quarantined += 1
                    else:
                        valid_batch.append(_arrow_event(event))
                        rows_valid += 1

                    if len(valid_batch) >= 1_000:
                        valid_writer.write_table(
                            pa.Table.from_pylist(valid_batch, schema=VALID_SCHEMA)
                        )
                        valid_batch.clear()
                    if len(quarantine_batch) >= 1_000:
                        quarantine_writer.write_table(
                            pa.Table.from_pylist(quarantine_batch, schema=QUARANTINE_SCHEMA)
                        )
                        quarantine_batch.clear()

                if valid_batch:
                    valid_writer.write_table(pa.Table.from_pylist(valid_batch, schema=VALID_SCHEMA))
                if quarantine_batch:
                    quarantine_writer.write_table(
                        pa.Table.from_pylist(quarantine_batch, schema=QUARANTINE_SCHEMA)
                    )
    return rows_in, rows_valid, rows_quarantined


def _partition_paths(partition_key: str) -> tuple[str, str]:
    partition_time = datetime.strptime(partition_key, "%Y-%m-%d-%H")
    partition = f"dt={partition_time:%Y-%m-%d}/hour={partition_time:%H}"
    return (
        f"raw/gh_events/{partition}/part.parquet",
        f"quarantine/gh_events/{partition}/part.parquet",
    )


@retry(
    retry=retry_if_exception(is_retryable_http_error),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    reraise=True,
)
def _ingest_partition(partition_key: str, storage: LakeStorage) -> tuple[int, int, int, int]:
    partition_time = datetime.strptime(partition_key, "%Y-%m-%d-%H")
    url = GH_ARCHIVE_URL.format(
        date=partition_time.strftime("%Y-%m-%d"),
        hour=str(partition_time.hour),
    )
    valid_path, quarantine_path = _partition_paths(partition_key)
    bytes_downloaded = 0

    def count_bytes(chunk_length: int) -> None:
        nonlocal bytes_downloaded
        bytes_downloaded += chunk_length

    def lines_from_response() -> Iterator[bytes]:
        with httpx.Client(timeout=120.0, follow_redirects=True) as client:
            with client.stream("GET", url) as response:
                if response.status_code == 404:
                    return
                if response.status_code in {408, 429} or response.status_code >= 500:
                    raise RetryableHTTPStatusError(
                        f"GH Archive returned retryable HTTP {response.status_code}"
                    )
                response.raise_for_status()
                yield from iter_gzip_lines(response.iter_raw(), count_bytes)

    rows_in, rows_valid, rows_quarantined = write_event_partition(
        lines_from_response(),
        storage,
        valid_path,
        quarantine_path,
    )
    return rows_in, rows_valid, rows_quarantined, bytes_downloaded


@asset(partitions_def=HOURLY_PARTITIONS, name="raw_gh_events")
def raw_gh_events(context: AssetExecutionContext) -> Output[None]:
    storage = create_lake_storage()
    rows_in, rows_valid, rows_quarantined, bytes_downloaded = _ingest_partition(
        context.partition_key,
        storage,
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
