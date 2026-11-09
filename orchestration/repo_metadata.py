import json
import os
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from io import BytesIO
from typing import Any
from urllib.parse import quote

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetExecutionContext, DailyPartitionsDefinition, MetadataValue, Output, asset
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt, wait_exponential

from orchestration.storage import LakeStorage, create_lake_storage

GITHUB_API_URL = "https://api.github.com/repos/{repo_full_name}"
GITHUB_USER_AGENT = "oss-pulse/0.1 (+https://github.com/oss-pulse; repository metadata)"
CACHE_TTL = timedelta(hours=24)
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date="2025-01-01")
METADATA_SCHEMA = pa.schema(
    [
        pa.field("repo_full_name", pa.string(), nullable=False),
        pa.field("language", pa.string()),
        pa.field("description", pa.string()),
        pa.field("topics", pa.list_(pa.string()), nullable=False),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


class RetryableGitHubError(Exception):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _wait_before_retry(retry_state: RetryCallState) -> float:
    error = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(error, RetryableGitHubError) and error.retry_after is not None:
        return error.retry_after
    return wait_exponential(multiplier=1, min=1, max=60)(retry_state)


def _is_retryable(error: BaseException) -> bool:
    if isinstance(error, (httpx.TransportError, RetryableGitHubError)):
        return True
    return isinstance(error, httpx.HTTPStatusError) and error.response.status_code >= 500


def _retry_after(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value:
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                retry_time = parsedate_to_datetime(value)
            except (TypeError, ValueError):
                retry_time = None
            if retry_time is not None:
                if retry_time.tzinfo is None:
                    retry_time = retry_time.replace(tzinfo=UTC)
                return max(0.0, (retry_time - datetime.now(UTC)).total_seconds())

    reset = response.headers.get("X-RateLimit-Reset")
    if reset and response.headers.get("X-RateLimit-Remaining") == "0":
        try:
            return max(0.0, float(reset) - datetime.now(UTC).timestamp())
        except ValueError:
            return None
    return None


def _cache_path(repo_full_name: str) -> str:
    return f"cache/github_repo_metadata/{quote(repo_full_name, safe='')}.json"


def _read_fresh_cache(
    storage: LakeStorage,
    repo_full_name: str,
    now: datetime,
) -> dict[str, Any] | None:
    try:
        cached = json.loads(storage.read_bytes(_cache_path(repo_full_name)))
    except FileNotFoundError:
        return None
    fetched_at = datetime.fromisoformat(cached["fetched_at"])
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=UTC)
    if now - fetched_at.astimezone(UTC) >= CACHE_TTL:
        return None
    return {
        "repo_full_name": repo_full_name,
        "language": cached["language"],
        "description": cached["description"],
        "topics": cached["topics"],
        "fetched_at": fetched_at.astimezone(UTC),
    }


def _write_cache(storage: LakeStorage, metadata: dict[str, Any]) -> None:
    cache_value = dict(metadata)
    cache_value["fetched_at"] = metadata["fetched_at"].isoformat()
    with storage.atomic_writer(_cache_path(metadata["repo_full_name"])) as output:
        output.write(json.dumps(cache_value, ensure_ascii=False).encode("utf-8"))


def _active_repositories(storage: LakeStorage, partition_date: date) -> list[str]:
    prefix = f"raw/gh_events/dt={partition_date.isoformat()}"
    event_counts: Counter[str] = Counter()
    for path in storage.list_files(prefix):
        if not path.endswith(".parquet"):
            continue
        table = pq.read_table(BytesIO(storage.read_bytes(path)), columns=["repo"])
        for value in table.column("repo").to_pylist():
            try:
                repo_name = json.loads(value).get("name")
            except (AttributeError, json.JSONDecodeError, TypeError):
                continue
            if isinstance(repo_name, str) and "/" in repo_name:
                event_counts[repo_name] += 1
    return [
        repo_name
        for repo_name, _ in sorted(
            event_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(5),
    wait=_wait_before_retry,
    reraise=True,
)
def _fetch_repository(client: httpx.Client, repo_full_name: str) -> httpx.Response:
    response = client.get(GITHUB_API_URL.format(repo_full_name=repo_full_name))
    if response.status_code == 429 or (
        response.status_code == 403 and response.headers.get("X-RateLimit-Remaining") == "0"
    ):
        raise RetryableGitHubError(
            f"GitHub API rate limit reached for {repo_full_name}",
            _retry_after(response),
        )
    if response.status_code == 408 or response.status_code >= 500:
        raise RetryableGitHubError(
            f"GitHub API returned HTTP {response.status_code} for {repo_full_name}",
            _retry_after(response),
        )
    response.raise_for_status()
    return response


def _metadata_rows(
    repo_names: Iterator[str],
    storage: LakeStorage,
    client: httpx.Client,
    now: datetime,
) -> list[dict[str, Any]]:
    rows = []
    for repo_full_name in repo_names:
        cached = _read_fresh_cache(storage, repo_full_name, now)
        if cached is not None:
            rows.append(cached)
            continue

        body = _fetch_repository(client, repo_full_name).json()
        metadata = {
            "repo_full_name": repo_full_name,
            "language": body.get("language"),
            "description": body.get("description"),
            "topics": body.get("topics") or [],
            "fetched_at": now,
        }
        _write_cache(storage, metadata)
        rows.append(metadata)
    return rows


def fetch_repo_metadata(
    partition_key: str,
    storage: LakeStorage,
    client: httpx.Client,
    top_n: int,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    if top_n < 1:
        raise ValueError("top_n must be at least one")
    partition_date = date.fromisoformat(partition_key)
    fetched_at = now or datetime.now(UTC)
    repos = iter(_active_repositories(storage, partition_date)[:top_n])
    return _metadata_rows(repos, storage, client, fetched_at)


@asset(partitions_def=DAILY_PARTITIONS, name="repo_metadata")
def repo_metadata(context: AssetExecutionContext) -> Output[None]:
    storage = create_lake_storage()
    top_n = int(os.getenv("REPO_METADATA_TOP_N", "50"))
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": GITHUB_USER_AGENT,
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token := os.getenv("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"

    with httpx.Client(timeout=30.0, follow_redirects=True, headers=headers) as client:
        rows = fetch_repo_metadata(context.partition_key, storage, client, top_n)

    output_path = f"raw/repo_metadata/dt={context.partition_key}/part.parquet"
    with storage.atomic_writer(output_path) as output:
        pq.write_table(
            pa.Table.from_pylist(rows, schema=METADATA_SCHEMA),
            output,
            compression="zstd",
        )

    return Output(
        value=None,
        metadata={
            "rows_in": MetadataValue.int(len(rows)),
            "rows_valid": MetadataValue.int(len(rows)),
            "rows_quarantined": MetadataValue.int(0),
            "bytes_downloaded": MetadataValue.int(0),
            "output_path": MetadataValue.path(output_path),
        },
    )
