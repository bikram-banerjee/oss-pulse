import os
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
import httpx
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetKey,
    DagsterEventType,
    EventRecordsFilter,
    MetadataValue,
    asset_check,
)

from orchestration.gh_archive import raw_gh_events
from orchestration.hn_algolia import raw_hn_stories
from orchestration.repo_metadata import repo_metadata

QUARANTINE_RATIO_LIMIT = 0.02
FRESHNESS_LIMIT = timedelta(hours=6)
METRICS_TABLE_SQL = """
create table if not exists main.pipeline_run_metrics (
    run_id varchar not null,
    asset varchar not null,
    partition varchar,
    status varchar not null,
    duration_seconds double,
    rows_in bigint,
    rows_valid bigint,
    rows_quarantined bigint,
    recorded_at timestamp with time zone not null,
    error_summary varchar
)
"""


def evaluate_freshness(partition_key: str, now: datetime) -> tuple[bool, float]:
    partition_start = datetime.strptime(partition_key, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    age_seconds = (now - (partition_start + timedelta(hours=1))).total_seconds()
    return age_seconds <= FRESHNESS_LIMIT.total_seconds(), age_seconds


def evaluate_nonzero_rows(rows_in: int | None) -> tuple[bool, str]:
    if rows_in is None:
        return False, "No materialization row-count metadata is available."
    if rows_in <= 0:
        return False, f"Partition contains {rows_in} input rows; at least one is required."
    return True, f"Partition contains {rows_in} input rows."


def evaluate_quarantine_ratio(
    rows_in: int | None,
    rows_quarantined: int | None,
    limit: float = QUARANTINE_RATIO_LIMIT,
) -> tuple[bool, float | None]:
    if rows_in is None or rows_quarantined is None or rows_in <= 0:
        return False, None
    ratio = rows_quarantined / rows_in
    return ratio < limit, ratio


def evaluate_reconciliation(raw_rows: int, staging_rows: int) -> tuple[bool, int]:
    difference = abs(raw_rows - staging_rows)
    return difference == 0, difference


def format_failure_alert(asset: str, partition: str | None, error_summary: str) -> dict[str, Any]:
    description = (
        f"**Partition:** `{partition or 'unpartitioned'}`\n"
        f"**Error:** {error_summary[:1500]}"
    )
    return {
        "embeds": [
            {
                "title": f"OSS Pulse pipeline failure: {asset}",
                "description": description,
                "color": 15158332,
                "timestamp": datetime.now(UTC).isoformat(),
            }
        ]
    }


def send_failure_alert(
    asset: str,
    partition: str | None,
    error_summary: str,
    webhook_url: str | None = None,
) -> bool:
    endpoint = webhook_url or os.getenv("DISCORD_WEBHOOK_URL")
    if not endpoint:
        return False
    response = httpx.post(
        endpoint,
        json=format_failure_alert(asset, partition, error_summary),
        timeout=10.0,
    )
    response.raise_for_status()
    return True


def _partition_materialization(
    context: AssetCheckExecutionContext,
    asset_key: AssetKey,
) -> Any | None:
    partition_key = context.partition_key
    if partition_key is None:
        return None
    records = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=asset_key,
            asset_partitions=[partition_key],
        ),
        limit=1,
    )
    return records[0].asset_materialization if records else None


def _metadata_int(materialization: Any | None, name: str) -> int | None:
    if materialization is None:
        return None
    value = materialization.metadata.get(name)
    return int(value.value) if value is not None else None


def _partition_quality_result(
    context: AssetCheckExecutionContext,
    asset_key: AssetKey,
    *,
    quarantined: bool = False,
) -> AssetCheckResult:
    materialization = _partition_materialization(context, asset_key)
    rows_in = _metadata_int(materialization, "rows_in")
    if quarantined:
        rows_quarantined = _metadata_int(materialization, "rows_quarantined")
        passed, ratio = evaluate_quarantine_ratio(rows_in, rows_quarantined)
        ratio_value = ratio if ratio is not None else -1.0
        message = (
            f"Quarantine ratio is {ratio:.2%}."
            if ratio is not None
            else "Cannot calculate quarantine ratio without non-zero row metadata."
        )
        return AssetCheckResult(
            passed=passed,
            metadata={
                "rows_in": MetadataValue.int(rows_in or 0),
                "rows_quarantined": MetadataValue.int(rows_quarantined or 0),
                "quarantine_ratio": MetadataValue.float(ratio_value),
                "limit": MetadataValue.float(QUARANTINE_RATIO_LIMIT),
            },
            description=message,
        )

    passed, message = evaluate_nonzero_rows(rows_in)
    return AssetCheckResult(
        passed=passed,
        metadata={"rows_in": MetadataValue.int(rows_in or 0)},
        description=message,
    )


@asset_check(asset=raw_gh_events, name="gh_partition_has_rows", blocking=True)
def gh_partition_has_rows(context: AssetCheckExecutionContext) -> AssetCheckResult:
    return _partition_quality_result(
        context,
        AssetKey("raw_gh_events"),
    )


@asset_check(asset=raw_hn_stories, name="hn_partition_has_rows", blocking=True)
def hn_partition_has_rows(context: AssetCheckExecutionContext) -> AssetCheckResult:
    return _partition_quality_result(
        context,
        AssetKey("raw_hn_stories"),
    )


@asset_check(asset=repo_metadata, name="repo_metadata_partition_has_rows", blocking=True)
def repo_metadata_partition_has_rows(context: AssetCheckExecutionContext) -> AssetCheckResult:
    return _partition_quality_result(
        context,
        AssetKey("repo_metadata"),
    )


@asset_check(asset=raw_gh_events, name="gh_quarantine_ratio_below_2pct", blocking=True)
def gh_quarantine_ratio_below_2pct(context: AssetCheckExecutionContext) -> AssetCheckResult:
    return _partition_quality_result(
        context,
        AssetKey("raw_gh_events"),
        quarantined=True,
    )


@asset_check(asset=raw_hn_stories, name="hn_quarantine_ratio_below_2pct", blocking=True)
def hn_quarantine_ratio_below_2pct(context: AssetCheckExecutionContext) -> AssetCheckResult:
    return _partition_quality_result(
        context,
        AssetKey("raw_hn_stories"),
        quarantined=True,
    )


@asset_check(asset=raw_gh_events, name="latest_gh_partition_fresh", blocking=True)
def check_latest_gh_freshness(context: AssetCheckExecutionContext) -> AssetCheckResult:
    materialized_partitions = context.instance.get_materialized_partitions(
        AssetKey("raw_gh_events")
    )
    if not materialized_partitions:
        return AssetCheckResult(
            passed=False,
            description="No GH Archive partitions have been materialized.",
        )
    latest_partition = max(materialized_partitions)
    passed, age_seconds = evaluate_freshness(latest_partition, datetime.now(UTC))
    return AssetCheckResult(
        passed=passed,
        metadata={
            "latest_partition": MetadataValue.text(latest_partition),
            "partition_age_minutes": MetadataValue.int(max(0, int(age_seconds // 60))),
        },
        description=(
            f"Latest materialized GH partition {latest_partition} is "
            f"{'fresh' if passed else 'stale'}."
        ),
    )


@asset_check(asset=raw_gh_events, name="gh_raw_staging_rows_reconcile", blocking=True)
def gh_raw_staging_rows_reconcile(context: AssetCheckExecutionContext) -> AssetCheckResult:
    partition_key = context.partition_key
    materialization = _partition_materialization(context, AssetKey("raw_gh_events"))
    raw_rows = _metadata_int(materialization, "rows_valid")
    if partition_key is None or raw_rows is None:
        return AssetCheckResult(
            passed=False,
            description="Partition key or raw rows_valid metadata is missing.",
        )

    database_path = os.getenv("DUCKDB_PATH", "data/oss_pulse.duckdb")
    if os.getenv("DBT_TARGET", "dev") == "prod":
        database_path = f"md:{os.environ['MOTHERDUCK_DATABASE']}"
        connection = duckdb.connect(
            database_path,
            config={"motherduck_token": os.environ["MOTHERDUCK_TOKEN"]},
            read_only=True,
        )
    else:
        connection = duckdb.connect(database_path, read_only=True)

    try:
        result = connection.execute(
            "select count(*) from stg_gh_events where dt = ? and hour = ?",
            [
                datetime.strptime(partition_key, "%Y-%m-%d-%H").date(),
                datetime.strptime(partition_key, "%Y-%m-%d-%H").hour,
            ],
        ).fetchone()
        if result is None:
            raise RuntimeError(f"No staging count returned for partition {partition_key}.")
        staging_rows = int(result[0])
    finally:
        connection.close()
    passed, difference = evaluate_reconciliation(raw_rows, staging_rows)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "rows_valid": MetadataValue.int(raw_rows),
            "staging_rows": MetadataValue.int(staging_rows),
            "difference": MetadataValue.int(difference),
        },
        description=(
            f"Raw valid row count {raw_rows} and staging row count {staging_rows} "
            f"{'match' if passed else 'do not match'} for {partition_key}."
        ),
    )


def initialize_pipeline_metrics_table(database_path: str | None = None) -> None:
    if database_path is None and os.getenv("DBT_TARGET", "dev") == "prod":
        database_path = f"md:{os.environ['MOTHERDUCK_DATABASE']}"
        connection = duckdb.connect(
            database_path,
            config={"motherduck_token": os.environ["MOTHERDUCK_TOKEN"]},
        )
    else:
        path = database_path or os.getenv("DUCKDB_PATH") or "data/oss_pulse.duckdb"
        connection = duckdb.connect(path)
    try:
        connection.execute(METRICS_TABLE_SQL)
    finally:
        connection.close()


def record_pipeline_metrics(
    *,
    run_id: str,
    asset: str,
    partition: str | None,
    status: str,
    duration_seconds: float | None,
    rows_in: int | None,
    rows_valid: int | None,
    rows_quarantined: int | None,
    error_summary: str | None,
    database_path: str | None = None,
) -> None:
    initialize_pipeline_metrics_table(database_path)
    if database_path is None and os.getenv("DBT_TARGET", "dev") == "prod":
        database_path = f"md:{os.environ['MOTHERDUCK_DATABASE']}"
        connection = duckdb.connect(
            database_path,
            config={"motherduck_token": os.environ["MOTHERDUCK_TOKEN"]},
        )
    else:
        path = database_path or os.getenv("DUCKDB_PATH") or "data/oss_pulse.duckdb"
        connection = duckdb.connect(path)
    try:
        connection.execute(
            """
            insert into main.pipeline_run_metrics
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                asset,
                partition,
                status,
                duration_seconds,
                rows_in,
                rows_valid,
                rows_quarantined,
                datetime.now(UTC),
                error_summary,
            ],
        )
    finally:
        connection.close()
