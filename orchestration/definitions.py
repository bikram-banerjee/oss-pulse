from datetime import UTC, datetime, timedelta

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    Definitions,
    MetadataValue,
    RunRequest,
    ScheduleEvaluationContext,
    asset_check,
    define_asset_job,
    schedule,
)

from orchestration.gh_archive import raw_gh_events
from orchestration.hn_algolia import raw_hn_stories
from orchestration.repo_metadata import repo_metadata

gh_archive_job = define_asset_job(
    "gh_archive_hourly",
    selection=AssetSelection.assets(raw_gh_events),
)
hn_stories_job = define_asset_job(
    "hn_stories_daily",
    selection=AssetSelection.assets(raw_hn_stories),
)
repo_metadata_job = define_asset_job(
    "repo_metadata_daily",
    selection=AssetSelection.assets(repo_metadata),
)


@schedule(job=gh_archive_job, cron_schedule="15 * * * *", execution_timezone="UTC")
def gh_archive_hourly_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scheduled_at = context.scheduled_execution_time or datetime.now(UTC)
    partition_time = (scheduled_at.astimezone(UTC) - timedelta(hours=2)).replace(
        minute=0,
        second=0,
        microsecond=0,
    )
    return RunRequest(partition_key=partition_time.strftime("%Y-%m-%d-%H"))


@schedule(job=hn_stories_job, cron_schedule="15 1 * * *", execution_timezone="UTC")
def hn_stories_daily_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scheduled_at = context.scheduled_execution_time or datetime.now(UTC)
    partition_date = scheduled_at.astimezone(UTC).date() - timedelta(days=1)
    return RunRequest(partition_key=partition_date.isoformat())


@schedule(job=repo_metadata_job, cron_schedule="15 2 * * *", execution_timezone="UTC")
def repo_metadata_daily_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scheduled_at = context.scheduled_execution_time or datetime.now(UTC)
    partition_date = scheduled_at.astimezone(UTC).date() - timedelta(days=1)
    return RunRequest(partition_key=partition_date.isoformat())


@asset_check(asset=raw_gh_events, name="latest_gh_partition_fresh", blocking=True)
def latest_gh_partition_fresh(context: AssetCheckExecutionContext) -> AssetCheckResult:
    materialized_partitions = context.instance.get_materialized_partitions(
        AssetKey("raw_gh_events")
    )
    if not materialized_partitions:
        return AssetCheckResult(
            passed=False,
            description="No GH Archive partitions have been materialized.",
        )

    latest_partition = max(materialized_partitions)
    partition_start = datetime.strptime(latest_partition, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    age = datetime.now(UTC) - (partition_start + timedelta(hours=1))
    stale = age > timedelta(hours=6)
    return AssetCheckResult(
        passed=not stale,
        metadata={
            "latest_partition": MetadataValue.text(latest_partition),
            "partition_age_minutes": MetadataValue.int(max(0, int(age.total_seconds() // 60))),
        },
        description=(
            f"Latest materialized GH partition {latest_partition} is "
            f"{'stale' if stale else 'fresh'}."
        ),
    )


defs = Definitions(
    assets=[raw_gh_events, raw_hn_stories, repo_metadata],
    asset_checks=[latest_gh_partition_fresh],
    schedules=[
        gh_archive_hourly_schedule,
        hn_stories_daily_schedule,
        repo_metadata_daily_schedule,
    ],
)
