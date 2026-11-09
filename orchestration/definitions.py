from datetime import UTC, datetime, timedelta

from dagster import (
    AssetSelection,
    Definitions,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

from orchestration.gh_archive import raw_gh_events
from orchestration.hn_algolia import raw_hn_stories
from orchestration.observability import (
    pipeline_metrics_failure_sensor,
    pipeline_metrics_success_sensor,
)
from orchestration.quality import (
    check_latest_gh_freshness,
    gh_partition_has_rows,
    gh_quarantine_ratio_below_2pct,
    gh_raw_staging_rows_reconcile,
    hn_partition_has_rows,
    hn_quarantine_ratio_below_2pct,
    repo_metadata_partition_has_rows,
)
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


defs = Definitions(
    assets=[raw_gh_events, raw_hn_stories, repo_metadata],
    asset_checks=[
        check_latest_gh_freshness,
        gh_partition_has_rows,
        hn_partition_has_rows,
        repo_metadata_partition_has_rows,
        gh_quarantine_ratio_below_2pct,
        hn_quarantine_ratio_below_2pct,
        gh_raw_staging_rows_reconcile,
    ],
    schedules=[
        gh_archive_hourly_schedule,
        hn_stories_daily_schedule,
        repo_metadata_daily_schedule,
    ],
    sensors=[pipeline_metrics_success_sensor, pipeline_metrics_failure_sensor],
)
