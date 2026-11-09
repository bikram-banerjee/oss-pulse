# Operations runbook

## Backfill a date range

Run Dagster backfills in UTC. The GH Archive asset uses hourly partitions; the
Hacker News and repository metadata assets use daily partitions. For example:

```sh
uv run dagster asset backfill -m orchestration.definitions \
  --select raw_gh_events --from 2025-01-01-00 --to 2025-01-02-23
uv run dagster asset backfill -m orchestration.definitions \
  --select raw_hn_stories --from 2025-01-01 --to 2025-01-02
```

Backfills can be retried safely: each asset replaces only the selected
partition. GH Archive hours that have not yet been published return no data;
retry the partition after the two-hour publication lag.

## Reprocess a quarantined partition

Inspect the matching file under
`data/lake/quarantine/gh_events/` or
`data/lake/quarantine/hn_stories/`, correct the source issue or validation
logic, then materialize the original partition again. A successful rerun
overwrites both that partition's valid Parquet file and its quarantine file;
unrelated partitions are not changed. For R2, the same relative paths are
under the configured bucket.

## Alerts and quality checks

- **Latest GH partition stale**: the newest materialized GH Archive hour is
  more than six hours old. Check the Dagster schedule and GH Archive
  availability, then backfill missing hours.
- **Partition has no rows**: ingestion produced no input rows. Confirm the
  source file/API response and partition date before retrying.
- **Quarantine ratio at or above 2%**: inspect quarantined records and error
  reasons; correct data or validation issues before trusting downstream
  aggregates.
- **Raw/staging reconciliation failed**: raw `rows_valid` differs from the
  staged row count for the same GH Archive date and hour. Check the Parquet
  source path, DuckDB external source configuration, and dbt staging model.
- **Discord pipeline failure**: inspect the Dagster run log using the asset
  and partition in the message. Failure alerts are not sent when
  `DISCORD_WEBHOOK_URL` is unset; the run remains visible in Dagster and
  `mart_pipeline_health`.

The `pipeline_metrics_success_sensor` and `pipeline_metrics_failure_sensor`
must be running in Dagster for metrics to be recorded in
`main.pipeline_run_metrics`, which feeds `mart_pipeline_health`.
