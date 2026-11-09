select
    max(recorded_at) as last_run_at,
    date_diff('minute', max(recorded_at), current_timestamp) / 60.0 as hours_since_last_run,
    count(*) filter (
        where recorded_at >= current_timestamp - interval '7 days'
    ) as runs_last_7_days,
    sum(rows_valid) filter (
        where recorded_at >= current_timestamp - interval '7 days'
    ) as rows_valid_last_7_days,
    sum(rows_quarantined) filter (
        where recorded_at >= current_timestamp - interval '7 days'
    ) / nullif(
        sum(rows_in) filter (
            where recorded_at >= current_timestamp - interval '7 days'
        ),
        0
    ) as quarantine_ratio_last_7_days
from mart_pipeline_health
