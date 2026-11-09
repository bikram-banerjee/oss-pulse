with assets as (
    select * from (
        values
            ('raw_gh_events'),
            ('raw_hn_stories')
    ) as ingestion_assets(asset)
),
latest_runs as (
    select
        asset,
        max(recorded_at) as last_run_at
    from mart_pipeline_health
    group by asset
)

select
    assets.asset,
    latest_runs.last_run_at,
    date_diff('hour', latest_runs.last_run_at, current_timestamp) as hours_since_last_run
from assets
left join latest_runs using (asset)
order by assets.asset
