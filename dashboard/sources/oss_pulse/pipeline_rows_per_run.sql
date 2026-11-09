with recent_runs as (
    select
        recorded_at,
        asset,
        "partition" as partition_key,
        status,
        rows_in,
        rows_valid,
        rows_quarantined,
        duration_seconds
    from mart_pipeline_health
    order by recorded_at desc
    limit 20
)

select * from recent_runs

union all

select
    null::timestamp as recorded_at,
    null::varchar as asset,
    null::varchar as partition_key,
    null::varchar as status,
    null::bigint as rows_in,
    null::bigint as rows_valid,
    null::bigint as rows_quarantined,
    null::double as duration_seconds
where not exists (select 1 from recent_runs)
