select max(updated_at) as last_updated
from (
    select max(recorded_at) as updated_at
    from mart_pipeline_health

    union all

    select max(dt)::timestamp as updated_at
    from mart_repo_daily

    union all

    select max(dt)::timestamp as updated_at
    from mart_hn_lead_lag
)
