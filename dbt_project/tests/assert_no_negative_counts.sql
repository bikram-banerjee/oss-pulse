with counts as (
    select 'mart_repo_daily' as model_name, stars as event_count
    from {{ ref('mart_repo_daily') }}
    union all
    select 'mart_repo_daily', forks from {{ ref('mart_repo_daily') }}
    union all
    select 'mart_repo_daily', pushes from {{ ref('mart_repo_daily') }}
    union all
    select 'mart_repo_daily', issues from {{ ref('mart_repo_daily') }}
    union all
    select 'mart_repo_daily', pull_requests from {{ ref('mart_repo_daily') }}
    union all
    select 'mart_repo_daily', unique_actors from {{ ref('mart_repo_daily') }}
    union all
    select 'mart_language_trends', stars from {{ ref('mart_language_trends') }}
    union all
    select 'mart_hn_lead_lag', stars_48h_before from {{ ref('mart_hn_lead_lag') }}
    union all
    select 'mart_hn_lead_lag', stars_48h_after from {{ ref('mart_hn_lead_lag') }}
)

select *
from counts
where event_count < 0
