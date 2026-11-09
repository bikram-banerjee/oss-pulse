with staged as (
    select
        repo_full_name,
        dt,
        count(*) as stars
    from {{ ref('stg_gh_events') }}
    where event_type = 'WatchEvent'
    group by repo_full_name, dt
),
mart as (
    select repo_full_name, dt, stars
    from {{ ref('mart_repo_daily') }}
),
comparison as (
    select
        coalesce(staged.repo_full_name, mart.repo_full_name) as repo_full_name,
        coalesce(staged.dt, mart.dt) as dt,
        coalesce(staged.stars, 0) as staged_stars,
        coalesce(mart.stars, 0) as mart_stars
    from staged
    full outer join mart
        on staged.repo_full_name = mart.repo_full_name
        and staged.dt = mart.dt
)

select *
from comparison
where abs(staged_stars - mart_stars) > {{ var('stars_reconcile_tolerance', 0) }}
