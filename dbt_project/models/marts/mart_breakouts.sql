with date_bounds as (
    select
        min(dt) as first_dt,
        max(dt) as last_dt
    from {{ ref('int_repo_daily_activity') }}
),
date_spine as (
    select generated_date::date as dt
    from date_bounds,
    lateral generate_series(first_dt, last_dt, interval '1 day') as dates(generated_date)
),
repos as (
    select distinct repo_full_name
    from {{ ref('int_repo_daily_activity') }}
),
daily_stars as (
    select
        repos.repo_full_name,
        date_spine.dt,
        coalesce(activity.stars, 0) as stars
    from repos
    cross join date_spine
    left join {{ ref('int_repo_daily_activity') }} as activity
        on repos.repo_full_name = activity.repo_full_name
        and date_spine.dt = activity.dt
),
rolling_baseline as (
    select
        repo_full_name,
        dt,
        stars,
        avg(stars) over (
            partition by repo_full_name
            order by dt
            rows between 14 preceding and 1 preceding
        ) as stars_mean_14d,
        stddev_samp(stars) over (
            partition by repo_full_name
            order by dt
            rows between 14 preceding and 1 preceding
        ) as stars_stddev_14d,
        count(*) over (
            partition by repo_full_name
            order by dt
            rows between 14 preceding and 1 preceding
        ) as baseline_days
    from daily_stars
)

select
    repo_full_name,
    dt,
    stars as stars_today,
    stars_mean_14d,
    stars_stddev_14d,
    case
        when baseline_days = 14 and stars_stddev_14d > 0
            then (stars - stars_mean_14d) / stars_stddev_14d
        else null
    end as stars_z_score,
    coalesce(
        baseline_days = 14
            and stars_stddev_14d > 0
            and (stars - stars_mean_14d) / stars_stddev_14d >= 3
            and stars >= {{ var('breakout_min_stars', 10) }},
        false
    ) as is_breakout
from rolling_baseline
