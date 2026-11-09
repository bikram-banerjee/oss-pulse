with repository_velocity as (
    select
        repo_full_name,
        count(*) as hn_mentions,
        avg(star_velocity_before_per_day) as avg_stars_per_day_before,
        avg(star_velocity_after_per_day) as avg_stars_per_day_after
    from mart_hn_lead_lag
    group by repo_full_name
    order by hn_mentions desc
    limit 25
),
hn_effect as (
    select
        repo_full_name,
        hn_mentions,
        'Before' as period,
        avg_stars_per_day_before as avg_stars_per_day
    from repository_velocity

    union all

    select
        repo_full_name,
        hn_mentions,
        'After' as period,
        avg_stars_per_day_after as avg_stars_per_day
    from repository_velocity
)

select * from hn_effect

union all

select
    null::varchar as repo_full_name,
    null::bigint as hn_mentions,
    null::varchar as period,
    null::double as avg_stars_per_day
where not exists (select 1 from hn_effect)
