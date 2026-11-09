with breakouts as (
    select
        breakout.repo_full_name,
        repo.language,
        breakout.dt,
        breakout.stars_today,
        breakout.stars_mean_14d,
        breakout.stars_z_score,
        breakout.is_breakout
    from mart_breakouts as breakout
    left join dim_repo as repo
        on breakout.repo_full_name = repo.repo_full_name
    where breakout.is_breakout
    order by breakout.dt desc, breakout.stars_z_score desc
    limit 25
)

select * from breakouts

union all

select
    null::varchar as repo_full_name,
    null::varchar as language,
    null::date as dt,
    null::bigint as stars_today,
    null::double as stars_mean_14d,
    null::double as stars_z_score,
    null::boolean as is_breakout
where not exists (select 1 from breakouts)
