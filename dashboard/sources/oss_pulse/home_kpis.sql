select
    (
        select count(*)
        from stg_gh_events
        where created_at >= current_timestamp - interval '7 days'
    ) as events_last_7_days,
    (
        select count(distinct repo_full_name)
        from dim_repo
    ) as repos_tracked,
    (
        select count(*)
        from mart_breakouts
        where dt = current_date
            and is_breakout
    ) as breakouts_today
