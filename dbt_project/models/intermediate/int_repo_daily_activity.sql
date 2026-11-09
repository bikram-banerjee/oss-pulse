{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['repo_full_name', 'dt'],
    on_schema_change='fail'
) }}

with events as (
    select
        repo_full_name,
        dt,
        actor_login,
        event_type
    from {{ ref('stg_gh_events') }}
    {% if is_incremental() %}
        where dt >= (
            select coalesce(max(dt) - interval '2 days', date '1900-01-01')
            from {{ this }}
        )
    {% endif %}
)

select
    repo_full_name,
    dt,
    count_if(event_type = 'WatchEvent') as stars,
    count_if(event_type = 'ForkEvent') as forks,
    count_if(event_type = 'PushEvent') as pushes,
    count_if(event_type = 'IssuesEvent') as issues,
    count_if(event_type = 'PullRequestEvent') as pull_requests,
    count(distinct actor_login) as unique_actors
from events
group by repo_full_name, dt
