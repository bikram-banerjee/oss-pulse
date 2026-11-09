with source as (
    select
        id,
        type,
        actor,
        repo,
        created_at,
        payload,
        dt,
        hour
    from {{ source('raw', 'gh_events') }}
)

select
    id,
    json_extract_string(repo, '$.name') as repo_full_name,
    json_extract_string(actor, '$.login') as actor_login,
    type as event_type,
    created_at,
    cast(dt as date) as dt,
    cast(hour as integer) as hour,
    json(payload) as payload
from source
