with source as (
    select
        object_id,
        title,
        url,
        author,
        points,
        num_comments,
        created_at,
        fetched_at,
        dt
    from {{ source('raw', 'hn_stories') }}
),
normalized as (
    select
        *,
        regexp_extract(
            url,
            '^https?://github[.]com/([^/?#]+)/([^/?#]+)',
            1
        ) as github_owner,
        regexp_extract(
            url,
            '^https?://github[.]com/([^/?#]+)/([^/?#]+)',
            2
        ) as github_repository
    from source
)

select
    object_id,
    title,
    url,
    author,
    points,
    num_comments,
    created_at,
    fetched_at,
    cast(dt as date) as dt,
    case
        when github_owner <> '' and github_repository <> ''
            then github_owner || '/' || regexp_replace(github_repository, '[.]git$', '')
        else null
    end as github_repo
from normalized
