select
    object_id,
    github_repo as repo_full_name,
    title,
    url,
    author,
    points,
    num_comments,
    created_at,
    fetched_at,
    dt
from {{ ref('stg_hn_stories') }}
where github_repo is not null
