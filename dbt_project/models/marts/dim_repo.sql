with known_repositories as (
    select repo_full_name from {{ ref('int_repo_daily_activity') }}
    union
    select repo_full_name from {{ ref('int_hn_repo_mentions') }}
    union
    select repo_full_name from {{ source('raw', 'repo_metadata') }}
),
current_metadata as (
    select
        repo_full_name,
        language,
        description,
        topics,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('repo_metadata') }}
    where dbt_valid_to is null
),
activity_bounds as (
    select
        repo_full_name,
        min(dt) as first_activity_dt,
        max(dt) as last_activity_dt
    from {{ ref('int_repo_daily_activity') }}
    group by repo_full_name
)

select
    repositories.repo_full_name,
    metadata.language,
    metadata.description,
    metadata.topics,
    metadata.dbt_valid_from as metadata_valid_from,
    metadata.dbt_valid_to as metadata_valid_to,
    activity.first_activity_dt,
    activity.last_activity_dt
from known_repositories as repositories
left join current_metadata as metadata
    on repositories.repo_full_name = metadata.repo_full_name
left join activity_bounds as activity
    on repositories.repo_full_name = activity.repo_full_name
