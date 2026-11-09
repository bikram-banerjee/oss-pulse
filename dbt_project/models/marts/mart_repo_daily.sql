select
    activity.repo_full_name,
    activity.dt,
    activity.stars,
    activity.forks,
    activity.pushes,
    activity.issues,
    activity.pull_requests,
    activity.unique_actors,
    repo.language,
    repo.description,
    repo.topics
from {{ ref('int_repo_daily_activity') }} as activity
left join {{ ref('dim_repo') }} as repo
    on activity.repo_full_name = repo.repo_full_name
