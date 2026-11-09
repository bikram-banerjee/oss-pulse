select
    repo_full_name,
    dt,
    count(*) as row_count
from {{ ref('mart_repo_daily') }}
group by repo_full_name, dt
having count(*) > 1
