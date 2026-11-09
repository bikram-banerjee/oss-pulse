select
    date_trunc('week', dt)::date as week_start,
    language,
    sum(stars) as stars
from {{ ref('mart_repo_daily') }}
where language is not null
group by week_start, language
