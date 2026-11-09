select
    count(*) as mentions_analyzed,
    avg(star_velocity_before_per_day) as avg_velocity_before,
    avg(star_velocity_after_per_day) as avg_velocity_after,
    avg(star_velocity_after_per_day - star_velocity_before_per_day) as avg_velocity_change,
    case
        when count(*) = 0 then 'No Hacker News mentions match GitHub repositories yet.'
        else
            'Across '
            || count(*)::varchar
            || ' repository mentions, observed star velocity changed from '
            || round(avg(star_velocity_before_per_day), 2)::varchar
            || ' to '
            || round(avg(star_velocity_after_per_day), 2)::varchar
            || ' stars/day. This is an observed association, not evidence that the mention caused the change.'
    end as takeaway
from mart_hn_lead_lag
