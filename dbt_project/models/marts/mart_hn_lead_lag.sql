with mentions as (
    select
        object_id,
        repo_full_name,
        title,
        url,
        author,
        points,
        num_comments,
        created_at as mention_created_at,
        dt
    from {{ ref('int_hn_repo_mentions') }}
)

select
    mentions.object_id,
    mentions.repo_full_name,
    mentions.title,
    mentions.url,
    mentions.author,
    mentions.points,
    mentions.num_comments,
    mentions.mention_created_at,
    mentions.dt,
    count(*) filter (
        where events.event_type = 'WatchEvent'
            and events.created_at >= mentions.mention_created_at - interval '48 hours'
            and events.created_at < mentions.mention_created_at
    ) as stars_48h_before,
    count(*) filter (
        where events.event_type = 'WatchEvent'
            and events.created_at >= mentions.mention_created_at
            and events.created_at < mentions.mention_created_at + interval '48 hours'
    ) as stars_48h_after,
    count(*) filter (
        where events.event_type = 'WatchEvent'
            and events.created_at >= mentions.mention_created_at - interval '48 hours'
            and events.created_at < mentions.mention_created_at
    ) / 2.0 as star_velocity_before_per_day,
    count(*) filter (
        where events.event_type = 'WatchEvent'
            and events.created_at >= mentions.mention_created_at
            and events.created_at < mentions.mention_created_at + interval '48 hours'
    ) / 2.0 as star_velocity_after_per_day
from mentions
left join {{ ref('stg_gh_events') }} as events
    on mentions.repo_full_name = events.repo_full_name
group by
    mentions.object_id,
    mentions.repo_full_name,
    mentions.title,
    mentions.url,
    mentions.author,
    mentions.points,
    mentions.num_comments,
    mentions.mention_created_at,
    mentions.dt
