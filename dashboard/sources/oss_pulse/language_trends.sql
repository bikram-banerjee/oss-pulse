with top_languages as (
    select language
    from mart_language_trends
    where week_start >= current_date - interval '26 weeks'
    group by language
    order by sum(stars) desc
    limit 8
),
trends as (
    select trends.week_start, trends.language, trends.stars
    from mart_language_trends as trends
    inner join top_languages using (language)
    where trends.week_start >= current_date - interval '26 weeks'
)

select * from trends

union all

select
    null::date as week_start,
    null::varchar as language,
    null::bigint as stars
where not exists (select 1 from trends)
