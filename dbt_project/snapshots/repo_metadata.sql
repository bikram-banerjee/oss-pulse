{% snapshot repo_metadata %}

{{
    config(
        target_schema='snapshots',
        unique_key='repo_full_name',
        strategy='check',
        check_cols=['language', 'description', 'topics']
    )
}}

with latest_metadata as (
    select
        repo_full_name,
        language,
        description,
        topics,
        fetched_at,
        row_number() over (
            partition by repo_full_name
            order by fetched_at desc, dt desc
        ) as row_num
    from {{ source('raw', 'repo_metadata') }}
)

select
    repo_full_name,
    language,
    description,
    topics,
    fetched_at
from latest_metadata
where row_num = 1

{% endsnapshot %}
