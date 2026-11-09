select *
from {{ ref('mart_pipeline_health') }}
where duration_seconds < 0
    or rows_in < 0
    or rows_valid < 0
    or rows_quarantined < 0
