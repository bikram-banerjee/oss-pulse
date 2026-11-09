select
    run_id,
    asset,
    partition,
    status,
    duration_seconds,
    rows_in,
    rows_valid,
    rows_quarantined,
    recorded_at,
    error_summary
from {{ source('operations', 'pipeline_run_metrics') }}
