{% macro ensure_pipeline_metrics_table() %}
    {% set create_table_sql %}
        create table if not exists main.pipeline_run_metrics (
            run_id varchar not null,
            asset varchar not null,
            partition varchar,
            status varchar not null,
            duration_seconds double,
            rows_in bigint,
            rows_valid bigint,
            rows_quarantined bigint,
            recorded_at timestamp with time zone not null,
            error_summary varchar
        )
    {% endset %}
    {% do run_query(create_table_sql) %}
{% endmacro %}
