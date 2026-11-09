---
title: Pipeline Health
---

# Pipeline health

Operational signals recorded by Dagster. A missing last-run timestamp means
the status sensors have not yet recorded an execution in this warehouse.

```sql data_updated
select last_updated from oss_pulse.dashboard_updated
```

```sql pipeline_summary
select * from oss_pulse.pipeline_health_summary
```

### Ingestion freshness

```sql freshness_by_asset
select * from oss_pulse.pipeline_freshness
where last_run_at is not null
```

{#if freshness_by_asset.length > 0}
    <DataTable data={freshness_by_asset} />
{:else}
No pipeline runs have been recorded for the ingestion assets yet.
{/if}

<Grid cols=3>
    <BigValue data={pipeline_summary} value=hours_since_last_run title="Hours since last run" fmt=num1 />
    <BigValue data={pipeline_summary} value=rows_valid_last_7_days title="Valid rows (7 days)" fmt=num0 />
    <BigValue data={pipeline_summary} value=quarantine_ratio_last_7_days title="Quarantine ratio (7 days)" fmt=pct1 />
</Grid>

## Latest runs

```sql recent_runs
select * from oss_pulse.pipeline_rows_per_run
where recorded_at is not null
```

{#if recent_runs.length > 0}
    <DataTable data={recent_runs} />
{:else}
No pipeline runs have been recorded yet.
{/if}

---

Data last updated: <Value data={data_updated} column=last_updated fmt="yyyy-mm-dd hh:mm" />
