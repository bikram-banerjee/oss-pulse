---
title: About
---

# About OSS Pulse

OSS Pulse turns public GitHub and Hacker News activity into daily repository
analytics and breakout signals.

## Architecture

<pre>
GH Archive ---------\
                     > Dagster -> Parquet lake -> DuckDB/MotherDuck -> dbt -> Evidence dashboard
Hacker News API ----/
</pre>

## Stack

- Python 3.11 and uv for ingestion tooling.
- Dagster assets, partitions, schedules, checks, and run observability.
- Cloudflare R2 for production Parquet storage; local filesystem for development.
- DuckDB for local analytics and MotherDuck for the hosted warehouse.
- dbt-duckdb for documented, tested transformations.
- Evidence.dev for a static, mobile-friendly dashboard.

## Design tradeoffs

- Parquet partitions remain replayable and portable, while the development
  setup runs without cloud credentials.
- The production dashboard build queries MotherDuck and exports only the
  aggregates it needs to a static site; no database credentials are sent to
  dashboard visitors.
- Breakout signals require a full 14-day baseline. Early or sparse data may
  correctly produce no breakouts.
- HN timing comparisons show correlation around a mention, not causation.

[View the OSS Pulse repository](https://github.com/bikram-banerjee/oss-pulse).

---

```sql data_updated
select last_updated from oss_pulse.dashboard_updated
```

Data last updated: <Value data={data_updated} column=last_updated fmt="yyyy-mm-dd hh:mm" />
