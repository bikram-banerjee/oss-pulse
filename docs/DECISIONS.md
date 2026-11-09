# Architecture Decision Records

## ADR-001: Free-tier-first analytics stack

- **Status:** Accepted
- **Date:** 2026-11-09

### Context

OSS Pulse needs to ingest public GitHub and Hacker News activity, retain
replayable raw data, transform it into analytics marts, and serve a dashboard.
The project must run end-to-end on free tiers only.

### Decision

Use Python 3.11 managed by uv and Dagster for partitioned orchestration. Store
compressed Parquet in a local development lake or Cloudflare R2 in production.
Use DuckDB locally and MotherDuck's free tier for hosted analytics. Use
dbt-duckdb for staging, intermediate, and mart transformations, and Evidence.dev
for dashboard delivery. Validate ingested records with Pydantic and cover
transforms with dbt tests.

### Consequences

- Local development can run without cloud credentials or paid services.
- Parquet partitions provide a portable, replayable raw-data boundary.
- R2 and MotherDuck credentials remain environment-provided and are not
  committed.
- Orchestration and transformation tooling can be exercised independently.
