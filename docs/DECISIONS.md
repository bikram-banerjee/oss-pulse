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

## ADR-002: Local Docker and Terraform infrastructure

- **Status:** Accepted
- **Date:** 2026-11-09

### Context

The project needs a reproducible local Dagster runtime and an optional
Cloudflare R2 bucket without requiring cloud resources for development.

### Decision

Package Dagster with a multi-stage, uv-based Docker image and run its webserver
and daemon as separate Compose services. Persist Dagster state in a named
volume and mount the local lake from `./data`. Reserve a `streaming` Compose
profile for future services. Use Terraform with the Cloudflare provider to
create the R2 bucket; provide account and API credentials only through
environment variables.

### Consequences

- Local orchestration runs without R2 credentials or Terraform.
- Dagster state persists across container restarts, while lake files remain
  accessible on the host.
- Terraform can validate and plan without applying changes; R2 resources are
  created only after an explicit apply.

## ADR-003: Static Evidence dashboard

- **Status:** Accepted
- **Date:** 2026-11-09

### Context

The dashboard must read local DuckDB during development and MotherDuck during
production builds, while serving public pages without exposing warehouse
credentials.

### Decision

Use Evidence.dev to generate static pages and source Parquet during the build.
Select DuckDB or MotherDuck through `DBT_TARGET`; provide MotherDuck credentials
only as build-time environment variables. Keep Python DuckDB below 1.5 so its
database files remain readable by Evidence's current DuckDB connector.

### Consequences

- Cloudflare Pages serves static assets and does not need warehouse credentials.
- Dashboard source queries are evaluated at build time and must be rebuilt to
  reflect refreshed warehouse data.
- Databases created by DuckDB 2.x must be rebuilt from the Parquet lake before
  use with the current Evidence DuckDB connector.
