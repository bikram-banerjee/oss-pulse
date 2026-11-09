# OSS Pulse: project rules

Portfolio-grade data engineering project. Must run end-to-end on FREE tiers only ($0).

## Goal
Ingest GitHub public events (GH Archive hourly files) and Hacker News stories,
transform into analytics marts, detect "breakout" repos, serve a public dashboard.

## Stack (do not substitute without asking)
- Python 3.11 managed with uv
- Orchestration: Dagster (partitioned assets)
- Raw storage: Cloudflare R2 (S3-compatible), Parquet (zstd), partitioned
  dt=YYYY-MM-DD/hour=HH; local filesystem lake (./data/lake) in dev
- Compute/warehouse: DuckDB locally, MotherDuck free tier in prod
- Transformations: dbt-duckdb (staging -> intermediate -> marts), incremental
  models, tests, snapshots (SCD2)
- Quality: dbt tests + pydantic validation on ingest, quarantine path for bad rows
- Infra: Terraform (Cloudflare provider), Docker / docker-compose
- CI/CD: GitHub Actions
- Serving: Evidence.dev dashboard

## Sources
- GH Archive: https://data.gharchive.org/YYYY-MM-DD-H.json.gz (hourly gzipped NDJSON)
- Hacker News: Algolia API https://hn.algolia.com/api/v1/search_by_date

## Engineering rules
- Every pipeline step is idempotent and safely re-runnable per partition.
- No hardcoded secrets; use env vars and keep .env.example updated.
- Type hints everywhere; ruff + mypy clean; pytest for pure logic.
- Conventional commit messages, small changes.
- Makefile targets: setup, lint, test, dbt-build, dagster-dev, up, down.
- Record design decisions in docs/DECISIONS.md (short ADR style).
- After making changes, run the relevant tests/commands and report the real
  results. Never claim something works without running it.

## Layout
/orchestration, /dbt_project, /infra, /dashboard, /tests, /docs, /scripts