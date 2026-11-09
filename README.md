# OSS Pulse

OSS Pulse analyzes open-source project activity to identify emerging repositories.

## Data sources

- [GH Archive](https://www.gharchive.org/) public GitHub events
- [Hacker News Algolia API](https://hn.algolia.com/api)

## Stack

Python 3.11 and uv, Dagster, a Parquet lake on local storage or Cloudflare R2,
DuckDB/MotherDuck, dbt-duckdb, and an Evidence.dev dashboard.

## Getting started

Install Python 3.11 and [uv](https://docs.astral.sh/uv/), then run:

```sh
make setup
```

Copy `.env.example` to `.env` and configure credentials only for the services
you use. Local lake storage and DuckDB are the development defaults.

## Development

Run `make lint` and `make test` before submitting changes. Additional workflow
targets are listed in the Makefile.

## Project layout

- `orchestration/` — Dagster assets and jobs
- `dbt_project/` — dbt models, tests, and snapshots
- `infra/` — Terraform and container infrastructure
- `dashboard/` — Evidence.dev dashboard
- `tests/` — unit and integration tests
- `docs/` — architecture and design decisions
- `scripts/` — development and operations scripts
