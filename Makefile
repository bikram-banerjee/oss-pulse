.PHONY: setup lint test dbt-build dagster-dev up down

setup:
	uv sync --all-groups

lint:
	uv run ruff check .
	uv run mypy orchestration

test:
	uv run pytest

dbt-build:
	uv run dbt build --project-dir dbt_project --profiles-dir dbt_project --target "$${DBT_TARGET:-dev}"

dagster-dev:
	uv run dagster dev -m orchestration

up:
	docker compose up --build

down:
	docker compose down
