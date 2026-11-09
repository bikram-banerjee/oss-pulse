FROM ghcr.io/astral-sh/uv:0.12.13 AS uv

FROM python:3.11-slim AS builder
COPY --from=uv /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

FROM python:3.11-slim AS runtime

ENV PATH="/opt/venv/bin:${PATH}" \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    PYTHONPATH=/app \
    LAKE_BACKEND=local \
    LAKE_ROOT=/app/data/lake \
    DUCKDB_PATH=/app/data/oss_pulse.duckdb \
    DBT_TARGET=dev

WORKDIR /app
RUN mkdir -p "${DAGSTER_HOME}" /app/data
COPY --from=builder /opt/venv /opt/venv
COPY orchestration ./orchestration
COPY dbt_project ./dbt_project
COPY workspace.yaml ./

EXPOSE 3000
