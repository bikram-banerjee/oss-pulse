from __future__ import annotations

import os
import shutil
import subprocess
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_repo_daily_activity_is_idempotent_on_repeated_run(tmp_path: Path) -> None:
    lake_root = tmp_path / "lake"
    partition_path = lake_root / "raw" / "gh_events" / "dt=2026-11-08" / "hour=00"
    partition_path.mkdir(parents=True)
    table = pa.Table.from_pylist(
        [
            {
                "id": "event-1",
                "type": "WatchEvent",
                "actor": '{"login":"alice"}',
                "repo": '{"name":"octo/project"}',
                "created_at": "2026-11-08T00:10:00Z",
                "payload": "{}",
            },
            {
                "id": "event-2",
                "type": "PushEvent",
                "actor": '{"login":"bob"}',
                "repo": '{"name":"octo/project"}',
                "created_at": "2026-11-08T00:20:00Z",
                "payload": "{}",
            },
            {
                "id": "event-3",
                "type": "PushEvent",
                "actor": '{"login":"alice"}',
                "repo": '{"name":"octo/project"}',
                "created_at": "2026-11-08T00:30:00Z",
                "payload": "{}",
            },
        ],
        schema=pa.schema(
            [
                pa.field("id", pa.string(), nullable=False),
                pa.field("type", pa.string(), nullable=False),
                pa.field("actor", pa.string(), nullable=False),
                pa.field("repo", pa.string(), nullable=False),
                pa.field("created_at", pa.string(), nullable=False),
                pa.field("payload", pa.string(), nullable=False),
            ]
        ),
    )
    pq.write_table(table, partition_path / "part.parquet", compression="zstd")
    database_path = tmp_path / "idempotency.duckdb"
    environment = os.environ.copy()
    environment["LAKE_ROOT"] = str(lake_root)
    environment["DUCKDB_PATH"] = str(database_path)
    dbt_executable = shutil.which("dbt")
    assert dbt_executable is not None, "dbt executable is required for the incremental model test"

    command = [
        dbt_executable,
        "run",
        "--project-dir",
        str(PROJECT_ROOT / "dbt_project"),
        "--profiles-dir",
        str(PROJECT_ROOT / "dbt_project"),
        "--target",
        "dev",
        "--select",
        "+int_repo_daily_activity",
    ]
    results = []
    for _ in range(2):
        run = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=180,
        )
        assert run.returncode == 0, f"dbt run failed:\n{run.stdout}\n{run.stderr}"
        connection = duckdb.connect(str(database_path), read_only=True)
        try:
            rows = connection.execute(
                """
                select repo_full_name, dt, stars, forks, pushes,
                       issues, pull_requests, unique_actors
                from int_repo_daily_activity
                order by repo_full_name, dt
                """
            ).fetchall()
        finally:
            connection.close()
        results.append(rows)

    assert results[0] == results[1]
    assert results[1] == [("octo/project", date(2026, 11, 8), 1, 0, 2, 0, 0, 2)]
