"""Materialize scheduled or manually requested production partitions."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta


def _materialize(asset: str, partition: str) -> None:
    command = [
        "uv",
        "run",
        "dagster",
        "asset",
        "materialize",
        "--module-name",
        "orchestration.definitions",
        "--select",
        asset,
        "--partition",
        partition,
    ]
    subprocess.run(command, check=True)


def _date_range(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def main() -> None:
    event_name = os.environ.get("GITHUB_EVENT_NAME", "schedule")
    start_text = os.environ.get("INPUT_START_DATE", "").strip()
    end_text = os.environ.get("INPUT_END_DATE", "").strip()

    if event_name == "workflow_dispatch":
        if not start_text or not end_text:
            raise ValueError("Both start_date and end_date are required for a backfill.")
        days = _date_range(date.fromisoformat(start_text), date.fromisoformat(end_text))
        for day in days:
            for hour in range(24):
                _materialize("raw_gh_events", f"{day.isoformat()}-{hour:02d}")
            _materialize("raw_hn_stories", day.isoformat())
            _materialize("repo_metadata", day.isoformat())
    else:
        now = datetime.now(UTC)
        gh_hour = (now - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
        _materialize("raw_gh_events", gh_hour.strftime("%Y-%m-%d-%H"))

        yesterday = now.date() - timedelta(days=1)
        if now.hour == 1:
            _materialize("raw_hn_stories", yesterday.isoformat())
        if now.hour == 2:
            _materialize("repo_metadata", yesterday.isoformat())

    subprocess.run(
        [
            "uv",
            "run",
            "dbt",
            "build",
            "--project-dir",
            "dbt_project",
            "--profiles-dir",
            "dbt_project",
            "--target",
            "prod",
        ],
        check=True,
    )


if __name__ == "__main__":
    try:
        main()
    except (ValueError, subprocess.CalledProcessError) as error:
        print(f"Pipeline execution failed: {error}", file=sys.stderr)
        raise
