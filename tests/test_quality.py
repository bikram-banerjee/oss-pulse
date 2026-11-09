from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from orchestration.quality import (
    evaluate_freshness,
    evaluate_nonzero_rows,
    evaluate_quarantine_ratio,
    evaluate_reconciliation,
    format_failure_alert,
    record_pipeline_metrics,
    send_failure_alert,
)


def test_quality_logic_passes_and_fails_expected_thresholds() -> None:
    assert evaluate_freshness(
        "2025-01-01-00", datetime(2025, 1, 1, 7, tzinfo=UTC)
    )[0] is True
    assert evaluate_nonzero_rows(3)[0] is True
    assert evaluate_nonzero_rows(0)[0] is False
    assert evaluate_quarantine_ratio(100, 1) == (True, 0.01)
    assert evaluate_quarantine_ratio(100, 2) == (False, 0.02)
    assert evaluate_quarantine_ratio(0, 0) == (False, None)
    assert evaluate_reconciliation(10, 10) == (True, 0)
    assert evaluate_reconciliation(10, 9) == (False, 1)


def test_discord_alert_formatter_and_unset_webhook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    payload = format_failure_alert("raw_gh_events", "2025-01-01-00", "bad input")
    embed = payload["embeds"][0]
    assert embed["title"] == "OSS Pulse pipeline failure: raw_gh_events"
    assert "2025-01-01-00" in embed["description"]
    assert "bad input" in embed["description"]
    assert send_failure_alert("raw_gh_events", None, "bad input") is False


def test_failing_check_prints_alert_payload_when_webhook_is_unset(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    passed, reason = evaluate_nonzero_rows(0)
    assert not passed
    payload = format_failure_alert("raw_gh_events", "2025-01-01-00", reason)
    with capsys.disabled():
        print(json.dumps(payload))
    assert send_failure_alert("raw_gh_events", "2025-01-01-00", reason) is False


def test_pipeline_metrics_are_persisted_in_duckdb(tmp_path: Path) -> None:
    database_path = str(tmp_path / "metrics.duckdb")
    record_pipeline_metrics(
        run_id="run-1",
        asset="raw_gh_events",
        partition="2025-01-01-00",
        status="SUCCESS",
        duration_seconds=2.5,
        rows_in=5,
        rows_valid=4,
        rows_quarantined=1,
        error_summary=None,
        database_path=database_path,
    )
    connection = duckdb.connect(database_path, read_only=True)
    try:
        row = connection.execute(
            "select asset, partition, status, rows_in, rows_valid, rows_quarantined "
            "from main.pipeline_run_metrics"
        ).fetchone()
    finally:
        connection.close()

    assert row == ("raw_gh_events", "2025-01-01-00", "SUCCESS", 5, 4, 1)
