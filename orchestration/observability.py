from collections.abc import Mapping
from typing import Any

from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    RunStatusSensorContext,
    run_status_sensor,
)

from orchestration.quality import record_pipeline_metrics, send_failure_alert

PARTITION_TAG = "dagster/partition"


def _failure_summary(records: list[Any]) -> str:
    for record in reversed(records):
        event = record.event_log_entry.dagster_event
        if event is None:
            continue
        if event.event_type == DagsterEventType.STEP_FAILURE:
            error = getattr(event.step_failure_data, "error", None)
            if error is not None:
                message = getattr(error, "message", None)
                return str(message or error)[:1500]
            return str(event.message)[:1500]
        if event.event_type == DagsterEventType.ASSET_CHECK_EVALUATION:
            evaluation = event.asset_check_evaluation
            if evaluation is not None and not evaluation.passed:
                return f"Asset check failed: {evaluation.check_name}"[:1500]
    return "Dagster run failed; see the run event log for details."


def _step_durations(records: list[Any]) -> dict[str, float]:
    starts: dict[str, float] = {}
    durations: dict[str, float] = {}
    for record in records:
        event = record.event_log_entry.dagster_event
        if event is None or event.node_name is None:
            continue
        timestamp = record.event_log_entry.timestamp
        if event.event_type == DagsterEventType.STEP_START:
            starts[event.node_name] = timestamp
        elif event.event_type in {
            DagsterEventType.STEP_SUCCESS,
            DagsterEventType.STEP_FAILURE,
        }:
            start = starts.get(event.node_name)
            if start is not None:
                durations[event.node_name] = max(0.0, timestamp - start)
    return durations


def record_run_metrics(context: RunStatusSensorContext, status: str) -> None:
    run = context.dagster_run
    records = context.instance.get_records_for_run(run.run_id).records
    asset_keys = sorted(run.asset_selection or (), key=lambda key: key.to_user_string())
    if not asset_keys:
        return

    partition = run.tags.get(PARTITION_TAG)
    materializations: dict[str, Mapping[str, Any]] = {}
    for record in records:
        if record.asset_materialization is None:
            continue
        asset_name = record.asset_key.to_user_string() if record.asset_key else ""
        materializations[asset_name] = record.asset_materialization.metadata

    durations = _step_durations(list(records))
    error_summary = _failure_summary(list(records)) if status == "FAILURE" else None
    for asset_key in asset_keys:
        asset_name = asset_key.to_user_string()
        metadata = materializations.get(asset_name, {})
        step_name = asset_key.path[-1]
        record_pipeline_metrics(
            run_id=run.run_id,
            asset=asset_name,
            partition=partition,
            status=status,
            duration_seconds=durations.get(step_name),
            rows_in=_metadata_integer(metadata, "rows_in"),
            rows_valid=_metadata_integer(metadata, "rows_valid"),
            rows_quarantined=_metadata_integer(metadata, "rows_quarantined"),
            error_summary=error_summary,
        )
        if status == "FAILURE":
            send_failure_alert(
                asset=asset_name,
                partition=partition,
                error_summary=error_summary or "Dagster run failed.",
            )


def _metadata_integer(metadata: Mapping[str, Any], key: str) -> int | None:
    value = metadata.get(key)
    if value is None:
        return None
    raw_value = getattr(value, "value", value)
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name="pipeline_metrics_success_sensor",
    monitor_all_code_locations=True,
)
def pipeline_metrics_success_sensor(context: RunStatusSensorContext) -> None:
    record_run_metrics(context, "SUCCESS")


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    name="pipeline_metrics_failure_sensor",
    monitor_all_code_locations=True,
)
def pipeline_metrics_failure_sensor(context: RunStatusSensorContext) -> None:
    record_run_metrics(context, "FAILURE")
