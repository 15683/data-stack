"""Run inside the Airflow image to check the deployed SDK and manual/scheduled contexts."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

from airflow.dag_processing.dagbag import DagBag

bag = DagBag("/opt/airflow/dags")
assert not bag.import_errors, bag.import_errors
assert set(bag.dags) == {"football_ingestion"}, bag.dags
dag = bag.dags["football_ingestion"]
assert set(dag.task_ids) == {"snapshot", "publish", "verify"}
assert dag.get_task("snapshot").downstream_task_ids == {"publish", "verify"}
assert dag.get_task("publish").downstream_task_ids == {"verify"}
snapshot = dag.get_task("snapshot").python_callable
assert dag.params["raw_key"] is None
assert "null" in dag.params.get_param("raw_key").schema["type"]
context = {
    "params": {"raw_key": None, "date_from": None, "date_to": None},
    "run_id": "manual__check",
    "data_interval_end": None,
    "dag_run": SimpleNamespace(run_after=datetime(2026, 9, 25, tzinfo=UTC)),
}
with patch.dict(snapshot.__globals__, {"get_current_context": lambda: context}):
    with patch(
        "pipeline.ingest.archive_snapshot", return_value="raw/football/check.json.gz"
    ) as archive:
        assert snapshot() == "raw/football/check.json.gz"
        archive.assert_called_once_with("manual__check", "2026-09-22", "2026-09-25", season=None)
        context["params"].update(date_from="2025-01-01", date_to="2025-01-31")
        snapshot()
        archive.assert_called_with("manual__check", "2025-01-01", "2025-01-31", season=None)
        context["params"]["raw_key"] = "raw/football/replay.json.gz"
        assert snapshot() == "raw/football/replay.json.gz"
        assert archive.call_count == 2
print("Airflow DAG import and task dependencies verified")
