from datetime import UTC, datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task


@dag(
    dag_id="football_ingestion",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"owner": "15683", "retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "raw_key": Param(
            None,
            type=["null", "string"],
            description="Optional: Garage object key for replay. Leave empty for a new API load.",
        ),
        "date_from": Param(None, type=["null", "string"], description="Optional YYYY-MM-DD"),
        "date_to": Param(None, type=["null", "string"], description="Optional YYYY-MM-DD"),
        "season": Param(
            None,
            type=["null", "integer"],
            minimum=2000,
            maximum=2100,
            description="Optional start year of the season for historical loads",
        ),
    },
    tags=["football", "garage", "kafka"],
)
def football_ingestion():
    @task
    def snapshot():
        from pipeline.ingest import archive_snapshot

        context = get_current_context()
        replay_key = context["params"]["raw_key"]
        if replay_key:
            return replay_key
        date_from = context["params"]["date_from"]
        date_to = context["params"]["date_to"]
        season = context["params"].get("season")
        if bool(date_from) != bool(date_to):
            raise ValueError("Set date_from and date_to together")
        if date_from and date_to:
            return archive_snapshot(context["run_id"], date_from, date_to, season=season)
        # Time comes from the run, never from the task retry's wall clock.
        interval_end = context.get("data_interval_end") or context["dag_run"].run_after
        end = interval_end.date()
        return archive_snapshot(
            context["run_id"], (end - timedelta(days=3)).isoformat(), end.isoformat(), season=season
        )

    @task
    def publish(raw_key):
        from pipeline.ingest import publish_snapshot

        return publish_snapshot(raw_key)

    @task(retries=0)
    def verify(raw_key):
        from pipeline.report import wait_for_ingestion

        return wait_for_ingestion(raw_key)

    raw_key = snapshot()
    published = publish(raw_key)
    verified = verify(raw_key)
    published >> verified


football_ingestion()
