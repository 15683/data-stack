"""Repeatable acceptance scenario, exclusively in a demo namespace."""

import os
import uuid

import psycopg

from pipeline.config import pg_options
from pipeline.ingest import archive_snapshot, publish_snapshot
from pipeline.report import wait_for_ingestion


def main():
    if os.getenv("SOURCE_MODE") != "demo" or os.getenv("STACK_NAMESPACE") != "data-stack-demo":
        raise SystemExit("This command requires an isolated demo deployment")
    run = "demo-check:" + uuid.uuid4().hex
    for revision in (1, 2):
        key = archive_snapshot(f"{run}:{revision}", "2026-01-01", "2026-01-05", revision=revision)
        for _ in range(2):
            publish_snapshot(key)
            wait_for_ingestion(key)
    with psycopg.connect(**pg_options()) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM ods.matches WHERE competition_code='DEMO'"
            ).fetchone()[0]
            == 3
        )
        assert (
            conn.execute(
                "SELECT score_full_time_home FROM ods.matches WHERE match_id=990001"
            ).fetchone()[0]
            == 4
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM dm.team_season_stats WHERE competition_code='DEMO'"
            ).fetchone()[0]
            == 3
        )
    print("Demo verified: 3 matches, 3 teams, replay without duplicates, corrected score=4")


if __name__ == "__main__":
    main()
