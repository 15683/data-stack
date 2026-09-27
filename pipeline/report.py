"""Bounded end-to-end completion check; also usable outside Airflow."""

import json
import time

import psycopg
from psycopg.rows import dict_row

from pipeline.config import pg_options


def wait_for_ingestion(raw_key, timeout=180, interval=2):
    if timeout <= 0 or interval <= 0:
        raise ValueError("timeout and interval must be positive")
    result = None
    deadline = time.monotonic() + timeout
    with psycopg.connect(**pg_options(), autocommit=True, row_factory=dict_row) as conn:
        conn.execute("SET statement_timeout='10s'")
        while time.monotonic() < deadline:
            result = conn.execute(
                "SELECT raw_object_key, match_count AS expected_count, processed_count, "
                "rejected_count, status FROM ops.ingestions WHERE raw_object_key=%s",
                (raw_key,),
            ).fetchone()
            if result and result["status"] in {"completed", "empty"}:
                print(json.dumps(result))
                return result
            time.sleep(interval)
    if result and result["status"] == "completed_with_errors":
        raise RuntimeError(f"Snapshot has rejected or missing events: {result}")
    raise TimeoutError(f"No DWH completion within {timeout}s: {raw_key}")
