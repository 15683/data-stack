import gzip
import json
import os
import subprocess
import time
import uuid

import psycopg
import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from pipeline.config import pg_options
from pipeline.consumer import handle_message
from pipeline.database import apply_event
from pipeline.events import make_events
from pipeline.ingest import publish_snapshot, s3_client
from pipeline.kafka import producer, send
from pipeline.migrate import migrate_dwh
from scripts.bootstrap_garage import bootstrap

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="isolated services needed"),
]


@pytest.fixture(scope="module", autouse=True)
def infrastructure():
    def api(operation, payload=None):
        command = [
            "docker",
            "exec",
            "-i",
            os.environ["GARAGE_TEST_CONTAINER"],
            "/garage",
            "json-api",
            operation,
        ]
        if payload is not None:
            command.append("-")
        return json.loads(
            subprocess.check_output(
                command, input=json.dumps(payload) if payload else None, text=True
            )
        )

    for attempt in range(90):
        try:
            with psycopg.connect(**pg_options(), autocommit=True) as conn:
                migrate_dwh(conn, "sql")
            credentials = bootstrap(api)
            os.environ.update(credentials)
            s3_client().head_bucket(Bucket="data-stack")
            AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]}).list_topics(
                timeout=5
            )
            # Bootstrap must be safe to rerun, including credentials.
            assert bootstrap(api) == credentials
            return
        except Exception:
            if attempt == 89:
                raise
            time.sleep(2)


@pytest.fixture
def db():
    with psycopg.connect(**pg_options(), autocommit=True) as conn:
        conn.execute(
            "TRUNCATE ods.matches, ops.processed_events, ops.ingestions, ops.event_results"
        )
        yield conn


def new_consumer(group, topic):
    client = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
        }
    )
    client.subscribe([topic])
    return client


def poll(client):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        message = client.poll(1)
        if message is not None:
            assert not message.error(), message.error()
            return message
    raise AssertionError("Kafka message not received")


@pytest.fixture
def topics(monkeypatch):
    topic = f"football.test.{uuid.uuid4().hex}"
    dlq = topic + ".dlq"
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    for result in admin.create_topics([NewTopic(topic, 1, 1), NewTopic(dlq, 1, 1)]).values():
        result.result(timeout=30)
    monkeypatch.setenv("KAFKA_TOPIC", topic)
    monkeypatch.setenv("KAFKA_DLQ", dlq)
    return topic, dlq


def test_postgres_idempotency_newer_revision_and_rollback(db, snapshot):
    event = next(make_events(snapshot, "raw/first", "2026-09-23T06:00:00Z"))
    assert apply_event(db, event)
    first_load = db.execute("SELECT load_ts FROM ods.matches").fetchone()[0]
    assert not apply_event(db, event)
    snapshot["matches"][0]["lastUpdated"] = "2026-09-24T21:00:00Z"
    snapshot["matches"][0]["score"]["halfTime"]["away"] = 2
    newer = next(make_events(snapshot, "raw/newer", "2026-09-25T06:00:00Z"))
    assert apply_event(db, newer)
    older = json.loads(json.dumps(event))
    # make_events preserves the payload object; explicitly create an older source version.
    older["match"]["lastUpdated"] = "2026-09-20T21:00:00Z"
    older["match"]["score"]["halfTime"]["away"] = 0
    older["event_id"] = "old-source-different-event"
    assert apply_event(db, older)
    row = db.execute("SELECT score_half_time_away, load_ts FROM ods.matches").fetchone()
    assert row[0] == 2 and row[1] >= first_load
    broken = json.loads(json.dumps(newer))
    broken["event_id"] = "rollback-event"
    broken["match"]["utcDate"] = "bad"
    with pytest.raises(ValueError):
        apply_event(db, broken)
    assert (
        db.execute("SELECT 1 FROM ops.processed_events WHERE event_id='rollback-event'").fetchone()
        is None
    )


def test_garage_kafka_postgres_replay_after_commit_before_offset(db, snapshot, topics):
    topic, _ = topics
    key = "raw/football/integration.json.gz"
    s3_client().put_object(
        Bucket="data-stack",
        Key=key,
        Body=gzip.compress(json.dumps(snapshot).encode()),
        Metadata={"fetched-at": "2026-09-23T06:00:00Z"},
    )
    assert publish_snapshot(key) == 1
    group = uuid.uuid4().hex
    client = new_consumer(group, topic)
    dlq = producer()
    try:
        message = poll(client)
        assert handle_message(message, db, dlq) == "applied"
        # Simulated crash window: PostgreSQL committed; Kafka offset was never committed.
    finally:
        client.close()
    client = new_consumer(group, topic)
    try:
        message = poll(client)
        assert handle_message(message, db, dlq) == "duplicate"
        client.commit(message=message, asynchronous=False)
        complete = poll(client)
        assert handle_message(complete, db, dlq) == "applied"
        client.commit(message=complete, asynchronous=False)
    finally:
        client.close()
    assert db.execute("SELECT count(*) FROM ods.matches").fetchone()[0] == 1
    assert db.execute("SELECT match_count FROM ops.ingestions").fetchone()[0] == 1


def test_empty_snapshot_completion_and_bad_event_dlq(db, topics):
    topic, dlq_topic = topics
    source = producer()
    empty = list(make_events({"matches": []}, "raw/empty", "2026-09-23T06:00:00Z"))[0]
    send(source, topic, "empty", empty)
    send(source, topic, "bad", {"wrong": "schema"})
    client = new_consumer(uuid.uuid4().hex, topic)
    try:
        assert handle_message(poll(client), db, source) == "applied"
        assert handle_message(poll(client), db, source) == "dlq"
    finally:
        client.close()
    dlq = new_consumer(uuid.uuid4().hex, dlq_topic)
    try:
        error = json.loads(poll(dlq).value())
        assert error["source_topic"] == topic
        assert error["error"] == "Unsupported event schema"
    finally:
        dlq.close()
    assert db.execute("SELECT match_count FROM ops.ingestions").fetchone()[0] == 0


def test_migrations_are_repeatable_and_detect_modified_history(db, tmp_path):
    migrate_dwh(db, "sql")
    assert db.execute("SELECT count(*) FROM ops.schema_migrations").fetchone()[0] == 2
    (tmp_path / "001_initial.sql").write_text("SELECT 1;")
    with pytest.raises(RuntimeError, match="modified"):
        migrate_dwh(db, tmp_path)


def test_snapshot_accounting_rejection_replay_and_repair(db, snapshot):
    from pipeline.database import record_rejection

    match, complete = make_events(snapshot, "raw/accounting", "2026-09-23T06:00:00Z")
    record_rejection(db, match)
    record_rejection(db, match)
    assert apply_event(db, complete)
    row = db.execute("SELECT processed_count,rejected_count,status FROM ops.ingestions").fetchone()
    assert row == (0, 1, "completed_with_errors")
    assert apply_event(db, match)
    assert not apply_event(db, match)
    assert not apply_event(db, complete)
    row = db.execute(
        "SELECT match_count,processed_count,rejected_count,status FROM ops.ingestions"
    ).fetchone()
    assert row == (1, 1, 0, "completed")


def test_completion_with_missing_match_is_not_success(db, snapshot):
    complete = list(make_events(snapshot, "raw/missing", "2026-09-23T06:00:00Z"))[-1]
    apply_event(db, complete)
    assert db.execute("SELECT status FROM ops.ingestions").fetchone()[0] == "completed_with_errors"


def test_demo_replay_and_team_season_view(db):
    from pipeline.demo import snapshot

    for revision in (1, 1, 2):
        for event in make_events(
            snapshot(revision), f"raw/demo-{revision}", "2026-01-05T06:00:00Z"
        ):
            apply_event(db, event)
    assert db.execute("SELECT count(*) FROM ods.matches").fetchone()[0] == 3
    assert (
        db.execute("SELECT score_full_time_home FROM ods.matches WHERE match_id=990001").fetchone()[
            0
        ]
        == 4
    )
    assert (
        db.execute("SELECT goals_for FROM dm.team_season_stats WHERE team_id=900001").fetchone()[0]
        == 7
    )


def test_legacy_import_preserves_existing_rows_and_allows_api_updates(db, snapshot):
    from datetime import datetime

    from pipeline.events import match_row
    from pipeline.import_legacy import LEGACY_COLUMNS, import_rows

    event = next(make_events(snapshot, "raw/new", "2026-09-23T06:00:00Z"))
    old = match_row(event)
    old["utc_date"] = datetime(2026, 9, 22, 19)
    old["load_ts"] = datetime(2026, 9, 23, 6)
    old["score_full_time_home"] = 0
    legacy = {name: old[name] for name in LEGACY_COLUMNS}
    assert import_rows([legacy], db) == 1
    assert import_rows([legacy], db) == 0
    assert apply_event(db, event)
    assert db.execute("SELECT score_full_time_home FROM ods.matches").fetchone()[0] == 2
    assert import_rows([legacy], db) == 0
    assert db.execute("SELECT score_full_time_home FROM ods.matches").fetchone()[0] == 2
