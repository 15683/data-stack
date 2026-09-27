from psycopg import sql

from pipeline.events import match_row, timestamp


def ensure_ingestion(conn, event):
    context = event.get("context", {})
    conn.execute(
        "INSERT INTO ops.ingestions(raw_object_key, fetched_at, match_count, status, "
        "run_id, date_from, date_to, competition, source_mode) "
        "VALUES (%s, %s, 0, 'processing', %s, %s, %s, %s, %s) "
        "ON CONFLICT (raw_object_key) DO NOTHING",
        (
            event["raw_object_key"],
            timestamp(event["fetched_at"]),
            context.get("run_id"),
            context.get("date_from"),
            context.get("date_to"),
            context.get("competition"),
            context.get("source_mode", "live"),
        ),
    )
    conn.execute(
        "SELECT 1 FROM ops.ingestions WHERE raw_object_key=%s FOR UPDATE",
        (event["raw_object_key"],),
    )


def refresh_result(conn, raw_key):
    conn.execute(
        "WITH counts AS (SELECT count(*) FILTER (WHERE outcome='processed') AS good, "
        "count(*) FILTER (WHERE outcome='rejected') AS bad FROM ops.event_results "
        "WHERE raw_object_key=%s) UPDATE ops.ingestions SET processed_count=good, "
        "rejected_count=bad, status=CASE WHEN NOT completion_received THEN 'processing' "
        "WHEN bad>0 OR good<>match_count THEN 'completed_with_errors' "
        "WHEN match_count=0 THEN 'empty' ELSE 'completed' END, "
        "completed_at=CASE WHEN completion_received THEN now() ELSE NULL END "
        "FROM counts WHERE raw_object_key=%s",
        (raw_key, raw_key),
    )


def record_result(conn, event, outcome):
    conn.execute(
        "INSERT INTO ops.event_results(event_id,raw_object_key,outcome) VALUES (%s,%s,%s) "
        "ON CONFLICT (event_id) DO UPDATE SET outcome=EXCLUDED.outcome, updated_at=now() "
        "WHERE ops.event_results.outcome='rejected' AND EXCLUDED.outcome='processed'",
        (event["event_id"], event["raw_object_key"], outcome),
    )


def record_rejection(conn, event):
    with conn.transaction():
        ensure_ingestion(conn, event)
        record_result(conn, event, "rejected")
        refresh_result(conn, event["raw_object_key"])


def apply_event(conn, event: dict) -> bool:
    with conn.transaction():
        ensure_ingestion(conn, event)
        inserted = conn.execute(
            "INSERT INTO ops.processed_events(event_id) VALUES (%s) "
            "ON CONFLICT DO NOTHING RETURNING event_id",
            (event["event_id"],),
        ).fetchone()
        if inserted is None:
            if event["kind"] == "match":
                record_result(conn, event, "processed")
            else:
                conn.execute(
                    "UPDATE ops.ingestions SET match_count=%s,completion_received=true "
                    "WHERE raw_object_key=%s",
                    (event["match_count"], event["raw_object_key"]),
                )
            refresh_result(conn, event["raw_object_key"])
            return False
        if event["kind"] == "snapshot_complete":
            conn.execute(
                "UPDATE ops.ingestions SET match_count=%s,completion_received=true "
                "WHERE raw_object_key=%s",
                (event["match_count"], event["raw_object_key"]),
            )
        else:
            row = match_row(event)
            columns = list(row)
            updates = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in columns
                if c != "match_id"
            )
            query = sql.SQL(
                "INSERT INTO ods.matches AS old ({columns}) VALUES ({values}) "
                "ON CONFLICT (match_id) DO UPDATE SET {updates}, load_ts = now() "
                "WHERE (EXCLUDED.source_updated_at, EXCLUDED.fetched_at) "
                "> (old.source_updated_at, old.fetched_at)"
            ).format(
                columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
                values=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                updates=updates,
            )
            conn.execute(query, list(row.values()))
            record_result(conn, event, "processed")
        refresh_result(conn, event["raw_object_key"])
    return True
