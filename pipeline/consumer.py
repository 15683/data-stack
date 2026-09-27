import base64
import hashlib
import json
import logging
import os
import signal
import time

import psycopg
from confluent_kafka import Consumer, KafkaException
from prometheus_client import Counter, Gauge, Histogram

from pipeline.config import pg_options
from pipeline.database import apply_event, record_rejection
from pipeline.events import InvalidEvent, decode_event, nullable_text, timestamp, validate_context
from pipeline.health import Health, serve_health
from pipeline.kafka import producer, send

LOG = logging.getLogger(__name__)


def handle_message(message, conn, dlq_producer):
    try:
        event = decode_event(message.value())
    except InvalidEvent as exc:
        payload = message.value() or b""
        send(
            dlq_producer,
            os.getenv("KAFKA_DLQ", "football.matches.dlq"),
            f"{message.topic()}:{message.partition()}:{message.offset()}",
            {
                "error": str(exc),
                "source_topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "payload_base64": base64.b64encode(payload[:65536]).decode(),
                "payload_bytes": len(payload),
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "payload_truncated": len(payload) > 65536,
            },
        )
        # Attribute a rejected match only when its envelope is usable. Unknown poison
        # messages remain in DLQ; completion count mismatch still catches missing rows.
        try:
            rejected = json.loads(payload)
            if not isinstance(rejected, dict) or rejected.get("kind") != "match":
                raise InvalidEvent("Not an attributable match")
            for key in ("event_id", "raw_object_key"):
                if not nullable_text(rejected.get(key)):
                    raise InvalidEvent("Missing envelope")
            timestamp(rejected.get("fetched_at"))
            validate_context(rejected.get("context", {}))
        except (ValueError, TypeError, UnicodeError):
            pass
        else:
            record_rejection(conn, rejected)
        return "dlq"
    return "applied" if apply_event(conn, event) else "duplicate"


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    processed = Counter("football_events", "Processed events", ["result"])
    duration = Histogram("football_write_seconds", "Event processing time")
    last_fetch = Gauge("football_last_snapshot_timestamp_seconds", "Last completed snapshot fetch")
    last_poll = Gauge("football_consumer_poll_timestamp_seconds", "Last successful consumer poll")
    broker_ready = Gauge("football_kafka_connected", "Kafka metadata request succeeded")
    incomplete = Gauge("football_incomplete_ingestions", "Snapshots processing for over 10 minutes")
    rejected = Gauge("football_ingestions_with_errors", "Completed snapshots with errors")
    state = Health()
    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": os.getenv("KAFKA_GROUP", "football-postgres-v1"),
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 300000,
            "allow.auto.create.topics": False,
        }
    )
    dlq_producer = producer()
    server = serve_health(state, int(os.getenv("METRICS_PORT", "8000")))
    try:
        with psycopg.connect(**pg_options(), autocommit=True) as conn:
            conn.execute("SET statement_timeout='15s'")
            conn.execute("SET lock_timeout='10s'")
            consumer.subscribe([os.getenv("KAFKA_TOPIC", "football.matches.v1")])
            refreshed = 0
            while running:
                message = consumer.poll(1)
                if message is not None:
                    if message.error():
                        raise KafkaException(message.error())
                    with duration.time():
                        result = handle_message(message, conn, dlq_producer)
                        # Database commit or confirmed DLQ publication happens first.
                        consumer.commit(message=message, asynchronous=False)
                    processed.labels(result=result).inc()
                    if result == "dlq":
                        LOG.error("Event quarantined at offset %s", message.offset())
                last_poll.set_to_current_time()
                state.last_loop = time.monotonic()
                if time.monotonic() - refreshed > 30:
                    value = conn.execute(
                        "SELECT extract(epoch FROM max(fetched_at)) FROM ops.ingestions "
                        "WHERE status IN ('completed', 'empty')"
                    ).fetchone()[0]
                    last_fetch.set(float(value or 0))
                    incomplete.set(
                        conn.execute(
                            "SELECT count(*) FROM ops.ingestions WHERE status='processing' "
                            "AND started_at < now()-interval '10 minutes'"
                        ).fetchone()[0]
                    )
                    rejected.set(
                        conn.execute(
                            "SELECT count(*) FROM ops.ingestions "
                            "WHERE status='completed_with_errors'"
                        ).fetchone()[0]
                    )
                    try:
                        metadata = consumer.list_topics(timeout=3)
                        state.dependencies_ready = bool(metadata.brokers)
                    except KafkaException:
                        state.dependencies_ready = False
                    broker_ready.set(int(state.dependencies_ready))
                    refreshed = time.monotonic()
    finally:
        state.dependencies_ready = False
        server.shutdown()
        consumer.close()
        dlq_producer.flush(10)


if __name__ == "__main__":
    main()
