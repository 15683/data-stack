from unittest.mock import MagicMock

import psycopg
import pytest

from pipeline import consumer


def test_bad_record_requires_successful_dlq_delivery(monkeypatch):
    message = MagicMock()
    message.value.return_value = b"not-json"
    send = MagicMock(side_effect=RuntimeError("broker unavailable"))
    monkeypatch.setattr(consumer, "send", send)
    conn = MagicMock()
    with pytest.raises(RuntimeError):
        consumer.handle_message(message, conn, MagicMock())
    conn.execute.assert_not_called()


def test_database_outage_does_not_send_valid_record_to_dlq(monkeypatch, snapshot):
    import json

    from pipeline.events import make_events

    message = MagicMock()
    message.value.return_value = json.dumps(
        next(make_events(snapshot, "raw/key", "2026-09-23T06:00:00Z"))
    ).encode()
    apply = MagicMock(side_effect=psycopg.OperationalError("database unavailable"))
    send = MagicMock()
    monkeypatch.setattr(consumer, "apply_event", apply)
    monkeypatch.setattr(consumer, "send", send)
    with pytest.raises(psycopg.OperationalError):
        consumer.handle_message(message, MagicMock(), MagicMock())
    send.assert_not_called()


def test_large_invalid_event_fits_dlq_message_limit(monkeypatch):
    message = MagicMock()
    message.value.return_value = b"!" * 900_000
    send = MagicMock()
    monkeypatch.setattr(consumer, "send", send)
    assert consumer.handle_message(message, MagicMock(), MagicMock()) == "dlq"
    record = send.call_args.args[3]
    assert record["payload_truncated"] is True
    assert record["payload_bytes"] == 900_000
    assert len(record["payload_base64"]) < 100_000
