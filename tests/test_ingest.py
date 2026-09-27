import gzip
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from pipeline import ingest


def test_retry_reuses_snapshot_without_http(monkeypatch):
    s3 = MagicMock()
    monkeypatch.setattr(ingest, "s3_client", lambda: s3)
    http = MagicMock()
    monkeypatch.setattr(ingest.requests, "Session", http)
    key = ingest.archive_snapshot("same-run", "2026-09-20", "2026-09-23")
    assert key == ingest.archive_snapshot("same-run", "2026-09-20", "2026-09-23")
    http.assert_not_called()
    s3.put_object.assert_not_called()


def test_permission_error_is_not_treated_as_missing_snapshot(monkeypatch):
    s3 = MagicMock()
    s3.head_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "HeadObject")
    monkeypatch.setattr(ingest, "s3_client", lambda: s3)
    with pytest.raises(ClientError):
        ingest.archive_snapshot("run", "2026-09-20", "2026-09-23")


def test_request_filters_and_exact_raw_bytes(monkeypatch):
    body = b'{ "matches": [], "extra": {"preserved": true} }'
    s3 = MagicMock()
    s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
    monkeypatch.setattr(ingest, "s3_client", lambda: s3)
    session = MagicMock()
    response = session.__enter__.return_value.get.return_value
    response.content = body
    response.json.return_value = {"matches": [], "extra": {"preserved": True}}
    monkeypatch.setattr(ingest.requests, "Session", lambda: session)
    monkeypatch.setenv("FOOTBALL_API_KEY", "test-only")
    ingest.archive_snapshot("run", "2026-09-20", "2026-09-23")
    options = session.__enter__.return_value.get.call_args.kwargs
    assert options["params"] == {"dateFrom": "2026-09-20", "dateTo": "2026-09-23"}
    assert options["timeout"] == (10, 60)
    assert gzip.decompress(s3.put_object.call_args.kwargs["Body"]) == body


def test_failed_publish_raises():
    from pipeline.kafka import send

    producer = MagicMock()
    producer.flush.return_value = 1
    with pytest.raises(RuntimeError, match="delivery failed"):
        send(producer, "test", "1", {"test": True})
