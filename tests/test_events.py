import json

import pytest

from pipeline.events import InvalidEvent, decode_event, make_events, match_row, timestamp


def test_empty_snapshot_still_has_completion():
    events = list(
        make_events({"matches": []}, "raw/football/empty.json.gz", "2026-09-23T06:00:00Z")
    )
    assert len(events) == 1
    assert events[0]["kind"] == "snapshot_complete"
    assert decode_event(json.dumps(events[0]).encode())["match_count"] == 0


def test_repeat_snapshot_has_stable_ids(snapshot):
    first = list(make_events(snapshot, "raw/football/1.json.gz", "2026-09-23T06:00:00Z"))
    second = list(make_events(snapshot, "raw/football/1.json.gz", "2026-09-24T06:00:00Z"))
    assert [x["event_id"] for x in first] == [x["event_id"] for x in second]
    assert first[-1]["kind"] == "snapshot_complete"


def test_mapping_preserves_zero_scores_and_timezone(snapshot):
    event = next(make_events(snapshot, "raw/key", "2026-09-23T06:00:00Z"))
    row = match_row(decode_event(json.dumps(event).encode()))
    assert row["score_half_time_away"] == 0
    assert row["utc_date"].utcoffset().total_seconds() == 0
    assert row["match_group"] is None


@pytest.mark.parametrize("body", [b"broken", b"null", b"[]", b"{}", b"\xff", None])
def test_invalid_envelope(body):
    with pytest.raises(InvalidEvent):
        decode_event(body)


@pytest.mark.parametrize("value", ["bad", "2026-09-01T12:00:00", None])
def test_invalid_timestamp(value):
    with pytest.raises(InvalidEvent):
        timestamp(value)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda e: e.update(schema_version=2),
        lambda e: e.update(schema_version=True),
        lambda e: e["match"].update(id=True),
        lambda e: e["match"].update(homeTeam=[]),
        lambda e: e["match"].update(status="bad\x00status"),
        lambda e: e["match"].update(matchday="1"),
    ],
)
def test_poison_records_are_rejected(snapshot, mutation):
    event = next(make_events(snapshot, "raw/key", "2026-09-23T06:00:00Z"))
    mutation(event)
    with pytest.raises(InvalidEvent):
        decode_event(json.dumps(event).encode())


def test_duplicate_ids_in_source_are_rejected(snapshot):
    snapshot["matches"] *= 2
    with pytest.raises(InvalidEvent):
        list(make_events(snapshot, "raw/key", "2026-09-23T06:00:00Z"))
