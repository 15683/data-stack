import hashlib
import json
from datetime import UTC, date, datetime


class InvalidEvent(ValueError):
    """Permanent input error; safe to quarantine in the DLQ."""


def timestamp(value: str) -> datetime:
    if not isinstance(value, str):
        raise InvalidEvent("Timestamp must be an ISO-8601 string")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidEvent("Invalid timestamp") from exc
    if result.tzinfo is None:
        raise InvalidEvent("Timestamp must include a timezone")
    return result.astimezone(UTC)


def object_value(parent: dict, name: str) -> dict:
    value = parent.get(name)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise InvalidEvent(f"{name} must be an object")
    return value


def nullable_int(value):
    if value is not None and (type(value) is not int or not -(2**31) <= value < 2**31):
        raise InvalidEvent("Expected a nullable 32-bit integer")
    return value


def nullable_text(value):
    if value is not None and (not isinstance(value, str) or "\x00" in value):
        raise InvalidEvent("Expected nullable text without NUL characters")
    return value


def make_events(snapshot: dict, raw_key: str, fetched_at: str, context=None):
    matches = snapshot.get("matches")
    if not isinstance(matches, list):
        raise InvalidEvent("API response must contain a matches array")
    seen = set()
    for match in matches:
        if not isinstance(match, dict) or type(match.get("id")) is not int:
            raise InvalidEvent("Match must have an integer id")
        if match["id"] in seen:
            raise InvalidEvent("Duplicate match id in snapshot")
        seen.add(match["id"])
        yield {
            "schema_version": 1,
            "kind": "match",
            "event_id": hashlib.sha256(f"{raw_key}:{match['id']}".encode()).hexdigest(),
            "raw_object_key": raw_key,
            "fetched_at": fetched_at,
            "match": match,
            "context": context or {},
        }
    yield {
        "schema_version": 1,
        "kind": "snapshot_complete",
        "event_id": hashlib.sha256(f"{raw_key}:complete".encode()).hexdigest(),
        "raw_object_key": raw_key,
        "fetched_at": fetched_at,
        "match_count": len(matches),
        "context": context or {},
    }


def decode_event(body: bytes) -> dict:
    try:
        event = json.loads(body)
    except (ValueError, UnicodeError, TypeError) as exc:
        raise InvalidEvent("Invalid JSON") from exc
    if (
        not isinstance(event, dict)
        or type(event.get("schema_version")) is not int
        or event["schema_version"] != 1
    ):
        raise InvalidEvent("Unsupported event schema")
    for name in ("event_id", "raw_object_key"):
        if not isinstance(event.get(name), str) or not event[name] or "\x00" in event[name]:
            raise InvalidEvent(f"Missing or invalid {name}")
    timestamp(event.get("fetched_at"))
    validate_context(event.get("context", {}))
    if event.get("kind") == "match":
        match_row(event)
    elif event.get("kind") == "snapshot_complete":
        count = nullable_int(event.get("match_count"))
        if count is None or count < 0:
            raise InvalidEvent("Invalid match_count")
    else:
        raise InvalidEvent("Unsupported event kind")
    return event


def validate_context(context):
    if not isinstance(context, dict):
        raise InvalidEvent("Invalid event context")
    for name in ("run_id", "competition", "source_mode"):
        nullable_text(context.get(name))
    if context.get("source_mode", "live") not in {"live", "demo"}:
        raise InvalidEvent("Invalid source mode")
    for name in ("date_from", "date_to"):
        if context.get(name) is not None:
            try:
                date.fromisoformat(context[name])
            except (TypeError, ValueError) as exc:
                raise InvalidEvent("Invalid context date") from exc


def nullable_date(value):
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise InvalidEvent("Invalid season date") from exc


def match_row(event: dict) -> dict:
    match = object_value(event, "match")
    match_id = nullable_int(match.get("id"))
    if match_id is None or match_id <= 0:
        raise InvalidEvent("Match id must be positive")
    home = object_value(match, "homeTeam")
    away = object_value(match, "awayTeam")
    score = object_value(match, "score")
    full = object_value(score, "fullTime")
    half = object_value(score, "halfTime")
    competition = object_value(match, "competition")
    season = object_value(match, "season")
    fetched = timestamp(event["fetched_at"])
    return {
        "match_id": match_id,
        "utc_date": timestamp(match.get("utcDate")),
        "status": nullable_text(match.get("status")),
        "matchday": nullable_int(match.get("matchday")),
        "stage": nullable_text(match.get("stage")),
        "match_group": nullable_text(match.get("group")),
        "home_team_id": nullable_int(home.get("id")),
        "home_team_name": nullable_text(home.get("name")),
        "away_team_id": nullable_int(away.get("id")),
        "away_team_name": nullable_text(away.get("name")),
        "winner": nullable_text(score.get("winner")),
        "score_full_time_home": nullable_int(full.get("home")),
        "score_full_time_away": nullable_int(full.get("away")),
        "score_half_time_home": nullable_int(half.get("home")),
        "score_half_time_away": nullable_int(half.get("away")),
        "competition_code": nullable_text(competition.get("code")),
        "competition_name": nullable_text(competition.get("name")),
        "season_id": nullable_int(season.get("id")),
        "season_start": nullable_date(season.get("startDate")),
        "season_end": nullable_date(season.get("endDate")),
        "source_updated_at": timestamp(match["lastUpdated"])
        if match.get("lastUpdated")
        else fetched,
        "fetched_at": fetched,
        "raw_object_key": event["raw_object_key"],
    }
