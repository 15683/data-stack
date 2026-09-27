import gzip
import hashlib
import json
import os
from datetime import UTC, date, datetime

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline.config import required
from pipeline.events import InvalidEvent, make_events
from pipeline.kafka import producer, send


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://garage:3900"),
        region_name="garage",
        aws_access_key_id=required("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=required("AWS_SECRET_ACCESS_KEY"),
        config=Config(
            s3={"addressing_style": "path"},
            connect_timeout=10,
            read_timeout=30,
            retries={"mode": "standard", "max_attempts": 3},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def archive_snapshot(run_id: str, date_from: str, date_to: str, season=None, revision=1) -> str:
    start, end = date.fromisoformat(date_from), date.fromisoformat(date_to)
    if end < start or (end - start).days > 31:
        raise ValueError("Date range must be ordered and no longer than 31 days")
    mode = os.getenv("SOURCE_MODE", "live")
    if mode not in {"live", "demo"}:
        raise ValueError("SOURCE_MODE must be live or demo")
    if mode == "demo" and os.getenv("STACK_NAMESPACE") != "data-stack-demo":
        raise ValueError("Demo is isolated: deploy it in data-stack-demo")
    competition = "DEMO" if mode == "demo" else os.getenv("FOOTBALL_COMPETITION", "CL")
    identity = f"{run_id}:{start}:{end}:{competition}"
    if season is not None or mode == "demo":
        identity += f":{season}:{mode}:{revision}"
    digest = hashlib.sha256(identity.encode()).hexdigest()
    key = f"raw/football/{end.isoformat()}/{digest}.json.gz"
    client = s3_client()
    bucket = os.getenv("S3_BUCKET", "data-stack")
    try:
        client.head_object(Bucket=bucket, Key=key)
        return key  # Retry always republishes the same immutable snapshot.
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in {"404", "NoSuchKey", "NotFound"}:
            raise
    context = {
        "run_id": run_id,
        "date_from": str(start),
        "date_to": str(end),
        "competition": competition,
        "source_mode": mode,
    }
    if mode == "demo":
        from pipeline.demo import snapshot as demo_snapshot

        body = json.dumps(demo_snapshot(revision)).encode()
        save_snapshot(client, bucket, key, body, context)
        return key
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    with requests.Session() as session:
        session.mount("https://", HTTPAdapter(max_retries=retry))
        params = {"dateFrom": start.isoformat(), "dateTo": end.isoformat()}
        if season is not None:
            params["season"] = int(season)
        response = session.get(
            f"https://api.football-data.org/v4/competitions/{competition}/matches",
            params=params,
            headers={"X-Auth-Token": required("FOOTBALL_API_KEY")},
            timeout=(10, 60),
        )
        response.raise_for_status()
        snapshot = response.json()
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get("matches"), list):
            raise InvalidEvent("Unexpected API response")
        save_snapshot(client, bucket, key, response.content, context)
    return key


def save_snapshot(client, bucket, key, body, context):
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=gzip.compress(body, mtime=0),
        ContentType="application/json",
        ContentEncoding="gzip",
        Metadata={
            "fetched-at": datetime.now(UTC).isoformat(),
            "context": json.dumps(context, ensure_ascii=True),
        },
    )


def publish_snapshot(key: str) -> int:
    if not key.startswith("raw/football/") or not key.endswith(".json.gz"):
        raise ValueError("Replay key must refer to a football raw snapshot")
    response = s3_client().get_object(Bucket=os.getenv("S3_BUCKET", "data-stack"), Key=key)
    with response["Body"] as stream:
        snapshot = json.loads(gzip.decompress(stream.read()))
    context = json.loads(response["Metadata"].get("context", "{}"))
    events = list(make_events(snapshot, key, response["Metadata"]["fetched-at"], context))
    client = producer()
    for event in events:
        # Single partition: completion follows every match, including an empty snapshot.
        key_value = str(event["match"]["id"]) if event["kind"] == "match" else key
        send(client, os.getenv("KAFKA_TOPIC", "football.matches.v1"), key_value, event)
    return len(events) - 1
