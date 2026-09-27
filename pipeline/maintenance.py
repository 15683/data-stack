"""Bound raw and dedup retention; preserve ODS and ingestion history."""

import os
from datetime import UTC, datetime, timedelta

import psycopg

from pipeline.config import pg_options
from pipeline.ingest import s3_client


def main():
    days = int(os.getenv("RAW_RETENTION_DAYS", "30"))
    if days < 7:
        raise ValueError("Raw retention must be at least seven days")
    cutoff = datetime.now(UTC) - timedelta(days=days)
    s3 = s3_client()
    bucket = os.getenv("S3_BUCKET", "data-stack")
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="raw/football/"):
        objects = [
            {"Key": obj["Key"]} for obj in page.get("Contents", []) if obj["LastModified"] < cutoff
        ]
        if objects:
            result = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects, "Quiet": True})
            if result.get("Errors"):
                raise RuntimeError("Some raw objects could not be deleted")
    with psycopg.connect(**pg_options()) as conn:
        conn.execute(
            "DELETE FROM ops.processed_events WHERE processed_at < now() - interval '90 days'"
        )
        conn.execute("DELETE FROM ops.event_results WHERE updated_at < now() - interval '90 days'")


if __name__ == "__main__":
    main()
