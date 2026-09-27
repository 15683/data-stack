"""Sequential bounded windows, using the same archive, publication and verification path."""

import argparse
from datetime import date, timedelta

from pipeline.ingest import archive_snapshot, publish_snapshot
from pipeline.report import wait_for_ingestion


def windows(start, end):
    start, end = date.fromisoformat(start), date.fromisoformat(end)
    if start > end:
        raise ValueError("date_from must be <= date_to")
    while start <= end:
        finish = min(start + timedelta(days=30), end)
        yield start.isoformat(), finish.isoformat()
        # One-day overlap works with both inclusive and exclusive API upper bounds.
        if finish == end:
            break
        start = finish


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--season", type=int)
    parser.add_argument("--batch-id", required=True, help="Reuse to resume; new ID to refetch API")
    args = parser.parse_args()
    for start, end in windows(args.date_from, args.date_to):
        key = archive_snapshot(f"backfill:{args.batch_id}:{start}:{end}", start, end, args.season)
        publish_snapshot(key)
        wait_for_ingestion(key)


if __name__ == "__main__":
    main()
