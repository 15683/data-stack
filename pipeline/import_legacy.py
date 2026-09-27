"""Copy old ODS rows to the new schema without overwriting existing matches."""

import os
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from pipeline.config import pg_options, required

LEGACY_COLUMNS = (
    "match_id",
    "utc_date",
    "status",
    "matchday",
    "stage",
    "match_group",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "winner",
    "score_full_time_home",
    "score_full_time_away",
    "score_half_time_home",
    "score_half_time_away",
    "competition_code",
    "competition_name",
    "load_ts",
)


def import_rows(rows, target, legacy_timezone="UTC"):
    zone = ZoneInfo(legacy_timezone)

    def aware(value):
        if value is None:
            raise ValueError("Legacy utc_date must not be null")
        return value.replace(tzinfo=zone) if value.tzinfo is None else value

    imported = 0
    with target.transaction():
        for original in rows:
            row = {name: original[name] for name in LEGACY_COLUMNS}
            row["utc_date"] = aware(row["utc_date"])
            row["load_ts"] = aware(row["load_ts"] or datetime(1970, 1, 1, tzinfo=UTC))
            row["fetched_at"] = row["load_ts"]
            # Unknown source revision must not prevent a future API update.
            row["source_updated_at"] = datetime(1970, 1, 1, tzinfo=UTC)
            row["raw_object_key"] = "legacy/postgresql/matches"
            query = sql.SQL(
                "INSERT INTO ods.matches ({}) VALUES ({}) ON CONFLICT (match_id) DO NOTHING"
            ).format(
                sql.SQL(", ").join(map(sql.Identifier, row)),
                sql.SQL(", ").join(sql.Placeholder() for _ in row),
            )
            imported += target.execute(query, list(row.values())).rowcount
    return imported


def main():
    with psycopg.connect(required("LEGACY_DWH_DSN")) as source:
        source.execute("SET TRANSACTION READ ONLY")
        with source.cursor(name="legacy_matches", row_factory=dict_row) as cursor:
            cursor.itersize = 500
            cursor.execute(
                sql.SQL("SELECT {} FROM ods.matches ORDER BY match_id").format(
                    sql.SQL(", ").join(map(sql.Identifier, LEGACY_COLUMNS))
                )
            )
            with psycopg.connect(**pg_options(), autocommit=True) as target:
                count = import_rows(cursor, target, os.getenv("LEGACY_TIMEZONE", "UTC"))
                print(f"Imported {count} legacy matches; existing rows were preserved")


if __name__ == "__main__":
    main()
