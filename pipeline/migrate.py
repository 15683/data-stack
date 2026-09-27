"""Bootstrap isolated application roles, databases and ordered DWH migrations."""

import hashlib
import os
import time
from pathlib import Path

import psycopg
from psycopg import sql

from pipeline.config import required


def admin_connection(host, password, database="postgres"):
    for attempt in range(60):
        try:
            return psycopg.connect(
                host=host,
                user="postgres",
                password=password,
                dbname=database,
                connect_timeout=5,
                autocommit=True,
            )
        except psycopg.OperationalError:
            if attempt == 59:
                raise
            time.sleep(2)


def role(conn, name, password):
    if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (name,)).fetchone():
        conn.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(name), sql.Literal(password)
            )
        )
    else:
        conn.execute(
            sql.SQL("ALTER ROLE {} PASSWORD {}").format(sql.Identifier(name), sql.Literal(password))
        )


def database(conn, name, owner):
    if not conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
        conn.execute(
            sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(name), sql.Identifier(owner)
            )
        )
    conn.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(name)))


def migrate_dwh(conn, directory):
    migrations = sorted(Path(directory).glob("[0-9]*.sql"))
    if not migrations:
        raise RuntimeError(f"No SQL migrations found in {directory}")
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(15683)")
        conn.execute("CREATE SCHEMA IF NOT EXISTS ops")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ops.schema_migrations "
            "(name TEXT PRIMARY KEY, checksum TEXT NOT NULL, "
            "applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        )
        for path in migrations:
            content = path.read_text()
            checksum = hashlib.sha256(content.encode()).hexdigest()
            old = conn.execute(
                "SELECT checksum FROM ops.schema_migrations WHERE name = %s", (path.name,)
            ).fetchone()
            if old:
                if old[0] != checksum:
                    raise RuntimeError(f"Applied migration was modified: {path.name}")
                continue
            conn.execute(content)
            conn.execute(
                "INSERT INTO ops.schema_migrations(name, checksum) VALUES (%s, %s)",
                (path.name, checksum),
            )


def main():
    with admin_connection("postgres-main", required("POSTGRES_MAIN_PASSWORD")) as conn:
        for username, dbname, secret in (
            ("airflow", "airflow_db", "AIRFLOW_DB_PASSWORD"),
            ("metabase", "metabase_db", "METABASE_DB_PASSWORD"),
        ):
            role(conn, username, required(secret))
            database(conn, dbname, username)
    password = required("POSTGRES_DWH_PASSWORD")
    with admin_connection("postgres-dwh", password) as conn:
        role(conn, "dwh_writer", required("DWH_PASSWORD"))
        role(conn, "metabase_reader", required("METABASE_READER_PASSWORD"))
        database(conn, "dwh_db", "postgres")
        conn.execute("GRANT CONNECT ON DATABASE dwh_db TO dwh_writer, metabase_reader")
    with admin_connection("postgres-dwh", password, "dwh_db") as conn:
        migrate_dwh(
            conn, os.getenv("SQL_DIRECTORY", str(Path(__file__).resolve().parents[1] / "sql"))
        )
        conn.execute("GRANT USAGE ON SCHEMA ods, ops TO dwh_writer")
        conn.execute("REVOKE ALL ON ALL TABLES IN SCHEMA ods, ops FROM dwh_writer")
        conn.execute("GRANT SELECT, INSERT, UPDATE ON ods.matches TO dwh_writer")
        conn.execute(
            "GRANT SELECT, INSERT, UPDATE ON ops.ingestions, ops.event_results TO dwh_writer"
        )
        conn.execute("GRANT SELECT, INSERT, DELETE ON ops.processed_events TO dwh_writer")
        conn.execute("GRANT DELETE ON ops.event_results TO dwh_writer")
        conn.execute("GRANT USAGE ON SCHEMA ods, dm TO metabase_reader")
        conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA ods, dm TO metabase_reader")


if __name__ == "__main__":
    main()
