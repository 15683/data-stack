import os


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def pg_options() -> dict:
    return {
        "host": os.getenv("PGHOST", "postgres-dwh"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "dwh_db"),
        "user": os.getenv("PGUSER", "dwh_writer"),
        "password": required("PGPASSWORD"),
        "connect_timeout": 10,
    }
