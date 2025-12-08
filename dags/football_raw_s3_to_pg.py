import logging
import duckdb
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset

# Подписываемся на тот же Dataset
S3_FOOTBALL_DATASET = Dataset("s3://data-stack/raw/football")

OWNER = "15683"
SCHEMA = "ods"
TARGET_TABLE = "matches"
COMPETITION = "CL"


def transfer_s3_to_pg(**context):
    try:
        s3_access_key = Variable.get("access_key")
        s3_secret_key = Variable.get("secret_key")
        pg_password = Variable.get("pg_password")
    except KeyError:
        logging.error("Variables not found!")
        raise

    # Получаем ту же дату, что была у первого DAG-а
    logical_date = context["logical_date"]
    date_str = logical_date.format("YYYY-MM-DD")

    s3_path = f"s3://data-stack/raw/football/{date_str}/{COMPETITION}_matches.parquet"

    logging.info(f"📅 Дата обработки: {date_str}")
    logging.info(f"🔎 Ищем файл: {s3_path}")

    con = duckdb.connect()
    try:
        setup_query = f"""
            INSTALL httpfs; LOAD httpfs;
            INSTALL postgres; LOAD postgres;
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{s3_access_key}';
            SET s3_secret_access_key = '{s3_secret_key}';
            SET s3_use_ssl = FALSE;

            CREATE SECRET IF NOT EXISTS dwh_postgres (
                TYPE postgres,
                HOST 'postgres_dwh',
                PORT 5432,
                DATABASE 'dwh_db',
                USER 'dwh_user',
                PASSWORD '{pg_password}'
            );
        """
        con.sql(setup_query)

        con.sql(
            f"ATTACH 'dbname=dwh_db user=dwh_user password={pg_password} host=postgres_dwh port=5432' AS pg_db (TYPE postgres)")
        con.sql(f"""
            INSERT INTO pg_db.{SCHEMA}.{TARGET_TABLE} 
            SELECT
                id as match_id,
                "utcDate"::TIMESTAMP as utc_date,
                status,
                matchday,
                stage,
                "group" as match_group,
                homeTeam_id as home_team_id,
                homeTeam_name as home_team_name,
                awayTeam_id as away_team_id,
                awayTeam_name as away_team_name,
                score_winner as winner,
                score_fullTime_home as score_full_time_home,
                score_fullTime_away as score_full_time_away,
                score_halfTime_home as score_half_time_home,
                score_halfTime_away as score_half_time_away,
                competition_code,
                competition_name,
                now() as load_ts
            FROM read_parquet('{s3_path}')
            ON CONFLICT (match_id) DO UPDATE SET
                utc_date = EXCLUDED.utc_date,
                status = EXCLUDED.status,
                winner = EXCLUDED.winner,
                score_full_time_home = EXCLUDED.score_full_time_home,
                score_full_time_away = EXCLUDED.score_full_time_away,
                load_ts = now();
        """)

        logging.info("✅ Успешно загружено в Postgres.")
    except Exception as e:
        logging.error(f"Ошибка DuckDB: {e}")
        raise
    finally:
        con.close()


with DAG(
        dag_id="ods_football_matches_from_s3_to_pg",
        schedule=[S3_FOOTBALL_DATASET],  # Запуск по триггеру
        start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
        default_args={"owner": OWNER},
        tags=["s3", "ods", "pg", "football"],
        description="S3 -> Postgres (Football)",
        catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    task_transfer = PythonOperator(
        task_id="transfer_s3_to_pg",
        python_callable=transfer_s3_to_pg,
    )

    end = EmptyOperator(task_id="end")

    start >> task_transfer >> end

