import logging
import duckdb
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset

S3_SCORERS_DATASET = Dataset("s3://data-stack/raw/football/scorers")

OWNER = "15683"
SCHEMA = "ods"
TARGET_TABLE = "scorers"
COMPETITION = "CL"
SEASON = "2024"


def transfer_scorers_to_pg(**context):
    try:
        s3_access_key = Variable.get("access_key")
        s3_secret_key = Variable.get("secret_key")
        pg_password = Variable.get("pg_password")
    except KeyError:
        raise

    logical_date = context["logical_date"]
    date_str = logical_date.format("YYYY-MM-DD")

    s3_path = f"s3://data-stack/raw/football/scorers/{date_str}/{COMPETITION}_{SEASON}.parquet"

    con = duckdb.connect()
    try:
        # 1. Настройка S3 и DuckDB
        con.sql(f"""
            INSTALL httpfs; LOAD httpfs;
            INSTALL postgres; LOAD postgres;
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{s3_access_key}';
            SET s3_secret_access_key = '{s3_secret_key}';
            SET s3_use_ssl = FALSE;
        """)

        # 2. Подключаемся к Postgres
        con.sql(
            f"ATTACH 'dbname=dwh_db user=dwh_user password={pg_password} host=postgres_dwh port=5432' AS pg_db (TYPE postgres)")

        # 3. Создаем в Postgres ВРЕМЕННУЮ таблицу (она исчезнет после закрытия сессии)
        # Мы создаем её БЕЗ колонки ID, чтобы DuckDB мог спокойно залить туда данные
        con.sql(f"""
            CREATE TEMPORARY TABLE pg_db.stg_scorers (
                competition_code VARCHAR,
                season_start_date DATE,
                season_end_date DATE,
                player_id INT,
                player_name VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                date_of_birth DATE,
                nationality VARCHAR,
                position VARCHAR,
                team_id INT,
                team_name VARCHAR,
                goals INT,
                assists INT,
                penalties INT,
                load_ts TIMESTAMP
            )
        """)

        # 4. Заливаем данные из Parquet во временную таблицу (тут колонок поровну, ошибок не будет)
        con.sql(f"""
            INSERT INTO pg_db.stg_scorers
            SELECT
                competition_code,
                season_start::DATE,
                season_end::DATE,
                player_id,
                player_name,
                player_firstName,
                player_lastName,
                player_dateOfBirth::DATE,
                player_nationality,
                player_position,
                team_id,
                team_name,
                goals,
                assists,
                penalties,
                now()
            FROM read_parquet('{s3_path}')
        """)

        # 5. Переливаем из временной в целевую уже средствами самого Postgres
        # Тут Postgres сам сгенерирует ID
        con.sql(f"""
            INSERT INTO pg_db.{SCHEMA}.{TARGET_TABLE} (
                competition_code, season_start_date, season_end_date,
                player_id, player_name, first_name, last_name, date_of_birth, nationality, position,
                team_id, team_name, goals, assists, penalties, load_ts
            )
            SELECT 
                competition_code, season_start_date, season_end_date,
                player_id, player_name, first_name, last_name, date_of_birth, nationality, position,
                team_id, team_name, goals, assists, penalties, load_ts
            FROM pg_db.stg_scorers
            ON CONFLICT (competition_code, season_start_date, player_id) DO UPDATE SET
                goals = EXCLUDED.goals,
                assists = EXCLUDED.assists,
                penalties = EXCLUDED.penalties,
                team_id = EXCLUDED.team_id,
                team_name = EXCLUDED.team_name,
                load_ts = now();
        """)

        logging.info("✅ Бомбардиры успешно загружены через staging таблицу.")

    except Exception as e:
        logging.error(f"Ошибка DuckDB: {e}")
        raise
    finally:
        con.close()


with DAG(
        dag_id="ods_football_scorers_from_s3_to_pg",
        schedule=[S3_SCORERS_DATASET],  # Триггер
        start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
        default_args={"owner": OWNER},
        tags=["s3", "ods", "pg", "scorers"],
        catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    task_transfer = PythonOperator(
        task_id="transfer_scorers_to_pg",
        python_callable=transfer_scorers_to_pg,
    )

    end = EmptyOperator(task_id="end")

    start >> task_transfer >> end
