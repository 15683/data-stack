'''import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

S3_DATASET = Dataset("s3://data-stack/raw/earthquake")
ODS_DATASET = Dataset("postgres://postgres_dwh/dwh_db/ods/fct_earthquake")

OWNER = "15683"
DAG_ID = "raw_from_s3_to_pg"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

LONG_DESCRIPTION = """
# Load data from S3 (MinIO) to Postgres (ODS layer)
"""
SHORT_DESCRIPTION = "Load S3 -> PG"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Moscow"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date

def get_and_transfer_raw_data_to_ods_pg(**context):
    try:
        access_key = Variable.get("access_key")
        secret_key = Variable.get("secret_key")
        pg_password = Variable.get("pg_password")
    except KeyError:
        logging.error("Variables not found in Airflow Admin!")
        raise

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load from S3 to PG for dates: {start_date}")

    con = duckdb.connect()

    try:
        con.sql("INSTALL httpfs; LOAD httpfs;")
        con.sql("INSTALL postgres; LOAD postgres;")

        con.sql(
            f"""
            SET TIMEZONE='UTC';
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{access_key}';
            SET s3_secret_access_key = '{secret_key}';
            SET s3_use_ssl = FALSE;
    
            CREATE SECRET dwh_postgres (
                TYPE postgres,
                HOST 'postgres_dwh', 
                PORT 5432,
                DATABASE 'dwh_db',
                USER 'dwh_user',
                PASSWORD '{pg_password}'
            );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
        """
        )

        query = f"""
        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
            time, latitude, longitude, depth, mag, mag_type, nst, gap, dmin, rms, net, id, 
            updated, place, type, horizontal_error, depth_error, mag_error, mag_nst, status, 
            location_source, mag_source
        )
        SELECT
            time,
            latitude,
            longitude,
            depth,
            mag,
            magType AS mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontalError AS horizontal_error,
            depthError AS depth_error,
            magError AS mag_error,
            magNst AS mag_nst,
            status,
            locationSource AS location_source,
            magSource AS mag_source
        FROM 's3://data-stack/{LAYER}/{SOURCE}/{start_date}/{start_date}_data.parquet';
        """

        logging.info("Executing DuckDB Insert query...")
        con.sql(query)
        logging.info(f"✅ Data transfer success for date: {start_date}")

    except Exception as e:
        logging.error(f"DuckDB/Transfer Error: {e}")
        raise
    finally:
        con.close()

with DAG(
        dag_id="raw_from_s3_to_pg",
        schedule=[S3_DATASET],
        default_args=args,
        tags=["s3", "ods", "pg"],
        description=SHORT_DESCRIPTION,
        max_active_runs=1,
        catchup=False,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    task_transfer = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
        outlets=[ODS_DATASET]
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> task_transfer >> end'''


import logging
import pendulum
import duckdb

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

# Подписываемся на Dataset от предыдущего DAG-а
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
        logging.error("Не найдены переменные S3 или PG в Airflow!")
        raise

    data_interval_start = context["data_interval_start"]
    date_str = data_interval_start.format("YYYY-MM-DD")
    s3_path = f"s3://data-stack/raw/football/{date_str}/{COMPETITION}_matches.parquet"

    logging.info(f"💻 Начинаем перенос данных из {s3_path} в PostgreSQL")

    con = duckdb.connect()
    try:
        # Настраиваем доступы к S3 и Postgres
        setup_query = f"""
            INSTALL httpfs; LOAD httpfs;
            INSTALL postgres; LOAD postgres;

            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{s3_access_key}';
            SET s3_secret_access_key = '{s3_secret_key}';
            SET s3_use_ssl = FALSE;

            -- Удаляем и создаем секрет, чтобы избежать ошибок при повторных запусках
            DROP SECRET IF EXISTS dwh_postgres;
            CREATE SECRET dwh_postgres (
                TYPE postgres,
                HOST 'postgres_dwh',
                PORT 5432,
                DATABASE 'dwh_db',
                USER 'dwh_user',
                PASSWORD '{pg_password}'
            );
            ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
        """
        con.sql(setup_query)

        # Основной запрос для переноса данных
        # Используем ON CONFLICT для идемпотентности
        insert_query = f"""
            INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE} (
                match_id, utc_date, status, matchday, stage, match_group,
                home_team_id, home_team_name, away_team_id, away_team_name, winner,
                score_full_time_home, score_full_time_away, score_half_time_home, score_half_time_away,
                competition_code, competition_name
            )
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
                competition_name
            FROM read_parquet('{s3_path}')
            ON CONFLICT (match_id) DO UPDATE SET
                utc_date = EXCLUDED.utc_date,
                status = EXCLUDED.status,
                winner = EXCLUDED.winner,
                score_full_time_home = EXCLUDED.score_full_time_home,
                score_full_time_away = EXCLUDED.score_full_time_away,
                load_ts = now();
        """
        con.sql(insert_query)
        logging.info("✅ Данные успешно перенесены в PostgreSQL.")
    except Exception as e:
        logging.error(f"Ошибка при работе с DuckDB/Postgres: {e}")
        raise
    finally:
        con.close()


with DAG(
        dag_id="ods_football_matches_from_s3_to_pg",
        schedule=[S3_FOOTBALL_DATASET],  # Запускается по завершению предыдущего DAG
        start_date=pendulum.datetime(2025, 12, 1, tz="Europe/Moscow"),
        default_args={"owner": OWNER},
        tags=["s3", "ods", "pg", "football"],
        description="Загрузка данных о матчах из S3 в ODS слой PostgreSQL",
        max_active_runs=1,
        catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    task_transfer = PythonOperator(
        task_id="transfer_s3_to_pg",
        python_callable=transfer_s3_to_pg,
    )

    end = EmptyOperator(task_id="end")

    start >> task_transfer >> end
