'''import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

S3_DATASET = Dataset("s3://data-stack/raw/earthquake")

OWNER = "15683"
DAG_ID = "raw_from_api_to_s3"

LAYER = "raw"
SOURCE = "earthquake"

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

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

def get_and_transfer_api_data_to_s3(**context):
    try:
        access_key = Variable.get("access_key")
        secret_key = Variable.get("secret_key")
    except KeyError:
        logging.error("Variables 'access_key' or 'secret_key' not found in Airflow Admin!")
        raise

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")

    con = duckdb.connect()

    try:
        con.sql("INSTALL httpfs; LOAD httpfs;")

        query = f"""
        SET TIMEZONE='UTC';
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{access_key}';
        SET s3_secret_access_key = '{secret_key}';
        SET s3_use_ssl = FALSE;
                            
        COPY
        (
            SELECT *
            FROM read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') 
        ) 
        TO 's3://data-stack/{LAYER}/{SOURCE}/{start_date}/{start_date}_data.parquet'
        (FORMAT 'PARQUET', CODEC 'GZIP');
        """

        logging.info("Executing DuckDB query...")
        con.sql(query)
        logging.info(f"✅ Download for date success: {start_date}")

    except Exception as e:
        logging.error(f"DuckDB Error: {e}")
        raise
    finally:
        con.close()

with DAG(
        dag_id="raw_from_api_to_s3",
        schedule_interval="@daily",
        default_args=args,
        tags=["s3", "raw"],
        description=SHORT_DESCRIPTION,
        max_active_runs=1,
        catchup=False
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    task_transfer = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
        outlets=[S3_DATASET]
    )

    end = EmptyOperator(task_id="end")

    start >> task_transfer >> end'''

import logging
import pandas as pd
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import pendulum

# Определяем Dataset
S3_FOOTBALL_DATASET = Dataset("s3://data-stack/raw/football")

OWNER = "15683"
SOURCE = "football-data.org"
COMPETITION = "CL"


def get_and_transfer_api_data_to_s3(**context):
    try:
        api_key = Variable.get("football_api_key")
        s3_access_key = Variable.get("access_key")
        s3_secret_key = Variable.get("secret_key")
    except KeyError:
        logging.error("Variables not found!")
        raise

    # Получаем логическую дату запуска DAG-а (она одинакова для manual и scheduled)
    logical_date = context["logical_date"]
    date_str = logical_date.format("YYYY-MM-DD")

    logging.info(f"📅 Дата обработки (Logical Date): {date_str}")

    url = f"https://api.football-data.org/v4/competitions/{COMPETITION}/matches"
    headers = {"X-Auth-Token": api_key}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        matches = data.get("matches", [])

        if not matches:
            logging.warning("API не вернул матчей. Пропуск.")
            return

        df = pd.json_normalize(matches, sep='_')

        s3_path = f"s3://data-stack/raw/football/{date_str}/{COMPETITION}_matches.parquet"

        storage_options = {
            "key": s3_access_key,
            "secret": s3_secret_key,
            "endpoint_url": "http://minio:9000",
            "client_kwargs": {"use_ssl": False}
        }

        logging.info(f"💾 Сохраняем в: {s3_path}")
        df.to_parquet(s3_path, index=False, storage_options=storage_options)
        logging.info("✅ Успешно сохранено.")

    except Exception as e:
        logging.error(f"Ошибка: {e}")
        raise


with DAG(
        dag_id="raw_football_matches_from_api_to_s3",
        schedule_interval=None,  # 👈 Сделали None, чтобы запускать ТОЛЬКО вручную (пока тестируем)
        start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
        default_args={"owner": OWNER},
        tags=["s3", "raw", "football"],
        description="API -> S3 (Football)",
        catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    task_transfer = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
        outlets=[S3_FOOTBALL_DATASET],  # Это триггернет второй DAG
    )

    end = EmptyOperator(task_id="end")

    start >> task_transfer >> end

