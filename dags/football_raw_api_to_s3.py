import logging
import pandas as pd
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import pendulum

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
        schedule_interval=None,  # None, чтобы запускать ТОЛЬКО вручную
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

