import logging
import pandas as pd
import requests
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

# Новый Dataset для бомбардиров
S3_SCORERS_DATASET = Dataset("s3://data-stack/raw/football/scorers")

OWNER = "15683"
COMPETITION = "CL"  # Champions League
SEASON = "2024"  # Сезон 2024/2025 (API использует год начала сезона)


def get_scorers_to_s3(**context):
    try:
        api_key = Variable.get("football_api_key")
        s3_access_key = Variable.get("access_key")
        s3_secret_key = Variable.get("secret_key")
    except KeyError:
        logging.error("Variables not found!")
        raise

    logical_date = context["logical_date"]
    date_str = logical_date.format("YYYY-MM-DD")

    # URL для получения бомбардиров
    url = f"https://api.football-data.org/v4/competitions/{COMPETITION}/scorers"
    params = {"season": SEASON, "limit": 50}  # Берем топ-50
    headers = {"X-Auth-Token": api_key}

    logging.info(f"Запрос бомбардиров: {COMPETITION}, сезон {SEASON}")

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        scorers = data.get("scorers", [])

        if not scorers:
            logging.warning("Нет данных о бомбардирах.")
            return

        # Добавляем мета-информацию о сезоне и соревновании к каждой строке
        competition_info = data.get("competition", {})
        season_info = data.get("season", {})

        for s in scorers:
            s['competition_code'] = competition_info.get('code')
            s['season_start'] = season_info.get('startDate')
            s['season_end'] = season_info.get('endDate')

        # Нормализуем JSON в плоскую таблицу
        df = pd.json_normalize(scorers, sep='_')

        # Путь сохранения (отдельная папка scorers)
        s3_path = f"s3://data-stack/raw/football/scorers/{date_str}/{COMPETITION}_{SEASON}.parquet"

        storage_options = {
            "key": s3_access_key,
            "secret": s3_secret_key,
            "endpoint_url": "http://minio:9000",
            "client_kwargs": {"use_ssl": False}
        }

        df.to_parquet(s3_path, index=False, storage_options=storage_options)
        logging.info(f"✅ Сохранено {len(df)} бомбардиров в {s3_path}")

    except Exception as e:
        logging.error(f"Ошибка API: {e}")
        raise


with DAG(
        dag_id="raw_football_scorers_from_api_to_s3",
        schedule_interval=None,  # Запуск вручную
        start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
        default_args={"owner": OWNER},
        tags=["s3", "raw", "football", "scorers"],
        catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    task_transfer = PythonOperator(
        task_id="get_scorers_to_s3",
        python_callable=get_scorers_to_s3,
        outlets=[S3_SCORERS_DATASET],
    )

    end = EmptyOperator(task_id="end")

    start >> task_transfer >> end
