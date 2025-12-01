import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER = "15683"
DAG_ID = "raw_from_api_to_s3"

LAYER = "raw"
SOURCE = "earthquake" #TODO потом добавить название моего API

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.now("Europe/Moscow").subtract(days=1),
    "catchup": False,  # Отключаем подгрузку истории для теста
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
            FROM read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv &starttime={start_date}&endtime={end_date}') 
        ) 
        TO 's3://data-stack/{LAYER}/{SOURCE}/{start_date}/{start_date}_data.parquet'
        (FORMAT 'PARQUET', CODEC 'GZIP');
        """                     # TODO заменить выше адрес на мой адрес API

        logging.info("Executing DuckDB query...")
        con.sql(query)
        logging.info(f"✅ Download for date success: {start_date}")

    except Exception as e:
        logging.error(f"DuckDB Error: {e}")
        raise
    finally:
        con.close()

with DAG(
        dag_id=DAG_ID,
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
    )

    end = EmptyOperator(task_id="end")

    start >> task_transfer >> end