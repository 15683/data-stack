'''import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "15683"
DAG_ID = "raw_from_s3_to_pg"

LAYER = "raw"
SOURCE = "earthquake" #TODO потом добавить название моего API
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# DuckDB
PASSWORD = Variable.get("pg_password")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_raw_data_to_ods_pg(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD '{PASSWORD}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
            time,
            latitude,
            longitude,
            depth,
            mag,
            mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontal_error,
            depth_error,
            mag_error,
            mag_nst,
            status,
            location_source,
            mag_source
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
        FROM 's3://data-stack/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )

    con.close()
    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="@daily",
    default_args=args,
    tags=["s3", "ods", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )

    get_and_transfer_raw_data_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end'''

import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
    "start_date": pendulum.now("Europe/Moscow").subtract(days=1),
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

        setup_query = f"""
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
            DATABASE postgres,
            USER 'postgres',
            PASSWORD '{pg_password}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
        """
        con.sql(setup_query)

        insert_query = f"""
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
        con.sql(insert_query)
        logging.info(f"✅ Data transfer success for date: {start_date}")

    except Exception as e:
        logging.error(f"DuckDB/Transfer Error: {e}")
        raise
    finally:
        con.close()

with DAG(
        dag_id=DAG_ID,
        schedule_interval="@daily",
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

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
    )

    task_transfer = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> task_transfer >> end