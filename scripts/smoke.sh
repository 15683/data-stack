#!/usr/bin/env bash
set -euo pipefail
export NAMESPACE=${NAMESPACE:-data-stack}
kubectl -n "$NAMESPACE" exec deployment/airflow-scheduler -c airflow -- \
  env 'PYTHONWARNINGS=ignore:Could not import graphviz:UserWarning' \
  AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR airflow dags list-import-errors --output=json |
  python3 -c 'import json,sys; errors=json.load(sys.stdin); assert errors == [], errors'
kubectl -n "$NAMESPACE" exec deployment/consumer -- python -c '
import psycopg
from pipeline.config import pg_options
with psycopg.connect(**pg_options()) as connection:
    connection.execute("SELECT count(*) FROM ods.matches")
print("DWH schema reachable")
'
kubectl -n "$NAMESPACE" exec deployment/airflow-scheduler -- python -c '
from pipeline.ingest import s3_client
s3_client().head_bucket(Bucket="data-stack")
print("Garage bucket reachable")
'
kubectl -n "$NAMESPACE" exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic football.matches.v1
kubectl -n "$NAMESPACE" exec deployment/prometheus -- promtool check config /etc/prometheus/prometheus.yml
echo 'Smoke checks passed. Unpause football_ingestion after configuring the API token.'
