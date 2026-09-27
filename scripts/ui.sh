#!/usr/bin/env bash
set -euo pipefail
namespace=${NAMESPACE:-data-stack}
airflow_port=${AIRFLOW_LOCAL_PORT:-8080}
metabase_port=${METABASE_LOCAL_PORT:-3001}
prometheus_port=${PROMETHEUS_LOCAL_PORT:-9090}
pids=()
cleanup() {
  if (( ${#pids[@]} )); then kill "${pids[@]}" 2>/dev/null || true; fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM HUP
kubectl -n "$namespace" port-forward --address=127.0.0.1 service/airflow-api "$airflow_port:8080" &
pids+=("$!")
kubectl -n "$namespace" port-forward --address=127.0.0.1 service/metabase "$metabase_port:3000" &
pids+=("$!")
kubectl -n "$namespace" port-forward --address=127.0.0.1 service/prometheus "$prometheus_port:9090" &
pids+=("$!")
wait -n "${pids[@]}"
