#!/usr/bin/env bash
# Opt-in, disposable full k3s acceptance test. Never uses the current kubeconfig.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${AIRFLOW_IMAGE:?Build an Airflow image first}"
: "${CONSUMER_IMAGE:?Build a consumer image first}"
command -v k3d >/dev/null
available_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
(( available_kb >= 6*1024*1024 )) || { echo 'Need 6 GiB available for disposable full stack.'; exit 1; }
name="data-stack-test-$(date +%s)-$$"
work=$(mktemp -d)
cleanup() {
  status=$?
  if (( status != 0 )) && [[ -s "$work/kubeconfig" ]]; then
    kubectl -n data-stack-demo get pods,jobs || true
    kubectl -n data-stack-demo get events --sort-by=.lastTimestamp | tail -30 || true
  fi
  k3d cluster delete "$name" || true
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT
k3d cluster create "$name" --image rancher/k3s:v1.35.5-k3s1 --servers 1 --agents 0 \
  --no-lb --kubeconfig-update-default=false --kubeconfig-switch-context=false \
  --k3s-arg '--disable=traefik,servicelb@server:0' --wait --timeout 180s
k3d kubeconfig get "$name" > "$work/kubeconfig"
export KUBECONFIG="$work/kubeconfig"
k3d image import "$AIRFLOW_IMAGE" "$CONSUMER_IMAGE" -c "$name"
export NAMESPACE=data-stack-demo SOURCE_MODE=demo VALUES_FILE=deploy/demo-values.yaml
kubectl create namespace "$NAMESPACE"
bash scripts/deploy.sh
kubectl -n "$NAMESPACE" exec deployment/airflow-scheduler -c airflow -- python -m pipeline.demo_check
# Probe routes and the Metabase JVM startup are exercised by rollout readiness.
kubectl -n "$NAMESPACE" get pods
