#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export NAMESPACE=${NAMESPACE:-data-stack}
: "${AIRFLOW_IMAGE:?Set AIRFLOW_IMAGE to a published tag or digest}"
: "${CONSUMER_IMAGE:?Set CONSUMER_IMAGE to a published tag or digest}"
stage=preflight
diagnose() {
  status=$?
  if (( status != 0 )); then
    echo "Deployment failed during: $stage (exit $status)" >&2
    kubectl -n "$NAMESPACE" get pods,jobs >&2 || true
    kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp | tail -20 >&2 || true
    echo 'Inspect failing Pod logs. PVC and secrets have been preserved.' >&2
  fi
}
trap diagnose EXIT
for tool in helm kubectl python3; do command -v "$tool" >/dev/null; done
stage=secrets
echo "[deploy] $stage"
python3 -m scripts.bootstrap_secrets
values_args=()
if [[ -z ${VALUES_FILE:-} && -f "$HOME/data-stack-config/values.yaml" ]]; then
  VALUES_FILE="$HOME/data-stack-config/values.yaml"
fi
if [[ -n ${VALUES_FILE:-} ]]; then values_args=(-f "$VALUES_FILE"); fi
helm lint deploy/chart --namespace "$NAMESPACE" "${values_args[@]}"
if [[ ${SOURCE_MODE:-live} == demo && "$NAMESPACE" != data-stack-demo ]]; then
  echo 'Demo requires namespace data-stack-demo' >&2; exit 1
fi
stage=helm-and-migrations
echo "[deploy] $stage"
# No --wait here: post-install migrations must run before Airflow becomes ready.
# Keep migration failure visible; don't claim that Helm can roll back database changes.
helm upgrade --install data-stack deploy/chart -n "$NAMESPACE" \
  --set-string "images.airflow=$AIRFLOW_IMAGE" \
  --set-string "images.consumer=$CONSUMER_IMAGE" \
  "${values_args[@]}" --timeout 12m --history-max 5
stage=metabase-encryption
echo "[deploy] $stage"
python3 -m scripts.metabase_encryption
stage=garage-bootstrap
echo "[deploy] $stage"
python3 -m scripts.bootstrap_garage
stage=kafka-and-topics
echo "[deploy] $stage"
kubectl -n "$NAMESPACE" rollout status statefulset/kafka --timeout=300s
for topic in football.matches.v1 football.matches.dlq; do
  kubectl -n "$NAMESPACE" exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
done
# ConfigMaps are projected into pods; restart readers whose configuration is loaded once.
kubectl -n "$NAMESPACE" rollout restart deployment/prometheus deployment/statsd
# Only workloads from this chart live in the dedicated namespace.
stage=workload-readiness
echo "[deploy] $stage"
kubectl -n "$NAMESPACE" rollout status statefulset --timeout=600s
kubectl -n "$NAMESPACE" rollout status deployment --timeout=600s
kubectl -n "$NAMESPACE" rollout status daemonset/node-exporter --timeout=180s
stage=smoke
echo "[deploy] $stage"
bash scripts/smoke.sh
