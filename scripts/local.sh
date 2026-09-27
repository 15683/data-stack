#!/usr/bin/env bash
# No credentials are stored in local.env; images/config persist between terminals.
set -euo pipefail
cd "$(dirname "$0")/.."
config=${STACK_CONFIG:-"$HOME/.config/data-stack/local.env"}
 # Local operator configuration is intentionally sourced.
# shellcheck source=/dev/null
if [[ -f "$config" ]]; then set -a; source "$config"; set +a; fi
export KUBECONFIG=${KUBECONFIG:-"$HOME/.kube/config"}
export NAMESPACE=${NAMESPACE:-data-stack}
export SOURCE_MODE=${SOURCE_MODE:-live}
case ${1:-doctor} in
  doctor)
    for tool in kubectl helm python3 docker; do
      command -v "$tool" >/dev/null || { echo "Missing tool: $tool" >&2; exit 1; }
    done
    echo "Context: $(kubectl config current-context); namespace: $NAMESPACE; mode: $SOURCE_MODE"
    kubectl get nodes
    df -h /
    free -h
    free_kb=$(df -Pk / | awk 'NR==2 {print $4}')
    (( free_kb >= 10*1024*1024 )) || { echo 'Less than 10 GiB free; check disk before deployment.' >&2; exit 1; }
    if [[ -n ${AIRFLOW_IMAGE:-} && -n ${CONSUMER_IMAGE:-} ]]; then
      for image in "$AIRFLOW_IMAGE" "$CONSUMER_IMAGE"; do
        docker image inspect "$image" >/dev/null || { echo "Missing Docker image: $image" >&2; exit 1; }
        if ! sudo k3s ctr -n k8s.io images list -q | grep -Fx "$image" >/dev/null; then
          echo "Image exists in Docker but is not imported in k3s: $image. Run make import-images." >&2
          exit 1
        fi
      done
    else
      echo 'Local images are not configured yet. Run make build.'
    fi
    ;;
  build)
    tag="local-$(date -u +%Y%m%d%H%M%S)"
    export AIRFLOW_IMAGE="docker.io/library/data-stack-airflow:$tag"
    export CONSUMER_IMAGE="docker.io/library/data-stack-consumer:$tag"
    docker build -f docker/airflow.Dockerfile -t "$AIRFLOW_IMAGE" .
    docker build -f docker/consumer.Dockerfile -t "$CONSUMER_IMAGE" .
    mkdir -p "$(dirname "$config")"
    # Preserve operator settings; replace only image references.
    touch "$config"
    sed -i '/^AIRFLOW_IMAGE=/d; /^CONSUMER_IMAGE=/d' "$config"
    printf 'AIRFLOW_IMAGE=%q\nCONSUMER_IMAGE=%q\n' "$AIRFLOW_IMAGE" "$CONSUMER_IMAGE" >> "$config"
    echo "Saved image references in $config"
    ;;
  import-images)
    : "${AIRFLOW_IMAGE:?Run make build}" "${CONSUMER_IMAGE:?Run make build}"
    sudo -v
    docker save "$AIRFLOW_IMAGE" "$CONSUMER_IMAGE" | sudo k3s ctr -n k8s.io images import -
    ;;
  deploy)
    if [[ "$SOURCE_MODE" == demo ]]; then
      [[ "$NAMESPACE" == data-stack-demo ]] || { echo 'Demo requires NAMESPACE=data-stack-demo'; exit 1; }
      export VALUES_FILE=${VALUES_FILE:-deploy/demo-values.yaml}
    fi
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    if [[ "$SOURCE_MODE" == live ]] && [[ -z $(kubectl -n "$NAMESPACE" get secret stack-secrets --ignore-not-found -o name) ]]; then
      if [[ -z ${FOOTBALL_API_KEY:-} ]]; then
        read -r -s -p 'football-data.org API token: ' FOOTBALL_API_KEY </dev/tty
        echo
        export FOOTBALL_API_KEY
      fi
    fi
    bash scripts/deploy.sh
    ;;
  backup)
    kubectl -n "$NAMESPACE" create job --from=cronjob/database-backup "database-backup-manual-$(date +%s)"
    ;;
  restore-check)
    python3 scripts/restore_check.py
    ;;
  smoke)
    bash scripts/smoke.sh
    ;;
  status)
    kubectl -n "$NAMESPACE" get pods,pvc,jobs
    kubectl -n "$NAMESPACE" exec deployment/consumer -- python -c '
import psycopg
from pipeline.config import pg_options
with psycopg.connect(**pg_options()) as c:
    for row in c.execute("SELECT raw_object_key,match_count,processed_count,rejected_count,status FROM ops.ingestions ORDER BY started_at DESC LIMIT 5"):
        print(row)
'
    ;;
  demo)
    [[ "$NAMESPACE" == data-stack-demo ]] || { echo 'Use demo namespace, never the live DWH.'; exit 1; }
    kubectl -n "$NAMESPACE" exec deployment/airflow-scheduler -c airflow -- python -m pipeline.demo_check
    ;;
  backfill)
    shift
    kubectl -n "$NAMESPACE" exec deployment/airflow-scheduler -c airflow -- python -m pipeline.backfill "$@"
    ;;
  *) echo 'Usage: local.sh doctor|build|import-images|deploy|status|demo|backfill|backup|restore-check|smoke'; exit 2 ;;
esac
