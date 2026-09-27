#!/usr/bin/env bash
# Run this on the backup destination machine, not on the VM itself.
set -euo pipefail
BACKUP_SSH_TARGET=${1:-${BACKUP_SSH_TARGET:-}}
BACKUP_DESTINATION=${2:-${BACKUP_DESTINATION:-}}
: "${BACKUP_SSH_TARGET:?Set user@VM}"
: "${BACKUP_DESTINATION:?Set an absolute destination outside the VM}"
[[ "$BACKUP_DESTINATION" == /* ]] || { echo 'Destination must be absolute'; exit 1; }
[[ "$BACKUP_SSH_TARGET" =~ ^[a-zA-Z0-9_.-]+@[a-zA-Z0-9_.-]+$ ]] || exit 1
namespace=${NAMESPACE:-data-stack}
[[ "$namespace" =~ ^[a-z0-9-]+$ ]] || exit 1
mkdir -p "$BACKUP_DESTINATION"
chmod 700 "$BACKUP_DESTINATION"
# The VM exposes only a read-only, short-lived helper pod and no public backup service.
ssh -o BatchMode=yes "$BACKUP_SSH_TARGET" "NAMESPACE='$namespace' bash -s" <<'REMOTE' |
set -euo pipefail
export KUBECONFIG=${KUBECONFIG:-"$HOME/.kube/config"}
name="backup-export-$(date +%s)-$$"
cleanup() { kubectl -n "$NAMESPACE" delete pod "$name" --ignore-not-found >/dev/null; }
trap cleanup EXIT
image=$(kubectl -n "$NAMESPACE" get statefulset postgres-dwh -o jsonpath='{.spec.template.spec.containers[0].image}')
kubectl -n "$NAMESPACE" run "$name" --image="$image" --restart=Never \
  --overrides='{"spec":{"automountServiceAccountToken":false,"activeDeadlineSeconds":300,"containers":[{"name":"reader","image":"'"$image"'","command":["sleep","300"],"resources":{"requests":{"memory":"16Mi","cpu":"10m"},"limits":{"memory":"64Mi","cpu":"100m"}},"volumeMounts":[{"name":"backups","mountPath":"/backups","readOnly":true}]}],"volumes":[{"name":"backups","persistentVolumeClaim":{"claimName":"database-backups"}}]}}' >/dev/null
kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/$name" --timeout=120s >/dev/null
kubectl -n "$NAMESPACE" exec "$name" -- sh -c 'cd /backups; find . -maxdepth 1 -type f \( -name "*.dump" -o -name "*.sha256" \) | tar -cf - -T -'
REMOTE
  tar --no-same-owner -xf - -C "$BACKUP_DESTINATION"
echo "Exported completed backups to $BACKUP_DESTINATION"
