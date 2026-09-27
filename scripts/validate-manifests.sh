#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON=${PYTHON:-python3}
work=$(mktemp -d /tmp/data-stack-validation.XXXXXXXX)
trap 'rm -rf "$work"' EXIT
helm template data-stack deploy/chart -n data-stack > "$work/rendered.yaml"
docker run --rm -i ghcr.io/yannh/kubeconform:v0.7.0 \
  -strict -summary -kubernetes-version 1.35.0 < "$work/rendered.yaml"
"$PYTHON" - "$work" <<'PY'
import pathlib,sys,yaml
path=pathlib.Path(sys.argv[1])
for document in yaml.safe_load_all((path/'rendered.yaml').read_text()):
    if document['kind']=='ConfigMap' and document['metadata']['name']=='prometheus-config':
        for name,content in document['data'].items():
            (path/name).write_text(content)
(path/'token').write_text('validation-only')
PY
image=$("$PYTHON" -c 'import yaml; print(yaml.safe_load(open("deploy/chart/values.yaml"))["images"]["prometheus"])')
chmod 755 "$work"
docker run --rm --entrypoint promtool -v "$work:/etc/prometheus:ro" \
  -v "$work:/etc/metrics-token:ro" "$image" check config /etc/prometheus/prometheus.yml
