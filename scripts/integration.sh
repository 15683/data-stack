#!/usr/bin/env bash
# Disposable containers only. Docker is a test/build tool, not the deployment platform.
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON=${PYTHON:-python3}
work=$(mktemp -d /tmp/data-stack-integration.XXXXXXXX)
suffix=${work##*.}
pg="data-stack-test-pg-$suffix"
garage="data-stack-test-garage-$suffix"
kafka="data-stack-test-kafka-$suffix"
cleanup() {
  status=$?
  if (( status != 0 )); then
    for container in "$pg" "$garage" "$kafka"; do docker logs --tail 60 "$container" 2>&1 || true; done
  fi
  docker rm -f -v "$pg" "$garage" "$kafka" >/dev/null 2>&1 || true
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT
image() { "$PYTHON" -c 'import sys,yaml; print(yaml.safe_load(open("deploy/chart/values.yaml"))["images"][sys.argv[1]])' "$1"; }
export KAFKA_TEST_PORT=${KAFKA_TEST_PORT:-19092}
"$PYTHON" -c 'import socket,os; s=socket.socket(); s.bind(("127.0.0.1",int(os.environ["KAFKA_TEST_PORT"]))); s.close()'
docker run -d --name "$pg" --memory=160m \
  -e POSTGRES_PASSWORD=integration-only -p 127.0.0.1::5432 \
  "$(image postgres)" -c shared_buffers=16MB -c max_connections=20 >/dev/null
cat > "$work/garage.toml" <<'TOML'
metadata_dir = "/data/meta"
data_dir = "/data/objects"
db_engine = "lmdb"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "1111111111111111111111111111111111111111111111111111111111111111"
block_ram_buffer_max = "16MiB"
[s3_api]
api_bind_addr = "[::]:3900"
s3_region = "garage"
[admin]
api_bind_addr = "[::]:3903"
TOML
docker run -d --name "$garage" --memory=160m \
  -v "$work/garage.toml:/etc/garage.toml:ro" -p 127.0.0.1::3900 \
  "$(image garage)" >/dev/null
# Render the actual deployment settings; override only addresses for host access.
helm template data-stack deploy/chart -n data-stack > "$work/rendered.yaml"
mapfile -t kafka_env < <("$PYTHON" - "$work/rendered.yaml" <<'INNER'
import os,sys,yaml
resources=list(yaml.safe_load_all(open(sys.argv[1])))
k=next(r for r in resources if r['kind']=='StatefulSet' and r['metadata']['name']=='kafka')
env={e['name']:e['value'] for e in k['spec']['template']['spec']['containers'][0]['env'] if 'value' in e}
env.update(CLUSTER_ID='MkU3OEVBNTcwNTJENDM2Qk', KAFKA_CONTROLLER_QUORUM_VOTERS='1@localhost:9093',
           KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://127.0.0.1:'+os.environ['KAFKA_TEST_PORT'])
for name,value in env.items():
    print('-e'); print(name+'='+value)
INNER
)
docker run -d --name "$kafka" --memory=896m \
  --tmpfs /var/lib/kafka/data:rw,uid=1000,gid=1000,size=128m \
  -p "127.0.0.1:$KAFKA_TEST_PORT:9092" \
  "${kafka_env[@]}" "$(image kafka)" >/dev/null
export PGHOST=127.0.0.1 PGDATABASE=postgres PGUSER=postgres PGPASSWORD=integration-only
PGPORT=$(docker port "$pg" 5432/tcp | awk -F: '{print $NF}')
export PGPORT
garage_port=$(docker port "$garage" 3900/tcp | awk -F: '{print $NF}')
export S3_ENDPOINT_URL="http://127.0.0.1:$garage_port" GARAGE_TEST_CONTAINER="$garage"
export KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:$KAFKA_TEST_PORT" RUN_INTEGRATION=1
"$PYTHON" -m pytest -m integration -q
