#!/usr/bin/env bash
# Invoked over SSH by the trusted main-branch workflow.
set -euo pipefail
revision=$1
repository=$2
airflow_digest=$3
consumer_digest=$4
[[ "$revision" =~ ^[a-f0-9]{40}$ ]]
[[ "$repository" =~ ^[a-z0-9_.-]+/[a-z0-9_.-]+$ ]]
[[ "$airflow_digest" =~ ^sha256:[a-f0-9]{64}$ ]]
[[ "$consumer_digest" =~ ^sha256:[a-f0-9]{64}$ ]]
release_dir="$HOME/data-stack-releases/$revision"
mkdir -p "$release_dir"
tar -xzf "/tmp/data-stack-$revision.tgz" -C "$release_dir"
cd "$release_dir"
export AIRFLOW_IMAGE="ghcr.io/$repository-airflow@$airflow_digest"
export CONSUMER_IMAGE="ghcr.io/$repository-consumer@$consumer_digest"
bash scripts/deploy.sh
rm -f "/tmp/data-stack-$revision.tgz"
