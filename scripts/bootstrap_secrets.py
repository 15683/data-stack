#!/usr/bin/env python3
"""Create credentials once without rotating them during subsequent deployments."""

import base64
import json
import os
import secrets
import subprocess


def apply_secret(namespace, name, data):
    manifest = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {"name": name, "namespace": namespace},
        "data": {key: base64.b64encode(value.encode()).decode() for key, value in data.items()},
    }
    subprocess.run(
        ["kubectl", "apply", "--server-side", "--field-manager=data-stack-bootstrap", "-f", "-"],
        input=json.dumps(manifest),
        text=True,
        check=True,
    )


def main():
    namespace = os.getenv("NAMESPACE", "data-stack")
    existing = subprocess.check_output(
        [
            "kubectl",
            "-n",
            namespace,
            "get",
            "secret",
            "stack-secrets",
            "--ignore-not-found",
            "-o",
            "json",
        ],
        text=True,
    )
    if existing.strip():
        data = json.loads(existing)["data"]
        missing = {}
        for name in ("METABASE_SESSION_SECRET", "METABASE_ENCRYPTION_KEY"):
            if name not in data:
                missing[name] = secrets.token_hex(32)
        if missing:
            patch = {"data": {k: base64.b64encode(v.encode()).decode() for k, v in missing.items()}}
            if "METABASE_ENCRYPTION_KEY" in missing:
                patch["metadata"] = {
                    "annotations": {"data-stack.local/metabase-encryption-pending": "true"}
                }
            # Merge preserves every existing credential and server-side field owner.
            subprocess.run(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "patch",
                    "secret",
                    "stack-secrets",
                    "--type=merge",
                    "--patch-file=/dev/stdin",
                ],
                input=json.dumps(patch),
                text=True,
                check=True,
            )
        print("Reusing stack-secrets (existing credentials unchanged)")
        return
    api_key = os.environ.get("FOOTBALL_API_KEY")
    if os.getenv("SOURCE_MODE", "live") == "demo":
        if namespace != "data-stack-demo":
            raise SystemExit("Demo requires namespace data-stack-demo")
        api_key = "demo-no-api-access"
    if not api_key:
        raise SystemExit("Set FOOTBALL_API_KEY for the first deployment")
    values = {
        name: secrets.token_hex(24)
        for name in (
            "POSTGRES_MAIN_PASSWORD",
            "POSTGRES_DWH_PASSWORD",
            "AIRFLOW_DB_PASSWORD",
            "METABASE_DB_PASSWORD",
            "DWH_PASSWORD",
            "METABASE_READER_PASSWORD",
            "AIRFLOW_JWT_SECRET",
            "AIRFLOW_API_SECRET",
            "GARAGE_METRICS_TOKEN",
            "METABASE_SESSION_SECRET",
            "METABASE_ENCRYPTION_KEY",
        )
    }
    values.update(
        {
            "FOOTBALL_API_KEY": api_key,
            "AIRFLOW_FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
            "AIRFLOW_AUTH_PASSWORDS": json.dumps({"admin": secrets.token_urlsafe(24)}),
            "GARAGE_RPC_SECRET": secrets.token_hex(32),
            "KAFKA_CLUSTER_ID": base64.urlsafe_b64encode(secrets.token_bytes(16))
            .decode()
            .rstrip("="),
            "AIRFLOW_DB_URI": "postgresql+psycopg2://airflow:"
            f"{values['AIRFLOW_DB_PASSWORD']}@postgres-main:5432/airflow_db",
        }
    )
    apply_secret(namespace, "stack-secrets", values)


if __name__ == "__main__":
    main()
