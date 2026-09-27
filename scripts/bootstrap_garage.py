#!/usr/bin/env python3
import json
import os
import subprocess
import time

from scripts.bootstrap_secrets import apply_secret


def bootstrap(api):
    status = api("GetClusterStatus")
    nodes = [node for node in status["nodes"] if node["isUp"]]
    if len(nodes) != 1:
        raise RuntimeError("This bootstrap supports exactly one Garage node")
    layout = api("GetClusterLayout")
    if not layout["roles"]:
        api(
            "UpdateClusterLayout",
            {
                "roles": [
                    {
                        "id": nodes[0]["id"],
                        "zone": "ubuntu-vm",
                        "capacity": 10_000_000_000,
                        "tags": ["football"],
                    }
                ]
            },
        )
        api("ApplyClusterLayout", {"version": layout["version"] + 1})
    keys = api("ListKeys")
    existing = next((key for key in keys if key["name"] == "football-pipeline"), None)
    if existing:
        key = api("GetKeyInfo", {"id": existing["id"], "showSecretKey": True})
    else:
        key = api("CreateKey", {"name": "football-pipeline"})
    buckets = api("ListBuckets")
    bucket = next((b for b in buckets if "data-stack" in b["globalAliases"]), None)
    if bucket is None:
        bucket = api("CreateBucket", {"globalAlias": "data-stack"})
    api(
        "UpdateBucket",
        {
            "id": bucket["id"],
            "body": {
                "quotas": {
                    "maxSize": 8_000_000_000,
                    "maxObjects": 100_000,
                }
            },
        },
    )
    api(
        "AllowBucketKey",
        {
            "bucketId": bucket["id"],
            "accessKeyId": key["accessKeyId"],
            "permissions": {"read": True, "write": True, "owner": False},
        },
    )
    return {
        "AWS_ACCESS_KEY_ID": key["accessKeyId"],
        "AWS_SECRET_ACCESS_KEY": key["secretAccessKey"],
    }


def main():
    namespace = os.getenv("NAMESPACE", "data-stack")

    def api(operation, payload=None):
        command = [
            "kubectl",
            "-n",
            namespace,
            "exec",
            "-i",
            "garage-0",
            "--",
            "/garage",
            "json-api",
            operation,
        ]
        if payload is not None:
            command.append("-")
        for attempt in range(60):
            result = subprocess.run(
                command,
                input=json.dumps(payload) if payload else None,
                text=True,
                capture_output=True,
            )
            if result.returncode == 0:
                return json.loads(result.stdout)
            if attempt == 59:
                raise RuntimeError(f"Garage operation failed: {operation}: {result.stderr}")
            time.sleep(2)

    credentials = bootstrap(api)
    apply_secret(namespace, "garage-s3", credentials)


if __name__ == "__main__":
    main()
