"""Update only the football token, without regenerating database or encryption secrets."""

import base64
import getpass
import json
import os
import subprocess


def main():
    namespace = os.getenv("NAMESPACE", "data-stack")
    token = getpass.getpass("New football-data.org token: ").strip()
    if not token:
        raise SystemExit("Empty token; nothing changed")
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
        input=json.dumps({"data": {"FOOTBALL_API_KEY": base64.b64encode(token.encode()).decode()}}),
        text=True,
        check=True,
    )
    subprocess.run(
        [
            "kubectl",
            "-n",
            namespace,
            "rollout",
            "restart",
            "deployment/airflow-scheduler",
            "deployment/airflow-dag-processor",
            "deployment/airflow-api",
        ],
        check=True,
    )
    print("API token updated; other credentials preserved. Wait for Airflow rollout readiness.")


if __name__ == "__main__":
    main()
