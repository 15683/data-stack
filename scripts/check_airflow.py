"""Exercise the rendered Airflow configuration against disposable PostgreSQL databases."""

import base64
import json
import os
import secrets
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

import yaml


def main():
    image = os.environ.get("AIRFLOW_IMAGE", "data-stack-airflow:worktree")
    namespace = "data-stack-smoke-" + secrets.token_hex(4)
    containers = []
    network_created = False

    def docker(*args, timeout=180):
        return subprocess.check_output(["docker", *args], text=True, timeout=timeout).strip()

    documents = list(
        yaml.safe_load_all(
            subprocess.check_output(["helm", "template", "data-stack", "deploy/chart"], text=True)
        )
    )
    api = next(
        doc
        for doc in documents
        if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "airflow-api"
    )["spec"]["template"]["spec"]
    pg_image = yaml.safe_load(Path("deploy/chart/values.yaml").read_text())["images"]["postgres"]
    secret = dict.fromkeys(
        (
            "POSTGRES_MAIN_PASSWORD",
            "POSTGRES_DWH_PASSWORD",
            "AIRFLOW_DB_PASSWORD",
            "METABASE_DB_PASSWORD",
            "DWH_PASSWORD",
            "METABASE_READER_PASSWORD",
            "AIRFLOW_JWT_SECRET",
            "AIRFLOW_API_SECRET",
        ),
        "integration-only-not-a-production-secret",
    )
    secret["AIRFLOW_FERNET_KEY"] = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
    secret["AIRFLOW_DB_URI"] = (
        f"postgresql+psycopg2://airflow:{secret['AIRFLOW_DB_PASSWORD']}"
        "@postgres-main:5432/airflow_db"
    )
    env = []
    for item in api["containers"][0]["env"]:
        if item["name"].startswith("AIRFLOW__"):
            value = item.get("value")
            if value is None:
                value = secret[item["valueFrom"]["secretKeyRef"]["key"]]
            env += ["-e", f"{item['name']}={value}"]
    try:
        docker("network", "create", namespace)
        network_created = True
        for service in ("postgres-main", "postgres-dwh"):
            name = f"{namespace}-{service}"
            containers.append(name)
            docker(
                "run",
                "-d",
                "--name",
                name,
                "--network",
                namespace,
                "--network-alias",
                service,
                "--memory=160m",
                "-e",
                f"POSTGRES_PASSWORD={secret['POSTGRES_MAIN_PASSWORD']}",
                pg_image,
                "-c",
                "shared_buffers=16MB",
                "-c",
                "max_connections=20",
            )
        name = namespace + "-migrate"
        containers.append(name)
        migrate_env = [arg for key, value in secret.items() for arg in ("-e", f"{key}={value}")]
        docker(
            "run",
            "--name",
            name,
            "--network",
            namespace,
            "--memory=512m",
            *env,
            *migrate_env,
            "--entrypoint",
            "bash",
            image,
            "-ec",
            "python -m pipeline.migrate && airflow db migrate && airflow db check-migrations",
        )
        with tempfile.TemporaryDirectory(prefix="data-stack-auth-") as directory:
            os.chmod(directory, 0o755)
            passwords = Path(directory) / "passwords.json"
            passwords.write_text('{"admin":"integration-only"}')
            passwords.chmod(0o644)
            # Run the chart's init command with its read-only source and writable destination.
            init_command = api["initContainers"][0]["args"][0]
            name = namespace + "-api"
            containers.append(name)
            memory = api["containers"][0]["resources"]["limits"]["memory"].replace("Mi", "m")
            docker(
                "run",
                "-d",
                "--name",
                name,
                "--network",
                namespace,
                f"--memory={memory}",
                "-p",
                "127.0.0.1::8080",
                *env,
                "-v",
                f"{directory}:/opt/airflow/auth-source:ro",
                "--entrypoint",
                "bash",
                image,
                "-ec",
                "mkdir -p /opt/airflow/auth && (" + init_command + ") && exec airflow api-server",
            )
            port = docker("port", name, "8080/tcp").rsplit(":", 1)[1]
            for attempt in range(90):
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{port}/api/v2/monitor/health", timeout=2
                    ) as response:
                        assert json.load(response)["metadatabase"]["status"] == "healthy"
                    request = urllib.request.Request(
                        f"http://127.0.0.1:{port}/auth/token",
                        data=b'{"username":"admin","password":"integration-only"}',
                        headers={"Content-Type": "application/json"},
                    )
                    with urllib.request.urlopen(request, timeout=5) as response:
                        assert json.load(response)["access_token"]
                    print(f"Airflow migration, API health and SimpleAuth login passed ({memory})")
                    break
                except Exception:
                    if attempt == 89:
                        raise
                    time.sleep(1)
    except Exception:
        for name in containers:
            subprocess.run(["docker", "logs", "--tail", "30", name], check=False)
        raise
    finally:
        for name in containers:
            subprocess.run(
                ["docker", "rm", "-f", "-v", name], stdout=subprocess.DEVNULL, check=False
            )
        if network_created:
            subprocess.run(
                ["docker", "network", "rm", namespace], stdout=subprocess.DEVNULL, check=False
            )


if __name__ == "__main__":
    main()
