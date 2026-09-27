"""Start the actual Helm mapping and verify a UDP heartbeat becomes a metric."""

import socket
import subprocess
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

import yaml


def main():
    docs = list(
        yaml.safe_load_all(
            subprocess.check_output(["helm", "template", "data-stack", "deploy/chart"], text=True)
        )
    )
    mapping = next(
        d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "statsd-config"
    )["data"]["mapping.yml"]
    image = yaml.safe_load(Path("deploy/chart/values.yaml").read_text())["images"]["statsd"]
    name = "data-stack-statsd-test-" + uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)
        path.chmod(0o755)
        (path / "mapping.yml").write_text(mapping)
        try:
            subprocess.run(
                [
                    "docker",
                    "run",
                    "-d",
                    "--name",
                    name,
                    "--memory=48m",
                    "-p",
                    "127.0.0.1::9102",
                    "-p",
                    "127.0.0.1::9125/udp",
                    "-v",
                    f"{path}:/etc/statsd:ro",
                    image,
                    "--statsd.mapping-config=/etc/statsd/mapping.yml",
                ],
                check=True,
                stdout=subprocess.DEVNULL,
            )

            def port(spec):
                return int(
                    subprocess.check_output(["docker", "port", name, spec], text=True)
                    .strip()
                    .rsplit(":", 1)[1]
                )

            http, udp = port("9102/tcp"), port("9125/udp")
            for _attempt in range(30):
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                    sock.sendto(b"airflow.scheduler_heartbeat:1|c", ("127.0.0.1", udp))
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{http}/metrics", timeout=2) as r:
                        if "airflow_scheduler_heartbeat" in r.read().decode():
                            print("Rendered StatsD mapping accepted and heartbeat exported")
                            return
                except OSError:
                    pass
                time.sleep(1)
            raise RuntimeError("No exported heartbeat")
        finally:
            subprocess.run(["docker", "logs", "--tail", "5", name], check=False)
            subprocess.run(["docker", "rm", "-f", name], check=False, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
