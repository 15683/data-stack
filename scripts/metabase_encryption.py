"""One-time encryption of an existing Metabase, only with bootstrap's pending marker."""

import copy
import json
import os
import subprocess
import time
import uuid

MARKER = "data-stack.local/metabase-encryption-pending"


def main():
    base = ["kubectl", "-n", os.getenv("NAMESPACE", "data-stack")]
    pending = subprocess.check_output(
        base
        + [
            "get",
            "secret",
            "stack-secrets",
            "-o",
            "jsonpath={.metadata.annotations.data-stack\\.local/metabase-encryption-pending}",
        ],
        text=True,
    )
    if pending != "true":
        return
    deployment = json.loads(
        subprocess.check_output(
            base + ["get", "deployment", "metabase", "-o", "json"],
            text=True,
        )
    )
    subprocess.run(base + ["scale", "deployment/metabase", "--replicas=0"], check=True)
    subprocess.run(
        base + ["wait", "--for=delete", "pod", "-l", "app=metabase,!job-name", "--timeout=120s"],
        check=True,
    )
    container = copy.deepcopy(deployment["spec"]["template"]["spec"]["containers"][0])
    for key in ("startupProbe", "readinessProbe", "livenessProbe", "ports"):
        container.pop(key, None)
    container["command"] = [
        "sh",
        "-ec",
        "exec java $JAVA_OPTS --add-opens java.base/java.nio=ALL-UNNAMED "
        "-jar /app/metabase.jar enable-encryption",
    ]
    name = "metabase-encryption-" + uuid.uuid4().hex[:8]
    job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name},
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": 900,
            "ttlSecondsAfterFinished": 86400,
            "template": {
                "metadata": {"labels": {"app": "metabase"}},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "containers": [container],
                },
            },
        },
    }
    subprocess.run(base + ["apply", "-f", "-"], input=json.dumps(job), text=True, check=True)
    deadline = time.monotonic() + 900
    try:
        while time.monotonic() < deadline:
            state = json.loads(
                subprocess.check_output(
                    base + ["get", "job", name, "-o", "json"],
                    text=True,
                )
            ).get("status", {})
            if state.get("succeeded"):
                break
            if state.get("failed"):
                raise RuntimeError("Metabase encryption failed; inspect Job, keep pending marker")
            time.sleep(2)
        else:
            raise TimeoutError("Metabase encryption timed out")
    finally:
        subprocess.run(base + ["logs", "job/" + name, "--tail=20"], check=False)
    subprocess.run(base + ["annotate", "secret", "stack-secrets", MARKER + "-"], check=True)
    subprocess.run(base + ["scale", "deployment/metabase", "--replicas=1"], check=True)
    print("Existing Metabase encrypted; restart requested")


if __name__ == "__main__":
    main()
