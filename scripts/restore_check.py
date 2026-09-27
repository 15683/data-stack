"""Restore the latest complete DWH dump into a temporary database on the same server."""

import json
import os
import subprocess
import time
import uuid


def main():
    namespace = os.getenv("NAMESPACE", "data-stack")
    name = "restore-check-" + uuid.uuid4().hex[:10]
    image = subprocess.check_output(
        [
            "kubectl",
            "-n",
            namespace,
            "get",
            "statefulset",
            "postgres-dwh",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].image}",
        ],
        text=True,
    )
    script = r"""
set -eu
attempt=0
until pg_isready -h postgres-dwh -U postgres >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 60 ] || { echo 'PostgreSQL unavailable'; exit 1; }
  sleep 2
done
db=restore_check_$(date +%s)_$$
cleanup() { dropdb -h postgres-dwh -U postgres --if-exists "$db"; }
trap cleanup EXIT
file=$(find /backups -name '*-dwh_db.dump' -type f | sort | tail -1)
test -n "$file" || { echo 'No completed DWH dump; run make backup first'; exit 1; }
createdb -h postgres-dwh -U postgres "$db"
pg_restore --exit-on-error --no-owner --no-acl -h postgres-dwh -U postgres -d "$db" "$file"
psql -v ON_ERROR_STOP=1 -h postgres-dwh -U postgres -d "$db" <<'SQL'
SELECT count(*) AS restored_matches FROM ods.matches;
SELECT count(*) AS restored_migrations FROM ops.schema_migrations;
-- Older valid backups predate the seasonal view.
DO $$ DECLARE groups bigint; BEGIN
  IF to_regclass('dm.team_season_stats') IS NOT NULL THEN
    EXECUTE 'SELECT count(*) FROM dm.team_season_stats' INTO groups;
    RAISE NOTICE 'Restored team groups: %', groups;
  ELSE
    RAISE NOTICE 'Legacy backup: seasonal view not present';
  END IF;
END $$;
DO $$ BEGIN
  IF EXISTS (SELECT match_id FROM ods.matches GROUP BY match_id HAVING count(*)>1)
  THEN RAISE EXCEPTION 'Duplicate match IDs'; END IF;
END $$;
SQL
echo "Restore verified from $file; temporary database will be deleted"
"""
    manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "ttlSecondsAfterFinished": 86400,
            "backoffLimit": 0,
            "activeDeadlineSeconds": 900,
            "template": {
                "metadata": {"labels": {"app": "restore-check"}},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "containers": [
                        {
                            "name": "restore",
                            "image": image,
                            # Kubernetes expands dollars before passing command to sh.
                            # Escape once so SQL dollar quotes and shell $$ survive.
                            "command": ["sh", "-ec", script.replace("$", "$$")],
                            "env": [
                                {
                                    "name": "PGPASSWORD",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "stack-secrets",
                                            "key": "POSTGRES_DWH_PASSWORD",
                                        }
                                    },
                                }
                            ],
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "32Mi"},
                                "limits": {"cpu": "500m", "memory": "128Mi"},
                            },
                            "volumeMounts": [
                                {"name": "backups", "mountPath": "/backups", "readOnly": True}
                            ],
                        }
                    ],
                    "volumes": [
                        {
                            "name": "backups",
                            "persistentVolumeClaim": {"claimName": "database-backups"},
                        }
                    ],
                },
            },
        },
    }
    subprocess.run(
        ["kubectl", "apply", "-f", "-"], input=json.dumps(manifest), text=True, check=True
    )
    try:
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            job = json.loads(
                subprocess.check_output(
                    ["kubectl", "-n", namespace, "get", "job", name, "-o", "json"],
                    text=True,
                )
            )
            conditions = job.get("status", {}).get("conditions", [])
            if any(
                c["type"] in {"Failed", "FailureTarget"} and c["status"] == "True"
                for c in conditions
            ):
                raise RuntimeError(f"Restore check failed: {name}")
            if any(c["type"] == "Complete" and c["status"] == "True" for c in conditions):
                break
            time.sleep(2)
        else:
            raise TimeoutError(f"Restore check timed out: {name}")
    finally:
        subprocess.run(["kubectl", "-n", namespace, "logs", f"job/{name}"], check=False)
    print("Restore check passed; retain the Job log as evidence")


if __name__ == "__main__":
    main()
