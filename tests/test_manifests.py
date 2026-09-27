import subprocess

import yaml


def rendered():
    return list(
        yaml.safe_load_all(
            subprocess.check_output(
                ["helm", "template", "data-stack", "deploy/chart", "-n", "data-stack"], text=True
            )
        )
    )


def test_memory_budget_and_no_unwanted_components():
    total_mib = 0
    for resource in rendered():
        if resource["kind"] not in {"StatefulSet", "Deployment", "DaemonSet"}:
            continue
        assert "grafana" not in resource["metadata"]["name"]
        for container in resource["spec"]["template"]["spec"]["containers"]:
            resources = container["resources"]
            assert "requests" in resources and "limits" in resources
            total_mib += int(resources["limits"]["memory"].removesuffix("Mi"))
            assert not container["image"].endswith(":latest")
    assert total_mib <= 6500  # leave room for Ubuntu/k3s and sequential maintenance jobs


def test_services_are_private_and_pvc_requests_fit_disk():
    storage = 0
    for resource in rendered():
        if resource["kind"] == "Service":
            assert resource["spec"]["type"] == "ClusterIP"
        claims = resource["spec"].get("volumeClaimTemplates", []) if "spec" in resource else []
        if resource["kind"] == "PersistentVolumeClaim":
            claims = [resource]
        for claim in claims:
            storage += int(claim["spec"]["resources"]["requests"]["storage"].removesuffix("Gi"))
    assert storage <= 40


def test_kafka_numeric_settings_use_decimal_integer_strings():
    kafka = next(
        r for r in rendered() if r["kind"] == "StatefulSet" and r["metadata"]["name"] == "kafka"
    )
    env = kafka["spec"]["template"]["spec"]["containers"][0]["env"]
    for item in env:
        if item["name"] in {"KAFKA_LOG_RETENTION_BYTES", "KAFKA_LOG_RETENTION_HOURS"}:
            assert isinstance(item["value"], str)
            assert item["value"].isdigit(), "Kafka LONG settings reject scientific notation"
            assert int(item["value"]) > 0


def test_demo_requires_separate_namespace():
    result = subprocess.run(
        [
            "helm",
            "template",
            "data-stack",
            "deploy/chart",
            "-n",
            "data-stack",
            "-f",
            "deploy/demo-values.yaml",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "sourceMode=demo requires namespace data-stack-demo" in result.stderr
    subprocess.run(
        ["helm", "lint", "deploy/chart", "-n", "data-stack-demo", "-f", "deploy/demo-values.yaml"],
        check=True,
        capture_output=True,
    )
