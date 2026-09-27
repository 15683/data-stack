import json
from unittest.mock import patch

from scripts.bootstrap_secrets import main


def test_first_encryption_key_marks_existing_metabase_for_migration(monkeypatch):
    monkeypatch.setenv("NAMESPACE", "data-stack")
    previous = {"data": {"FOOTBALL_API_KEY": "preserved"}}
    with patch(
        "scripts.bootstrap_secrets.subprocess.check_output", return_value=json.dumps(previous)
    ):
        with patch("scripts.bootstrap_secrets.subprocess.run") as run:
            main()
    change = json.loads(run.call_args.kwargs["input"])
    assert (
        change["metadata"]["annotations"]["data-stack.local/metabase-encryption-pending"] == "true"
    )
    assert set(change["data"]) == {"METABASE_SESSION_SECRET", "METABASE_ENCRYPTION_KEY"}
    assert "FOOTBALL_API_KEY" not in change["data"]


def test_existing_encryption_key_never_requests_reencryption():
    previous = {"data": {"METABASE_ENCRYPTION_KEY": "existing-key"}}
    with patch(
        "scripts.bootstrap_secrets.subprocess.check_output", return_value=json.dumps(previous)
    ):
        with patch("scripts.bootstrap_secrets.subprocess.run") as run:
            main()
    change = json.loads(run.call_args.kwargs["input"])
    assert "metadata" not in change
    assert set(change["data"]) == {"METABASE_SESSION_SECRET"}


def test_repeat_bootstrap_does_not_mutate_existing_keys():
    previous = {"data": {"METABASE_ENCRYPTION_KEY": "key", "METABASE_SESSION_SECRET": "session"}}
    with patch(
        "scripts.bootstrap_secrets.subprocess.check_output", return_value=json.dumps(previous)
    ):
        with patch("scripts.bootstrap_secrets.subprocess.run") as run:
            main()
    run.assert_not_called()
