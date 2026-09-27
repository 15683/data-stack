from pipeline.backfill import windows
from pipeline.health import Health


def test_backfill_covers_long_window_with_boundary_overlap():
    assert list(windows("2026-01-01", "2026-03-05")) == [
        ("2026-01-01", "2026-01-31"),
        ("2026-01-31", "2026-03-02"),
        ("2026-03-02", "2026-03-05"),
    ]


def test_readiness_failure_does_not_kill_live_process():
    state = Health()
    assert state.live()
    assert not state.ready()
    state.dependencies_ready = True
    assert state.ready()
    state.last_loop -= 121
    assert not state.live()
    assert not state.ready()


def test_verification_rejects_nonpositive_wait_settings():
    import pytest

    from pipeline.report import wait_for_ingestion

    for timeout, interval in [(0, 1), (1, 0), (-1, 1)]:
        with pytest.raises(ValueError, match="must be positive"):
            wait_for_ingestion("raw/test.json.gz", timeout=timeout, interval=interval)
