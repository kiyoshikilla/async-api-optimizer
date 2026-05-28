import pytest

from async_api_optimizer.benchmark.metrics import LatencyMetrics


def test_metrics_percentiles() -> None:
    metrics = LatencyMetrics()
    for value in [0.1, 0.2, 0.3, 0.4]:
        metrics.record(value)
    summary = metrics.summary()
    assert summary["p50"] == pytest.approx(0.25)
    assert summary["p95"] > summary["p50"]


def test_metrics_empty() -> None:
    metrics = LatencyMetrics()
    summary = metrics.summary()
    assert summary["count"] == 0.0
    assert summary["p99"] == 0.0


def test_metrics_rejects_negative() -> None:
    metrics = LatencyMetrics()
    with pytest.raises(ValueError):
        metrics.record(-0.1)
