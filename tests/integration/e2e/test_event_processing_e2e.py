"""Layer 2 — Event Processing: async pipeline + rate limiter under concurrency."""

from __future__ import annotations

import asyncio

import pytest

from harness import make_raw_frames, SlidingWindowRateLimiter, CircuitBreaker  # type: ignore

pytestmark = pytest.mark.e2e_layer(name="event_processing")


def test_pipeline_processes_all_tuples_without_loss(layer_report, metrics, sut):
    metrics.start()
    try:
        frames = make_raw_frames(2000, 1_700_000_000)
        tuples = sut.ingest(frames)
        written = asyncio.run(sut.process(tuples))
        assert written == len(tuples), f"lost {len(tuples) - written} tuples"
        # Allow the background flush to drain to the DB.
        import time

        time.sleep(0.5)
        assert sut.db_count() >= len(tuples)
        layer_report.checks = 2
        layer_report.metrics["processed"] = written
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0
    layer_report.notes.append("async pipeline + BatchSink persisted every tuple")


def test_rate_limiter_enforces_window(layer_report, metrics):
    metrics.start()
    try:
        limiter = SlidingWindowRateLimiter(window_size_s=1.0, max_requests=10)
        allowed = sum(1 for _ in range(10) if limiter.allow("k").allowed)
        assert allowed == 10
        assert limiter.allow("k").allowed is False
        layer_report.checks = 2
        layer_report.metrics["allowed_in_window"] = allowed
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0


def test_pipeline_backpressure_under_surge(layer_report, metrics, sut):
    """The semaphore-guarded pipeline must bound concurrency, not drop work."""
    metrics.start()
    try:
        frames = make_raw_frames(5000, 1_700_000_000)
        tuples = sut.ingest(frames)
        written = asyncio.run(sut.process(tuples, rate_limit_key="surge"))
        assert written == len(tuples)
        layer_report.checks = 1
        layer_report.metrics["surge_processed"] = written
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0


def test_circuit_breaker_available_if_psutil(layer_report, metrics):
    metrics.start()
    try:
        cb = CircuitBreaker()
        assert cb.call(lambda: 42) == 42
        layer_report.checks = 1
        layer_report.metrics["circuit_breaker"] = "ok"
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0
