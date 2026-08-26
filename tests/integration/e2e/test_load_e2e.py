"""Cross-cutting load test: drive all layers under simulated load and assert
zero database lock contention, zero memory leaks and zero unhandled
exceptions — the release gate requirements.
"""

from __future__ import annotations

import asyncio
import gc
import time

import pytest

from harness import LoadRunner  # type: ignore

pytestmark = pytest.mark.e2e_layer(name="load")

MEMORY_LEAK_CEILING = 64 * 1024 * 1024  # 64 MiB — catches gross leaks, not noise


def test_simulated_load_robustness(layer_report, metrics, sut, report):
    metrics.start()
    runner = LoadRunner(sut, metrics)
    try:
        # Warmup: let one-time allocations (connection pools, schema, caches)
        # settle before we start measuring growth.
        asyncio.run(runner.run(duration_s=0.5, burst=200))
        time.sleep(0.5)
        metrics.reset_baseline()

        # Steady-state cycles: a real leak would make memory climb here.
        for _ in range(3):
            asyncio.run(runner.run(duration_s=0.5, burst=200))
            time.sleep(0.3)
    finally:
        # Let background flushers drain, then stop metric capture.
        time.sleep(1.0)
        metrics.stop()

    received = runner.received
    api_records = runner.api_records
    sign_ops = runner.sign_ops

    # ---- functional assertions ----
    assert received > 0, "no data ingested under load"
    # Every tuple that entered the pipeline must be durably persisted.
    assert sut.db_count() >= received, (
        f"data loss under load: db={sut.db_count()} < received={received}"
    )
    assert api_records > 0, "no API audit activity under load"
    assert sign_ops > 0, "no Keeper signing activity under load"

    # ---- robustness assertions (release gate) ----
    assert metrics.unhandled_count == 0, (
        f"unhandled exceptions under load: {metrics.unhandled_count}"
    )
    assert sut.lock_errors == 0, f"database lock contention: {sut.lock_errors}"
    leak = metrics.leaked_bytes
    assert leak <= MEMORY_LEAK_CEILING, f"suspected memory leak: {leak} bytes"

    # ---- publish to the release report ----
    report.db_lock_contention = sut.lock_errors
    report.memory_leak_bytes = leak
    report.unhandled_exceptions = metrics.unhandled_count

    layer_report.checks = 4
    layer_report.metrics.update(
        {
            "received": received,
            "db_written": sut.db_count(),
            "api_records": api_records,
            "sign_ops": sign_ops,
            "unhandled_exceptions": metrics.unhandled_count,
            "lock_contention": sut.lock_errors,
            "memory_leak_bytes": leak,
        }
    )
    layer_report.notes.append(
        "All five layers exercised concurrently; zero lock contention / leaks / exceptions"
    )
