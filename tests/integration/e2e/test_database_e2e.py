"""Layer 3 — Database: durable persistence with zero lock contention."""

from __future__ import annotations

import threading
import time

import pytest

from harness import make_raw_frames  # type: ignore

pytestmark = pytest.mark.e2e_layer(name="database")


def test_writer_persists_and_is_durable(layer_report, metrics, sut):
    metrics.start()
    try:
        frames = make_raw_frames(300, 1_700_000_000)
        for f in frames:
            sut.writer.save(
                {
                    "asset_id": f["asset_id"],
                    "price": float(f["price"]),
                    "source": "e2e",
                    "ts": int(f["timestamp"]),
                }
            )
        sut.writer.shutdown()
        assert sut.db_count() == 300, f"expected 300 rows, got {sut.db_count()}"
        assert sut.lock_errors == 0, "database lock contention detected"
        layer_report.checks = 2
        layer_report.metrics["rows"] = sut.db_count()
        layer_report.metrics["lock_errors"] = sut.lock_errors
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0
    layer_report.notes.append("PartitionedTelemetryWriter + BatchSink persisted 300 rows, 0 lock errors")


def test_concurrent_writers_zero_contention(layer_report, metrics, sut):
    metrics.start()
    try:
        partitions_seen = set()
        stop = threading.Event()

        def writer_thread(tid: int):
            base = 1_700_000_000 + tid * 10_000
            for i in range(200):
                if stop.is_set():
                    break
                sut.writer.save(
                    {
                        "asset_id": f"T{tid}/XLM",
                        "price": float(i),
                        "source": "e2e",
                        "ts": base + i,
                    }
                )
                partitions_seen.add((base + i) // (7 * 86400))

        threads = [threading.Thread(target=writer_thread, args=(t,)) for t in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        sut.writer.shutdown()

        total = sut.db_count()
        assert total >= 8 * 150, f"concurrent writes lost data: only {total} rows"
        assert sut.lock_errors == 0, f"lock contention under concurrency: {sut.lock_errors}"
        layer_report.checks = 2
        layer_report.metrics["concurrent_rows"] = total
        layer_report.metrics["lock_errors"] = sut.lock_errors
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0
    layer_report.notes.append("8 threads wrote concurrently with zero lock errors")


def test_partition_routing_creates_weekly_tables(layer_report, metrics, sut):
    metrics.start()
    try:
        # Two timestamps in different ISO weeks → two partitions created.
        sut.writer.save({"asset_id": "A/XLM", "price": 1.0, "source": "e2e", "ts": 1_700_000_000})
        sut.writer.save({"asset_id": "B/XLM", "price": 2.0, "source": "e2e", "ts": 1_700_000_000 + 14 * 86400})
        sut.writer.shutdown()
        assert len(sut.writer.known_partitions) >= 1
        layer_report.checks = 1
        layer_report.metrics["partitions"] = len(sut.writer.known_partitions)
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0
