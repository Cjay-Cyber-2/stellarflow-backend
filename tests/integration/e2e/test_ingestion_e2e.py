"""Layer 1 — Ingestion: raw ticker frames → normalised telemetry tuples."""

from __future__ import annotations

import pytest

from harness import make_raw_frames  # type: ignore

pytestmark = pytest.mark.e2e_layer(name="ingestion")


def test_ingestion_flattens_single_and_batched_frames(layer_report, metrics, sut):
    metrics.start()
    try:
        frames = [
            {"asset_id": "NGN/XLM", "price": 1234.5, "timestamp": 1700000000, "sequence": 1},
            {"frames": [
                {"asset_id": "KES/XLM", "price": 42.0, "timestamp": 1700000001, "sequence": 2},
                {"asset_id": "GHS/XLM", "price": 7.7, "timestamp": 1700000002, "sequence": 3},
            ]},
        ]
        tuples = sut.ingest(frames)
        assert len(tuples) == 3, f"expected 3 tuples, got {len(tuples)}"
        for t in tuples:
            assert len(t) >= 3
            assert isinstance(t[0], str) and t[0]
            assert isinstance(t[1], (int, float))
        layer_report.checks = 3
        layer_report.metrics["tuples"] = len(tuples)
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0
    layer_report.notes.append("ingestion parser normalised nested + batched frames")


def test_ingestion_drops_invalid_frames(layer_report, metrics, sut):
    metrics.start()
    try:
        frames = [
            {"asset_id": "NGN/XLM", "price": 1.0, "timestamp": 1, "sequence": 1},
            {"foo": "bar"},  # not a ticker frame
            {"asset_id": "USD/XLM"},  # missing price
        ]
        tuples = sut.ingest(frames)
        assert len(tuples) == 1, f"invalid frames should be dropped, got {len(tuples)}"
        layer_report.checks = 2
        layer_report.metrics["valid"] = 1
        layer_report.metrics["dropped"] = 2
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0


def test_ingestion_high_volume(layer_report, metrics, sut):
    metrics.start()
    try:
        frames = make_raw_frames(5000, 1_700_000_000)
        tuples = sut.ingest(frames)
        assert len(tuples) == 5000
        layer_report.checks = 1
        layer_report.metrics["volume"] = len(tuples)
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0
