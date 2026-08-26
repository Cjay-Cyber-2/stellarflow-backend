"""Layer 4 — API: tamper-evident admin audit trail."""

from __future__ import annotations

import pytest

from api.admin_audit import AdminActor, ClientInfo  # type: ignore

pytestmark = pytest.mark.e2e_layer(name="api")


def test_audit_records_and_verifies_chain(layer_report, metrics, sut):
    metrics.start()
    try:
        actor = AdminActor(user_id="u1", user_name="alice", user_role="admin")
        client = ClientInfo(ip_address="10.0.0.1", user_agent="e2e")
        for i in range(20):
            sut.audit.record(
                command="config.set",
                actor=actor,
                client=client,
                before={"v": i},
                after={"v": i + 1},
                params={"key": "rate_limit"},
            )
        result = sut.audit.verify_chain()
        assert result.valid, f"chain broken: {result.reason}"
        assert result.entries_checked == 20
        layer_report.checks = 2
        layer_report.metrics["entries"] = result.entries_checked
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0
    layer_report.notes.append("20 audit entries chained and verified")


def test_audit_detects_tampering(layer_report, metrics, sut):
    metrics.start()
    try:
        sut.audit.record(
            command="config.set",
            actor=AdminActor(user_id="u1", user_name="alice", user_role="admin"),
            client=ClientInfo(ip_address="10.0.0.1"),
            before={"v": 0},
            after={"v": 1},
        )
        # Tamper with the on-disk log.
        path = sut.audit.log_path
        text = path.read_text("utf-8")
        tampered = text.replace('"seq": 1', '"seq": 999', 1)
        path.write_text(tampered, "utf-8")
        result = sut.audit.verify_chain()
        assert not result.valid, "tampering was not detected"
        layer_report.checks = 1
        layer_report.metrics["tamper_detected"] = (not result.valid)
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0
