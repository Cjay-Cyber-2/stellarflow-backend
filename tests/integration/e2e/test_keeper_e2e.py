"""Layer 5 — Keeper: secure secret safekeeping, signing and zeroisation."""

from __future__ import annotations

import pytest

from state.keeper import KeyKeeper, SecretNotFoundError  # type: ignore

pytestmark = pytest.mark.e2e_layer(name="keeper")


def test_keeper_sign_and_verify(layer_report, metrics, sut):
    metrics.start()
    try:
        sut.keeper.put("stellar_signer", b"super-secret-key-material")
        assert sut.keeper.has("stellar_signer")
        sig = sut.keeper.sign("stellar_signer", b"transfer 100 XLM")
        assert sut.keeper.verify("stellar_signer", b"transfer 100 XLM", sig)
        assert not sut.keeper.verify("stellar_signer", b"tampered", sig)
        # A different secret cannot forge another's signature.
        sut.keeper.put("other", b"other-material")
        other_sig = sut.keeper.sign("other", b"transfer 100 XLM")
        assert not sut.keeper.verify("stellar_signer", b"transfer 100 XLM", other_sig)
        layer_report.checks = 3
        layer_report.metrics["signers"] = 2
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0
    layer_report.notes.append("HMAC signing scoped per-secret; cross-forgery rejected")


def test_keeper_secure_wipe_zeroises_memory(layer_report, metrics, sut):
    metrics.start()
    try:
        sut.keeper.put("victim", b"plaintext-secret")
        handle = sut.keeper._secrets["victim"]
        sut.keeper.delete("victim")
        # After delete the buffer is zeroised and removed.
        assert not sut.keeper.has("victim")
        assert len(handle) == 0, "secret bytes were not zeroised on delete"
        with pytest.raises(SecretNotFoundError):
            sut.keeper.sign("victim", b"x")
        layer_report.checks = 2
        layer_report.metrics["zeroised"] = True
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0


def test_keeper_state_persist_contains_no_secrets(layer_report, metrics, sut):
    metrics.start()
    try:
        sut.keeper.put("signer-a", b"secret-a")
        sut.keeper.put("signer-b", b"secret-b")
        path = sut.keeper.persist_state()
        text = path.read_text("utf-8")
        assert "secret-a" not in text and "secret-b" not in text
        assert '"enrollments"' in text
        layer_report.checks = 2
        layer_report.metrics["enrollments"] = len(sut.keeper.list_enrollments())
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0


def test_keeper_root_rotation_invalidates_old_signatures(layer_report, metrics, sut):
    metrics.start()
    try:
        sut.keeper.put("k", b"secret")
        sig_old = sut.keeper.sign("k", b"msg")
        sut.keeper.rotate_root_key(b"new-root")
        # Derivation changed → old signature no longer verifies.
        assert not sut.keeper.verify("k", b"msg", sig_old)
        assert sut.keeper.verify("k", b"msg", sut.keeper.sign("k", b"msg"))
        layer_report.checks = 1
        layer_report.metrics["rotated"] = True
    finally:
        metrics.stop()
    assert metrics.unhandled_count == 0
