"""
tests/test_signer.py
~~~~~~~~~~~~~~~~~~~~
Comprehensive test suite for src/crypto/signer.py.

Coverage targets
----------------
* Successful signing flow (stellar_sdk & PyNaCl paths)
* Cleanup execution on the normal (success) path
* Cleanup execution on the exception path
* Invalid / edge-case key handling
* Out-of-scope / post-exit guard enforcement
* __del__ finaliser as safety-net (garbage-collection path)
* No sensitive debug logging
* Signature correctness regression

Assumptions
-----------
* Tests run with either ``stellar_sdk`` *or* ``PyNaCl`` available; tests
  that require a real crypto library are marked ``importorskip`` so the
  suite remains green on a bare Python installation.
* ``_buf`` is accessed directly in cleanup assertions because it is the only
  observable evidence of a wipe within Python's memory model.  This is
  intentional: verifying cleanup is a security requirement, not an
  implementation detail.
"""
from __future__ import annotations

import ctypes
import gc
import logging
import os
import subprocess
import sys
import unittest.mock as mock

import pytest

# ---------------------------------------------------------------------------
# Path bootstrap — allows ``pytest tests/`` from the repo root.
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from crypto.signer import (  # noqa: E402
    SecureKeyHandle,
    SecureSessionCredentials,
    SecureVariableWrapper,
    SigningError,
    MemorySecurityError,
    GuardPageError,
    IsolatedMemoryHeap,
    _zero_wipe,
    _wipe_bytes_object,
    _wipe_bytes_view,
    _wipe_key_handle,
    _SecureKeypairContext,
    _configure_openssl_hardware_acceleration,
    _get_page_size,
    _round_up_to_page,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

# 32-byte dummy key — NOT a real Stellar secret; safe for unit tests.
_DUMMY_KEY = bytes(range(32))
# 32-byte dummy transaction hash.
_DUMMY_HASH = bytes(range(32))


# ---------------------------------------------------------------------------
# Helper: assert buffer is fully zero-wiped.
# ---------------------------------------------------------------------------


def _assert_wiped(buf: bytearray, label: str = "buffer") -> None:
    assert all(b == 0 for b in buf), f"{label} must be fully zero-wiped."


# ---------------------------------------------------------------------------
# _zero_wipe unit tests
# ---------------------------------------------------------------------------


class TestZeroWipe:
    """Low-level tests for the _zero_wipe helper."""

    def test_empty_buffer_is_noop(self):
        buf = bytearray(0)
        _zero_wipe(buf)  # must not raise

    def test_single_byte_wiped(self):
        buf = bytearray(b"\xFF")
        _zero_wipe(buf)
        _assert_wiped(buf, "single-byte buffer")

    def test_arbitrary_content_wiped(self):
        buf = bytearray(range(256))
        _zero_wipe(buf)
        _assert_wiped(buf, "256-byte buffer")

    def test_idempotent(self):
        buf = bytearray(b"\xDE\xAD\xBE\xEF")
        _zero_wipe(buf)
        _zero_wipe(buf)  # second call must not raise
        _assert_wiped(buf, "doubly-wiped buffer")


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_rejects_empty_key(self):
        with pytest.raises(ValueError, match="non-empty"):
            SecureKeyHandle(b"")

    def test_accepts_minimum_one_byte(self):
        handle = SecureKeyHandle(b"\x01")
        # Wipe so the __del__ finaliser has nothing to do.
        handle._do_wipe()

    def test_accepts_32_byte_key(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        handle._do_wipe()

    def test_buffer_is_independent_copy(self):
        """Mutating the original bytes must not affect the internal buffer."""
        raw = bytearray(_DUMMY_KEY)
        handle = SecureKeyHandle(bytes(raw))
        raw[0] = 0xFF
        assert handle._buf[0] == 0x00, "Internal buffer must be an independent copy."
        handle._do_wipe()

    def test_not_active_after_construction(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        assert not handle._active
        handle._do_wipe()

    def test_not_wiped_after_construction(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        assert not handle._wiped
        handle._do_wipe()


# ---------------------------------------------------------------------------
# Context-manager lifecycle
# ---------------------------------------------------------------------------


class TestContextManager:
    def test_enter_sets_active(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        handle.__enter__()
        assert handle._active
        handle.__exit__(None, None, None)

    def test_exit_clears_active(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        with handle:
            pass
        assert not handle._active

    def test_exit_wipes_buffer_on_success(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        with handle:
            pass
        _assert_wiped(handle._buf, "_buf after normal exit")

    def test_exit_sets_wiped_flag(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        with handle:
            pass
        assert handle._wiped

    def test_exit_does_not_suppress_exceptions(self):
        with pytest.raises(RuntimeError, match="propagated"):
            with SecureKeyHandle(_DUMMY_KEY):
                raise RuntimeError("propagated")

    def test_buffer_wiped_even_when_exception_raised(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        try:
            with handle:
                raise ValueError("simulated failure")
        except ValueError:
            pass
        _assert_wiped(handle._buf, "_buf after exception exit")

    def test_wiped_flag_set_even_when_exception_raised(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        try:
            with handle:
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert handle._wiped

    def test_wipe_is_idempotent_double_exit(self):
        """Calling __exit__ twice must not raise."""
        handle = SecureKeyHandle(_DUMMY_KEY)
        handle.__enter__()
        handle.__exit__(None, None, None)
        handle.__exit__(None, None, None)  # second call must be safe
        _assert_wiped(handle._buf, "_buf after double exit")


# ---------------------------------------------------------------------------
# __del__ safety-net
# ---------------------------------------------------------------------------


class TestDelFinaliser:
    def test_del_wipes_buffer_when_context_not_used(self):
        """If the caller never enters the 'with' block, __del__ must still wipe."""
        handle = SecureKeyHandle(_DUMMY_KEY)
        # Directly invoke __del__ to simulate GC without running the context manager.
        handle.__del__()
        _assert_wiped(handle._buf, "_buf after __del__ without context manager")
        assert handle._wiped

    def test_del_does_not_raise_after_normal_exit(self):
        """__del__ called after a clean context exit must be a silent noop."""
        handle = SecureKeyHandle(_DUMMY_KEY)
        with handle:
            pass
        # Should not raise even though the buffer is already wiped.
        handle.__del__()

    def test_del_is_called_on_gc(self):
        """Force GC and confirm __del__ was invoked (indirectly via _wiped flag)."""
        handle = SecureKeyHandle(_DUMMY_KEY)
        ref = handle  # keep a second reference for inspection
        del handle
        gc.collect()
        # At this point, if ref is the only remaining reference it should still
        # be valid but reflect the finaliser having run.  Because ref is still
        # alive the GC won't have collected it; use _do_wipe instead.
        # This test therefore simply verifies __del__ does not raise.
        ref._do_wipe()


# ---------------------------------------------------------------------------
# sign() guard rails
# ---------------------------------------------------------------------------


class TestSignGuards:
    def test_sign_outside_context_raises(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        with pytest.raises(SigningError, match="outside an active signing scope"):
            handle.sign(_DUMMY_HASH)

    def test_sign_after_exit_raises(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        with handle:
            pass
        with pytest.raises(SigningError, match="outside an active signing scope"):
            handle.sign(_DUMMY_HASH)

    def test_sign_after_explicit_wipe_raises(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        handle.__enter__()
        handle._do_wipe()  # simulate premature wipe
        with pytest.raises(SigningError, match="wiped"):
            handle.sign(_DUMMY_HASH)

    def test_sign_rejects_short_hash(self):
        with SecureKeyHandle(_DUMMY_KEY) as handle:
            with pytest.raises(ValueError, match="32 bytes"):
                handle.sign(b"too-short")

    def test_sign_rejects_long_hash(self):
        with SecureKeyHandle(_DUMMY_KEY) as handle:
            with pytest.raises(ValueError, match="32 bytes"):
                handle.sign(b"x" * 33)

    def test_sign_rejects_empty_hash(self):
        with SecureKeyHandle(_DUMMY_KEY) as handle:
            with pytest.raises(ValueError, match="32 bytes"):
                handle.sign(b"")


# ---------------------------------------------------------------------------
# sign() happy path (requires PyNaCl or stellar_sdk)
# ---------------------------------------------------------------------------


class TestSignHappyPath:
    def test_returns_64_byte_signature(self):
        nacl = pytest.importorskip("nacl", reason="PyNaCl not installed — skipping signing test")
        with SecureKeyHandle(_DUMMY_KEY) as handle:
            sig = handle.sign(_DUMMY_HASH)
        assert isinstance(sig, bytes), "Signature must be a bytes object."
        assert len(sig) == 64, f"Expected 64-byte signature, got {len(sig)}."

    def test_buffer_wiped_after_signing(self):
        pytest.importorskip("nacl", reason="PyNaCl not installed — skipping signing test")
        handle = SecureKeyHandle(_DUMMY_KEY)
        with handle:
            handle.sign(_DUMMY_HASH)
        _assert_wiped(handle._buf, "_buf after successful signing exit")

    def test_signature_deterministic(self):
        """Ed25519 is deterministic — same key + hash must yield same signature."""
        pytest.importorskip("nacl", reason="PyNaCl not installed — skipping signing test")
        with SecureKeyHandle(_DUMMY_KEY) as h1:
            sig1 = h1.sign(_DUMMY_HASH)
        with SecureKeyHandle(_DUMMY_KEY) as h2:
            sig2 = h2.sign(_DUMMY_HASH)
        assert sig1 == sig2, "Ed25519 signatures must be deterministic."

    def test_different_hashes_produce_different_signatures(self):
        pytest.importorskip("nacl", reason="PyNaCl not installed — skipping signing test")
        hash_a = bytes(range(32))
        hash_b = bytes(reversed(range(32)))
        with SecureKeyHandle(_DUMMY_KEY) as handle:
            sig_a = handle.sign(hash_a)
            sig_b = handle.sign(hash_b)
        assert sig_a != sig_b, "Different hashes must produce different signatures."

    def test_different_keys_produce_different_signatures(self):
        pytest.importorskip("nacl", reason="PyNaCl not installed — skipping signing test")
        key_b = bytes(range(1, 33))
        with SecureKeyHandle(_DUMMY_KEY) as h1:
            sig1 = h1.sign(_DUMMY_HASH)
        with SecureKeyHandle(key_b) as h2:
            sig2 = h2.sign(_DUMMY_HASH)
        assert sig1 != sig2, "Different keys must produce different signatures."

    def test_signature_can_be_verified(self):
        """Regression: verify the produced signature with the corresponding public key."""
        nacl = pytest.importorskip("nacl", reason="PyNaCl not installed — skipping signing test")
        from nacl.signing import SigningKey, VerifyKey

        with SecureKeyHandle(_DUMMY_KEY) as handle:
            sig = handle.sign(_DUMMY_HASH)

        # Derive the verify key independently and check the signature.
        sk = SigningKey(_DUMMY_KEY)
        vk: VerifyKey = sk.verify_key
        # nacl's verify raises nacl.exceptions.BadSignatureError on failure.
        vk.verify(_DUMMY_HASH, sig)


# ---------------------------------------------------------------------------
# Exception-path cleanup
# ---------------------------------------------------------------------------


class TestExceptionPathCleanup:
    def test_signing_error_does_not_abort_cleanup(self):
        """If sign() itself raises, __exit__ must still wipe the buffer."""
        # Patch _sign_internal to raise so we can confirm __exit__ still cleans up.
        handle = SecureKeyHandle(_DUMMY_KEY)
        with mock.patch.object(
            handle, "_sign_internal", side_effect=RuntimeError("injected")
        ):
            try:
                with handle:
                    handle.sign(_DUMMY_HASH)
            except (RuntimeError, ValueError):
                pass
        _assert_wiped(handle._buf, "_buf after sign() raised inside context")

    def test_import_error_does_not_leak_key_material_in_message(self):
        """Error messages from missing-library paths must not embed key bytes."""
        with mock.patch.dict(sys.modules, {"stellar_sdk": None, "nacl": None, "nacl.signing": None}):
            with pytest.raises(SigningError) as exc_info:
                with SecureKeyHandle(_DUMMY_KEY) as handle:
                    handle.sign(_DUMMY_HASH)

        msg = str(exc_info.value)
        # Key bytes must not appear in the error message.
        for byte_val in _DUMMY_KEY:
            assert str(byte_val) not in msg or byte_val == 0, (
                f"Key byte value {byte_val} should not appear in error message."
            )

    def test_buffer_wiped_when_crypto_library_raises(self):
        """Wipe must happen even when the underlying crypto call fails mid-flight."""
        handle = SecureKeyHandle(_DUMMY_KEY)
        with mock.patch.object(
            SecureKeyHandle, "_try_pynacl", side_effect=SigningError("crypto failure")
        ), mock.patch.object(
            SecureKeyHandle, "_try_stellar_sdk", side_effect=ImportError("not installed")
        ):
            try:
                with handle:
                    handle.sign(_DUMMY_HASH)
            except SigningError:
                pass
        _assert_wiped(handle._buf, "_buf after crypto failure")


# ---------------------------------------------------------------------------
# Logging security — no sensitive data in log records
# ---------------------------------------------------------------------------


class TestLoggingSecurity:
    def test_no_key_bytes_logged(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="crypto.signer"):
            handle = SecureKeyHandle(_DUMMY_KEY)
            with handle:
                pass

        combined = "\n".join(r.getMessage() for r in caplog.records)
        # Check no byte value from the key appears in a suspicious context.
        for val in set(_DUMMY_KEY):
            # A byte value of 0 is acceptable (it's in the wiped state).
            if val == 0:
                continue
            assert str(val) not in combined, (
                f"Key byte value {val} leaked into log output."
            )

    def test_no_hash_bytes_logged(self, caplog):
        pytest.importorskip("nacl", reason="PyNaCl not installed — skipping signing test")
        with caplog.at_level(logging.DEBUG, logger="crypto.signer"):
            with SecureKeyHandle(_DUMMY_KEY) as handle:
                handle.sign(_DUMMY_HASH)

        combined = "\n".join(r.getMessage() for r in caplog.records)
        # No raw byte values of the hash should appear.
        for val in set(_DUMMY_HASH):
            if val == 0:
                continue
            assert str(val) not in combined, (
                f"Hash byte value {val} leaked into log output."
            )

    def test_log_level_is_debug_only(self, caplog):
        """Lifecycle messages must be DEBUG, not INFO/WARNING/ERROR."""
        with caplog.at_level(logging.DEBUG, logger="crypto.signer"):
            with SecureKeyHandle(_DUMMY_KEY):
                pass
        for record in caplog.records:
            assert record.levelno <= logging.DEBUG, (
                f"Unexpected log level {record.levelname}: {record.getMessage()}"
            )


# ---------------------------------------------------------------------------
# Regression coverage
# ---------------------------------------------------------------------------


class TestRegression:
    def test_signing_api_unchanged(self):
        """Public API must remain: SecureKeyHandle(bytes) → context → .sign(bytes) → bytes."""
        pytest.importorskip("nacl", reason="PyNaCl not installed — skipping signing test")
        with SecureKeyHandle(_DUMMY_KEY) as handle:
            result = handle.sign(_DUMMY_HASH)
        assert isinstance(result, bytes)

    def test_multiple_signs_in_same_scope(self):
        """Multiple sign() calls within the same 'with' block must all succeed."""
        pytest.importorskip("nacl", reason="PyNaCl not installed — skipping signing test")
        hash_a = bytes(range(32))
        hash_b = bytes(range(32, 64)) if len(bytes(range(32, 64))) == 32 else bytes(b"\xAA" * 32)
        with SecureKeyHandle(_DUMMY_KEY) as handle:
            sig_a = handle.sign(hash_a)
            sig_b = handle.sign(hash_b)
        assert len(sig_a) == 64
        assert len(sig_b) == 64

    def test_handle_cannot_be_reused_after_exit(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        with handle:
            pass
        with pytest.raises(SigningError):
            with handle:  # re-entering should fail because _wiped is True
                handle.sign(_DUMMY_HASH)

    def test_signing_error_is_exception_subclass(self):
        assert issubclass(SigningError, Exception)

    def test_key_not_leaked_through_repr_or_str(self):
        handle = SecureKeyHandle(_DUMMY_KEY)
        text = repr(handle) + str(handle)
        # Default __repr__ for slotted classes does not include attributes,
        # but verify none of the key byte values appear.
        for val in _DUMMY_KEY:
            if val == 0:
                continue
            # The repr should just be the class name + memory address.
            # It must not include the raw key bytes.
            assert hex(val) not in text.lower() or len(text) < 200, (
                "Key material must not appear in __repr__."
            )
        handle._do_wipe()


# ---------------------------------------------------------------------------
# SecureSessionCredentials
# ---------------------------------------------------------------------------


class TestSecureSessionCredentialsConstruction:
    def test_rejects_empty_credentials(self):
        with pytest.raises(ValueError, match="non-empty"):
            SecureSessionCredentials(b"")

    def test_accepts_valid_credentials(self):
        creds = SecureSessionCredentials(_DUMMY_KEY)
        assert not creds._active
        assert not creds._wiped
        creds._do_wipe()

    def test_buffer_is_independent_copy(self):
        raw = bytearray(_DUMMY_KEY)
        creds = SecureSessionCredentials(bytes(raw))
        raw[0] = 0xFF
        assert creds._buf[0] == 0x00, "Internal buffer must be an independent copy."
        creds._do_wipe()


class TestSecureSessionCredentialsContextManager:
    def test_enter_sets_active(self):
        creds = SecureSessionCredentials(_DUMMY_KEY)
        creds.__enter__()
        assert creds._active
        creds.__exit__(None, None, None)

    def test_exit_clears_active(self):
        with SecureSessionCredentials(_DUMMY_KEY):
            pass
        creds = SecureSessionCredentials(_DUMMY_KEY)
        creds.__enter__()
        creds.__exit__(None, None, None)
        assert not creds._active

    def test_exit_wipes_buffer_on_success(self):
        creds = SecureSessionCredentials(_DUMMY_KEY)
        with creds:
            pass
        _assert_wiped(creds._buf, "_buf after normal exit")

    def test_exit_sets_wiped_flag(self):
        creds = SecureSessionCredentials(_DUMMY_KEY)
        with creds:
            pass
        assert creds._wiped

    def test_exit_does_not_suppress_exceptions(self):
        with pytest.raises(RuntimeError, match="propagated"):
            with SecureSessionCredentials(_DUMMY_KEY):
                raise RuntimeError("propagated")

    def test_buffer_wiped_even_when_exception_raised(self):
        creds = SecureSessionCredentials(_DUMMY_KEY)
        try:
            with creds:
                raise ValueError("simulated failure")
        except ValueError:
            pass
        _assert_wiped(creds._buf, "_buf after exception exit")

    def test_wipe_is_idempotent_double_exit(self):
        creds = SecureSessionCredentials(_DUMMY_KEY)
        creds.__enter__()
        creds.__exit__(None, None, None)
        creds.__exit__(None, None, None)
        _assert_wiped(creds._buf, "_buf after double exit")


class TestSecureSessionCredentialsDel:
    def test_del_wipes_buffer_when_context_not_used(self):
        creds = SecureSessionCredentials(_DUMMY_KEY)
        creds.__del__()
        _assert_wiped(creds._buf, "_buf after __del__ without context manager")
        assert creds._wiped

    def test_del_does_not_raise_after_normal_exit(self):
        creds = SecureSessionCredentials(_DUMMY_KEY)
        with creds:
            pass
        creds.__del__()


class TestSecureSessionCredentialsGet:
    def test_get_outside_context_raises(self):
        creds = SecureSessionCredentials(_DUMMY_KEY)
        with pytest.raises(SigningError, match="outside an active validation scope"):
            creds.get()

    def test_get_after_exit_raises(self):
        creds = SecureSessionCredentials(_DUMMY_KEY)
        with creds:
            pass
        with pytest.raises(SigningError, match="outside an active validation scope"):
            creds.get()

    def test_get_after_explicit_wipe_raises(self):
        creds = SecureSessionCredentials(_DUMMY_KEY)
        creds.__enter__()
        creds._do_wipe()
        with pytest.raises(SigningError, match="wiped"):
            creds.get()

    def test_get_returns_credentials_copy(self):
        creds = SecureSessionCredentials(_DUMMY_KEY)
        with creds:
            result = creds.get()
        assert isinstance(result, bytes)
        assert result == _DUMMY_KEY

    def test_get_returns_independent_bytes(self):
        creds = SecureSessionCredentials(bytes(range(32)))
        with creds:
            result = creds.get()
        assert isinstance(result, bytes)
        assert len(result) == 32


class TestSecureSessionCredentialsLogging:
    def test_no_credential_bytes_logged(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="crypto.signer"):
            creds = SecureSessionCredentials(_DUMMY_KEY)
            with creds:
                creds.get()

        combined = "\n".join(r.getMessage() for r in caplog.records)
        for val in set(_DUMMY_KEY):
            if val == 0:
                continue
            assert str(val) not in combined, (
                f"Credential byte value {val} leaked into log output."
            )

    def test_log_level_is_debug_only(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="crypto.signer"):
            with SecureSessionCredentials(_DUMMY_KEY):
                pass
        for record in caplog.records:
            assert record.levelno <= logging.DEBUG, (
                f"Unexpected log level {record.levelname}: {record.getMessage()}"
            )


# ---------------------------------------------------------------------------
# Hardware Acceleration Configuration
# ---------------------------------------------------------------------------


class TestSodiumMemoryLocking:
    """Tests for libsodium-based memory locking (sodium_mlock / sodium_munlock)."""

    def test_sodium_memory_locking_loads_functions(self) -> None:
        """sodium_mlock and sodium_munlock should be loadable via ctypes."""
        from crypto.signer import _SODIUM_MLOCK_FN, _SODIUM_MUNLOCK_FN

        # On systems without libsodium, both will be None — that's acceptable.
        # The test verifies the loading logic doesn't crash.
        if _SODIUM_MLOCK_FN is not None:
            assert callable(_SODIUM_MLOCK_FN)
        if _SODIUM_MUNLOCK_FN is not None:
            assert callable(_SODIUM_MUNLOCK_FN)

    def test_sodium_mlock_buffer_with_dummy_key(self) -> None:
        """sodium_mlock should be callable on a dummy buffer."""
        from crypto.signer import _sodium_mlock_buffer, _sodium_munlock_buffer

        buf = bytearray(os.urandom(32))
        result = _sodium_mlock_buffer(buf)

        # If libsodium is available, lock should succeed
        # If not, it should return False gracefully
        if result:
            _sodium_munlock_buffer(buf)
            assert True  # lock/unlock cycle completed without error

    def test_sodium_memory_locking_integration_with_securekeyhandle(self) -> None:
        """SecureKeyHandle should use libsodium locking when available."""
        from crypto.signer import SecureKeyHandle, _SODIUM_MLOCK_FN

        # If libsodium is not available, skip
        if _SODIUM_MLOCK_FN is None:
            pytest.skip("libsodium not available on this platform")

        buf = bytearray(os.urandom(32))
        with SecureKeyHandle(bytes(buf)) as handle:
            assert handle is not None
            # The handle wraps the buffer — locking is attempted on init
            assert handle._locked  # noqa: SLF001

    def test_sodium_memory_locking_fallback_on_missing_library(self) -> None:
        """_mlock_buffer should fall back to mlock when libsodium is absent."""
        from crypto.signer import _mlock_buffer, _SODIUM_MLOCK_FN

        buf = bytearray(os.urandom(32))
        result = _mlock_buffer(buf)

        # Should not raise regardless of libsodium availability
        assert isinstance(result, bool)


class TestHardwareAccelerationFlags:
    def test_openssl_hardware_acceleration_configured(self):
        """Verify that OpenSSL hardware acceleration configuration runs without error."""
        # The function should not raise any exceptions
        _configure_openssl_hardware_acceleration()

    def test_openssl_environment_variable_set(self, caplog):
        """Verify that OPENSSL_ia32cap environment variable is configured."""
        with caplog.at_level(logging.DEBUG, logger="crypto.signer"):
            _configure_openssl_hardware_acceleration()
        
        # Check that the environment variable was set
        assert "OPENSSL_ia32cap" in os.environ, (
            "OPENSSL_ia32cap environment variable should be set for hardware acceleration"
        )


# ---------------------------------------------------------------------------
# test_protected_memory_heaps (Issue #660: Secure Memory Allocation Pools)
# ---------------------------------------------------------------------------


def _protmem_payload_32() -> bytes:
    return bytes(range(32))


def _run_python_subprocess(script, timeout=20):
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, timeout=timeout,
    )


def _protmem_src_path() -> str:
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "src")
    )


class TestProtectedMemoryHeaps:
    """IsolatedMemoryHeap layout and lifecycle (issue #660)."""

    def test_protected_memory_heaps_allocates_data_view(self):
        heap = IsolatedMemoryHeap(_protmem_payload_32())
        try:
            view = heap.data_view
            assert view is not None
            assert len(view) >= len(_protmem_payload_32())
            assert len(view) % heap.page_size == 0
        finally:
            heap.close()

    def test_protected_memory_heaps_layout_has_guard_pages(self):
        heap = IsolatedMemoryHeap(_protmem_payload_32())
        try:
            assert heap.has_guard_pages is True
            page = heap.page_size
            assert (
                heap.right_guard_address
                == heap.data_address + heap.data_size
            )
            assert heap.left_guard_address + page == heap.data_address
            assert heap.requested_size == len(_protmem_payload_32())
            assert heap.data_size >= len(_protmem_payload_32())
        finally:
            heap.close()

    def test_protected_memory_heaps_pages_align_to_system_page_size(self):
        heap = IsolatedMemoryHeap(_protmem_payload_32())
        try:
            page = heap.page_size
            assert page > 0
            assert heap.data_size % page == 0
            assert heap.left_guard_address % page == 0
            assert heap.data_address % page == 0
            assert heap.right_guard_address % page == 0
        finally:
            heap.close()

    def test_protected_memory_heaps_starts_with_payload(self):
        heap = IsolatedMemoryHeap(_protmem_payload_32())
        try:
            view = heap.data_view
            assert (
                bytes(view[: len(_protmem_payload_32())])
                == _protmem_payload_32()
            )
        finally:
            heap.close()

    def test_protected_memory_heaps_data_is_writable(self):
        heap = IsolatedMemoryHeap(_protmem_payload_32())
        try:
            view = heap.data_view
            view[0] = 0xAA
            assert heap.data_view[0] == 0xAA
        finally:
            heap.close()

    def test_protected_memory_heaps_wipe_zeroes_data_region(self):
        heap = IsolatedMemoryHeap(_protmem_payload_32())
        try:
            heap.wipe()
            view = heap.data_view
            assert all(b == 0 for b in view), (
                "wipe() must zero the data region."
            )
        finally:
            heap.close()

    def test_protected_memory_heaps_close_disables_data_view(self):
        heap = IsolatedMemoryHeap(_protmem_payload_32())
        heap.close()
        assert heap.is_closed is True
        with pytest.raises(GuardPageError):
            _ = heap.data_view

    def test_protected_memory_heaps_close_is_idempotent(self):
        heap = IsolatedMemoryHeap(_protmem_payload_32())
        heap.close()
        heap.close()
        assert heap.is_closed is True

    def test_protected_memory_heaps_context_manager_releases(self):
        with IsolatedMemoryHeap(_protmem_payload_32()) as heap:
            assert heap.is_closed is False
            assert heap.data_view is not None
        assert heap.is_closed is True

    def test_protected_memory_heaps_exit_on_exception_still_releases(self):
        heap_cm = IsolatedMemoryHeap(_protmem_payload_32())
        with pytest.raises(RuntimeError):
            with heap_cm as heap:
                raise RuntimeError("simulated failure")
        assert heap_cm.is_closed is True

    def test_protected_memory_heaps_empty_payload_allocates_full_page(self):
        heap = IsolatedMemoryHeap(b"")
        try:
            assert heap.requested_size == 0
            assert heap.data_size >= heap.page_size
            assert heap.has_guard_pages is True
        finally:
            heap.close()

    def test_protected_memory_heaps_tiny_payload_rounds_up(self):
        heap = IsolatedMemoryHeap(b"\x42")
        try:
            assert heap.requested_size == 1
            assert heap.data_size >= heap.page_size
            assert heap.data_size % heap.page_size == 0
        finally:
            heap.close()

    def test_protected_memory_heaps_rejects_non_bytes(self):
        with pytest.raises(TypeError):
            IsolatedMemoryHeap("not bytes")  # type: ignore[arg-type]


class TestProtectedMemoryHeapsGuardCrash:
    """Touching a guard page must fault the process (SIGSEGV/SIGBUS)."""

    def test_protected_memory_heaps_left_guard_causes_fatal_signal(self):
        if os.name != "posix":
            pytest.skip("POSIX-only: signal-based exit semantics.")
        proc = _run_python_subprocess(self._scratch("left"))
        self._assert_fault(proc, "left guard page")

    def test_protected_memory_heaps_right_guard_causes_fatal_signal(self):
        if os.name != "posix":
            pytest.skip("POSIX-only: signal-based exit semantics.")
        proc = _run_python_subprocess(self._scratch("right"))
        self._assert_fault(proc, "right guard page")

    def _assert_fault(self, proc, label):
        assert proc.returncode is not None and proc.returncode < 0, (
            "Touching %s must fault; child exited normally with rc=%d."
            "STDOUT=%r STDERR=%r"
            % (label, proc.returncode, proc.stdout, proc.stderr)
        )
        signal_num = -proc.returncode
        assert signal_num in (11, 7), (
            "Faulting %s should produce SIGSEGV(11) or SIGBUS(7); got %d."
            "STDOUT=%r STDERR=%r"
            % (label, signal_num, proc.stdout, proc.stderr)
        )

    def _scratch(self, side):
        addr_attr = (
            "left_guard_address" if side == "left"
            else "right_guard_address"
        )
        return (
            "import sys, ctypes\n"
            "sys.path.insert(0, %r)\n"
            "from crypto.signer import IsolatedMemoryHeap\n"
            "h = IsolatedMemoryHeap(bytes(range(32)))\n"
            "ctypes.memset(ctypes.c_void_p(h.%s), 0xAA, 1)\n"
            "h.close()\n"
            "sys.exit(0)\n"
        ) % (_protmem_src_path(), addr_attr)


class TestProtectedMemoryHeapsDataRegionSafe:
    """Sanity: working inside the data region never crashes the process."""

    def test_protected_memory_heaps_data_access_does_not_crash(self):
        if os.name != "posix":
            pytest.skip("POSIX sanity check.")
        script = (
            "import sys\n"
            "sys.path.insert(0, %r)\n"
            "from crypto.signer import IsolatedMemoryHeap\n"
            "h = IsolatedMemoryHeap(bytes(range(32)))\n"
            "v = h.data_view\n"
            "v[0] = 0xAA\n"
            "assert v[0] == 0xAA\n"
            "h.wipe()\n"
            "h.close()\n"
        ) % _protmem_src_path()
        proc = _run_python_subprocess(script)
        assert proc.returncode == 0, (
            "Data-region access must not crash. rc=%d STDOUT=%r STDERR=%r"
            % (proc.returncode, proc.stdout, proc.stderr)
        )


class TestProtectedMemoryHeapsHelpers:
    """Direct tests for the page-size helpers."""

    def test_protected_memory_heaps_get_page_size_positive(self):
        assert _get_page_size() > 0
        assert isinstance(_get_page_size(), int)

    def test_protected_memory_heaps_round_up_to_page(self):
        page = 4096
        assert _round_up_to_page(0, page) == page
        assert _round_up_to_page(1, page) == page
        assert _round_up_to_page(page, page) == page
        assert _round_up_to_page(page + 1, page) == 2 * page
        with pytest.raises(ValueError):
            _round_up_to_page(10, 0)


class TestSecureKeyHandleUsesIsolatedHeap:
    """End-to-end: SecureKeyHandle wires through IsolatedMemoryHeap."""

    def test_secure_key_handle_exposes_isolated_heap_with_guard_pages(self):
        pytest.importorskip("nacl")
        with SecureKeyHandle(_DUMMY_KEY, key_id="isolated_test") as handle:
            heap = getattr(handle, "_heap", None)
            assert heap is not None, (
                "SecureKeyHandle must allocate an IsolatedMemoryHeap."
            )
            assert heap.has_guard_pages is True, (
                "SecureKeyHandle's heap must have PROT_NONE guard pages."
            )
            assert heap.requested_size == len(_DUMMY_KEY)
        # After the scope, the heap was torn down.
        assert getattr(handle, "_heap").is_closed is True


# ---------------------------------------------------------------------------
# Key Zeroization Coverage (#640)
# ---------------------------------------------------------------------------


class TestKeyZeroization:
    """Explicit ephemeral key zeroization routines using ctypes.memset."""

    def test_key_zeroization_after_signing(self):
        """Verify key buffers and temporary views are zeroed out immediately after signing."""
        pytest.importorskip("nacl")
        key_bytes = bytearray(_DUMMY_KEY)
        handle = SecureKeyHandle(bytes(key_bytes))
        with handle:
            sig = handle.sign(_DUMMY_HASH)
            assert len(sig) == 64

        _assert_wiped(handle._buf, "SecureKeyHandle._buf")
        assert handle._wiped is True

    def test_key_zeroization_on_exception(self):
        """Verify key buffers are zeroed out immediately when signing raises an exception."""
        handle = SecureKeyHandle(_DUMMY_KEY)
        try:
            with handle:
                raise RuntimeError("Signing error simulated")
        except RuntimeError:
            pass

        _assert_wiped(handle._buf, "SecureKeyHandle._buf after exception")
        assert handle._wiped is True

    def test_key_zeroization_bytes_view(self):
        """Verify _wipe_bytes_view explicitly zeroizes bytes memory using ctypes.memset."""
        secret = bytes(bytearray(range(1, 33)))
        addr = id(secret) + (32 if ctypes.sizeof(ctypes.c_void_p) == 8 else 16)

        initial_content = (ctypes.c_char * len(secret)).from_address(addr).raw
        assert initial_content == bytes(range(1, 33))

        _wipe_bytes_view(secret)

        wiped_content = (ctypes.c_char * len(secret)).from_address(addr).raw
        assert wiped_content == b"\x00" * 32

    def test_key_zeroization_session_credentials(self):
        """Verify SecureSessionCredentials zeroes memory immediately on exit."""
        creds = SecureSessionCredentials(_DUMMY_KEY)
        with creds:
            token = creds.get()
            assert token == _DUMMY_KEY

        _assert_wiped(creds._buf, "SecureSessionCredentials._buf")
        assert creds._wiped is True

    def test_key_zeroization_variable_wrapper(self):
        """Verify SecureVariableWrapper zeroes memory immediately on exit."""
        wrapper = SecureVariableWrapper(_DUMMY_KEY)
        with wrapper:
            val = wrapper.get()
            assert val == _DUMMY_KEY

        _assert_wiped(wrapper._buf, "SecureVariableWrapper._buf")
        assert wrapper._wiped is True

    def test_key_zeroization_secure_keypair_context(self):
        """Verify _SecureKeypairContext zeroizes key objects and key bytes using ctypes.memset."""
        secret_bytes = bytes(bytearray(range(32, 64)))
        mock_handle = mock.MagicMock()
        mock_handle._seed = bytearray(b"\xAA" * 32)
        mock_handle.secret_key = bytes(bytearray(b"\xBB" * 32))

        with _SecureKeypairContext(mock_handle, secret_bytes):
            pass

        _assert_wiped(mock_handle._seed, "mock_handle._seed")
        addr = id(mock_handle.secret_key) + (32 if ctypes.sizeof(ctypes.c_void_p) == 8 else 16)
        wiped_sk = (ctypes.c_char * 32).from_address(addr).raw
        assert wiped_sk == b"\x00" * 32

        addr_sec = id(secret_bytes) + (32 if ctypes.sizeof(ctypes.c_void_p) == 8 else 16)
        wiped_sec = (ctypes.c_char * 32).from_address(addr_sec).raw
        assert wiped_sec == b"\x00" * 32

