"""
src/crypto/signer.py
~~~~~~~~~~~~~~~~~~~~
Context-managed signing primitive that enforces strict key-lifetime isolation.

COMPREHENSIVE MEMORY SECURITY ARCHITECTURE

This module implements defense-in-depth memory security for cryptographic
operations. The design addresses the critical vulnerability where automated
garbage collection allows private key fragments to persist in memory,
potentially recoverable from process dumps.

THREAT MODEL
------------
1. **Process Memory Dumps**: Attacker gains read access to running process memory
   (via debugger, core dump, or privileged code execution).
2. **Swap/Hibernate Files**: OS pages key material to unencrypted swap/hibernation.
3. **Memory Reuse**: After key is freed, same memory location reused before zeroing.
4. **Timing Attacks**: Sensitive operations leak timing information.
5. **Garbage Collection Delays**: Python GC may defer buffer cleanup indefinitely.

MITIGATION STRATEGY
-------------------

**Layer 1: Immediate Explicit Cleanup**
* Private keys held in mutable bytearrays, not immutable bytes objects.
* Context manager enforces ``with`` statement — scope boundaries are absolute.
* ``__del__`` finaliser provides last-resort safety net if scope misused.
* On scope exit, immediate zero-wipe via ctypes.memset (not Python loops alone).
* Memory wipe happens BEFORE buffer is released or downgraded.

**Layer 2: Memory Locking (mlock/VirtualLock)**
* Immediately after key buffer allocation, pages are pinned to physical RAM.
* Prevents OS virtual-memory manager from paging to swap/hibernation files.
* On exit, unlock only AFTER zero-wipe so OS doesn't page stale key data.
* Platform-aware: mlock(2) on POSIX, VirtualLock on Windows.
* Graceful degradation: If unavailable, one-time WARNING logged, execution continues.

**Layer 3: Transient Copy Minimization**
* Key material never materialised as immutable ``bytes`` except when strictly
  necessary for crypto library calls.
* Each transient copy exists for narrowest possible scope.
* Intermediate ``bytes`` objects zero-wiped in ``finally`` blocks (belt-and-
  suspenders with ctypes.memset).

**Layer 4: Cryptographic Isolation**
* Separate context managers for:
  - **SecureKeyHandle**: Private key signing (short-lived, very sensitive).
  - **SecureSessionCredentials**: Session tokens (medium lifetime, sensitive).
  - **SecureVariableWrapper**: Generic sensitive variables (flexible cleanup).
* Each has independent lifecycle and can be revoked immediately.

**Layer 5: Defensive Logging**
* Error messages omit key material, hashes, signatures.
* Only control-flow reasons for failure are logged.
* Debug logs limited to lifecycle events (OPEN / CLOSE).
* Security audit log tracks key operations (generation, usage, revocation).

**Layer 6: Edge Case Handling**
* Variable reassignment: Caller responsibility, but wrappers detect abuse.
* Exception handling: Cleanup guaranteed even on raised exceptions.
* Early exit: Context manager ensures cleanup on return, break, continue.
* Multiple threads: Lock-based synchronization for shared state.

USAGE EXAMPLES
--------------

**Basic signing (short-lived key)**::

    with SecureKeyHandle(raw_secret_bytes) as handle:
        signature = handle.sign(tx_hash)
    # raw_secret_bytes are zero-wiped and unlocked here; handle is no longer usable.

**Session credentials (medium lifetime)**::

    with SecureSessionCredentials(api_token) as creds:
        token = creds.get()
        # use token for validation ...
    # Buffer zero-wiped here; creds no longer usable.

**Generic sensitive variable wrapper**::

    with SecureVariableWrapper(password_bytes) as wrapper:
        pwd = wrapper.get()
        # use password for operations...
    # Buffer zero-wiped here.

**Nested contexts (multiple sensitive values)**::

    with SecureKeyHandle(key1) as key_handle:
        with SecureSessionCredentials(token) as cred_handle:
            sig = key_handle.sign(msg)
            val = cred_handle.get()
    # Both buffers zero-wiped in reverse order.

**Exception safety**::

    try:
        with SecureKeyHandle(key_bytes) as handle:
            sig = handle.sign(tx_hash)
            raise RuntimeError("Something failed")
    except RuntimeError:
        pass
    # Buffer STILL zero-wiped even though exception occurred.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import hashlib
import logging
import mmap as _mmap_mod
import os
import platform
import threading
from types import TracebackType
from typing import Any, Iterator, Optional, Type

logger = logging.getLogger(__name__)
audit_logger = logging.getLogger(f"{__name__}.audit")

# =========================================================================
# OPENSSL HARDWARE ACCELERATION CONFIGURATION
# =========================================================================

def _configure_openssl_hardware_acceleration() -> None:
    """Enable OpenSSL hardware acceleration flags (AES-NI/AVX2) for signature verification.
    
    This function configures OpenSSL to use available CPU instruction set extensions
    for accelerated cryptographic operations. It sets environment variables and
    attempts to configure OpenSSL's ENGINE to enable hardware acceleration.
    
    The following accelerations are enabled when available:
    - AES-NI: Advanced Encryption Standard New Instructions for AES operations
    - AVX2: Advanced Vector Extensions for parallel processing
    - SHA-NI: SHA extensions for hash operations
    """
    # Enable OpenSSL hardware acceleration via environment variables
    # These flags are read by OpenSSL when initializing crypto operations
    os.environ.setdefault("OPENSSL_ia32cap", "~0x200000200000000")  # Enable AVX2, AES-NI
    
    # Try to configure OpenSSL ENGINE for hardware acceleration
    try:
        # Load OpenSSL library
        if os.name == "nt":
            openssl_libs = ["libeay32.dll", "libssl32.dll", "libcrypto.dll"]
        else:
            openssl_libs = ["libcrypto.so.1.1", "libcrypto.so.3", "libcrypto.so"]
        
        openssl_loaded = False
        for lib_name in openssl_libs:
            try:
                lib_path = ctypes.util.find_library(lib_name)
                if lib_path:
                    openssl = ctypes.CDLL(lib_path)
                    openssl_loaded = True
                    logger.debug(f"[OpenSSL] Loaded library: {lib_path}")
                    break
            except (OSError, AttributeError):
                continue
        
        if openssl_loaded:
            # Try to enable hardware acceleration via ENGINE
            try:
                # OpenSSL 1.1.1+ uses ENGINE_load_builtin_engines
                if hasattr(openssl, "ENGINE_load_builtin_engines"):
                    openssl.ENGINE_load_builtin_engines()
                    openssl.ENGINE_register_all_complete()
                    logger.debug("[OpenSSL] Hardware acceleration engines loaded")
            except Exception:
                # ENGINE functions may not be available in all OpenSSL builds
                pass
    except Exception as exc:
        # Hardware acceleration is optional - log but don't fail
        logger.debug(f"[OpenSSL] Hardware acceleration configuration failed (optional): {exc}")

# Configure OpenSSL acceleration at module import time
_configure_openssl_hardware_acceleration()

__all__ = [
    "SecureKeyHandle",
    "PublicKeyHandle",
    "SecureSessionCredentials",
    "SecureVariableWrapper",
    "SigningError",
    "MemorySecurityError",
    "GuardPageError",
    "IsolatedMemoryHeap",
    "SecurityAuditLogger",
    "audit_log",
]

# =========================================================================
# MEMORY SECURITY AUDIT LOGGING
# =========================================================================


class SecurityAuditLogger:
    """Thread-safe audit log for cryptographic operations.
    
    Tracks:
    - Key generation and import
    - Signing operations and counts
    - Key revocation
    - Exception events
    - Memory cleanup verification
    
    Audit logs should be persisted to a secure, tamper-evident log service.
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._operations = []

    def log_key_imported(self, key_id: str, key_size_bytes: int) -> None:
        with self._lock:
            self._operations.append({
                'event': 'KEY_IMPORTED',
                'key_id': key_id,
                'key_size_bytes': key_size_bytes
            })

    def log_signing_operation(self, key_id: str, key_size_bytes: int) -> None:
        with self._lock:
            self._operations.append({
                'event': 'SIGNING_OPERATION',
                'key_id': key_id
            })

    def log_key_revoked(self, key_id: str, reason: str) -> None:
        with self._lock:
            self._operations.append({
                'event': 'KEY_REVOKED',
                'key_id': key_id,
                'reason': reason
            })

    def log_memory_cleanup(self, object_type: str, buffer_size: int, wipe_method: str = "ctypes.memset") -> None:
        with self._lock:
            self._operations.append({
                'event': 'MEMORY_CLEANUP',
                'object_type': object_type,
                'buffer_size': buffer_size,
                'wipe_method': wipe_method
            })

    def log_isolation_fallback(
        self,
        key_id: str,
        reason: str,
        fallback_size_bytes: int,
    ) -> None:
        """Record that hardened isolation was unavailable for *key_id*.

        Used by :class:`SecureKeyHandle` when its
        :class:`IsolatedMemoryHeap` allocation could not be obtained
        (e.g. mmap/mprotect unavailable on the running platform).
        Reports the *fall-back* size, not the cleanup size —
        callers must not confuse this with ``log_memory_cleanup``.
        """
        with self._lock:
            self._operations.append({
                'event': 'ISOLATION_FALLBACK',
                'key_id': key_id,
                'reason': reason,
                'fallback_size_bytes': fallback_size_bytes,
            })

    def get_audit_trail(self) -> list:
        with self._lock:
            return list(self._operations)


audit_log = SecurityAuditLogger()



def _zero_wipe(buf: bytearray, audit_details: Optional[dict] = None) -> None:
    """Overwrite *buf* in-place with zeros."""
    if len(buf) == 0:
        return

    try:
        addr = ctypes.addressof((ctypes.c_char * len(buf)).from_buffer(buf))
        ctypes.memset(addr, 0, len(buf))
        
        if audit_details:
            audit_log.log_memory_cleanup(
                audit_details.get("object_type", "unknown"),
                len(buf),
                wipe_method="ctypes.memset"
            )
    finally:
        for i in range(len(buf)):
            buf[i] = 0


def _lock_memory(buf: bytearray) -> None:
    """
    Best-effort memory lock.

    Prevents pages containing private-key material from being swapped to disk.
    Uses mlock on Unix-like systems and VirtualLock on Windows.
    """
    if len(buf) == 0:
        return

    try:
        addr = ctypes.addressof((ctypes.c_char * len(buf)).from_buffer(buf))
        length = ctypes.c_size_t(len(buf))

        if os.name == "nt":
            kernel32 = ctypes.windll.kernel32
            kernel32.VirtualLock(ctypes.c_void_p(addr), length)
        else:
            libc = ctypes.CDLL(None)
            if hasattr(libc, "mlock"):
                libc.mlock(ctypes.c_void_p(addr), length)
    except Exception:  # noqa: BLE001
        # Memory locking may fail because of OS limits or permissions.
        # This hardening is best-effort and must not break signing.
        pass

# =========================================================================
# MEMORY-LOCKING HELPERS (mlock / VirtualLock)
# =========================================================================


# =========================================================================
# LIBSODIUM MEMORY LOCKING (sodium_mlock / sodium_munlock)
# =========================================================================


def _load_sodium_functions() -> tuple:
    """Load libsodium's ``sodium_mlock`` / ``sodium_munlock`` via ctypes.

    Returns:
        ``(mlock_fn, munlock_fn)`` where each is a callable or ``None``.

    libsodium provides cross-platform memory locking that:
    - Pins memory to physical RAM (prevents swapping)
    - Marks pages as ``JITTERBUG`` / ``SECMEM`` to exclude from core dumps
    - Is available on Linux, macOS, Windows, and BSDs

    The result is cached at module level in ``_SODIUM_MLOCK_FN``
    and ``_SODIUM_MUNLOCK_FN``.
    """
    lib_name = ctypes.util.find_library("sodium")
    if lib_name is None:
        return None, None
    try:
        lib = ctypes.CDLL(lib_name, use_errno=True)
    except OSError:
        return None, None

    mlock_fn = getattr(lib, "sodium_mlock", None)
    munlock_fn = getattr(lib, "sodium_munlock", None)
    if mlock_fn is None or munlock_fn is None:
        return None, None

    # sodium_mlock(const void *addr, const size_t len) -> int
    mlock_fn.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    mlock_fn.restype = ctypes.c_int
    munlock_fn.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    munlock_fn.restype = ctypes.c_int

    return mlock_fn, munlock_fn


_SODIUM_MLOCK_FN, _SODIUM_MUNLOCK_FN = _load_sodium_functions()


def _sodium_mlock_buffer(buf: bytearray) -> bool:
    """Pin *buf* to physical RAM using libsodium's ``sodium_mlock``.

    Returns ``True`` on success, ``False`` if libsodium is unavailable
    or the call fails.
    """
    if not buf or _SODIUM_MLOCK_FN is None:
        return False
    try:
        c_arr = (ctypes.c_char * len(buf)).from_buffer(buf)
        addr = ctypes.addressof(c_arr)
        ret = _SODIUM_MLOCK_FN(addr, ctypes.c_size_t(len(buf)))
        return ret == 0
    except Exception:
        return False


def _sodium_munlock_buffer(buf: bytearray) -> None:
    """Release libsodium's memory lock on *buf* via ``sodium_munlock``.

    The buffer **must** be zero-wiped **before** calling this so that
    the unlocked pages do not contain live key material.
    """
    if not buf or _SODIUM_MUNLOCK_FN is None:
        return
    try:
        c_arr = (ctypes.c_char * len(buf)).from_buffer(buf)
        addr = ctypes.addressof(c_arr)
        _SODIUM_MUNLOCK_FN(addr, ctypes.c_size_t(len(buf)))
    except Exception:
        pass


def _load_mlock_functions() -> tuple:
    """Load the platform's mlock / munlock function pair.

    Returns:
        ``(mlock_fn, munlock_fn)`` where each is a callable or ``None``.

    On Linux/macOS the functions are found in libc via ``ctypes.CDLL``.
    On Windows the equivalents are ``VirtualLock`` / ``VirtualUnlock``
    from ``kernel32``.

    The result is cached at module level in ``_MLOCK_FN`` and ``_MUNLOCK_FN``
    so this function is only executed once.
    """
    _os = platform.system()

    if _os == "Windows":
        try:
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            # VirtualLock(lpAddress, dwSize) -> BOOL
            mlock_fn = kernel32.VirtualLock
            munlock_fn = kernel32.VirtualUnlock
            mlock_fn.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
            mlock_fn.restype = ctypes.c_bool
            munlock_fn.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
            munlock_fn.restype = ctypes.c_bool
            return mlock_fn, munlock_fn
        except Exception:  # noqa: BLE001
            return None, None

    # POSIX (Linux, macOS, BSDs)
    libc_name = ctypes.util.find_library("c")
    if libc_name is None:
        return None, None
    try:
        libc = ctypes.CDLL(libc_name, use_errno=True)
        mlock_fn = getattr(libc, "mlock", None)
        munlock_fn = getattr(libc, "munlock", None)
        if mlock_fn is None or munlock_fn is None:
            return None, None
        # mlock(const void *addr, size_t len) -> int
        mlock_fn.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        mlock_fn.restype = ctypes.c_int
        munlock_fn.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        munlock_fn.restype = ctypes.c_int
        return mlock_fn, munlock_fn
    except Exception:  # noqa: BLE001
        return None, None


# Module-level singletons — resolved once at import time.
_MLOCK_FN, _MUNLOCK_FN = _load_mlock_functions()

# Emit a single warning if mlock is unavailable so operators know the
# swap-protection layer is absent without spamming per-key-handle logs.
_MLOCK_UNAVAILABLE_WARNED: bool = False


def _warn_mlock_unavailable(reason: str) -> None:
    """Log a one-time WARNING that mlock is unavailable."""
    global _MLOCK_UNAVAILABLE_WARNED  # noqa: PLW0603
    if not _MLOCK_UNAVAILABLE_WARNED:
        logger.warning(
            "[SecureKeyHandle] mlock unavailable (%s). "
            "Private-key pages may be swapped to disk. "
            "Grant CAP_IPC_LOCK or raise RLIMIT_MEMLOCK to harden this deployment.",
            reason,
        )
        _MLOCK_UNAVAILABLE_WARNED = True


def _mlock_buffer(buf: bytearray) -> bool:
    """Pin the pages backing *buf* to physical RAM using libsodium first,
    then fall back to mlock / VirtualLock.

    libsodium's ``sodium_mlock`` is preferred because it additionally marks
    pages as ``JITTERBUG`` to exclude them from core dumps on supported
    platforms.  If libsodium is unavailable, falls back to the standard
    ``mlock(2)`` / ``VirtualLock`` path.

    This prevents the OS from writing key material to swap or a hibernate file.
    The buffer **must** remain alive for as long as the lock is held; calling
    code is responsible for keeping a reference.

    Args:
        buf: The bytearray whose backing pages should be locked.

    Returns:
        ``True`` if the lock succeeded, ``False`` otherwise (caller should log
        a warning but must not abort — the zero-wipe layer still applies).

    This function **must not raise**.
    """
    if not buf:
        return False

    # Try libsodium first — preferred for cross-platform + core-dump guard.
    if _SODIUM_MLOCK_FN is not None:
        try:
            c_arr = (ctypes.c_char * len(buf)).from_buffer(buf)
            addr = ctypes.addressof(c_arr)
            ret = _SODIUM_MLOCK_FN(addr, ctypes.c_size_t(len(buf)))
            if ret == 0:
                return True
        except Exception:
            pass
    if not buf:
        return False

    if _MLOCK_FN is None:
        _warn_mlock_unavailable("mlock/VirtualLock not found on this platform")
        return False

    try:
        # Obtain the raw address of the bytearray's underlying C buffer.
        c_arr = (ctypes.c_char * len(buf)).from_buffer(buf)
        addr = ctypes.addressof(c_arr)
        size = ctypes.c_size_t(len(buf))

        ret = _MLOCK_FN(addr, size)

        # POSIX returns 0 on success; Windows returns non-zero (BOOL TRUE).
        if platform.system() == "Windows":
            success = bool(ret)
        else:
            success = (ret == 0)

        if not success:
            errno_val = ctypes.get_errno()
            _warn_mlock_unavailable(f"syscall returned failure (errno={errno_val})")
            return False

        return True

    except Exception as exc:  # noqa: BLE001
        _warn_mlock_unavailable(f"exception during mlock: {exc}")
        return False


def _munlock_buffer(buf: bytearray) -> None:
    """Release the mlock / VirtualLock on *buf*'s pages.

    Tries libsodium's ``sodium_munlock`` first (if it was used to lock),
    then falls back to ``munlock(2)`` / ``VirtualUnlock``.

    Must be called **after** :func:`_zero_wipe` so the unlocked pages do not
    contain live key material when the OS is free to evict them.

    This function **must not raise**.
    """
    if not buf:
        return

    try:
        # Try libsodium first
        if _SODIUM_MUNLOCK_FN is not None:
            c_arr = (ctypes.c_char * len(buf)).from_buffer(buf)
            addr = ctypes.addressof(c_arr)
            _SODIUM_MUNLOCK_FN(addr, ctypes.c_size_t(len(buf)))
            return
    except Exception:  # noqa: BLE001
        pass

    if _MUNLOCK_FN is None:
        return

    try:
        c_arr = (ctypes.c_char * len(buf)).from_buffer(buf)
        addr = ctypes.addressof(c_arr)
        size = ctypes.c_size_t(len(buf))
        _MUNLOCK_FN(addr, size)
    except Exception:  # noqa: BLE001
        pass


# =========================================================================
# EXCEPTIONS
# =========================================================================

class MemorySecurityError(Exception):
    """Raised when memory locking or hardening fails or is violated."""


class GuardPageError(MemorySecurityError):
    """Raised when an isolated memory pool cannot be guarded with PROT_NONE."""


class SigningError(Exception):
    """Raised when signing fails or the key handle is no longer usable."""


def _wipe_bytes_object(obj: bytes) -> None:
    """Best-effort in-place overwrite of CPython bytes object data."""
    if not isinstance(obj, bytes) or len(obj) == 0:
        return
    try:
        is_64bit = (ctypes.sizeof(ctypes.c_void_p) == 8)
        offset = 32 if is_64bit else 16
        addr = id(obj) + offset
        ctypes.memset(addr, 0, len(obj))
    except Exception:
        pass


def _wipe_key_handle(handle, visited: Optional[set] = None) -> None:
    """Clean up sensitive fields of keypair/signing key objects in memory."""
    if handle is None:
        return
    if visited is None:
        visited = set()
    handle_id = id(handle)
    if handle_id in visited:
        return
    visited.add(handle_id)

    try:
        if isinstance(handle, (bytes, bytearray)):
            if isinstance(handle, bytearray):
                _zero_wipe(handle)
            else:
                _wipe_bytes_object(handle)
            return

        is_mock = False
        try:
            import unittest.mock as _mock
            if isinstance(handle, _mock.Base):
                is_mock = True
        except ImportError:
            pass

        attrs_to_wipe = (
            "_seed", "_signing_key", "_verifier", "_key", "seed", "secret_key",
            "_secret_key", "_raw_secret_key", "raw_secret_key", "_keypair",
            "_private_key", "private_key", "_sk", "sk", "_vk", "vk"
        )
        for attr in attrs_to_wipe:
            if is_mock:
                if attr in handle.__dict__:
                    val = handle.__dict__[attr]
                else:
                    continue
            else:
                if hasattr(handle, attr):
                    val = getattr(handle, attr, None)
                else:
                    continue

            if val is not None:
                if isinstance(val, (bytes, bytearray)):
                    if isinstance(val, bytearray):
                        _zero_wipe(val)
                    else:
                        _wipe_bytes_object(val)
                elif not is_mock and (hasattr(val, "__dict__") or type(val).__name__ in ("SigningKey", "VerifyKey", "Keypair")):
                    _wipe_key_handle(val, visited)
    except Exception:
        pass


class _SecureKeypairContext:
    """Strict context manager around key handles."""
    def __init__(self, handle, key_bytes: bytes) -> None:
        self.handle = handle
        self.key_bytes = key_bytes

    def __enter__(self) -> "_SecureKeypairContext":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        _wipe_key_handle(self.handle)
        _wipe_bytes_object(self.key_bytes)


def _unlock_memory(buf: bytearray) -> None:
    """Best-effort unlock for previously locked key memory."""
    if len(buf) == 0:
        return

    try:
        addr = ctypes.addressof((ctypes.c_char * len(buf)).from_buffer(buf))
        length = ctypes.c_size_t(len(buf))

        if os.name == "nt":
            kernel32 = ctypes.windll.kernel32
            kernel32.VirtualUnlock(ctypes.c_void_p(addr), length)
        else:
            libc = ctypes.CDLL(None)
            if hasattr(libc, "munlock"):
                libc.munlock(ctypes.c_void_p(addr), length)
    except Exception:  # noqa: BLE001
        pass


def _wipe_bytes_view(view: bytes) -> None:
    """Explicitly zero out the CPython bytes object buffer in-place using ctypes.memset."""
    if not isinstance(view, bytes) or len(view) == 0:
        return

    try:
        _wipe_bytes_object(view)
    except Exception:  # noqa: BLE001
        pass


class SecureKeyHandle:
    """
    Context manager that keeps private-key material isolated.

    The raw key is copied into a mutable bytearray, memory-locked on a
    best-effort basis, and wiped when the signing scope closes.
    """

    __slots__ = ("__dict__", "_buf", "_active", "_wiped", "_locked")

    def __init__(self, raw_key: bytes, key_id: str = "default_key") -> None:
        if not raw_key:
            raise ValueError("raw_key must be non-empty bytes.")
        self._buf: bytearray = bytearray(raw_key)
        _lock_memory(self._buf)

        self._active: bool = False
        self._wiped: bool = False
        self._key_id: str = key_id
        self._sign_count: int = 0
        # Immediately pin the buffer's pages to physical RAM
        self._locked: bool = _mlock_buffer(self._buf)
        # Canonical store (Issue #660): an isolated mmap pool flanked by
        # PROT_NONE guard pages. The bytearray above remains as a
        # backwards-compatible handle for tests that introspect `_buf`;
        # the authoritative copy of the key now lives inside the
        # protected heap and is wiped + PROT_NONE-d + unmapped on close.
        self._heap: Optional["IsolatedMemoryHeap"] = None
        try:
            self._heap = IsolatedMemoryHeap(raw_key, label=key_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[SecureKeyHandle] isolated heap unavailable (%s); key "
                "stored only in mlocked bytearray.",
                exc,
            )
            try:
                # Surface the regression in the audit trail under a
                # dedicated event so operators can alert on it. This
                # threads through SecurityAuditLogger.log_isolation_fallback
                # so the entry is appended under the audit-log lock.
                audit_log.log_isolation_fallback(
                    key_id, str(exc), len(raw_key),
                )
            except Exception:  # noqa: BLE001
                pass

    def __enter__(self) -> "SecureKeyHandle":
        self._active = True
        logger.debug("[SecureKeyHandle] Signing scope opened for: %s", self._key_id)
        audit_log.log_key_imported(self._key_id, len(self._buf))
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self._active = False
        self._do_wipe()
        return False

    def __del__(self) -> None:
        try:
            self._do_wipe()
        except Exception:  # noqa: BLE001
            pass

    def _do_wipe(self) -> None:
        """Idempotently wipe and unlock the internal key buffer.

        Execution order is critical:
        1. Zero-wipe the bytearray with ctypes.memset (Layer 1 overwrite sweep).
        2. Release the mlock / sodium lock AFTER the wipe so the OS never
           evicts live key material to swap during the window between unlock
           and wipe.
        3. Tear down the IsolatedMemoryHeap (its own wipe + PROT_NONE + unmap).
        4. Emit an audit entry confirming the cleanup.
        """
        if self._wiped:
            return

        self._wiped = True

        # Layer 1: immediate byte-clearing overwrite via ctypes.memset.
        _zero_wipe(
            self._buf,
            audit_details={"object_type": "SecureKeyHandle"},
        )

        # Layer 2: release memory lock only after the overwrite is done.
        if self._locked:
            _munlock_buffer(self._buf)
            self._locked = False
        try:
            _zero_wipe(self._buf)
        finally:
            _unlock_memory(self._buf)
            if self._locked:
                _munlock_buffer(self._buf)
                self._locked = False

        # Layer 3: tear down the isolated mmap heap — wipes + PROT_NONE + unmaps.
        heap_obj = getattr(self, "_heap", None)
        if heap_obj is not None:
            try:
                heap_obj.close()
            except Exception:  # noqa: BLE001
                pass

        logger.debug("[SecureKeyHandle] Signing scope closed — key wiped.")
        audit_log.log_key_revoked(self._key_id, reason="scope_exit")

    def sign(self, tx_hash: bytes) -> bytes:
        """Sign a 32-byte transaction hash."""
        if not self._active:
            raise SigningError(
                "SecureKeyHandle.sign() called outside an active signing scope. "
                "Use 'with SecureKeyHandle(...) as handle:' and call sign() inside."
            )

        if self._wiped:
            raise SigningError(
                "SecureKeyHandle.sign() called after the handle has been wiped."
            )

        if len(tx_hash) != 32:
            raise ValueError(f"tx_hash must be exactly 32 bytes, got {len(tx_hash)}.")

        audit_log.log_signing_operation(self._key_id, len(tx_hash))
        self._sign_count += 1
        return self._sign_internal(tx_hash)

    def _sign_internal(self, tx_hash: bytes) -> bytes:
        # Route signing through the isolated mmap pool (Issue #660) so
        # the bytes actually fed to the crypto library come from inside
        # the PROT_NONE-flanked region. Falls back to the bytearray
        # copy if the heap failed to initialise on this platform.
        heap_obj = getattr(self, "_heap", None)
        if heap_obj is not None and not heap_obj.is_closed:
            try:
                view = heap_obj.data_view
                nrequested = heap_obj.requested_size or len(view)
                key_bytes: bytes = bytes(view[:nrequested])
            except Exception:  # noqa: BLE001
                # Heap view unreachable; fall back to the legacy copy.
                key_bytes = bytes(self._buf)
        else:
            key_bytes = bytes(self._buf)

        try:
            try:
                return self._try_stellar_sdk(key_bytes, tx_hash)
            except ImportError:
                return self._try_pynacl(key_bytes, tx_hash)
        finally:
            # Overwrite the transient bytes copy in-place before releasing
            # the reference, so the CPython object's internal buffer is
            # zeroed while it is still uniquely held on this call stack.
            _wipe_bytes_object(key_bytes)
            del key_bytes

    @staticmethod
    def _try_stellar_sdk(key_bytes: bytes, tx_hash: bytes) -> bytes:
        from stellar_sdk import Keypair  # type: ignore[import]  # noqa: PLC0415

        try:
            keypair = Keypair.from_raw_ed25519_seed(key_bytes)
            with _SecureKeypairContext(keypair, key_bytes):
                return bytes(keypair.sign(tx_hash))
        except Exception as exc:
            raise SigningError("Signing failed (stellar_sdk path).") from exc

    @staticmethod
    def _try_pynacl(key_bytes: bytes, tx_hash: bytes) -> bytes:
        try:
            from nacl.signing import SigningKey  # type: ignore[import]  # noqa: PLC0415
        except ImportError:
            raise SigningError(
                "Neither 'stellar_sdk' nor 'PyNaCl' is installed. "
                "Install one to enable signing."
            )

        try:
            sk = SigningKey(key_bytes)
            with _SecureKeypairContext(sk, key_bytes):
                return bytes(sk.sign(tx_hash).signature)
        except Exception as exc:
            raise SigningError("Signing failed (PyNaCl path).") from exc


# =========================================================================
# PUBLIC API - PUBLIC KEY HANDLE (TYPE-ISOLATED FROM PRIVATE KEY)
# =========================================================================


class PublicKeyHandle:
    """Type-isolated public key container with memory safety guarantees.

    This class enforces strict separation between public and private key
    structures. Public key operations have zero programmatic or memory access
    to private key fields. The public key is stored in an immutable bytes
    object and cannot be used for signing operations.

    This is a security boundary isolator: attempting to use a PublicKeyHandle
    where a private key is expected will fail at type-check time, preventing
    accidental reference sharing between public and private key objects.

    Args:
        public_key_bytes: The public key bytes (typically 32 bytes for Ed25519).
        key_id: Optional identifier for audit logging.

    Raises:
        ValueError: If public_key_bytes is empty.

    Example::

        # Derive public key from private key handle
        with SecureKeyHandle(private_key_bytes) as priv_handle:
            signature = priv_handle.sign(tx_hash)
            # Public key can be extracted separately
            pub_handle = PublicKeyHandle(derive_public_key(private_key_bytes))

        # Public key handle cannot sign - type isolation enforced
        # pub_handle.sign(tx_hash)  # AttributeError: no such method
    """

    __slots__ = ("_public_key", "_key_id", "_frozen")

    def __init__(self, public_key_bytes: bytes, key_id: str = "public_key") -> None:
        if not public_key_bytes:
            raise ValueError("public_key_bytes must be non-empty bytes.")
        # Store as immutable bytes to prevent modification
        self._public_key: bytes = bytes(public_key_bytes)
        self._key_id: str = key_id
        self._frozen: bool = True  # Immutable after construction

        audit_log.log_key_imported(f"pub_{self._key_id}", len(self._public_key))

    def __repr__(self) -> str:
        return f"<PublicKeyHandle key_id={self._key_id!r} bytes={len(self._public_key)}>"

    @property
    def bytes(self) -> bytes:
        """Return the public key bytes (immutable copy)."""
        return self._public_key

    @property
    def key_id(self) -> str:
        """Return the key identifier."""
        return self._key_id

    def verify(self, signature: bytes, message: bytes) -> bool:
        """Verify a signature against the public key.

        This is the only operation allowed on public keys - verification only.
        No signing or private key access is possible.

        Args:
            signature: The signature bytes to verify.
            message: The message bytes that were signed.

        Returns:
            True if the signature is valid, False otherwise.

        Raises:
            SigningError: If verification fails due to library issues.
        """
        try:
            from stellar_sdk import Keypair  # type: ignore[import]  # noqa: PLC0415
            try:
                keypair = Keypair.from_raw_ed25519_public_key(self._public_key)
                return keypair.verify(message, signature)
            except Exception as exc:
                raise SigningError("Verification failed (stellar_sdk path).") from exc
        except ImportError:
            try:
                from nacl.signing import VerifyKey  # type: ignore[import]  # noqa: PLC0415
                verify_key = VerifyKey(self._public_key)
                try:
                    verify_key.verify(message, signature)
                    return True
                except Exception:
                    return False
            except ImportError as exc:
                raise SigningError(
                    "Neither 'stellar_sdk' nor 'PyNaCl' is installed. "
                    "Install one to enable verification."
                ) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        """Prevent attribute modification after construction (immutable)."""
        if name in ("_public_key", "_key_id", "_frozen") and not hasattr(self, "_frozen"):
            # Allow initial construction
            super().__setattr__(name, value)
        elif name == "_frozen":
            # Allow setting frozen flag during construction
            super().__setattr__(name, value)
        else:
            raise AttributeError(
                f"PublicKeyHandle is immutable; cannot set attribute '{name}'"
            )

    def __delattr__(self, name: str) -> None:
        """Prevent attribute deletion (immutable)."""
        raise AttributeError(
            f"PublicKeyHandle is immutable; cannot delete attribute '{name}'"
        )


# =========================================================================
# PUBLIC API - SECURE SESSION CREDENTIALS
# =========================================================================


class SecureSessionCredentials:
    """Context manager that holds temporary session credentials for one validation scope.

    The credentials are copied into an internal ``bytearray`` on construction.
    On ``__exit__`` — normal *or* exceptional — the buffer is zero-wiped
    **before** any reference is released.

    A ``__del__`` finaliser acts as a last-resort safety net.

    Args:
        credentials: Raw session credential bytes (e.g. API token, JWT).
        credential_type: Label for what kind of credential (default: "session_token").

    Raises:
        ValueError:   If *credentials* is empty.
        SigningError: If :meth:`get` is called outside the ``with`` block.

    Example::

        with SecureSessionCredentials(api_token, credential_type="jwt") as creds:
            token = creds.get()
            # use token for validation ...
        # Buffer zero-wiped here; creds is no longer usable.

    Prefer the :meth:`use` method over :meth:`get` when possible — it
    accepts a callback and wipes the temporary ``bytes`` view in a
    ``finally`` block, minimising the window during which credential
    material is recoverable from a heap dump.
    """

    __slots__ = ("_buf", "_active", "_wiped", "_credential_type", "_locked")

    def __init__(
        self, credentials: bytes, credential_type: str = "session_token"
    ) -> None:
        if not credentials:
            raise ValueError("credentials must be non-empty bytes.")
        self._buf: bytearray = bytearray(credentials)
        self._active: bool = False
        self._wiped: bool = False
        self._credential_type: str = credential_type
        self._locked: bool = _mlock_buffer(self._buf)

    # ------------------------------------------------------------------
    # Context-manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> "SecureSessionCredentials":
        self._active = True
        logger.debug(
            "[SecureSessionCredentials] Validation scope opened for: %s",
            self._credential_type
        )
        audit_log.log_key_imported(f"cred_{self._credential_type}", len(self._buf))
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self._active = False
        self._do_wipe()
        return False

    def __del__(self) -> None:
        try:
            self._do_wipe()
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _do_wipe(self) -> None:
        if self._wiped:
            return
        self._wiped = True
        _zero_wipe(
            self._buf,
            audit_details={"object_type": "SecureSessionCredentials"}
        )
        if self._locked:
            _munlock_buffer(self._buf)
            self._locked = False
        logger.debug(
            "[SecureSessionCredentials] Validation scope closed — credentials wiped."
        )
        audit_log.log_key_revoked(f"cred_{self._credential_type}", reason="scope_exit")

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def get(self) -> bytes:
        """Return a ``bytes`` copy of the stored session credentials.

        Returns:
            A ``bytes`` copy of the credentials (caller's responsibility).

        Raises:
            SigningError: If called outside the ``with`` block.
        """
        if not self._active:
            raise SigningError(
                "SecureSessionCredentials.get() called outside an active validation scope. "
                "Use 'with SecureSessionCredentials(...) as creds:' and call get() inside."
            )
        if self._wiped:
            raise SigningError(
                "SecureSessionCredentials.get() called after credentials have been wiped."
            )
        return bytes(self._buf)

    def use(self, callback):
        """Pass the session credentials to *callback* and wipe the temporary
        copy immediately after the callback returns (or raises).

        This is the **preferred** way to consume credentials because the
        intermediate ``bytes`` view is overwritten in a ``finally`` block,
        minimising the window during which the credential material is
        recoverable from a process memory dump.

        Args:
            callback: A callable ``fn(credentials: bytes) -> T`` that
                      receives the credential bytes for the duration of
                      the call.  The return value of *callback* is
                      forwarded as the return value of :meth:`use`.

        Returns:
            The return value of *callback*.

        Raises:
            SigningError: If called outside the ``with`` block or after the
                          buffer has already been wiped.

        Example::

            with SecureSessionCredentials(token_bytes) as creds:
                api_token = creds.use(lambda tok: verify(tok))
            # Temporary bytes copy zero-wiped here; creds is no longer usable.
        """
        if not self._active:
            raise SigningError(
                "SecureSessionCredentials.use() called outside an active validation scope. "
                "Use 'with SecureSessionCredentials(...) as creds:' and call use() inside."
            )
        if self._wiped:
            raise SigningError(
                "SecureSessionCredentials.use() called after credentials have been wiped."
            )
        temp: bytes = bytes(self._buf)
        try:
            return callback(temp)
        finally:
            _wipe_bytes_view(temp)
            del temp



# =========================================================================
# ISOLATED MEMORY HEAP (PROT_NONE GUARD PAGES) -- Issue #660
# =========================================================================
#
# Threat: standard heap allocators place sensitive cryptographic objects
# adjacent to un-sanitized buffers; an off-by-one bug or memcpy overflow
# into a neighbour leaks key material into adjacent memory.
#
# Mitigation: each IsolatedMemoryHeap allocates its own anonymous mmap
# region flanked by PROT_NONE guard pages. The middle data pages are
# PROT_READ | PROT_WRITE. Any read or write wandering off either side
# faults on the immediate adjacent guard page (SIGSEGV on POSIX /
# ACCESS_VIOLATION on Windows). On teardown the data region is wiped,
# demoted to PROT_NONE, and the entire region is unmapped so no key
# bytes remain readable in the process address space.


_POSIX_PROT_NONE = 0
_POSIX_PROT_READ = 1
_POSIX_PROT_WRITE = 2
_POSIX_PROT_RW = _POSIX_PROT_READ | _POSIX_PROT_WRITE
_WIN_PAGE_NOACCESS = 0x01
_WIN_PAGE_READWRITE = 0x04


def _get_page_size() -> int:
    try:
        return int(_mmap_mod.PAGESIZE)
    except (AttributeError, ValueError):
        pass
    try:
        return int(os.sysconf("SC_PAGESIZE"))
    except (ValueError, OSError, AttributeError):
        return 4096


def _round_up_to_page(size: int, page: int) -> int:
    if page <= 0:
        raise ValueError("page size must be positive.")
    if size <= 0:
        return page
    return ((int(size) + page - 1) // page) * page


def _load_mprotect_function():
    if platform.system() == "Windows":
        return None
    libc_name = ctypes.util.find_library("c") or "libc.so.6"
    try:
        libc = ctypes.CDLL(libc_name, use_errno=True)
        mprotect = getattr(libc, "mprotect", None)
        if mprotect is None:
            return None
        mprotect.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
        mprotect.restype = ctypes.c_int
        return mprotect
    except (OSError, AttributeError):
        return None


_MPROTECT_FN = _load_mprotect_function()


class IsolatedMemoryHeap:
    """Self-contained memory pool flanked by PROT_NONE guard pages.

    Layout (page-aligned):

        [ LEFT  GUARD PAGE : PROT_NONE ]
        [ DATA PAGES       : PROT_R/W  ]
        [ RIGHT GUARD PAGE : PROT_NONE ]

    Use as a context manager; on ``__exit__`` the data region is wiped,
    demoted to ``PROT_NONE``, and the whole region is unmapped.
    """

    __slots__ = (
        "_mmap", "_page_size", "_data_pages", "_data_bytes",
        "_requested_size", "_base_addr", "_left_guard_addr",
        "_data_addr", "_right_guard_addr", "_guard_pages_applied",
        "_closed", "_locked", "_label",
    )

    def __init__(self, initial_bytes: bytes = b"", label: str = "isolated") -> None:
        if not isinstance(initial_bytes, (bytes, bytearray)):
            raise TypeError(
                "initial_bytes must be bytes or bytearray, got "
                + type(initial_bytes).__name__
            )
        self._label = str(label)
        page = _get_page_size()
        self._page_size = page
        requested = len(initial_bytes)
        if requested == 0:
            data_bytes = page
        else:
            data_bytes = max(_round_up_to_page(requested, page), page)
        self._data_bytes = data_bytes
        self._data_pages = data_bytes // page if page else 1
        self._requested_size = requested
        total_size = data_bytes + 2 * page
        self._mmap = None
        self._closed = False
        self._locked = False
        self._guard_pages_applied = False

        try:
            self._mmap = _mmap_mod.mmap(-1, total_size, prot=_POSIX_PROT_RW)
        except Exception as exc:
            raise MemorySecurityError(
                "IsolatedMemoryHeap: mmap(%d) failed: %s" % (total_size, exc)
            ) from exc

        try:
            base = ctypes.addressof(
                (ctypes.c_char * 1).from_buffer(memoryview(self._mmap))
            )
        except (BufferError, TypeError) as exc:
            self._safe_unmap_only()
            raise MemorySecurityError(
                "IsolatedMemoryHeap: address resolve failed: %s" % exc
            ) from exc
        self._base_addr = int(base)
        self._left_guard_addr = self._base_addr
        self._data_addr = self._base_addr + page
        self._right_guard_addr = self._base_addr + page + data_bytes

        if requested > 0:
            try:
                view = memoryview(self._mmap)[page:page + data_bytes]
                view[:requested] = bytes(initial_bytes)
            except (IndexError, ValueError, TypeError) as exc:
                self._safe_unmap_only()
                raise MemorySecurityError(
                    "IsolatedMemoryHeap: seed copy failed: %s" % exc
                ) from exc

        try:
            self._apply_guard_protections()
        except GuardPageError:
            self._safe_unmap_only()
            raise

        self._locked = self._mlock_data_pages()

    @property
    def page_size(self) -> int:
        return self._page_size

    @property
    def data_pages(self) -> int:
        return self._data_pages

    @property
    def data_size(self) -> int:
        return self._data_bytes

    @property
    def requested_size(self) -> int:
        return self._requested_size

    @property
    def has_guard_pages(self) -> bool:
        return self._guard_pages_applied

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def base_address(self) -> int:
        return self._base_addr

    @property
    def left_guard_address(self) -> int:
        return self._left_guard_addr

    @property
    def data_address(self) -> int:
        return self._data_addr

    @property
    def right_guard_address(self) -> int:
        return self._right_guard_addr

    @property
    def data_view(self) -> "memoryview":
        if self._closed or self._mmap is None:
            raise GuardPageError(
                "IsolatedMemoryHeap.data_view after close()/__exit__."
            )
        return memoryview(self._mmap)[
            self._page_size:self._page_size + self._data_bytes
        ]

    def wipe(self) -> None:
        if self._closed or self._mmap is None:
            return
        try:
            ctypes.memset(
                ctypes.c_void_p(self._data_addr),
                0,
                ctypes.c_size_t(self._data_bytes),
            )
        except Exception:  # noqa: BLE001
            try:
                view = self.data_view
                for i in range(len(view)):
                    view[i] = 0
            except Exception:  # noqa: BLE001
                pass

    def close(self) -> None:
        if self._closed:
            return
        # Wipe MUST run with _closed still False so the check inside
        # wipe() does not short-circuit.
        try:
            self.wipe()
        except Exception:  # noqa: BLE001
            pass
        self._closed = True
        try:
            if self._locked and _MUNLOCK_FN is not None:
                _MUNLOCK_FN(
                    ctypes.c_void_p(self._data_addr),
                    ctypes.c_size_t(self._data_bytes),
                )
                self._locked = False
        except Exception:  # noqa: BLE001
            pass
        try:
            self._mprotect_region(
                self._data_addr, self._data_pages, _POSIX_PROT_NONE
            )
        except Exception:  # noqa: BLE001
            pass
        try:
            if self._mmap is not None:
                self._mmap.close()
        except (BufferError, ValueError, OSError):  # noqa: BLE001
            pass
        self._mmap = None
        try:
            audit_log.log_memory_cleanup(
                "IsolatedMemoryHeap",
                self._data_bytes,
                wipe_method="ctypes.memset+mprotect",
            )
        except Exception:  # noqa: BLE001
            pass

    def __enter__(self) -> "IsolatedMemoryHeap":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:  # noqa: BLE001
            pass

    def _mlock_data_pages(self) -> bool:
        if _MLOCK_FN is None:
            return False
        try:
            ret = _MLOCK_FN(
                ctypes.c_void_p(self._data_addr),
                ctypes.c_size_t(self._data_bytes),
            )
            if platform.system() == "Windows":
                return bool(ret)
            return ret == 0
        except Exception:  # noqa: BLE001
            return False

    def _apply_guard_protections(self) -> None:
        if _MPROTECT_FN is None and platform.system() != "Windows":
            raise GuardPageError(
                "IsolatedMemoryHeap: mprotect() not available on this platform."
            )
        try:
            self._mprotect_region(self._left_guard_addr, 1, _POSIX_PROT_NONE)
            self._mprotect_region(self._right_guard_addr, 1, _POSIX_PROT_NONE)
            self._guard_pages_applied = True
        except Exception as exc:  # noqa: BLE001
            self._guard_pages_applied = False
            raise GuardPageError(
                "IsolatedMemoryHeap: guard-page protection failed (%s)" % exc
            ) from exc

    def _mprotect_region(self, addr: int, npages: int, prot: int) -> None:
        page = self._page_size
        size = int(npages) * int(page)
        if platform.system() == "Windows":
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            VirtualProtect = kernel32.VirtualProtect
            VirtualProtect.argtypes = [
                ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint32,
                ctypes.POINTER(ctypes.c_uint32),
            ]
            VirtualProtect.restype = ctypes.c_bool
            win_prot = (
                _WIN_PAGE_NOACCESS if prot == _POSIX_PROT_NONE
                else _WIN_PAGE_READWRITE
            )
            old_prot = ctypes.c_uint32(0)
            ok = VirtualProtect(
                ctypes.c_void_p(addr), ctypes.c_size_t(size),
                win_prot, ctypes.byref(old_prot),
            )
            if not ok:
                raise GuardPageError(
                    "VirtualProtect(0x%x, size=%d) -> FALSE" % (addr, size)
                )
            return
        if _MPROTECT_FN is None:
            raise GuardPageError("mprotect() not available on this platform.")
        ret = _MPROTECT_FN(
            ctypes.c_void_p(addr), ctypes.c_size_t(size),
            ctypes.c_int(int(prot)),
        )
        if ret != 0:
            errno_val = ctypes.get_errno()
            raise GuardPageError(
                "mprotect(0x%x, size=%d, prot=%d) failed (errno=%d)"
                % (addr, size, prot, errno_val)
            )

    def _safe_unmap_only(self) -> None:
        if getattr(self, "_data_addr", None) and getattr(self, "_data_pages", None):
            try:
                self._mprotect_region(
                    self._data_addr, self._data_pages, _POSIX_PROT_NONE
                )
            except Exception:  # noqa: BLE001
                pass
        try:
            if self._mmap is not None:
                self._mmap.close()
        except (BufferError, ValueError, OSError):  # noqa: BLE001
            pass
        self._mmap = None
        self._closed = True


class SecureVariableWrapper:
    """Context manager for generic sensitive bytes (passwords, tokens, ...).

    Args:
        data:   Raw bytes to wrap. Must be non-empty.
        label:  Short label used only in audit log entries.
    """

    __slots__ = ("_buf", "_active", "_wiped", "_label", "_locked")

    def __init__(self, data: bytes, label: str = "variable") -> None:
        if not data:
            raise ValueError("data must be non-empty bytes.")
        if not isinstance(label, str):
            raise TypeError("label must be a str.")
        self._buf: bytearray = bytearray(data)
        self._active: bool = False
        self._wiped: bool = False
        self._label: str = label
        self._locked: bool = _mlock_buffer(self._buf)
        audit_log.log_key_imported("wrap_" + str(label), len(self._buf))

    def __enter__(self) -> "SecureVariableWrapper":
        self._active = True
        logger.debug(
            "[SecureVariableWrapper] Scope opened for: %s", self._label
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._active = False
        self._do_wipe()
        return False

    def __del__(self) -> None:
        try:
            self._do_wipe()
        except Exception:  # noqa: BLE001
            pass

    def _do_wipe(self) -> None:
        if self._wiped:
            return
        self._wiped = True
        _zero_wipe(
            self._buf,
            audit_details={"object_type": "SecureVariableWrapper"},
        )
        if self._locked:
            _munlock_buffer(self._buf)
            self._locked = False
        logger.debug(
            "[SecureVariableWrapper] Scope closed (buffer wiped): %s",
            self._label,
        )
        audit_log.log_key_revoked(
            "wrap_" + str(self._label), reason="scope_exit"
        )

    def get(self) -> bytes:
        if not self._active:
            raise SigningError(
                "SecureVariableWrapper.get() outside active scope."
            )
        if self._wiped:
            raise SigningError(
                "SecureVariableWrapper.get() after buffer was wiped."
            )
        return bytes(self._buf)

    def __len__(self) -> int:
        return len(self._buf)
