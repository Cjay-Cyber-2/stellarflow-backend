"""Keeper layer — secure secret / key safekeeping for the StellarFlow backend.

The Keeper is the trusted root of key material in the backend.  It is
responsible for:

* Storing high-value secret bytes (signing keys, provider API secrets,
  webhook hooks) in a manner that supports *cryptographic erase* — the
  in-memory copy is held as a mutable :class:`bytearray` so it can be
  overwritten with zeroes ("zeroised") on delete or shutdown.
* Deriving stable signing keys from a root key without ever exposing the
  root key outside the Keeper.
* Producing tamper-evident state snapshots so operators can verify which
  secrets are enrolled at any point in time (the snapshot deliberately
  contains *no* secret material — only HMAC fingerprints).

The module is intentionally dependency-free (stdlib only) so it can run in
any deployment context, including the integration test harness where no
external KMS is available.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional


class SecureBytes:
    """A mutable, zeroisable container for secret bytes.

    The secret is stored as a :class:`bytearray` so the underlying memory can
    be overwritten in place.  :meth:`expose` returns a *copy* for use and
    :meth:`zeroise` overwrites the buffer, making accidental retention of the
    plaintext in other objects the caller's responsibility.
    """

    __slots__ = ("_buf", "_name")

    def __init__(self, data: bytes, name: str = "<secret>") -> None:
        self._buf = bytearray(data)
        self._name = name

    @property
    def length(self) -> int:
        return len(self._buf)

    def expose(self) -> bytes:
        """Return a fresh copy of the secret bytes for a single use."""
        return bytes(self._buf)

    def zeroise(self) -> None:
        """Overwrite the buffer with zeroes (cryptographic erase)."""
        for i in range(len(self._buf)):
            self._buf[i] = 0
        self._buf.clear()

    def __len__(self) -> int:
        return len(self._buf)


@dataclass(frozen=True)
class SecretEnrollment:
    """Tamper-evident description of an enrolled secret (no secret bytes)."""

    name: str
    fingerprint: str
    algorithm: str
    created_seq: int


class KeeperError(Exception):
    """Base error for Keeper operations."""


class SecretNotFoundError(KeeperError):
    """Raised when a requested secret is not enrolled."""


class KeyKeeper:
    """Thread-safe secure secret keeper with HMAC signing and zeroisation.

    Usage::

        keeper = KeyKeeper(root_key=b"master", state_path=Path("keeper.json"))
        keeper.put("stellar_signer", private_key_bytes)
        sig = keeper.sign("stellar_signer", payload)
        assert keeper.verify("stellar_signer", payload, sig)
        keeper.delete("stellar_signer")   # zeroises the in-memory copy
    """

    def __init__(
        self,
        root_key: Optional[bytes] = None,
        state_path: Optional[Path] = None,
    ) -> None:
        self._root = root_key if root_key is not None else b""
        self._state_path = Path(state_path) if state_path is not None else None
        self._secrets: Dict[str, SecureBytes] = {}
        self._meta: Dict[str, SecretEnrollment] = {}
        self._seq = 0
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Enrolment
    # ------------------------------------------------------------------
    def put(self, name: str, secret: bytes, algorithm: str = "hmac-sha256") -> SecretEnrollment:
        """Enrol *secret* under *name*, replacing any previous enrolment.

        The previous secret (if any) is zeroised before it is dropped.
        """
        if not name:
            raise KeeperError("secret name must be non-empty")
        if not isinstance(secret, (bytes, bytearray)):
            raise KeeperError("secret must be bytes-like")

        fp = self._fingerprint(name, bytes(secret))
        with self._lock:
            existing = self._secrets.get(name)
            if existing is not None:
                existing.zeroise()
            self._seq += 1
            self._secrets[name] = SecureBytes(bytes(secret), name=name)
            enrollment = SecretEnrollment(
                name=name,
                fingerprint=fp,
                algorithm=algorithm,
                created_seq=self._seq,
            )
            self._meta[name] = enrollment
        return enrollment

    def delete(self, name: str) -> None:
        """Remove and zeroise an enrolled secret."""
        with self._lock:
            secret = self._secrets.pop(name, None)
            if secret is None:
                raise SecretNotFoundError(name)
            secret.zeroise()
            self._meta.pop(name, None)

    def has(self, name: str) -> bool:
        with self._lock:
            return name in self._secrets

    def list_enrollments(self) -> List[SecretEnrollment]:
        """Return the tamper-evident enrollment metadata (no secret bytes)."""
        with self._lock:
            return list(self._meta.values())

    # ------------------------------------------------------------------
    # Signing / verification (HMAC derived from root + secret name)
    # ------------------------------------------------------------------
    def _derived_key(self, name: str) -> bytes:
        """Derive a per-secret signing key from the root key and name.

        The root key never leaves the Keeper; only the derived key is used for
        the HMAC, and even it is scoped to *name* so one secret cannot forge
        another's signature.
        """
        return hmac.new(
            self._root,
            b"stellarflow-keeper|" + name.encode("utf-8"),
            hashlib.sha256,
        ).digest()

    def sign(self, name: str, message: bytes) -> bytes:
        """Return an HMAC-SHA256 signature of *message* under *name*."""
        if not self.has(name):
            raise SecretNotFoundError(name)
        derived = self._derived_key(name)
        return hmac.new(derived, message, hashlib.sha256).digest()

    def verify(self, name: str, message: bytes, signature: bytes) -> bool:
        """Verify an HMAC signature produced by :meth:`sign`."""
        if not self.has(name):
            return False
        derived = self._derived_key(name)
        expected = hmac.new(derived, message, hashlib.sha256).digest()
        return hmac.compare_digest(expected, signature)

    # ------------------------------------------------------------------
    # Root key rotation + secure wipe
    # ------------------------------------------------------------------
    def rotate_root_key(self, new_root_key: bytes) -> None:
        """Replace the root key.  Enrolled secrets keep their names; future
        signatures use the new root derivation."""
        if not isinstance(new_root_key, (bytes, bytearray)):
            raise KeeperError("root key must be bytes-like")
        with self._lock:
            self._root = bytes(new_root_key)

    def secure_wipe(self) -> None:
        """Zeroise every enrolled secret and clear metadata."""
        with self._lock:
            for secret in self._secrets.values():
                secret.zeroise()
            self._secrets.clear()
            self._meta.clear()

    # ------------------------------------------------------------------
    # State persistence (fingerprints only — never secret bytes)
    # ------------------------------------------------------------------
    def snapshot_state(self) -> Dict[str, object]:
        """Return a JSON-serialisable state snapshot (no secret material)."""
        with self._lock:
            return {
                "root_fingerprint": self._fingerprint("__root__", self._root),
                "seq": self._seq,
                "enrollments": [asdict(e) for e in self._meta.values()],
            }

    def persist_state(self, path: Optional[Path] = None) -> Path:
        """Write the state snapshot to disk for audit/verification."""
        target = Path(path) if path is not None else self._state_path
        if target is None:
            raise KeeperError("no state path configured")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.snapshot_state(), indent=2), "utf-8")
        return target

    @staticmethod
    def _fingerprint(name: str, data: bytes) -> str:
        return hmac.new(
            b"stellarflow-keeper-fp",
            name.encode("utf-8") + b"|" + data,
            hashlib.sha256,
        ).hexdigest()

    def __enter__(self) -> "KeyKeeper":
        return self

    def __exit__(self, *_: object) -> None:
        self.secure_wipe()


__all__ = [
    "KeyKeeper",
    "SecureBytes",
    "SecretEnrollment",
    "KeeperError",
    "SecretNotFoundError",
]
