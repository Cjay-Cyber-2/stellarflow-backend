"""Fast SHA-256 helpers for validator payload state maps.

The module resolves the fastest available CPython-backed SHA-256 constructor at
import time and keeps that callable in a module-local binding for hot paths.
"""
from __future__ import annotations

import hashlib
import platform
from dataclasses import dataclass
from typing import Iterable, Mapping

Payload = bytes | bytearray | memoryview


@dataclass(frozen=True)
class HashBackend:
    name: str
    c_accelerated: bool
    detail: str


def _resolve_sha256():
    runtime = platform.python_implementation()

    if runtime == "CPython":
        try:
            from _hashlib import openssl_sha256

            openssl_sha256(b"").digest()
            return openssl_sha256, HashBackend(
                name="openssl_sha256",
                c_accelerated=True,
                detail="_hashlib OpenSSL SHA-256 binding",
            )
        except Exception:
            pass

    try:
        sha256 = hashlib.sha256
        sha256(b"").digest()
        module = getattr(sha256, "__module__", "")
        accelerated = runtime == "CPython" and module in {"_hashlib", "_sha2"}
        return sha256, HashBackend(
            name="hashlib.sha256",
            c_accelerated=accelerated,
            detail=f"{module or 'hashlib'} SHA-256 constructor",
        )
    except Exception as exc:
        raise RuntimeError("No usable SHA-256 backend is available") from exc


_SHA256, BACKEND = _resolve_sha256()
BACKEND_NAME = BACKEND.name
C_BINDINGS_LOADED = BACKEND.c_accelerated


def sha256_digest(payload: Payload) -> bytes:
    """Return a 32-byte SHA-256 digest for a validator payload."""
    return _SHA256(payload).digest()


def sha256_hexdigest(payload: Payload) -> str:
    """Return a hex SHA-256 digest for storage and diagnostics."""
    return _SHA256(payload).hexdigest()


def digest_payload_map(payloads: Iterable[Payload]) -> dict[bytes, Payload]:
    """Map binary SHA-256 digests to payloads with minimal hot-path overhead."""
    sha256 = _SHA256
    return {sha256(payload).digest(): payload for payload in payloads}


def hexdigest_payload_map(payloads: Iterable[Payload]) -> dict[str, Payload]:
    """Map hex SHA-256 digests to payloads for JSON-friendly state maps."""
    sha256 = _SHA256
    return {sha256(payload).hexdigest(): payload for payload in payloads}


def verify_digest_map(digest_map: Mapping[bytes, Payload]) -> bool:
    """Validate that every key matches the SHA-256 digest of its payload."""
    sha256 = _SHA256
    return all(digest == sha256(payload).digest() for digest, payload in digest_map.items())
