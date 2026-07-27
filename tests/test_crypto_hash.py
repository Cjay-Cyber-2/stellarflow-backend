import hashlib
import platform

import pytest

from utils import crypto_hash


PAYLOAD = (
    b'{"validator":"stellar-africa-01","sequence":18446744073709551615,'
    b'"signature":"b3d1a247c4f72a94e4f9279c0af6c7f8"}'
)


def _mean_seconds(benchmark) -> float:
    stats = getattr(benchmark, "stats", None)
    stats = getattr(stats, "stats", stats)
    mean = getattr(stats, "mean", None)
    if mean is None:
        mean = stats["mean"]
    return float(mean)


def test_c_bindings_loaded_on_cpython():
    if platform.python_implementation() != "CPython":
        pytest.skip("C-extension binding requirement applies to CPython")

    assert crypto_hash.C_BINDINGS_LOADED is True
    assert crypto_hash.BACKEND.name in {"openssl_sha256", "hashlib.sha256"}


def test_sha256_digest_matches_hashlib():
    assert crypto_hash.sha256_digest(PAYLOAD) == hashlib.sha256(PAYLOAD).digest()
    assert crypto_hash.sha256_hexdigest(PAYLOAD) == hashlib.sha256(PAYLOAD).hexdigest()


def test_digest_payload_map_and_verify():
    payloads = [PAYLOAD, PAYLOAD + b"-2", memoryview(PAYLOAD + b"-3")]

    digest_map = crypto_hash.digest_payload_map(payloads)

    assert len(digest_map) == len(payloads)
    assert crypto_hash.verify_digest_map(digest_map)


@pytest.mark.benchmark(group="crypto_hash")
def test_payload_hashing_under_15_microseconds_per_packet(benchmark):
    result = benchmark(crypto_hash.sha256_digest, PAYLOAD)

    assert result == hashlib.sha256(PAYLOAD).digest()
    assert _mean_seconds(benchmark) < 15e-6


@pytest.mark.benchmark(group="crypto_hash")
def test_payload_map_hashing_under_15_microseconds_per_packet(benchmark):
    payloads = tuple(PAYLOAD + bytes([idx]) for idx in range(32))

    digest_map = benchmark(crypto_hash.digest_payload_map, payloads)
    per_packet_mean = _mean_seconds(benchmark) / len(payloads)

    assert crypto_hash.verify_digest_map(digest_map)
    assert per_packet_mean < 15e-6
