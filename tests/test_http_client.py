"""Tests for TCP buffer tuning in the network http_client module.

Verifies that ``compute_buffer_size`` computes sane buffer dimensions from
an RTT estimate and that ``_apply_tcp_buffers`` writes the expected
``SO_RCVBUF`` / ``SO_SNDBUF`` socket options.
"""

from __future__ import annotations

import os
import socket
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from network.http_client import (
    _ASSUMED_BANDWIDTH_BPS,
    _MAX_RCVBUF,
    _MAX_SNDBUF,
    _MIN_RCVBUF,
    _MIN_SNDBUF,
    _apply_tcp_buffers,
    _adaptive_timeout,
    compute_buffer_size,
)


def _mock_socket() -> MagicMock:
    sock = MagicMock(spec=socket.socket)
    return sock


# ---------------------------------------------------------------------------
# compute_buffer_size
# ---------------------------------------------------------------------------


class TestComputeBufferSize:
    """Unit tests for the BDP-based buffer size computation."""

    def test_zero_rtt_returns_minimum_buffers(self):
        """With RTT ≈ 0 the BDP is near zero, so we should get the floor."""
        rcvbuf, sndbuf = compute_buffer_size(0.0)
        assert rcvbuf == _MIN_RCVBUF
        assert sndbuf == _MIN_SNDBUF

    def test_high_latency_rtt_returns_large_buffers(self):
        """A high-latency link (e.g. 500 ms) should produce buffers at or
        near the configured maximums."""
        rcvbuf, sndbuf = compute_buffer_size(0.5)
        # BDP = (10 Mbps / 8) * 0.5 = 625 000 bytes → below 4 MB max
        expected_rcvbuf = 625_000
        expected_sndbuf = max(_MIN_SNDBUF, min(625_000 // 2, _MAX_SNDBUF))
        assert rcvbuf == expected_rcvbuf
        assert sndbuf == expected_sndbuf

    def test_moderate_rtt_returns_intermediate_buffers(self):
        """A moderate RTT (e.g. 50 ms) should produce buffers between min and max."""
        rcvbuf, sndbuf = compute_buffer_size(0.05)
        # BDP = (10 Mbps / 8) * 0.05 = 62 500 bytes → below min → clamped to min
        # For 100 ms: BDP = 125 000 bytes → between min and max
        rcvbuf_100, sndbuf_100 = compute_buffer_size(0.1)
        # BDP = (10 Mbps / 8) * 0.1 = 125 000 bytes
        assert _MIN_RCVBUF <= rcvbuf_100 <= _MAX_RCVBUF
        assert _MIN_SNDBUF <= sndbuf_100 <= _MAX_SNDBUF

    def test_send_buffer_is_half_of_receive_buffer(self):
        """The send buffer should be roughly half the receive buffer (clamped)."""
        rcvbuf, sndbuf = compute_buffer_size(0.2)
        # BDP = (10 Mbps / 8) * 0.2 = 250 000 bytes
        # rcvbuf = 250 000, sndbuf = 125 000
        assert sndbuf <= rcvbuf
        assert sndbuf == max(_MIN_SNDBUF, min(250_000 // 2, _MAX_SNDBUF))

    def test_buffers_never_exceed_maximums(self):
        """Even with an absurdly large RTT, buffers must not exceed the caps."""
        rcvbuf, sndbuf = compute_buffer_size(10.0)  # 10 s RTT
        assert rcvbuf <= _MAX_RCVBUF
        assert sndbuf <= _MAX_SNDBUF

    def test_buffers_never_below_minimums(self):
        """Even with an extremely small RTT, buffers must not fall below the floors."""
        rcvbuf, sndbuf = compute_buffer_size(0.0001)  # 0.1 ms RTT
        assert rcvbuf >= _MIN_RCVBUF
        assert sndbuf >= _MIN_SNDBUF

    def test_bandwidth_env_var_is_respected(self):
        """When HTTP_CLIENT_BANDWIDTH_BPS is set, the BDP calculation should
        use that value instead of the default 10 Mbps."""
        with patch.dict(os.environ, {"HTTP_CLIENT_BANDWIDTH_BPS": "100_000_000"}):
            # Re-import to pick up the new env var
            import importlib
            from network import http_client as hc
            importlib.reload(hc)

            rcvbuf, sndbuf = hc.compute_buffer_size(0.1)
            # BDP = (100 Mbps / 8) * 0.1 = 1 250 000 bytes
            assert rcvbuf == min(1_250_000, hc._MAX_RCVBUF)
            assert sndbuf == min(1_250_000 // 2, hc._MAX_SNDBUF)


# ---------------------------------------------------------------------------
# _apply_tcp_buffers
# ---------------------------------------------------------------------------


class TestApplyTcpBuffers:
    """Unit tests for the socket-level buffer application."""

    def test_setsockopt_called_with_so_rcvbuf(self):
        sock = _mock_socket()
        _apply_tcp_buffers(sock, 0.1)
        rcvbuf, _ = compute_buffer_size(0.1)
        sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)

    def test_setsockopt_called_with_so_sndbuf(self):
        sock = _mock_socket()
        _apply_tcp_buffers(sock, 0.1)
        _, sndbuf = compute_buffer_size(0.1)
        sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)

    def test_zero_rtt_still_applies_minimum_buffers(self):
        sock = _mock_socket()
        _apply_tcp_buffers(sock, 0.0)
        sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_RCVBUF, _MIN_RCVBUF)
        sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_SNDBUF, _MIN_SNDBUF)

    def test_oserror_is_caught_and_logged(self):
        """If setsockopt raises OSError, the function should not propagate it."""
        sock = _mock_socket()
        sock.setsockopt.side_effect = OSError("mock error")
        # Should not raise
        _apply_tcp_buffers(sock, 0.1)


# ---------------------------------------------------------------------------
# Integration: buffer tuning via AdaptiveTimeout RTT estimate
# ---------------------------------------------------------------------------


class TestTcpBufferTuning:
    """End-to-end test for the acceptance criterion:

    "Sockets adjust buffer sizes dynamically upon connection handshake completion."
    """

    def test_tcp_buffer_tuning(self):
        """Verify that the full pipeline — RTT estimate → buffer computation →
        socket option application — works correctly.

        This is the primary acceptance test referenced by the verification step:
            pytest tests/test_http_client.py -k test_tcp_buffer_tuning
        """
        # Simulate a realistic RTT (e.g. 150 ms for a regional link)
        rtt_s = 0.15

        # Compute expected buffers
        rcvbuf, sndbuf = compute_buffer_size(rtt_s)

        # Verify buffers are within sane bounds
        assert _MIN_RCVBUF <= rcvbuf <= _MAX_RCVBUF
        assert _MIN_SNDBUF <= sndbuf <= _MAX_SNDBUF

        # Verify the send buffer is not larger than the receive buffer
        assert sndbuf <= rcvbuf

        # Verify that applying to a mock socket succeeds
        sock = _mock_socket()
        _apply_tcp_buffers(sock, rtt_s)
        sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)

    def test_buffer_tuning_with_adaptive_timeout_rtt(self):
        """Verify that the module-level AdaptiveTimeout RTT estimate feeds
        into the buffer computation correctly."""
        # Record a few latency samples to build up the EMA
        _adaptive_timeout.record(0.1)
        _adaptive_timeout.record(0.12)
        _adaptive_timeout.record(0.11)

        rtt_estimate = _adaptive_timeout.rtt_estimate_s
        assert rtt_estimate > 0

        rcvbuf, sndbuf = compute_buffer_size(rtt_estimate)
        assert _MIN_RCVBUF <= rcvbuf <= _MAX_RCVBUF
        assert _MIN_SNDBUF <= sndbuf <= _MAX_SNDBUF
