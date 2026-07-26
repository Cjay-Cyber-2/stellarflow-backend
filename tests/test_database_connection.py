from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from database.connection import (
    HEARTBEAT_QUERY,
    PooledConnectionRecycler,
    _default_broken_exceptions,
)


class StaleSocketError(Exception):
    """Stand-in for a driver's stale-socket error."""


def _make_pool() -> MagicMock:
    """A fake connection pool whose connections hand out recording cursors."""
    pool = MagicMock()
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    pool.getconn.return_value = conn
    return pool


def _make_recycler(pool=None, **kwargs) -> PooledConnectionRecycler:
    kwargs.setdefault("broken_exceptions", (StaleSocketError,))
    return PooledConnectionRecycler(pool or _make_pool(), **kwargs)


class TestConnectionPoolRecycling:
    """Tests for PooledConnectionRecycler — validates connections before
    issuing queries and recycles stale TCP paths automatically."""

    def test_healthy_connection_validated_and_returned(self):
        pool = _make_pool()
        recycler = _make_recycler(pool)

        conn = recycler.getconn()

        conn.cursor.return_value.execute.assert_called_once_with(HEARTBEAT_QUERY)
        # The same validated connection is returned to the caller.
        assert conn is pool.getconn.return_value

    def test_stale_connection_recycled_and_retried(self):
        pool = _make_pool()
        # First connection is stale; second one is healthy.
        stale_conn = MagicMock()
        stale_conn.cursor.return_value.execute.side_effect = StaleSocketError("broken pipe")
        healthy_conn = MagicMock()
        pool.getconn.side_effect = [stale_conn, healthy_conn]

        recycler = _make_recycler(pool)

        conn = recycler.getconn()

        # The stale connection was discarded with close=True.
        pool.putconn.assert_any_call(stale_conn, close=True)
        # The returned connection is the healthy one.
        assert conn is healthy_conn
        # The healthy connection passed validation.
        healthy_conn.cursor.return_value.execute.assert_called_once_with(HEARTBEAT_QUERY)

    def test_all_stale_connections_raise_runtime_error(self):
        pool = _make_pool()
        stale_conn = MagicMock()
        stale_conn.cursor.return_value.execute.side_effect = StaleSocketError("dead")
        pool.getconn.return_value = stale_conn

        recycler = _make_recycler(pool, max_retries=1)

        with pytest.raises(RuntimeError, match="all 2 attempt"):
            recycler.getconn()

        # Both attempts discarded their stale connection.
        assert pool.putconn.call_count == 2

    def test_non_broken_errors_bubble_up(self):
        pool = _make_pool()
        pool.getconn.return_value.cursor.return_value.execute.side_effect = ValueError("unexpected")

        recycler = _make_recycler(pool)

        with pytest.raises(ValueError, match="unexpected"):
            recycler.getconn()

    def test_putconn_delegates_to_pool(self):
        pool = _make_pool()
        recycler = _make_recycler(pool)
        conn = object()

        recycler.putconn(conn, close=True)

        pool.putconn.assert_called_once_with(conn, close=True)

    def test_putconn_without_close_falls_back(self):
        pool = _make_pool()

        def putconn(_conn, **kwargs):
            if kwargs:
                raise TypeError("putconn() got an unexpected keyword argument")

        pool.putconn.side_effect = putconn
        recycler = _make_recycler(pool)

        conn = object()
        # Should not raise even though the pool rejects close=True.
        recycler.putconn(conn, close=True)

    def test_closeall_delegates_to_pool(self):
        pool = _make_pool()
        recycler = _make_recycler(pool)

        recycler.closeall()

        pool.closeall.assert_called_once()

    def test_invalid_arguments_rejected(self):
        pool = _make_pool()
        with pytest.raises(ValueError, match="pool must not be None"):
            PooledConnectionRecycler(None)
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            PooledConnectionRecycler(pool, max_retries=-1)

    def test_getconn_retries_default_behavior(self):
        pool = _make_pool()
        stale = MagicMock()
        stale.cursor.return_value.execute.side_effect = StaleSocketError("stale")
        healthy = MagicMock()
        pool.getconn.side_effect = [stale, healthy]

        recycler = _make_recycler(pool)

        conn = recycler.getconn()
        assert conn is healthy
        pool.putconn.assert_called_once_with(stale, close=True)

    def test_concurrent_getconn_returns_independent_validated_conns(self):
        import threading

        pool = _make_pool()
        recycler = _make_recycler(pool)
        results: list = []

        def worker():
            conn = recycler.getconn()
            results.append(conn)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 4
        # Each call to getconn validated the connection with a SELECT 1.
        assert pool.getconn.call_count == 4

    def test_default_broken_exceptions_includes_oserror(self):
        assert OSError in _default_broken_exceptions()


def test_connection_pool_recycling():
    """Integration-level: PooledConnectionRecycler detects stale TCP paths
    and recycles them before issuing queries."""
    pool = _make_pool()
    stale_conn = MagicMock()
    stale_conn.cursor.return_value.execute.side_effect = StaleSocketError(
        "server closed the connection unexpectedly"
    )
    healthy_conn = MagicMock()
    pool.getconn.side_effect = [stale_conn, healthy_conn]

    recycler = _make_recycler(pool)

    conn = recycler.getconn()

    pool.putconn.assert_any_call(stale_conn, close=True)
    assert conn is healthy_conn
    healthy_conn.cursor.return_value.execute.assert_called_once_with(HEARTBEAT_QUERY)
