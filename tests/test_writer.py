from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from database.writer import (
    DEFAULT_BATCH_SIZE,
    RelationalWriter,
    AsyncRelationalWriter,
    asyncpg,
)


def _make_connection() -> MagicMock:
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def _make_asyncpg_connection() -> AsyncMock:
    conn = AsyncMock()
    conn.copy_records_to_table = AsyncMock()
    return conn


def test_save_rejects_non_dict():
    writer = RelationalWriter(_make_connection())
    try:
        with pytest.raises(TypeError):
            writer.save(["not", "a", "dict"])  # type: ignore[arg-type]
    finally:
        writer.shutdown()


def test_save_rejects_invalid_arguments():
    with pytest.raises(ValueError):
        RelationalWriter(None)
    with pytest.raises(ValueError):
        RelationalWriter(_make_connection(), batch_size=0)
    with pytest.raises(ValueError):
        RelationalWriter(_make_connection(), flush_interval_ms=0)


def test_flush_on_batch_size_uses_execute_values():
    conn = _make_connection()
    writer = RelationalWriter(conn, batch_size=3, flush_interval_ms=60_000)

    try:
        with patch("database.writer._execute_values_bulk") as execute_values:
            for index in range(3):
                writer.save({"asset_id": f"asset-{index}", "price": index})

            execute_values.assert_called_once()
            sql = execute_values.call_args.args[1]
            values = execute_values.call_args.args[2]
            assert "INSERT INTO telemetry (asset_id, price) VALUES %s" == sql
            assert len(values) == 3
            conn.commit.assert_called_once()
    finally:
        writer.shutdown()


def test_interval_flush_runs_in_background():
    conn = _make_connection()
    flushed = threading.Event()

    def _execute_values(_cursor, _sql, _values, page_size):
        flushed.set()

    writer = RelationalWriter(conn, batch_size=50, flush_interval_ms=50)

    try:
        with patch("database.writer._execute_values_bulk", side_effect=_execute_values):
            writer.save({"asset_id": "NGN/XLM", "price": 1})
            assert flushed.wait(timeout=2.0), "expected timed flush within 2s"
    finally:
        writer.shutdown()


def test_failed_flush_requeues_records():
    conn = _make_connection()
    writer = RelationalWriter(conn, batch_size=2, flush_interval_ms=60_000)

    try:
        with patch(
            "database.writer._execute_values_bulk",
            side_effect=RuntimeError("connection reset"),
        ):
            writer.save({"asset_id": "a", "price": 1})
            with pytest.raises(RuntimeError, match="connection reset"):
                writer.save({"asset_id": "b", "price": 2})

        conn.rollback.assert_called_once()

        with patch("database.writer._execute_values_bulk") as execute_values:
            writer._flush()
            assert execute_values.call_args.args[2] == [("a", 1), ("b", 2)]
    finally:
        writer.shutdown()


def test_shutdown_flushes_remaining_rows():
    conn = _make_connection()
    writer = RelationalWriter(conn, batch_size=DEFAULT_BATCH_SIZE, flush_interval_ms=60_000)

    with patch("database.writer._execute_values_bulk") as execute_values:
        writer.save({"asset_id": "solo", "price": 99})
        writer.shutdown()
        execute_values.assert_called_once()
        assert execute_values.call_args.args[2] == [("solo", 99)]


@pytest.mark.asyncio
@pytest.mark.skipif(asyncpg is None, reason="asyncpg not installed")
async def test_copy_binary_stream():
    conn = _make_asyncpg_connection()
    writer = AsyncRelationalWriter(
        conn, batch_size=3, flush_interval_ms=60_000
    )

    try:
        await writer.start()
        for index in range(3):
            await writer.save({"asset_id": f"asset-{index}", "price": index})

        # Give it a moment to flush
        await asyncio.sleep(0.1)

        conn.copy_records_to_table.assert_called_once()
        call_args = conn.copy_records_to_table.call_args
        assert call_args.args[0] == "telemetry"
        assert call_args.kwargs["columns"] == ["asset_id", "price"]
        assert call_args.kwargs["format"] == "binary"
        assert len(call_args.kwargs["records"]) == 3
    finally:
        await writer.shutdown()
