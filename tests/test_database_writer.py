from __future__ import annotations

import sqlite3
import threading
from unittest.mock import MagicMock, call, patch

import pytest

from database.writer import PartitionedTelemetryWriter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_sink():
    """Return a mock BatchSink with a sqlite3.Connection-like mock."""
    sink = MagicMock()
    sink._conn = MagicMock()
    sink._lock = threading.Lock()
    sink._buffer = []
    return sink


def _make_mock_cursor(execute_results=None):
    """Return a mock cursor with configurable execute results.

    ``execute_results`` is a list of return values per execute() call.
    """
    cursor = MagicMock()
    cursor.execute = MagicMock()
    if execute_results:
        cursor.fetchone.side_effect = execute_results
    else:
        cursor.fetchone.return_value = None
    return cursor


# ---------------------------------------------------------------------------
# ANALYZE trigger tests (Issue #634)
# ---------------------------------------------------------------------------


class TestPartitionAnalyzeTrigger:
    """Test suite for automatic ANALYZE after new partition creation."""

    def test_partition_analyze_trigger(self):
        """Primary entry-point: new partition creation triggers ANALYZE.

        Named to match the Issue #634 verification filter:
        ``pytest -k test_partition_analyze_trigger``.
        """
        self.test_new_partition_triggers_analyze()

    def test_new_partition_triggers_analyze(self):
        """Successful partition creation triggers ANALYZE on the new table."""
        sink = _make_mock_sink()
        cursor = _make_mock_cursor(
            # First fetchone (table-exists check): None → table does NOT exist
            # Second fetchone not called because ANALYZE returns nothing
            execute_results=[None]
        )
        sink._conn.cursor.return_value = cursor

        writer = PartitionedTelemetryWriter(
            sink, base_table="telemetry", timestamp_field="ts"
        )

        record = {"asset_id": "NGN/XLM", "price": 123.45, "ts": 1700000000}
        writer.save(record)

        # Verify the sequence of SQL calls
        execute_calls = [c.args[0] for c in cursor.execute.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS" in call_str for call_str in execute_calls), \
            "Expected CREATE TABLE IF NOT EXISTS to be executed"
        assert any("ANALYZE" in call_str for call_str in execute_calls), \
            "Expected ANALYZE to be executed on the new partition"

        # Verify ANALYZE was called after CREATE
        create_idx = next(
            i for i, c in enumerate(execute_calls) if "CREATE TABLE IF NOT EXISTS" in c
        )
        analyze_indices = [
            i
            for i, c in enumerate(execute_calls)
            if c.strip().upper().startswith("ANALYZE")
        ]
        assert analyze_indices, "ANALYZE was not executed"
        assert all(idx > create_idx for idx in analyze_indices), \
            "ANALYZE must execute after CREATE TABLE"

        # Verify cursor was closed
        cursor.close.assert_called_once()

    def test_existing_partition_skips_analyze(self):
        """Existing partitions do not trigger duplicate ANALYZE."""
        sink = _make_mock_sink()
        cursor = _make_mock_cursor(
            # First fetchone: returns a row → table EXISTS
            execute_results=[("telemetry_2024_W01",)]
        )
        sink._conn.cursor.return_value = cursor

        writer = PartitionedTelemetryWriter(
            sink, base_table="telemetry", timestamp_field="ts"
        )

        record = {"asset_id": "NGN/XLM", "price": 123.45, "ts": 1700000000}
        writer.save(record)

        execute_calls = [c.args[0] for c in cursor.execute.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS" in call_str for call_str in execute_calls), \
            "CREATE TABLE IF NOT EXISTS should still execute for safety"

        # ANALYZE must NOT appear in any executed SQL
        analyze_calls = [
            c
            for c in execute_calls
            if isinstance(c, str) and c.strip().upper().startswith("ANALYZE")
        ]
        assert not analyze_calls, \
            "ANALYZE must NOT be executed for an existing partition"

        cursor.close.assert_called_once()

    def test_failed_partition_creation_no_analyze(self):
        """Failed partition creation does not execute ANALYZE."""
        sink = _make_mock_sink()
        cursor = _make_mock_cursor(
            # Table does not exist
            execute_results=[None]
        )
        # The table-exists query succeeds, but CREATE TABLE fails
        cursor.execute.side_effect = [
            None,  # SELECT from sqlite_master succeeds
            sqlite3.Error("disk I/O error"),  # CREATE TABLE fails
        ]
        sink._conn.cursor.return_value = cursor

        writer = PartitionedTelemetryWriter(
            sink, base_table="telemetry", timestamp_field="ts"
        )

        record = {"asset_id": "NGN/XLM", "price": 123.45, "ts": 1700000000}

        with pytest.raises(sqlite3.Error, match="disk I/O error"):
            writer.save(record)

        # ANALYZE must never have been called
        analyze_calls = [
            c
            for c in cursor.execute.call_args_list
            if isinstance(c.args[0], str)
            and c.args[0].strip().upper().startswith("ANALYZE")
        ]
        assert not analyze_calls, \
            "ANALYZE must NOT execute when partition creation fails"

        cursor.close.assert_called_once()

    def test_analyze_db_error_is_propagated(self):
        """If ANALYZE itself fails, the error propagates correctly."""
        sink = _make_mock_sink()
        cursor = _make_mock_cursor(
            # Table does not exist
            execute_results=[None]
        )
        cursor.execute.side_effect = [
            None,  # SELECT from sqlite_master succeeds
            None,  # CREATE TABLE succeeds
            sqlite3.Error("database is locked"),  # ANALYZE fails
        ]
        sink._conn.cursor.return_value = cursor

        writer = PartitionedTelemetryWriter(
            sink, base_table="telemetry", timestamp_field="ts"
        )

        record = {"asset_id": "NGN/XLM", "price": 123.45, "ts": 1700000000}

        with pytest.raises(sqlite3.Error, match="database is locked"):
            writer.save(record)

        cursor.close.assert_called_once()

    def test_analyze_not_called_for_known_partition(self):
        """Partitions already tracked in _known_partitions skip _create_partition entirely."""
        sink = _make_mock_sink()
        cursor = _make_mock_cursor(execute_results=[None])
        sink._conn.cursor.return_value = cursor

        writer = PartitionedTelemetryWriter(
            sink, base_table="telemetry", timestamp_field="ts"
        )

        record = {"asset_id": "NGN/XLM", "price": 123.45, "ts": 1700000000}

        # First save: partition is new → CREATE + ANALYZE
        writer.save(record)

        execute_calls_first = [c.args[0] for c in cursor.execute.call_args_list]
        analyze_count_first = sum(
            1
            for c in execute_calls_first
            if isinstance(c, str) and c.strip().upper().startswith("ANALYZE")
        )
        assert analyze_count_first == 1, "First save should trigger ANALYZE"

        # Reset mock for second save
        cursor.reset_mock()
        cursor.execute = MagicMock()
        cursor.fetchone = MagicMock()
        cursor.fetchone.return_value = None
        sink._conn.cursor.return_value = cursor

        # Second save: same partition → _known_partitions has it → no _create_partition
        writer.save(record)

        # The second save should not touch the cursor (no _create_partition call)
        execute_calls_second = [
            c.args[0]
            for c in cursor.execute.call_args_list
            if isinstance(c.args[0], str)
        ]
        # No DDL calls expected for the second save to the same partition
        ddl_calls = [
            c
            for c in execute_calls_second
            if "CREATE TABLE" in c or c.strip().upper().startswith("ANALYZE")
        ]
        assert not ddl_calls, \
            "No DDL/ANALYZE should be called for an already-known partition"

    def test_analyze_schema_source_preserved(self):
        """ANALYZE works correctly even when a custom schema_source is provided."""
        sink = _make_mock_sink()
        cursor = _make_mock_cursor(execute_results=[None])
        sink._conn.cursor.return_value = cursor

        custom_schema = {
            "asset_id": "TEXT",
            "price": "REAL",
            "volume": "REAL",
            "ts": "INTEGER",
        }

        writer = PartitionedTelemetryWriter(
            sink,
            base_table="telemetry",
            timestamp_field="ts",
            schema_source=custom_schema,
        )

        record = {
            "asset_id": "NGN/XLM",
            "price": 123.45,
            "volume": 5000.0,
            "ts": 1700000000,
        }
        writer.save(record)

        execute_calls = [c.args[0] for c in cursor.execute.call_args_list]
        # Verify custom schema columns appear in CREATE
        create_call = next(c for c in execute_calls if "CREATE TABLE IF NOT EXISTS" in c)
        assert "volume" in create_call, "Custom schema columns should appear in DDL"

        # ANALYZE should still fire
        analyze_calls = [
            c
            for c in execute_calls
            if isinstance(c, str) and c.strip().upper().startswith("ANALYZE")
        ]
        assert analyze_calls, "ANALYZE should fire with custom schema_source"

    def test_create_if_missing_disabled_skips_create_and_analyze(self):
        """When create_if_missing=False, neither CREATE nor ANALYZE executes."""
        sink = _make_mock_sink()
        cursor = _make_mock_cursor()
        sink._conn.cursor.return_value = cursor

        writer = PartitionedTelemetryWriter(
            sink,
            base_table="telemetry",
            timestamp_field="ts",
            create_if_missing=False,
        )

        record = {"asset_id": "NGN/XLM", "price": 123.45, "ts": 1700000000}

        with pytest.raises(RuntimeError, match="create_if_missing is disabled"):
            writer.save(record)

        # No DDL or ANALYZE should have been executed
        execute_calls = [
            c.args[0]
            for c in cursor.execute.call_args_list
            if isinstance(c.args[0], str)
        ]
        assert not execute_calls, \
            "No SQL should be executed when create_if_missing is disabled"


# ---------------------------------------------------------------------------
# Regression: existing partition behavior preserved
# ---------------------------------------------------------------------------


class TestPartitionRegression:
    """Ensure existing partition logic is not broken by the ANALYZE change."""

    def test_record_routing_to_correct_partition(self):
        """Records are still routed to the correct weekly partition table."""
        sink = _make_mock_sink()
        cursor = _make_mock_cursor(execute_results=[None])
        sink._conn.cursor.return_value = cursor

        writer = PartitionedTelemetryWriter(
            sink, base_table="telemetry", timestamp_field="ts"
        )

        # 2024-01-01 = ISO week 2024-W01
        record = {"asset_id": "NGN/XLM", "price": 123.45, "ts": 1704067200}
        writer.save(record)

        # Verify the record was tagged with the correct partition
        assert len(sink._buffer) == 1
        assert sink._buffer[0]["__partition_table"] == "telemetry_2024_W01"

    def test_missing_timestamp_field_raises_keyerror(self):
        """Records without the timestamp field still raise KeyError."""
        sink = _make_mock_sink()
        writer = PartitionedTelemetryWriter(
            sink, base_table="telemetry", timestamp_field="ts"
        )

        with pytest.raises(KeyError, match="missing required timestamp field"):
            writer.save({"asset_id": "NGN/XLM", "price": 123.45})

    def test_partition_table_name_format(self):
        """Partition table names follow the expected <base>_<YEAR>_W<WW> format."""
        from database.writer import _partition_table_name

        name = _partition_table_name("telemetry", 2024, 1)
        assert name == "telemetry_2024_W01"

        name = _partition_table_name("metrics", 2025, 52)
        assert name == "metrics_2025_W52"

    def test_known_partitions_tracks_created_tables(self):
        """known_partitions property returns all created partition names."""
        sink = _make_mock_sink()
        # Provide enough fetchone results for both saves
        cursor = _make_mock_cursor(execute_results=[None, None])
        sink._conn.cursor.return_value = cursor

        writer = PartitionedTelemetryWriter(
            sink, base_table="telemetry", timestamp_field="ts"
        )

        record1 = {"asset_id": "A", "price": 1.0, "ts": 1704067200}  # 2024-W01
        record2 = {"asset_id": "B", "price": 2.0, "ts": 1704672000}  # 2024-W02

        writer.save(record1)
        writer.save(record2)

        partitions = writer.known_partitions
        assert "telemetry_2024_W01" in partitions
        assert "telemetry_2024_W02" in partitions
