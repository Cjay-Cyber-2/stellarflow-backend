"""
tests/test_database_connection.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Test suite for src/database/connection.py index usage telemetry tracking.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from database.connection import (
    IndexUsageTelemetry,
    DEFAULT_INDEX_USAGE_THRESHOLD,
    DEFAULT_MIN_INDEX_SCANS,
    DEFAULT_ALERT_INTERVAL_S,
)


# ---------------------------------------------------------------------------
# IndexUsageTelemetry construction
# ---------------------------------------------------------------------------


class TestIndexUsageTelemetryConstruction:
    def test_rejects_none_connection(self):
        with pytest.raises(ValueError, match="connection must not be None"):
            IndexUsageTelemetry(None)

    def test_rejects_invalid_usage_threshold(self):
        mock_conn = MagicMock()
        with pytest.raises(ValueError, match="usage_threshold must be between 0.0 and 1.0"):
            IndexUsageTelemetry(mock_conn, usage_threshold=1.5)
        with pytest.raises(ValueError, match="usage_threshold must be between 0.0 and 1.0"):
            IndexUsageTelemetry(mock_conn, usage_threshold=-0.1)

    def test_rejects_negative_min_scans(self):
        mock_conn = MagicMock()
        with pytest.raises(ValueError, match="min_scans must be non-negative"):
            IndexUsageTelemetry(mock_conn, min_scans=-1)

    def test_rejects_non_positive_alert_interval(self):
        mock_conn = MagicMock()
        with pytest.raises(ValueError, match="alert_interval must be positive"):
            IndexUsageTelemetry(mock_conn, alert_interval=0)
        with pytest.raises(ValueError, match="alert_interval must be positive"):
            IndexUsageTelemetry(mock_conn, alert_interval=-1)

    def test_accepts_valid_parameters(self):
        mock_conn = MagicMock()
        telemetry = IndexUsageTelemetry(
            mock_conn,
            usage_threshold=0.1,
            min_scans=50,
            alert_interval=1800.0,
        )
        assert telemetry._usage_threshold == 0.1
        assert telemetry._min_scans == 50
        assert telemetry._alert_interval == 1800.0


# ---------------------------------------------------------------------------
# IndexUsageTelemetry record_index_usage
# ---------------------------------------------------------------------------


class TestIndexUsageTelemetryRecordIndexUsage:
    def test_handles_missing_psycopg2_gracefully(self):
        """When psycopg2 is not available, recording should be a no-op."""
        mock_conn = MagicMock()
        with patch("database.connection.psycopg2", None):
            telemetry = IndexUsageTelemetry(mock_conn)
            telemetry.record_index_usage()  # Should not raise
            assert len(telemetry._index_stats) == 0

    def test_handles_query_failure_gracefully(self):
        """When the query fails, should log and continue without raising."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Query failed")
        
        telemetry = IndexUsageTelemetry(mock_conn)
        telemetry.record_index_usage()  # Should not raise
        assert len(telemetry._index_stats) == 0

    def test_records_index_stats_on_success(self):
        """Successfully records index statistics when query succeeds."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("public", "users", "users_pkey", 1000, 5000, 5000),
            ("public", "orders", "orders_pkey", 500, 2000, 2000),
        ]
        
        telemetry = IndexUsageTelemetry(mock_conn)
        telemetry.record_index_usage()
        
        assert len(telemetry._index_stats) == 2
        assert "public.users.users_pkey" in telemetry._index_stats
        assert "public.orders.orders_pkey" in telemetry._index_stats


# ---------------------------------------------------------------------------
# IndexUsageTelemetry get_underutilized_alerts
# ---------------------------------------------------------------------------


class TestIndexUsageTelemetryGetUnderutilizedAlerts:
    def test_returns_empty_when_no_stats(self):
        mock_conn = MagicMock()
        telemetry = IndexUsageTelemetry(mock_conn)
        alerts = telemetry.get_underutilized_alerts()
        assert alerts == []

    def test_returns_empty_when_below_min_scans(self):
        mock_conn = MagicMock()
        telemetry = IndexUsageTelemetry(mock_conn, min_scans=100)
        
        # Manually inject stats with low scan count
        telemetry._index_stats = {
            "public.users.users_pkey": {
                "schemaname": "public",
                "table_name": "users",
                "index_name": "users_pkey",
                "index_scans": 50,  # Below min_scans
                "tuples_read": 100,
                "tuples_fetched": 100,
                "last_updated": 0,
            }
        }
        
        alerts = telemetry.get_underutilized_alerts()
        assert alerts == []

    def test_generates_alert_for_underutilized_index(self):
        mock_conn = MagicMock()
        telemetry = IndexUsageTelemetry(mock_conn, usage_threshold=0.1, min_scans=100)
        
        # Inject stats: one index with very low usage ratio
        telemetry._index_stats = {
            "public.users.users_pkey": {
                "schemaname": "public",
                "table_name": "users",
                "index_name": "users_pkey",
                "index_scans": 100,  # Meets min_scans
                "tuples_read": 1000,
                "tuples_fetched": 1000,
                "last_updated": 0,
            },
            "public.orders.orders_pkey": {
                "schemaname": "public",
                "table_name": "orders",
                "index_name": "orders_pkey",
                "index_scans": 1000,  # Much higher usage
                "tuples_read": 10000,
                "tuples_fetched": 10000,
                "last_updated": 0,
            },
        }
        
        alerts = telemetry.get_underutilized_alerts()
        # users_pkey has 100/(100+1000) = 9% usage, below 10% threshold
        assert len(alerts) == 1
        assert alerts[0]["index_name"] == "users_pkey"
        assert alerts[0]["usage_ratio"] < 0.1

    def test_respects_alert_interval_to_prevent_spam(self):
        mock_conn = MagicMock()
        telemetry = IndexUsageTelemetry(
            mock_conn,
            usage_threshold=0.1,
            min_scans=100,
            alert_interval=10.0,
        )
        
        # Inject underutilized index stats
        telemetry._index_stats = {
            "public.users.users_pkey": {
                "schemaname": "public",
                "table_name": "users",
                "index_name": "users_pkey",
                "index_scans": 100,
                "tuples_read": 1000,
                "tuples_fetched": 1000,
                "last_updated": 0,
            },
            "public.orders.orders_pkey": {
                "schemaname": "public",
                "table_name": "orders",
                "index_name": "orders_pkey",
                "index_scans": 1000,
                "tuples_read": 10000,
                "tuples_fetched": 10000,
                "last_updated": 0,
            },
        }
        
        # First call should generate alert
        alerts1 = telemetry.get_underutilized_alerts()
        assert len(alerts1) == 1
        
        # Immediate second call should not generate alert (within interval)
        alerts2 = telemetry.get_underutilized_alerts()
        assert len(alerts2) == 0

    def test_alert_structure(self):
        """Verify alert dictionary contains required fields."""
        mock_conn = MagicMock()
        telemetry = IndexUsageTelemetry(mock_conn, usage_threshold=0.1, min_scans=100)
        
        telemetry._index_stats = {
            "public.users.users_pkey": {
                "schemaname": "public",
                "table_name": "users",
                "index_name": "users_pkey",
                "index_scans": 100,
                "tuples_read": 1000,
                "tuples_fetched": 1000,
                "last_updated": 0,
            },
            "public.orders.orders_pkey": {
                "schemaname": "public",
                "table_name": "orders",
                "index_name": "orders_pkey",
                "index_scans": 1000,
                "tuples_read": 10000,
                "tuples_fetched": 10000,
                "last_updated": 0,
            },
        }
        
        alerts = telemetry.get_underutilized_alerts()
        assert len(alerts) == 1
        
        alert = alerts[0]
        assert "index_name" in alert
        assert "table_name" in alert
        assert "schemaname" in alert
        assert "index_scans" in alert
        assert "usage_ratio" in alert
        assert "timestamp" in alert


# ---------------------------------------------------------------------------
# IndexUsageTelemetry get_index_stats
# ---------------------------------------------------------------------------


class TestIndexUsageTelemetryGetIndexStats:
    def test_returns_copy_of_stats(self):
        mock_conn = MagicMock()
        telemetry = IndexUsageTelemetry(mock_conn)
        
        telemetry._index_stats = {
            "public.users.users_pkey": {
                "schemaname": "public",
                "table_name": "users",
                "index_name": "users_pkey",
                "index_scans": 100,
                "tuples_read": 1000,
                "tuples_fetched": 1000,
                "last_updated": 0,
            }
        }
        
        stats = telemetry.get_index_stats()
        assert stats == telemetry._index_stats
        
        # Verify it's a copy, not the same object
        stats["new_key"] = "value"
        assert "new_key" not in telemetry._index_stats
