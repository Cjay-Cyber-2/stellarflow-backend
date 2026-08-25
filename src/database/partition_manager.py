#!/usr/bin/env python3
"""src/database/partition_manager.py — Automated monthly partition manager.

Creates and drops range-partitioned child tables for the ``ledger_events``
parent table.  Designed to be run:

* As a **standalone script** (cron / systemd timer / container entrypoint)::

      python -m src.database.partition_manager --months-ahead 3 --retention-months 12

* Imported as a library::

      from src.database.partition_manager import PartitionManager

      manager = PartitionManager(dsn=os.environ["DATABASE_URL"])
      created = manager.create_future_partitions(months_ahead=3)
      dropped = manager.drop_expired_partitions(retention_months=12)

Design
------
PostgreSQL range partitioning requires that a child table exists **before**
any row whose partition key falls into that range is inserted.  This manager
ensures the current month plus ``months_ahead`` future months always have a
partition, and optionally drops partitions older than ``retention_months``.

Every ``CREATE TABLE IF NOT EXISTS … PARTITION OF`` is idempotent so the
script can be safely re-run at any frequency.

Partition naming convention
^^^^^^^^^^^^^^^^^^^^^^^^^^^
``ledger_events_{YYYY}_{MM}``  — e.g. ``ledger_events_2026_08``

Retention policy
^^^^^^^^^^^^^^^^
Older partitions are dropped only when ``--retention-months`` is set (>0).
A dropped partition permanently removes its data.  Ensure backups are
retained separately before enabling aggressive retention.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_PARENT_TABLE = "ledger_events"
DEFAULT_MONTHS_AHEAD = 3
DEFAULT_RETENTION_MONTHS = 12


# ---------------------------------------------------------------------------
# PartitionManager
# ---------------------------------------------------------------------------

class PartitionManager:
    """Manages PostgreSQL range-partitioned child tables for ``ledger_events``.

    Parameters
    ----------
    dsn : str
        PostgreSQL connection string (e.g. ``postgresql://user:pass@host/db``).
    parent_table : str
        Name of the partitioned parent table (default: ``ledger_events``).
    """

    def __init__(self, dsn: str, parent_table: str = DEFAULT_PARENT_TABLE) -> None:
        if not dsn:
            raise ValueError("dsn must not be empty")
        self._dsn = dsn
        self._parent = parent_table

    # ------------------------------------------------------------------
    # Connection helper
    # ------------------------------------------------------------------

    def _connect(self):
        """Return a new psycopg2 connection. Caller must close it."""
        try:
            import psycopg2
        except ImportError as exc:
            raise RuntimeError("psycopg2 is required for PartitionManager") from exc
        return psycopg2.connect(self._dsn)

    # ------------------------------------------------------------------
    # Partition creation
    # ------------------------------------------------------------------

    def create_future_partitions(
        self,
        months_ahead: int = DEFAULT_MONTHS_AHEAD,
        reference_date: Optional[date] = None,
    ) -> List[str]:
        """Create monthly partition tables for the current and future months.

        Parameters
        ----------
        months_ahead :
            Number of future months to create partitions for (inclusive of
            the current month).
        reference_date :
            Override the "today" date (useful for testing).

        Returns
        -------
        list of str
            Names of partitions that were created (excludes those that
            already existed).
        """
        ref = reference_date or date.today()
        created: List[str] = []

        conn = self._connect()
        try:
            conn.autocommit = True
            cur = conn.cursor()

            for offset in range(months_ahead):
                # Calculate the target month
                target_year = ref.year
                target_month = ref.month + offset
                while target_month > 12:
                    target_month -= 12
                    target_year += 1

                partition_name = self._partition_name(target_year, target_month)
                lower_bound = f"{target_year}-{target_month:02d}-01"

                # Upper bound: first day of the following month
                if target_month == 12:
                    upper_year, upper_month = target_year + 1, 1
                else:
                    upper_year, upper_month = target_year, target_month + 1
                upper_bound = f"{upper_year}-{upper_month:02d}-01"

                cur.execute(
                    f"SELECT 1 FROM pg_class WHERE relname = %s",
                    (partition_name,),
                )
                already_exists = cur.fetchone() is not None

                if not already_exists:
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS "{partition_name}"
                        PARTITION OF "{self._parent}"
                        FOR VALUES FROM (%s) TO (%s)
                        """,
                        (lower_bound, upper_bound),
                    )
                    created.append(partition_name)
                    logger.info(
                        "Created partition %s  [FROM '%s' TO '%s')",
                        partition_name,
                        lower_bound,
                        upper_bound,
                    )
                else:
                    logger.debug(
                        "Partition %s already exists, skipping", partition_name
                    )

            cur.close()
        finally:
            conn.close()

        logger.info(
            "Partition creation complete: %d created, %d already existed",
            len(created),
            months_ahead - len(created),
        )
        return created

    # ------------------------------------------------------------------
    # Partition pruning (drop old partitions)
    # ------------------------------------------------------------------

    def drop_expired_partitions(
        self,
        retention_months: int = DEFAULT_RETENTION_MONTHS,
        dry_run: bool = False,
        reference_date: Optional[date] = None,
    ) -> List[str]:
        """Drop partition tables older than ``retention_months``.

        Parameters
        ----------
        retention_months :
            Partitions whose entire date range is before this many months
            ago are dropped.
        dry_run :
            If *True*, return the list of partitions that *would* be dropped
            without actually issuing DROP statements.
        reference_date :
            Override the "today" date.

        Returns
        -------
        list of str
            Names of partitions that were (or would be) dropped.
        """
        ref = reference_date or date.today()
        cutoff_year = ref.year
        cutoff_month = ref.month - retention_months
        while cutoff_month < 1:
            cutoff_month += 12
            cutoff_year -= 1

        # Everything strictly before the first day of the cutoff month
        # qualifies for deletion.
        cutoff_date = f"{cutoff_year}-{cutoff_month:02d}-01"

        conn = self._connect()
        try:
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute(
                """
                SELECT inhrelid::regclass::text
                FROM pg_inherits
                WHERE inhparent = %s::regclass
                """,
                (self._parent,),
            )
            all_partitions = [row[0] for row in cur.fetchall()]

            to_drop: List[str] = []
            for part_name in all_partitions:
                # Extract year and month from partition name
                parts = part_name.rsplit("_", 2)
                if len(parts) < 2:
                    continue
                try:
                    part_year = int(parts[-2])
                    part_month = int(parts[-1])
                except ValueError:
                    continue

                # Partition's exclusive upper bound (first day of next month)
                if part_month == 12:
                    part_upper = f"{part_year + 1}-01-01"
                else:
                    part_upper = f"{part_year}-{part_month + 1:02d}-01"

                # Drop if the partition's upper bound is on or before the cutoff
                if part_upper <= cutoff_date:
                    to_drop.append(part_name)

            dropped: List[str] = []
            for part_name in sorted(to_drop):
                if dry_run:
                    logger.info("[DRY RUN] Would drop partition %s", part_name)
                else:
                    cur.execute(f'DROP TABLE IF EXISTS "{part_name}"')
                    logger.info("Dropped partition %s", part_name)
                dropped.append(part_name)

            cur.close()
        finally:
            conn.close()

        action = "would drop" if dry_run else "dropped"
        logger.info(
            "Partition retention: %d %s (%d-month retention)",
            len(dropped),
            action,
            retention_months,
        )
        return dropped

    # ------------------------------------------------------------------
    # Partition inventory
    # ------------------------------------------------------------------

    def list_partitions(self) -> List[Tuple[str, str, str]]:
        """Return a list of (partition_name, lower_bound, upper_bound) tuples.

        Bounds are ISO-8601 date strings extracted from the partition
        constraint definition.
        """
        conn = self._connect()
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    c.relname AS partition_name,
                    pg_get_expr(c.relpartbound, c.oid) AS bound_expr
                FROM pg_inherits i
                JOIN pg_class p ON i.inhparent = p.oid
                JOIN pg_class c ON i.inhrelid = c.oid
                WHERE p.relname = %s
                ORDER BY c.relname
                """,
                (self._parent,),
            )
            results = []
            for row in cur.fetchall():
                name, bound = row
                # bound_expr looks like: FOR VALUES FROM ('2026-08-01') TO ('2026-09-01')
                lower, upper = "", ""
                if "FROM" in bound and "TO" in bound:
                    parts = bound.split("'")
                    if len(parts) >= 5:
                        lower = parts[1]
                        upper = parts[3]
                results.append((name, lower, upper))
            cur.close()
            return results
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _partition_name(year: int, month: int) -> str:
        """Return the canonical partition table name for a given year/month."""
        return f"ledger_events_{year}_{month:02d}"

    # ------------------------------------------------------------------
    # Partition pruning verification
    # ------------------------------------------------------------------

    def verify_partition_pruning(self) -> dict:
        """Verify the query planner uses partition pruning on ``ledger_events``.

        Executes ``EXPLAIN`` on several representative queries and checks
        that only the relevant partitions are scanned.  Returns a summary
        dict with test results.

        Returns
        -------
        dict
            ``{"passed": bool, "tests": [{"query": str, "pruned": bool,
            "partitions_scanned": list[str], "detail": str}, ...]}``
        """
        conn = self._connect()
        results: List[dict] = []
        all_passed = True

        try:
            conn.autocommit = True
            cur = conn.cursor()
            partitions = self.list_partitions()
            partition_names = [p[0] for p in partitions]

            if not partitions:
                return {
                    "passed": False,
                    "tests": [{
                        "query": "(none)",
                        "pruned": False,
                        "partitions_scanned": [],
                        "detail": "No partitions exist for ledger_events",
                    }],
                }

            today = date.today()
            current_month_start = f"{today.year}-{today.month:02d}-01"
            if today.month == 12:
                next_month_start = f"{today.year + 1}-01-01"
            else:
                next_month_start = f"{today.year}-{today.month + 1:02d}-01"
            two_months_start = f"{today.year}-{today.month + 2:02d}-01" if today.month < 11 else (
                f"{today.year + 1}-{(today.month + 2) % 12:02d}-01"
            )

            # Test 1: Point query on a single month
            q1 = f"""
                EXPLAIN (FORMAT JSON)
                SELECT * FROM {self._parent}
                WHERE created_at >= '{current_month_start}'
                  AND created_at <  '{next_month_start}'
                LIMIT 1
            """
            r1 = self._explain_to_result(cur, q1, partition_names)
            results.append(r1)
            if not r1["pruned"]:
                all_passed = False

            # Test 2: Range spanning two months
            q2 = f"""
                EXPLAIN (FORMAT JSON)
                SELECT * FROM {self._parent}
                WHERE created_at >= '{current_month_start}'
                  AND created_at <  '{two_months_start}'
            """
            r2 = self._explain_to_result(cur, q2, partition_names)
            results.append(r2)
            if not r2["pruned"]:
                all_passed = False

            # Test 3: Query with no time filter (full scan — should hit all partitions)
            q3 = f"""
                EXPLAIN (FORMAT JSON)
                SELECT count(*) FROM {self._parent}
            """
            r3 = self._explain_to_result(cur, q3, partition_names, expect_all=True)
            results.append(r3)

            # Test 4: Query with ledger_sequence filter (no time filter)
            q4 = f"""
                EXPLAIN (FORMAT JSON)
                SELECT * FROM {self._parent}
                WHERE ledger_sequence = 42000
            """
            r4 = self._explain_to_result(cur, q4, partition_names)
            results.append(r4)

            cur.close()
        finally:
            conn.close()

        return {"passed": all_passed, "tests": results}

    def _explain_to_result(
        self,
        cur,
        query: str,
        partition_names: List[str],
        expect_all: bool = False,
    ) -> dict:
        """Run EXPLAIN and determine which partitions were scanned."""
        try:
            cur.execute(query)
            plan = cur.fetchone()[0]
        except Exception as exc:
            return {
                "query": query.strip(),
                "pruned": False,
                "partitions_scanned": [],
                "detail": f"EXPLAIN failed: {exc}",
            }

        plan_text = str(plan)
        scanned = [pn for pn in partition_names if pn in plan_text]
        pruned = len(scanned) < len(partition_names) if not expect_all else True

        return {
            "query": query.strip(),
            "pruned": pruned,
            "partitions_scanned": scanned,
            "detail": (
                f"Scanned {len(scanned)}/{len(partition_names)} partitions"
                + (" (pruning active)" if pruned else " (full scan)")
            ),
        }


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry-point for the partition manager."""
    parser = argparse.ArgumentParser(
        description="Manage PostgreSQL monthly partitions for ledger_events.",
    )
    parser.add_argument(
        "--months-ahead",
        type=int,
        default=DEFAULT_MONTHS_AHEAD,
        help=f"Months of future partitions to maintain (default: {DEFAULT_MONTHS_AHEAD}).",
    )
    parser.add_argument(
        "--retention-months",
        type=int,
        default=DEFAULT_RETENTION_MONTHS,
        help=(
            f"Drop partitions older than N months "
            f"(default: {DEFAULT_RETENTION_MONTHS}; 0 to disable)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be dropped without actually dropping.",
    )
    parser.add_argument(
        "--dsn",
        type=str,
        default=None,
        help="PostgreSQL DSN (defaults to DATABASE_URL env var).",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        dest="list_partitions",
        help="List current partitions and exit.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify partition pruning and exit.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    dsn = args.dsn or os.environ.get("DATABASE_URL", "")
    if not dsn:
        logger.error("DATABASE_URL is not set and --dsn was not provided.")
        sys.exit(1)

    manager = PartitionManager(dsn)

    if args.verify:
        result = manager.verify_partition_pruning()
        print(f"\nPartition Pruning Verification: {'PASSED' if result['passed'] else 'FAILED'}\n")
        for i, test in enumerate(result["tests"], 1):
            status = "PASS" if test["pruned"] else "FAIL"
            print(f"  Test {i}: [{status}] {test['detail']}")
            print(f"          Partitions: {', '.join(test['partitions_scanned']) or '(none)'}")
            print()
        sys.exit(0 if result["passed"] else 1)

    if args.list_partitions:
        parts = manager.list_partitions()
        if not parts:
            print("No partitions found for table 'ledger_events'.")
        else:
            print(f"{'Partition':<35} {'From':<15} {'To':<15}")
            print("-" * 65)
            for name, lower, upper in parts:
                print(f"{name:<35} {lower:<15} {upper:<15}")
        return

    # Create future partitions
    created = manager.create_future_partitions(months_ahead=args.months_ahead)
    if created:
        logger.info("Created partitions: %s", ", ".join(created))

    # Drop expired partitions (if retention is enabled)
    if args.retention_months > 0:
        dropped = manager.drop_expired_partitions(
            retention_months=args.retention_months,
            dry_run=args.dry_run,
        )
        if dropped:
            action = "Would drop" if args.dry_run else "Dropped"
            logger.info("%s partitions: %s", action, ", ".join(dropped))
    else:
        logger.info("Retention disabled (retention_months=0), skipping cleanup.")


if __name__ == "__main__":
    main()
