"""Distributed tasks for heavy StellarFlow analytics workloads."""

import asyncio
import os
from datetime import datetime, timedelta, timezone

import asyncpg
from celery import Task

from app.celery_app import celery_app


class DatabaseTask(Task):
    """Base task that exposes the configured PostgreSQL connection string."""

    _database_url = os.getenv("DATABASE_URL", os.getenv("DB_URL"))


async def _aggregate(granularity: str, cutoff: datetime) -> int:
    database_url = DatabaseTask._database_url
    if not database_url:
        raise RuntimeError("DATABASE_URL or DB_URL must be configured")

    interval = {"MINUTE": "minute", "HOUR": "hour", "DAY": "day"}[granularity]
    pool = await asyncpg.create_pool(database_url)
    try:
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT currency,
                       date_trunc('{interval}', timestamp) AS open_time,
                       min(rate) AS low,
                       max(rate) AS high,
                       (array_agg(rate ORDER BY timestamp, id))[1] AS open,
                       (array_agg(rate ORDER BY timestamp DESC, id DESC))[1] AS close,
                       count(*)::int AS count
                FROM "PriceHistory"
                WHERE timestamp >= $1
                GROUP BY currency, date_trunc('{interval}', timestamp)
                """,
                cutoff,
            )

            updated = 0
            for row in rows:
                await connection.execute(
                    """
                    INSERT INTO "OhlcCandle"
                      (currency, granularity, "openTime", "closeTime", open, high, low, close, count)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (currency, granularity, "openTime") DO UPDATE SET
                      "closeTime" = EXCLUDED."closeTime",
                      open = EXCLUDED.open,
                      high = EXCLUDED.high,
                      low = EXCLUDED.low,
                      close = EXCLUDED.close,
                      count = EXCLUDED.count,
                      "updatedAt" = CURRENT_TIMESTAMP
                    """,
                    row["currency"],
                    granularity,
                    row["open_time"],
                    row["open_time"] + timedelta(minutes={"MINUTE": 1, "HOUR": 60, "DAY": 1440}[granularity]),
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["count"],
                )
                updated += 1
            return updated
    finally:
        await pool.close()


@celery_app.task(
    bind=True,
    base=DatabaseTask,
    name="app.tasks.aggregate_ledger_analytics",
    autoretry_for=(OSError, asyncpg.PostgresError),
    retry_backoff=True,
    max_retries=3,
)
def aggregate_ledger_analytics(
    self: DatabaseTask,
    granularity: str = "HOUR",
    lookback_hours: int = 25,
) -> int:
    """Aggregate recent ledger price history into idempotent OHLC candles."""
    if granularity not in {"MINUTE", "HOUR", "DAY"}:
        raise ValueError("granularity must be MINUTE, HOUR, or DAY")
    if lookback_hours < 1:
        raise ValueError("lookback_hours must be positive")

    DatabaseTask._database_url = os.getenv("DATABASE_URL", os.getenv("DB_URL"))
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=lookback_hours)
    return asyncio.run(_aggregate(granularity, cutoff))


# ---------------------------------------------------------------------------
# Flash-loan revenue accounting tasks
# ---------------------------------------------------------------------------

import hashlib
from typing import Any, Dict, Optional


def _event_id(tx_hash: str, event_index: int) -> str:
    """Deterministic dedup key for a flash-loan revenue event."""
    raw = f"{tx_hash}:{event_index}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _snapshot_id(granularity: str, window_start: datetime) -> str:
    """Deterministic dedup key for a yield snapshot."""
    raw = f"{granularity}:{window_start.isoformat()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _parse_flash_loan_event(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse a raw Soroban ledger event into flash-loan revenue fields.

    Returns ``None`` when the payload is not a ``FlashLoanFeesDistributed``
    event or when required fields are missing.
    """
    event_type = payload.get("topic") or payload.get("type") or ""
    if "FlashLoanFeesDistributed" not in str(event_type):
        return None

    tx_hash = payload.get("txHash", "0x0")
    event_index = int(payload.get("index", 0))

    amount = payload.get("amount")
    treasury_account = payload.get("treasury") or payload.get("treasury_account")
    block_time = payload.get("blockTime") or payload.get("ledger_close_time")

    if isinstance(block_time, (int, float)):
        try:
            block_time = datetime.fromtimestamp(block_time, tz=timezone.utc)
        except (OSError, ValueError, OverflowError):
            block_time = None
    elif isinstance(block_time, str):
        try:
            block_time = datetime.fromisoformat(block_time.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            block_time = None

    if amount is None or treasury_account is None:
        return None

    return {
        "id": _event_id(str(tx_hash), event_index),
        "ledger_sequence": int(payload.get("ledger", 0)),
        "tx_hash": str(tx_hash),
        "event_index": event_index,
        "amount": amount,
        "treasury_account": str(treasury_account),
        "block_time": block_time,
        "payload": payload,
    }


def _safe_numeric(value: Any) -> Optional[float]:
    """Coerce *value* to a float suitable for SQL ``NUMERIC`` binding."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def _ingest_flash_loan_events(lookback_minutes: int = 60) -> int:
    """Scan recent ``ledger_events`` for ``FlashLoanFeesDistributed`` events
    and persist any new rows into ``flash_loan_revenue``."""
    database_url = DatabaseTask._database_url
    if not database_url:
        raise RuntimeError("DATABASE_URL or DB_URL must be configured")

    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=lookback_minutes)
    pool = await asyncpg.create_pool(database_url)
    try:
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT event_hash, ledger_sequence, tx_hash, event_type, payload, created_at
                FROM ledger_events
                WHERE created_at >= $1
                  AND event_type = 'contract'
                ORDER BY created_at ASC
                """,
                cutoff,
            )

            inserted = 0
            for row in rows:
                payload = row["payload"]
                if not isinstance(payload, dict):
                    continue

                parsed = _parse_flash_loan_event(payload)
                if parsed is None:
                    continue

                try:
                    await connection.execute(
                        """
                        INSERT INTO flash_loan_revenue (
                            id, ledger_sequence, tx_hash, event_index,
                            amount, treasury_account, block_time, payload
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        parsed["id"],
                        parsed["ledger_sequence"],
                        parsed["tx_hash"],
                        parsed["event_index"],
                        _safe_numeric(parsed["amount"]),
                        parsed["treasury_account"],
                        parsed["block_time"],
                        parsed["payload"],
                    )
                    inserted += 1
                except Exception:
                    continue

            return inserted
    finally:
        await pool.close()


async def _compute_yield_snapshots(granularity: str = "DAILY") -> int:
    """Aggregate ``flash_loan_revenue`` into ``protocol_yield_snapshot`` rows."""
    database_url = DatabaseTask._database_url
    if not database_url:
        raise RuntimeError("DATABASE_URL or DB_URL must be configured")

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    if granularity == "HOURLY":
        window = timedelta(hours=1)
        lookback = timedelta(hours=26)
    elif granularity == "DAILY":
        window = timedelta(days=1)
        lookback = timedelta(days=2)
    else:
        raise ValueError("granularity must be HOURLY or DAILY")

    cutoff = now - lookback
    pool = await asyncpg.create_pool(database_url)
    try:
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT
                    date_trunc('hour', created_at) AS window_start,
                    COUNT(*) AS event_count,
                    SUM(amount) AS total_revenue,
                    SUM(amount) AS fee_volume
                FROM flash_loan_revenue
                WHERE created_at >= $1
                GROUP BY 1
                ORDER BY 1 ASC
                """,
                cutoff,
            )

            inserted = 0
            for row in rows:
                window_start_dt: datetime = row["window_start"]
                window_end_dt = window_start_dt + window

                if granularity == "DAILY":
                    window_start_dt = window_start_dt.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    window_end_dt = window_start_dt + timedelta(days=1)

                snapshot_id = _snapshot_id(granularity, window_start_dt)
                total_revenue = _safe_numeric(row["total_revenue"]) or 0.0
                event_count = int(row["event_count"] or 0)
                fee_volume = _safe_numeric(row["fee_volume"]) or 0.0

                try:
                    await connection.execute(
                        """
                        INSERT INTO protocol_yield_snapshot (
                            id, granularity, window_start, window_end,
                            total_flash_loan_revenue, total_treasury_balance,
                            yield_apy, fee_volume, event_count
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        snapshot_id,
                        granularity,
                        window_start_dt,
                        window_end_dt,
                        total_revenue,
                        None,
                        None,
                        fee_volume,
                        event_count,
                    )
                    inserted += 1
                except Exception:
                    continue

            return inserted
    finally:
        await pool.close()


@celery_app.task(
    bind=True,
    base=DatabaseTask,
    name="app.tasks.ingest_flash_loan_revenue",
    autoretry_for=(OSError, asyncpg.PostgresError),
    retry_backoff=True,
    max_retries=3,
)
def ingest_flash_loan_revenue(
    self: DatabaseTask,
    lookback_minutes: int = 60,
) -> int:
    """Ingest ``FlashLoanFeesDistributed`` events from ``ledger_events``."""
    DatabaseTask._database_url = os.getenv("DATABASE_URL", os.getenv("DB_URL"))
    if lookback_minutes < 1:
        raise ValueError("lookback_minutes must be positive")
    return int(asyncio.run(_ingest_flash_loan_events(lookback_minutes)))


@celery_app.task(
    bind=True,
    base=DatabaseTask,
    name="app.tasks.compute_yield_snapshots",
    autoretry_for=(OSError, asyncpg.PostgresError),
    retry_backoff=True,
    max_retries=3,
)
def compute_yield_snapshots(
    self: DatabaseTask,
    granularity: str = "DAILY",
) -> int:
    """Compute aggregate protocol yield snapshots from ``flash_loan_revenue``."""
    DatabaseTask._database_url = os.getenv("DATABASE_URL", os.getenv("DB_URL"))
    if granularity not in {"HOURLY", "DAILY"}:
        raise ValueError("granularity must be HOURLY or DAILY")
    return int(asyncio.run(_compute_yield_snapshots(granularity)))