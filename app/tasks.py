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