// src/jobs/ohlcvJob.ts
import cron from "node-cron";
import prisma from "../lib/prisma";
import { subMinutes, subHours, subDays, startOfHour, startOfDay } from "date-fns";

interface AggregationWindow {
  start: Date;
  end: Date;
}

export class OhlcvAggregator {
  constructor() {}

  private getWindow(timeframe: string): AggregationWindow {
    const now = new Date();
    switch (timeframe) {
      case "1m":
        return { start: subMinutes(now, 1), end: now };
      case "15m":
        return { start: subMinutes(now, 15), end: now };
      case "1h":
        return { start: subHours(now, 1), end: now };
      case "1d":
        return { start: subDays(now, 1), end: now };
      default:
        throw new Error(`Unsupported timeframe ${timeframe}`);
    }
  }

  /**
   * Aggregates raw PriceHistory rows into OHLCV candles for each active currency.
   * Saves results to the OhlcvCandle table.
   */
  async runAggregation(timeframe: string): Promise<void> {
    const { start, end } = this.getWindow(timeframe);
    const activeCurrencies = await prisma.currency.findMany({ where: { isActive: true } });
    for (const cur of activeCurrencies) {
      const rows = await prisma.priceHistory.findMany({
        where: { currency: cur.code, timestamp: { gte: start, lt: end } },
        orderBy: { timestamp: "asc" },
        select: { rate: true, timestamp: true },
      });
      if (rows.length === 0) continue;
      const open = rows[0].rate;
      const close = rows[rows.length - 1].rate;
      const high = rows.reduce((max, r) => (r.rate > max ? r.rate : max), rows[0].rate);
      const low = rows.reduce((min, r) => (r.rate < min ? r.rate : min), rows[0].rate);
      const volume = rows.length; // Simple count as volume placeholder

      const timestamp = start;
      await prisma.ohlcvCandle.upsert({
        where: {
          pair_timeframe_timestamp: {
            pair: cur.code,
            timeframe,
            timestamp,
          },
        },
        create: {
          pair: cur.code,
          timeframe,
          open,
          high,
          low,
          close,
          volume: volume as any,
          timestamp,
        },
        update: {
          open,
          high,
          low,
          close,
          volume: volume as any,
        },
      });
    }
    console.info(`[OhlcvAggregator] Completed ${timeframe} aggregation from ${start.toISOString()} to ${end.toISOString()}`);
  }

  /**
   * Placeholder for pool liquidity aggregation.
   */
  async aggregatePoolLiquidity(): Promise<void> {
    // TODO: Implement actual pool liquidity calculation based on appropriate tables.
    console.info(`[OhlcvAggregator] Pool liquidity aggregation executed (placeholder).`);
  }

  /**
   * Purge raw transaction logs older than 90 days.
   */
  async purgeOldLogs(): Promise<void> {
    const cutoff = subDays(new Date(), 90);
    const deleted = await prisma.priceHistory.deleteMany({ where: { timestamp: { lt: cutoff } } });
    console.info(`[OhlcvAggregator] Purged ${deleted.count} PriceHistory rows older than ${cutoff.toISOString()}`);
  }

  /**
   * Register cron jobs for each timeframe and maintenance tasks.
   */
  start(): void {
    // 1‑minute candles – every minute at second 0
    cron.schedule("0 * * * * *", () => this.runAggregation("1m"));
    // 15‑minute candles – every 15 min
    cron.schedule("0 */15 * * * *", () => this.runAggregation("15m"));
    // Hourly candles – at minute 0
    cron.schedule("0 0 * * * *", () => this.runAggregation("1h"));
    // Daily candles – at hour 0 minute 0
    cron.schedule("0 0 0 * * *", () => this.runAggregation("1d"));
    // Pool liquidity – every 5 minutes
    cron.schedule("0 */5 * * * *", () => this.aggregatePoolLiquidity());
    // Purge old logs – daily at 02:00
    cron.schedule("0 0 2 * * *", () => this.purgeOldLogs());
    console.info("[OhlcvAggregator] Scheduler started.");
  }
}

export const ohlcvAggregator = new OhlcvAggregator();
