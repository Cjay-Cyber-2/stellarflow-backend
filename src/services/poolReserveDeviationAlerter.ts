import { broadcastToSessions } from "../lib/socket";
import { getRedisClient } from "../lib/redis";
import {
  AlertSeverity,
  AlertType,
  NotificationService,
} from "./notificationService";

export interface PoolReserveSnapshot {
  poolId: string;
  blockHeight: number;
  transactionHash: string;
  reserveA: number;
  reserveB: number;
  observedAt?: Date;
}

export interface PoolReserveDeviationAlert {
  poolId: string;
  blockHeight: number;
  transactionHash: string;
  previousRatio: number;
  currentRatio: number;
  deviationPercent: number;
  reserveA: number;
  reserveB: number;
  observedAt: string;
}

/** Detects reserve-ratio moves between consecutive confirmed swaps in a ledger. */
export class PoolReserveDeviationAlerter {
  private readonly snapshots = new Map<string, PoolReserveSnapshot>();

  constructor(
    private readonly notifications = new NotificationService(),
    private readonly thresholdPercent = Number(
      process.env.POOL_RESERVE_DEVIATION_THRESHOLD_PERCENT ?? "10",
    ),
  ) {}

  async observe(
    snapshot: PoolReserveSnapshot,
  ): Promise<PoolReserveDeviationAlert | null> {
    if (
      !Number.isFinite(snapshot.reserveA) ||
      !Number.isFinite(snapshot.reserveB) ||
      snapshot.reserveA <= 0 ||
      snapshot.reserveB <= 0
    ) {
      throw new Error("Pool reserves must be finite positive numbers");
    }

    const previous = this.snapshots.get(snapshot.poolId);
    this.snapshots.set(snapshot.poolId, snapshot);
    if (
      !previous ||
      previous.blockHeight !== snapshot.blockHeight ||
      previous.reserveA <= 0 ||
      previous.reserveB <= 0
    ) {
      return null;
    }

    const previousRatio = previous.reserveA / previous.reserveB;
    const currentRatio = snapshot.reserveA / snapshot.reserveB;
    const deviationPercent = Math.abs((currentRatio / previousRatio - 1) * 100);
    if (deviationPercent <= this.thresholdPercent) return null;

    const alert: PoolReserveDeviationAlert = {
      poolId: snapshot.poolId,
      blockHeight: snapshot.blockHeight,
      transactionHash: snapshot.transactionHash,
      previousRatio,
      currentRatio,
      deviationPercent,
      reserveA: snapshot.reserveA,
      reserveB: snapshot.reserveB,
      observedAt: (snapshot.observedAt ?? new Date()).toISOString(),
    };
    await this.dispatch(alert);
    return alert;
  }

  private async dispatch(alert: PoolReserveDeviationAlert): Promise<void> {
    broadcastToSessions("pool.reserve_deviation", alert);
    const redis = getRedisClient();
    if (redis?.isOpen) {
      await redis.xAdd("events:pool-reserve-alerts", "*", {
        payload: JSON.stringify(alert),
      });
    }
    await this.notifications.sendAlert({
      type: AlertType.POOL_RESERVE_DEVIATION,
      severity: AlertSeverity.HIGH,
      title: "Pool reserve ratio deviation detected",
      message: `Pool ${alert.poolId} moved ${alert.deviationPercent.toFixed(2)}% in block ${alert.blockHeight}.`,
      details: alert,
      timestamp: new Date(alert.observedAt),
      service: "pool-reserve-deviation-alerter",
    });
  }
}

export const poolReserveDeviationAlerter = new PoolReserveDeviationAlerter();
