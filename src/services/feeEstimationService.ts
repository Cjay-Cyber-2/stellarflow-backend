import { Horizon } from "@stellar/stellar-sdk";
import stellarProvider from "../lib/stellarProvider";
import { logger } from "../utils/logger";

export interface FeeEstimate {
  baseFee: number;
  low: number;
  medium: number;
  urgent: number;
  networkCongestion: "low" | "medium" | "high" | "critical";
  lastLedgerCloseTime: string;
  ledgerCapacityUsedPercent: number;
}

interface FeeStatsResponse {
  last_ledger: string;
  last_ledger_base_fee: string;
  ledger_capacity_usage: string;
  fee_charged: {
    min: string;
    max: string;
    mode: string;
    p10: string;
    p20: string;
    p30: string;
    p40: string;
    p50: string;
    p60: string;
    p70: string;
    p80: string;
    p90: string;
    p95: string;
    p99: string;
  };
  max_fee_charged: {
    min: string;
    max: string;
    mode: string;
    p10: string;
    p20: string;
    p30: string;
    p40: string;
    p50: string;
    p60: string;
    p70: string;
    p80: string;
    p90: string;
    p95: string;
    p99: string;
  };
}

interface CachedEstimate {
  estimate: FeeEstimate;
  fetchedAt: number;
}

const CACHE_TTL_MS = 30_000;

class FeeEstimationService {
  private cache: CachedEstimate | null = null;

  private static parseSafe(value: string, fallback: number): number {
    const parsed = parseInt(value, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  }

  private classifyCongestion(
    ledgerCapacityUsedPercent: number,
  ): "low" | "medium" | "high" | "critical" {
    if (ledgerCapacityUsedPercent < 30) return "low";
    if (ledgerCapacityUsedPercent < 60) return "medium";
    if (ledgerCapacityUsedPercent < 85) return "high";
    return "critical";
  }

  private async fetchFeeStats(): Promise<FeeStatsResponse> {
    const server: Horizon.Server = stellarProvider.getServer();
    const response = await server.feeStats();
    return response as unknown as FeeStatsResponse;
  }

  async getFeeEstimate(): Promise<FeeEstimate> {
    if (this.cache && Date.now() - this.cache.fetchedAt < CACHE_TTL_MS) {
      return this.cache.estimate;
    }

    try {
      const stats = await this.fetchFeeStats();

      const baseFee = FeeEstimationService.parseSafe(
        stats.last_ledger_base_fee,
        100,
      );

      const feeChargedP50 = FeeEstimationService.parseSafe(
        stats.fee_charged.p50,
        baseFee,
      );
      const feeChargedP90 = FeeEstimationService.parseSafe(
        stats.fee_charged.p90,
        baseFee * 2,
      );
      const feeChargedP99 = FeeEstimationService.parseSafe(
        stats.fee_charged.p99,
        baseFee * 3,
      );

      const ledgerCapacityUsed = FeeEstimationService.parseSafe(
        stats.ledger_capacity_usage,
        0,
      );
      const ledgerCapacityUsedPercent = Math.min(
        Math.round(ledgerCapacityUsed * 100),
        100,
      );

      const networkCongestion = this.classifyCongestion(
        ledgerCapacityUsedPercent,
      );

      // Priority fee recommendations based on percentile tiers
      const low = Math.max(feeChargedP50, 100);
      const medium = Math.max(feeChargedP90, baseFee * 2, 100);
      const urgent = Math.max(feeChargedP99, baseFee * 5, 100);

      const estimate: FeeEstimate = {
        baseFee,
        low,
        medium,
        urgent,
        networkCongestion,
        lastLedgerCloseTime: new Date().toISOString(),
        ledgerCapacityUsedPercent,
      };

      this.cache = { estimate, fetchedAt: Date.now() };
      return estimate;
    } catch (error: any) {
      logger.error("[FeeEstimationService] Failed to fetch fee stats:", {
        error: error.message,
      });

      if (this.cache) {
        logger.warn(
          "[FeeEstimationService] Returning stale cached estimate due to fetch failure",
        );
        return this.cache.estimate;
      }

      // Fallback estimate when no cache is available
      return {
        baseFee: 100,
        low: 100,
        medium: 200,
        urgent: 500,
        networkCongestion: "medium",
        lastLedgerCloseTime: new Date().toISOString(),
        ledgerCapacityUsedPercent: 50,
      };
    }
  }
}

export const feeEstimationService = new FeeEstimationService();
