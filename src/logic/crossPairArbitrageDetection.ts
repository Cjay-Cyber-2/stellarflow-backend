/**
 * Cross-Pair Arbitrage Detection
 *
 * Detects inconsistencies between the NGN/USD implied rate (derived from
 * NGN/XLM ÷ XLM/USD) and a direct NGN/USD reference rate.
 * Deviations above the configured threshold are flagged for investigation.
 *
 * This is a pure-function module with no external dependencies.
 */

/**
 * Structured result returned by `checkCrossPairConsistency`.
 */
export interface CrossPairCheckResult {
  /** NGN/XLM ÷ XLM/USD — the cross-market implied NGN/USD rate */
  impliedRate: number;
  /** The direct NGN/USD rate passed in as a reference */
  directRate: number;
  /** Absolute difference |implied - direct| */
  deviation: number;
  /** Percentage deviation: |implied - direct| / direct × 100 */
  deviationPercent: number;
  /** true if deviationPercent is strictly greater than thresholdPercent */
  flagged: boolean;
  /** Timestamp of the check */
  timestamp: Date;
}

/**
 * Calculates the implied NGN/USD rate from the NGN/XLM and XLM/USD rates.
 *
 * @param ngnXlmRate - NGN per XLM (must be positive)
 * @param xlmUsdRate - USD per XLM (must be positive)
 * @returns ngnXlmRate / xlmUsdRate, or 0 for zero/negative inputs
 */
export function calculateImpliedNgnUsd(
  ngnXlmRate: number,
  xlmUsdRate: number,
): number {
  if (ngnXlmRate <= 0 || xlmUsdRate <= 0) return 0;
  return ngnXlmRate / xlmUsdRate;
}

/**
 * Calculates the absolute percentage deviation between two rates.
 *
 * @param rate1 - The rate to compare (e.g. implied rate)
 * @param rate2 - The reference rate (e.g. direct rate); used as denominator
 * @returns |rate1 - rate2| / rate2 × 100, or 0 if rate2 is zero or negative
 */
export function calculateDeviationPercent(
  rate1: number,
  rate2: number,
): number {
  if (rate2 <= 0) return 0;
  return Math.abs((rate1 - rate2) / rate2) * 100;
}

/**
 * Checks whether the NGN/XLM rate is consistent with the broader market by
 * comparing the cross-pair implied NGN/USD rate against a direct reference.
 *
 * Returns a zeroed result with `flagged=false` when any input is invalid
 * (zero or negative), so callers never need to guard against bad output.
 *
 * @param ngnXlmRate      - NGN per XLM from the oracle fetcher
 * @param xlmUsdRate      - USD per XLM from CoinGecko (or equivalent)
 * @param ngnUsdRate      - Direct NGN/USD reference rate
 * @param thresholdPercent - Deviation limit above which the result is flagged (default 2.0)
 * @returns CrossPairCheckResult
 */
export function checkCrossPairConsistency(
  ngnXlmRate: number,
  xlmUsdRate: number,
  ngnUsdRate: number,
  thresholdPercent: number = 2.0,
): CrossPairCheckResult {
  const zero: CrossPairCheckResult = {
    impliedRate: 0,
    directRate: 0,
    deviation: 0,
    deviationPercent: 0,
    flagged: false,
    timestamp: new Date(),
  };

  if (ngnXlmRate <= 0 || xlmUsdRate <= 0 || ngnUsdRate <= 0) {
    return zero;
  }

  const impliedRate = calculateImpliedNgnUsd(ngnXlmRate, xlmUsdRate);
  const deviationPercent = calculateDeviationPercent(impliedRate, ngnUsdRate);
  const deviation = Math.abs(impliedRate - ngnUsdRate);

  return {
    impliedRate,
    directRate: ngnUsdRate,
    deviation,
    deviationPercent,
    flagged: deviationPercent > thresholdPercent,
    timestamp: new Date(),
  };
}
