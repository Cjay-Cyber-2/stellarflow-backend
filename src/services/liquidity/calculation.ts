import type { RebalancingPlan, ValuedReserve } from "./types";

export const RESERVE_RATIO_UPPER_BOUND = 0.7;

/**
 * Returns the swap required to bring a breached reserve pair back to 50/50.
 * Reserve values must be normalized into the same unit (XLM in production).
 */
export function calculateRebalancingPlan(
  poolKey: string,
  anchorAccount: string,
  reserves: [ValuedReserve, ValuedReserve],
  managerAccounts: string[],
): RebalancingPlan | null {
  const [first, second] = reserves;
  const totalValue = first.normalizedValue + second.normalizedValue;

  if (
    !Number.isFinite(totalValue) ||
    totalValue <= 0 ||
    first.normalizedValue < 0 ||
    second.normalizedValue < 0
  ) {
    throw new Error(`Pool ${poolKey} contains invalid reserve values`);
  }

  const firstRatio = first.normalizedValue / totalValue;
  if (
    firstRatio <= RESERVE_RATIO_UPPER_BOUND &&
    firstRatio >= 1 - RESERVE_RATIO_UPPER_BOUND
  ) {
    return null;
  }

  const [from, to, fromRatio, toRatio] =
    firstRatio > RESERVE_RATIO_UPPER_BOUND
      ? [first, second, firstRatio, 1 - firstRatio]
      : [second, first, 1 - firstRatio, firstRatio];

  const normalizedVolume = (from.normalizedValue - to.normalizedValue) / 2;

  return {
    poolKey,
    anchorAccount,
    fromCurrency: from.code,
    toCurrency: to.code,
    fromAmount: normalizedVolume * from.unitsPerXlm,
    estimatedToAmount: normalizedVolume * to.unitsPerXlm,
    normalizedVolume,
    fromReserveRatio: fromRatio,
    toReserveRatio: toRatio,
    managerAccounts: [...managerAccounts],
  };
}
