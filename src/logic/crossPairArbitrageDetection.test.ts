import { describe, it, expect } from '@jest/globals';
import {
  calculateImpliedNgnUsd,
  calculateDeviationPercent,
  checkCrossPairConsistency,
} from './crossPairArbitrageDetection';

// ---------------------------------------------------------------------------
// Unit tests (Task 2.4)
// ---------------------------------------------------------------------------

describe('calculateImpliedNgnUsd', () => {
  it('returns the correct quotient for typical inputs', () => {
    expect(calculateImpliedNgnUsd(1500, 0.1)).toBeCloseTo(15000, 5);
  });

  it('returns the correct quotient for a second set of inputs', () => {
    expect(calculateImpliedNgnUsd(750, 0.15)).toBeCloseTo(5000, 5);
  });

  it('returns 0 when ngnXlmRate is zero', () => {
    expect(calculateImpliedNgnUsd(0, 0.1)).toBe(0);
  });

  it('returns 0 when xlmUsdRate is zero', () => {
    expect(calculateImpliedNgnUsd(1500, 0)).toBe(0);
  });

  it('returns 0 when ngnXlmRate is negative', () => {
    expect(calculateImpliedNgnUsd(-100, 0.1)).toBe(0);
  });

  it('returns 0 when xlmUsdRate is negative', () => {
    expect(calculateImpliedNgnUsd(1500, -0.1)).toBe(0);
  });
});

describe('checkCrossPairConsistency', () => {
  it('returns flagged=false when rates are consistent within 2%', () => {
    // implied = 1500 / 0.1 = 15000, direct = 15000 → deviation = 0%
    const result = checkCrossPairConsistency(1500, 0.1, 15000);
    expect(result.flagged).toBe(false);
    expect(result.impliedRate).toBeCloseTo(15000, 5);
    expect(result.deviationPercent).toBeCloseTo(0, 5);
  });

  it('returns flagged=true when deviation exceeds 2%', () => {
    // implied = 1500 / 0.1 = 15000, direct = 14000 → deviation ≈ 7.14%
    const result = checkCrossPairConsistency(1500, 0.1, 14000);
    expect(result.flagged).toBe(true);
    expect(result.impliedRate).toBeCloseTo(15000, 5);
    expect(result.deviationPercent).toBeCloseTo(7.142857, 4);
  });

  it('returns flagged=false with all numeric fields 0 for zero ngnXlmRate', () => {
    const result = checkCrossPairConsistency(0, 0.1, 15000);
    expect(result.flagged).toBe(false);
    expect(result.impliedRate).toBe(0);
    expect(result.directRate).toBe(0);
    expect(result.deviation).toBe(0);
    expect(result.deviationPercent).toBe(0);
  });

  it('returns flagged=false with all numeric fields 0 for negative ngnXlmRate', () => {
    const result = checkCrossPairConsistency(-100, 0.1, 15000);
    expect(result.flagged).toBe(false);
    expect(result.impliedRate).toBe(0);
    expect(result.directRate).toBe(0);
    expect(result.deviation).toBe(0);
    expect(result.deviationPercent).toBe(0);
  });

  it('returns flagged=false at exactly the 2% boundary (boundary is inclusive)', () => {
    // implied = 1530 / 0.1 = 15300, direct = 15000
    // deviationPercent = |15300 - 15000| / 15000 * 100 = 300/15000*100 = 2.0 exactly
    const result = checkCrossPairConsistency(1530, 0.1, 15000);
    expect(result.deviationPercent).toBeCloseTo(2.0, 10);
    expect(result.flagged).toBe(false);
  });

  it('returns flagged=true just above the 2% boundary', () => {
    // implied = 1530.1 / 0.1 = 15301, direct = 15000
    // deviationPercent = |15301 - 15000| / 15000 * 100 = 301/15000*100 ≈ 2.0067%
    const result = checkCrossPairConsistency(1530.1, 0.1, 15000);
    expect(result.deviationPercent).toBeGreaterThan(2.0);
    expect(result.flagged).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Property-style tests (Tasks 2.1, 2.2, 2.3)
// ---------------------------------------------------------------------------

/**
 * Property 3: Result fields are internally consistent
 * Validates: Requirements 1.3, 1.4, 1.5
 *
 * For any valid positive triple, the result's impliedRate, deviationPercent,
 * and flagged must be mutually consistent with the helper functions.
 */
describe('Property 3: result fields are internally consistent', () => {
  const validTriples: Array<[number, number, number]> = [
    [1500, 0.1, 15000],
    [1500, 0.1, 14000],
    [2000, 0.2, 10000],
    [500, 0.05, 9000],
    [1530, 0.1, 15000],
    [1530.1, 0.1, 15000],
    [100, 0.01, 10000],
    [9999, 0.5, 20000],
  ];

  it.each(validTriples)(
    'ngnXlm=%f, xlmUsd=%f, ngnUsd=%f — impliedRate, deviationPercent, and flagged are consistent',
    (ngnXlm, xlmUsd, ngnUsd) => {
      const result = checkCrossPairConsistency(ngnXlm, xlmUsd, ngnUsd);
      const expectedImplied = calculateImpliedNgnUsd(ngnXlm, xlmUsd);
      const expectedDeviation = calculateDeviationPercent(expectedImplied, ngnUsd);

      expect(result.impliedRate).toBeCloseTo(expectedImplied, 8);
      expect(result.deviationPercent).toBeCloseTo(expectedDeviation, 8);
      expect(result.flagged).toBe(result.deviationPercent > 2.0);
    },
  );
});

/**
 * Property 4: Invalid inputs produce safe zeroed result
 * Validates: Requirements 1.6
 *
 * For any input where at least one value is zero or negative,
 * checkCrossPairConsistency must return flagged=false and all numeric fields 0.
 */
describe('Property 4: invalid inputs produce safe zeroed result', () => {
  const invalidCases: Array<[string, number, number, number]> = [
    ['zero ngnXlmRate', 0, 0.1, 15000],
    ['zero xlmUsdRate', 1500, 0, 15000],
    ['zero ngnUsdRate', 1500, 0.1, 0],
    ['negative ngnXlmRate', -100, 0.1, 15000],
    ['negative xlmUsdRate', 1500, -0.1, 15000],
    ['negative ngnUsdRate', 1500, 0.1, -15000],
    ['all zeros', 0, 0, 0],
    ['all negative', -1, -1, -1],
  ];

  it.each(invalidCases)(
    '%s → flagged=false and all numeric fields are 0',
    (_label, ngnXlm, xlmUsd, ngnUsd) => {
      const result = checkCrossPairConsistency(ngnXlm, xlmUsd, ngnUsd);
      expect(result.flagged).toBe(false);
      expect(result.impliedRate).toBe(0);
      expect(result.directRate).toBe(0);
      expect(result.deviation).toBe(0);
      expect(result.deviationPercent).toBe(0);
    },
  );
});

/**
 * Property 5: Flagging threshold is strictly greater-than
 * Validates: Requirements 1.4, 1.5
 *
 * Exactly at threshold → flagged=false; strictly above threshold → flagged=true.
 */
describe('Property 5: flagging threshold is strictly greater-than', () => {
  it('returns flagged=false when deviationPercent is exactly 2.0%', () => {
    // implied = 15300, direct = 15000 → deviationPercent = 2.0 exactly
    const result = checkCrossPairConsistency(1530, 0.1, 15000);
    expect(result.deviationPercent).toBeCloseTo(2.0, 10);
    expect(result.flagged).toBe(false);
  });

  it('returns flagged=true when deviationPercent is strictly above 2.0%', () => {
    // implied = 15301, direct = 15000 → deviationPercent ≈ 2.0067%
    const result = checkCrossPairConsistency(1530.1, 0.1, 15000);
    expect(result.deviationPercent).toBeGreaterThan(2.0);
    expect(result.flagged).toBe(true);
  });

  it('returns flagged=false with a custom threshold when deviation equals that threshold', () => {
    // implied = 15500, direct = 15000 → deviationPercent ≈ 3.333%
    // threshold = 3.333... → should not flag
    const ngnXlm = 1550;
    const xlmUsd = 0.1;
    const ngnUsd = 15000;
    const implied = calculateImpliedNgnUsd(ngnXlm, xlmUsd); // 15500
    const deviation = calculateDeviationPercent(implied, ngnUsd); // ≈ 3.333%
    const result = checkCrossPairConsistency(ngnXlm, xlmUsd, ngnUsd, deviation);
    expect(result.flagged).toBe(false);
  });

  it('returns flagged=true with a custom threshold when deviation is just above it', () => {
    // Same setup but threshold is slightly below the actual deviation
    const ngnXlm = 1550;
    const xlmUsd = 0.1;
    const ngnUsd = 15000;
    const implied = calculateImpliedNgnUsd(ngnXlm, xlmUsd); // 15500
    const deviation = calculateDeviationPercent(implied, ngnUsd); // ≈ 3.333%
    const result = checkCrossPairConsistency(ngnXlm, xlmUsd, ngnUsd, deviation - 0.001);
    expect(result.flagged).toBe(true);
  });
});
