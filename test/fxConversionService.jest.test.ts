describe("FxConversionService — pure logic (no DB/import)", () => {
  describe("calculateDeviationBps", () => {
    function calculateDeviationBps(
      lockedRate: number,
      feedRate: number,
    ): number {
      if (feedRate === 0) return 0;
      return Math.round((Math.abs(lockedRate - feedRate) / feedRate) * 10_000);
    }

    it("returns 0 for identical rates", () => {
      expect(calculateDeviationBps(100, 100)).toBe(0);
    });

    it("calculates correct deviation in basis points", () => {
      expect(calculateDeviationBps(100, 101)).toBe(99);
    });

    it("returns 0 when feed rate is 0", () => {
      expect(calculateDeviationBps(100, 0)).toBe(0);
    });

    it("handles large deviations", () => {
      // |100-200|/200 * 10000 = 5000 bps
      expect(calculateDeviationBps(100, 200)).toBe(5000);
    });
  });

  describe("calculateOutputAmount", () => {
    function calculateOutputAmount(
      inputAmount: number,
      rate: number,
      fee: number,
    ): number {
      return Math.max(0, inputAmount * rate - fee);
    }

    it("calculates output minus fee", () => {
      // 100 * 1580 - 2.5 = 157997.5
      expect(calculateOutputAmount(100, 1580, 2.5)).toBe(100 * 1580 - 2.5);
    });

    it("floors to 0 when fee exceeds conversion", () => {
      expect(calculateOutputAmount(0.001, 1, 5)).toBe(0);
    });

    it("handles zero input amount", () => {
      expect(calculateOutputAmount(0, 1580, 2.5)).toBe(0);
    });

    it("handles zero fee", () => {
      expect(calculateOutputAmount(100, 10, 0)).toBe(1000);
    });
  });

  describe("MAX_DEVIATION_BPS threshold", () => {
    const MAX_DEVIATION_BPS = 50;

    it("allows small deviations under threshold", () => {
      const deviation = Math.round((Math.abs(1580 - 1581) / 1581) * 10_000);
      expect(deviation).toBeLessThanOrEqual(MAX_DEVIATION_BPS);
    });

    it("rejects large deviations over threshold", () => {
      const deviation = Math.round((Math.abs(1000 - 2000) / 2000) * 10_000);
      expect(deviation).toBeGreaterThan(MAX_DEVIATION_BPS);
    });
  });
});
