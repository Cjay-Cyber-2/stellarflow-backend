import { jest, describe, it, expect, beforeEach, afterEach } from "@jest/globals";
import { Keypair } from "@stellar/stellar-sdk";

const sourceKeypair = Keypair.random();
const fakeServer = {
  feeStats: jest.fn(),
  submitTransaction: jest.fn(),
};
const reportFailure = jest.fn();
const getNextSequence = jest.fn();
const invalidate = jest.fn();
const sign = jest.fn();

jest.unstable_mockModule("../src/lib/stellarProvider", () => ({
  default: {
    getServer: () => fakeServer,
    reportFailure,
  },
}));

jest.unstable_mockModule("../src/services/sequence-manager", () => ({
  sequenceManager: {
    getNextSequence,
    invalidate,
  },
}));

jest.unstable_mockModule("../src/state/appState", () => ({
  assertSigningAllowed: jest.fn(async () => undefined),
}));

jest.unstable_mockModule("../src/signer", () => ({
  signer: {
    getPublicKey: jest.fn(async () => sourceKeypair.publicKey()),
    sign,
  },
}));

const { StellarService } = await import("../src/services/stellarService");

describe("StellarService time-bound enforcement", () => {
  beforeEach(() => {
    jest.useRealTimers();
    jest.clearAllMocks();
    fakeServer.feeStats.mockResolvedValue({
      fee_charged: { p50: "100" },
    } as never);
    fakeServer.submitTransaction.mockResolvedValue({
      hash: "confirmed-hash",
    } as never);
    getNextSequence.mockResolvedValue("1" as never);
    sign.mockResolvedValue(Buffer.alloc(64) as never);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("enforces explicit 15-second time bounds on single, batched, and multi-sig transactions", async () => {
    const observedMaxTimes: number[] = [];

    fakeServer.submitTransaction.mockImplementation(async (transaction: any) => {
      expect(transaction.timeBounds).toBeDefined();

      const maxTime = Number(transaction.timeBounds.maxTime);
      const nowSeconds = Math.floor(Date.now() / 1000);
      expect(maxTime).toBeGreaterThan(nowSeconds);
      expect(maxTime - nowSeconds).toBeLessThanOrEqual(15);

      observedMaxTimes.push(maxTime);
      return { hash: "confirmed-hash" };
    });

    const service = new StellarService();

    await service.submitPriceUpdate("KES", 123.45, "SF-KES-TEST-001");
    await service.submitBatchedPriceUpdates(
      [
        { currency: "KES", price: 123.45 },
        { currency: "NGN", price: 1500.25 },
      ],
      "SF-BATCH-TEST-001",
    );
    await service.submitMultiSignedPriceUpdate(
      "GHS",
      15.2,
      "SF-GHS-TEST-001",
      [],
    );

    expect(observedMaxTimes).toHaveLength(3);
    expect(fakeServer.submitTransaction).toHaveBeenCalledTimes(3);
  });

  it("recycles locally timed-out transactions and retries with a new assignment immediately", async () => {
    jest.useFakeTimers();

    let secondSubmitResolve!: (value: unknown) => void;
    const secondSubmit = new Promise((resolve) => {
      secondSubmitResolve = resolve;
    });

    fakeServer.submitTransaction
      .mockImplementationOnce(() => new Promise(() => undefined))
      .mockImplementationOnce(() => secondSubmit);
    getNextSequence
      .mockResolvedValueOnce("1" as never)
      .mockResolvedValueOnce("2" as never);

    const service = new StellarService();
    const submitPromise = service.submitPriceUpdate(
      "KES",
      456.78,
      "SF-KES-TEST-002",
    );

    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
    await jest.advanceTimersByTimeAsync(15_000);

    expect(invalidate).toHaveBeenCalledWith(sourceKeypair.publicKey());
    expect(fakeServer.submitTransaction).toHaveBeenCalledTimes(2);
    expect(getNextSequence).toHaveBeenNthCalledWith(
      2,
      sourceKeypair.publicKey(),
    );

    secondSubmitResolve({ hash: "reassigned-hash" });
    await expect(submitPromise).resolves.toBe("reassigned-hash");
    expect(reportFailure).not.toHaveBeenCalled();
  });
});
