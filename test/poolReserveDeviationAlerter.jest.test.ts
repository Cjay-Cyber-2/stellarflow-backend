import { describe, expect, it, jest } from "@jest/globals";
import { PoolReserveDeviationAlerter } from "../src/services/poolReserveDeviationAlerter";

describe("PoolReserveDeviationAlerter", () => {
  it("alerts only when consecutive confirmed swaps in one block exceed 10%", async () => {
    const sendAlert = jest.fn().mockResolvedValue(true);
    const alerter = new PoolReserveDeviationAlerter({ sendAlert } as any, 10);

    expect(
      await alerter.observe({
        poolId: "pool-1",
        blockHeight: 99,
        transactionHash: "tx-1",
        reserveA: 100,
        reserveB: 100,
      }),
    ).toBeNull();
    expect(
      await alerter.observe({
        poolId: "pool-1",
        blockHeight: 99,
        transactionHash: "tx-2",
        reserveA: 105,
        reserveB: 100,
      }),
    ).toBeNull();

    const alert = await alerter.observe({
      poolId: "pool-1",
      blockHeight: 99,
      transactionHash: "tx-3",
      reserveA: 120,
      reserveB: 100,
    });
    expect(alert?.deviationPercent).toBeCloseTo(14.2857, 3);
    expect(sendAlert).toHaveBeenCalledTimes(1);
  });

  it("does not compare reserves across blocks", async () => {
    const alerter = new PoolReserveDeviationAlerter({
      sendAlert: jest.fn(),
    } as any);
    await alerter.observe({
      poolId: "pool-1",
      blockHeight: 1,
      transactionHash: "tx-1",
      reserveA: 100,
      reserveB: 100,
    });
    expect(
      await alerter.observe({
        poolId: "pool-1",
        blockHeight: 2,
        transactionHash: "tx-2",
        reserveA: 200,
        reserveB: 100,
      }),
    ).toBeNull();
  });
});
