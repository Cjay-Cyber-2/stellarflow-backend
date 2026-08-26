import assert from "node:assert/strict";
import test from "node:test";
import { calculateRebalancingPlan } from "../src/services/liquidity/calculation";
import {
  FIVE_MINUTES_MS,
  LiquidityRebalancingWorker,
} from "../src/services/liquidity/worker";
import type {
  LiquidityPoolConfig,
  QueuedRebalancingSwap,
  RebalancingPlan,
} from "../src/services/liquidity/types";

const pool: LiquidityPoolConfig = {
  key: "ngn-ghs",
  anchorAccount: "GANCHOR",
  assets: [{ code: "NGN" }, { code: "GHS" }],
  managerAccounts: ["manager@example.com"],
};

test("uses a five-minute polling interval", () => {
  assert.equal(FIVE_MINUTES_MS, 300_000);
});

test("does not rebalance reserves inside the 70/30 boundary", () => {
  const plan = calculateRebalancingPlan(
    pool.key,
    pool.anchorAccount,
    [
      { code: "NGN", balance: 700, unitsPerXlm: 1, normalizedValue: 700 },
      { code: "GHS", balance: 300, unitsPerXlm: 1, normalizedValue: 300 },
    ],
    pool.managerAccounts,
  );
  assert.equal(plan, null);
});

test("calculates the volume needed to restore a breached pool to 50/50", () => {
  const plan = calculateRebalancingPlan(
    pool.key,
    pool.anchorAccount,
    [
      { code: "NGN", balance: 8000, unitsPerXlm: 10, normalizedValue: 800 },
      { code: "GHS", balance: 400, unitsPerXlm: 2, normalizedValue: 200 },
    ],
    pool.managerAccounts,
  );

  assert.ok(plan);
  assert.equal(plan.fromCurrency, "NGN");
  assert.equal(plan.toCurrency, "GHS");
  assert.equal(plan.normalizedVolume, 300);
  assert.equal(plan.fromAmount, 3000);
  assert.equal(plan.estimatedToAmount, 600);
  assert.equal(plan.fromReserveRatio, 0.8);
});

test("worker queues one swap and alerts configured manager accounts", async () => {
  const queuedPlans: RebalancingPlan[] = [];
  const alerts: QueuedRebalancingSwap[] = [];
  const worker = new LiquidityRebalancingWorker(
    [pool],
    { getReserves: async () => [8000, 400] },
    {
      getUnitsPerXlm: async (currency) => (currency === "NGN" ? 10 : 2),
    },
    {
      enqueueUnlessPending: async (plan) => {
        queuedPlans.push(plan);
        return {
          ...plan,
          id: "swap-1",
          status: "QUEUED",
          createdAt: new Date("2026-08-25T00:00:00Z"),
        };
      },
    },
    {
      send: async (swap, configuredPool) => {
        assert.deepEqual(configuredPool.managerAccounts, [
          "manager@example.com",
        ]);
        alerts.push(swap);
      },
    },
  );

  await worker.poll();

  assert.equal(queuedPlans.length, 1);
  assert.equal(alerts.length, 1);
  assert.equal(alerts[0]?.id, "swap-1");
});

test("worker does not queue a balanced pool", async () => {
  let queueCalls = 0;
  const worker = new LiquidityRebalancingWorker(
    [pool],
    { getReserves: async () => [6000, 800] },
    {
      getUnitsPerXlm: async (currency) => (currency === "NGN" ? 10 : 2),
    },
    {
      enqueueUnlessPending: async () => {
        queueCalls += 1;
        return null;
      },
    },
    { send: async () => undefined },
  );

  await worker.poll();
  assert.equal(queueCalls, 0);
});
