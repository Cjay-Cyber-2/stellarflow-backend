import prisma from "../../lib/prisma";
import { WebhookRebalancingAlertSender } from "./alertSender";
import { loadLiquidityPoolConfig } from "./config";
import { PrismaRebalancingQueue } from "./prismaQueue";
import { MarketLiquidityRateSource } from "./rateSource";
import { StellarReserveSource } from "./stellarReserveSource";
import { FIVE_MINUTES_MS, LiquidityRebalancingWorker } from "./worker";

export function startLiquidityRebalancingWorker():
  | LiquidityRebalancingWorker
  | undefined {
  const pools = loadLiquidityPoolConfig();
  if (pools.length === 0) {
    console.info(
      "Liquidity rebalancing worker disabled: no anchor pools configured",
    );
    return undefined;
  }

  const worker = new LiquidityRebalancingWorker(
    pools,
    new StellarReserveSource(),
    new MarketLiquidityRateSource(),
    new PrismaRebalancingQueue(prisma),
    new WebhookRebalancingAlertSender(),
    FIVE_MINUTES_MS,
  );
  worker.start();
  console.info(
    `Liquidity rebalancing worker monitoring ${pools.length} pool(s) every ${FIVE_MINUTES_MS}ms`,
  );
  return worker;
}
