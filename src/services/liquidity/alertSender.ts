import axios from "axios";
import type {
  LiquidityPoolConfig,
  QueuedRebalancingSwap,
  RebalancingAlertSender,
} from "./types";

export class WebhookRebalancingAlertSender
  implements RebalancingAlertSender
{
  async send(
    swap: QueuedRebalancingSwap,
    pool: LiquidityPoolConfig,
  ): Promise<void> {
    const webhookUrl =
      pool.alertWebhookUrl ??
      process.env.LIQUIDITY_MANAGER_ALERT_WEBHOOK_URL;

    if (!webhookUrl) {
      throw new Error(
        `No liquidity manager alert webhook configured for pool ${pool.key}`,
      );
    }

    await axios.post(
      webhookUrl,
      {
        event: "liquidity.rebalancing_queued",
        recipients: pool.managerAccounts,
        swap,
      },
      {
        headers: { "Content-Type": "application/json" },
        timeout: 5000,
      },
    );
  }
}
