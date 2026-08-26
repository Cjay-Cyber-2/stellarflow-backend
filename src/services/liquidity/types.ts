export interface LiquidityAsset {
  code: string;
  issuer?: string;
}

export interface LiquidityPoolConfig {
  key: string;
  anchorAccount: string;
  assets: [LiquidityAsset, LiquidityAsset];
  managerAccounts: string[];
  alertWebhookUrl?: string;
}

export interface ValuedReserve extends LiquidityAsset {
  balance: number;
  unitsPerXlm: number;
  normalizedValue: number;
}

export interface RebalancingPlan {
  poolKey: string;
  anchorAccount: string;
  fromCurrency: string;
  toCurrency: string;
  fromAmount: number;
  estimatedToAmount: number;
  normalizedVolume: number;
  fromReserveRatio: number;
  toReserveRatio: number;
  managerAccounts: string[];
}

export interface QueuedRebalancingSwap extends RebalancingPlan {
  id: string;
  status: "QUEUED";
  createdAt: Date;
}

export interface RebalancingQueue {
  enqueueUnlessPending(
    plan: RebalancingPlan,
  ): Promise<QueuedRebalancingSwap | null>;
}

export interface ReserveSource {
  getReserves(pool: LiquidityPoolConfig): Promise<[number, number]>;
}

export interface LiquidityRateSource {
  getUnitsPerXlm(currency: string): Promise<number>;
}

export interface RebalancingAlertSender {
  send(
    swap: QueuedRebalancingSwap,
    pool: LiquidityPoolConfig,
  ): Promise<void>;
}
