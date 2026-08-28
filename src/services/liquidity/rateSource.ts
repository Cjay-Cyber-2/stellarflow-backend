import { GHSRateFetcher } from "../marketRate/ghsFetcher";
import { KESRateFetcher } from "../marketRate/kesFetcher";
import { NGNRateFetcher } from "../marketRate/ngnFetcher";
import type { MarketRateFetcher } from "../marketRate/types";
import type { LiquidityRateSource } from "./types";

export class MarketLiquidityRateSource implements LiquidityRateSource {
  private readonly fetchers: Map<string, MarketRateFetcher>;

  constructor(fetchers?: MarketRateFetcher[]) {
    const configured = fetchers ?? [
      new GHSRateFetcher(),
      new KESRateFetcher(),
      new NGNRateFetcher(),
    ];
    this.fetchers = new Map(
      configured.map((fetcher) => [fetcher.getCurrency(), fetcher]),
    );
  }

  async getUnitsPerXlm(currency: string): Promise<number> {
    if (currency.toUpperCase() === "XLM") return 1;

    const fetcher = this.fetchers.get(currency.toUpperCase());
    if (!fetcher) {
      throw new Error(`No liquidity rate provider for ${currency}`);
    }
    const { rate } = await fetcher.fetchRate();
    if (!Number.isFinite(rate) || rate <= 0) {
      throw new Error(`Invalid liquidity rate for ${currency}`);
    }
    return rate;
  }
}
