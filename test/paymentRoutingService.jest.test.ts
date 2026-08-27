import {
  PaymentRoutingService,
  scoreRoute,
  RouteCandidate,
} from "../src/services/paymentRoutingService";

function makeCandidate(
  overrides: Partial<RouteCandidate> = {},
): RouteCandidate {
  return {
    id: "route-1",
    senderCurrency: "XLM",
    receiverCurrency: "NGN",
    sourceAsset: "native",
    targetRail: "MOBILE_MONEY",
    provider: "provider-a",
    rate: 1580.5,
    fee: 2.5,
    estimatedAmount: 158050,
    slippageBps: 30,
    liquidityPoolId: null,
    priority: 1,
    ...overrides,
  };
}

describe("scoreRoute (pure logic)", () => {
  it("prefers higher effective rate", () => {
    const a = makeCandidate({
      rate: 1600,
      fee: 10,
      slippageBps: 10,
      priority: 0,
    });
    const b = makeCandidate({
      rate: 1580,
      fee: 1,
      slippageBps: 10,
      priority: 0,
    });
    expect(scoreRoute(a, 100)).toBeGreaterThan(scoreRoute(b, 100));
  });

  it("penalizes high slippage", () => {
    const a = makeCandidate({
      rate: 1600,
      fee: 0,
      slippageBps: 100,
      priority: 0,
    });
    const b = makeCandidate({
      rate: 1600,
      fee: 0,
      slippageBps: 10,
      priority: 0,
    });
    expect(scoreRoute(b, 100)).toBeGreaterThan(scoreRoute(a, 100));
  });

  it("rewards higher priority", () => {
    const a = makeCandidate({
      rate: 1600,
      fee: 0,
      slippageBps: 0,
      priority: 0,
    });
    const b = makeCandidate({
      rate: 1600,
      fee: 0,
      slippageBps: 0,
      priority: 5,
    });
    expect(scoreRoute(b, 100)).toBeGreaterThan(scoreRoute(a, 100));
  });

  it("returns higher score for lower fee at same rate", () => {
    const a = makeCandidate({
      rate: 1600,
      fee: 100,
      slippageBps: 0,
      priority: 0,
    });
    const b = makeCandidate({
      rate: 1600,
      fee: 0,
      slippageBps: 0,
      priority: 0,
    });
    expect(scoreRoute(b, 100)).toBeGreaterThan(scoreRoute(a, 100));
  });
});

describe("PaymentRoutingService — validation logic", () => {
  let service: PaymentRoutingService;

  beforeEach(() => {
    service = new PaymentRoutingService();
  });

  it("rejects same-currency payments without hitting DB", async () => {
    const result = await service.findOptimalRoutes({
      senderCurrency: "NGN",
      receiverCurrency: "NGN",
      inputAmount: 100,
    });
    expect(result.success).toBe(false);
    expect(result.error).toContain("must differ");
    expect(result.senderCurrency).toBe("NGN");
    expect(result.receiverCurrency).toBe("NGN");
  });

  it("rejects zero amount without hitting DB", async () => {
    const result = await service.findOptimalRoutes({
      senderCurrency: "XLM",
      receiverCurrency: "NGN",
      inputAmount: 0,
    });
    expect(result.success).toBe(false);
    expect(result.error).toContain("positive");
  });

  it("rejects negative amount without hitting DB", async () => {
    const result = await service.findOptimalRoutes({
      senderCurrency: "XLM",
      receiverCurrency: "NGN",
      inputAmount: -50,
    });
    expect(result.success).toBe(false);
    expect(result.error).toContain("positive");
  });

  it("normalizes currencies to uppercase", async () => {
    const result = await service.findOptimalRoutes({
      senderCurrency: "xlm",
      receiverCurrency: "ngn",
      inputAmount: 100,
    });
    expect(result.senderCurrency).toBe("XLM");
    expect(result.receiverCurrency).toBe("NGN");
  });
});
