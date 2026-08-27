import prisma from "../lib/prisma";

export interface RouteCandidate {
  id: string;
  senderCurrency: string;
  receiverCurrency: string;
  sourceAsset: string;
  targetRail: string;
  provider: string;
  rate: number;
  fee: number;
  estimatedAmount: number;
  slippageBps: number;
  liquidityPoolId: string | null;
  priority: number;
}

export interface RoutingRequest {
  senderCurrency: string;
  receiverCurrency: string;
  inputAmount: number;
  targetRail?: string;
}

export interface RoutingResult {
  success: boolean;
  routes: RouteCandidate[];
  inputAmount: number;
  senderCurrency: string;
  receiverCurrency: string;
  error?: string;
}

export interface RouteCreateParams {
  senderCurrency: string;
  receiverCurrency: string;
  sourceAsset: string;
  targetRail: string;
  provider: string;
  rate: number;
  fee: number;
  estimatedAmount: number;
  slippageBps?: number;
  liquidityPoolId?: string;
  priority?: number;
}

export function scoreRoute(route: RouteCandidate, inputAmount: number): number {
  const effectiveRate = route.rate - route.fee / inputAmount;
  const slippagePenalty = route.slippageBps / 10_000;
  return effectiveRate * (1 - slippagePenalty) + route.priority * 0.001;
}

export class PaymentRoutingService {
  async findOptimalRoutes(request: RoutingRequest): Promise<RoutingResult> {
    try {
      const senderCurrency = request.senderCurrency.toUpperCase();
      const receiverCurrency = request.receiverCurrency.toUpperCase();

      if (senderCurrency === receiverCurrency) {
        return {
          success: false,
          routes: [],
          inputAmount: request.inputAmount,
          senderCurrency,
          receiverCurrency,
          error: "Sender and receiver currencies must differ",
        };
      }

      if (request.inputAmount <= 0) {
        return {
          success: false,
          routes: [],
          inputAmount: request.inputAmount,
          senderCurrency,
          receiverCurrency,
          error: "Input amount must be positive",
        };
      }

      const where: Record<string, unknown> = {
        senderCurrency,
        receiverCurrency,
        status: "ACTIVE",
      };

      if (request.targetRail) {
        where.targetRail = request.targetRail.toUpperCase();
      }

      const dbRoutes = await prisma.paymentRoute.findMany({
        where,
        orderBy: [{ priority: "desc" }, { rate: "desc" }],
      });

      if (dbRoutes.length === 0) {
        return {
          success: false,
          routes: [],
          inputAmount: request.inputAmount,
          senderCurrency,
          receiverCurrency,
          error: "No active routes found for this currency pair",
        };
      }

      const candidates: RouteCandidate[] = dbRoutes.map(
        (r: Record<string, unknown>) => ({
          id: r.id as string,
          senderCurrency: r.senderCurrency as string,
          receiverCurrency: r.receiverCurrency as string,
          sourceAsset: r.sourceAsset as string,
          targetRail: r.targetRail as string,
          provider: r.provider as string,
          rate: Number(r.rate),
          fee: Number(r.fee),
          estimatedAmount: Number(r.estimatedAmount),
          slippageBps: r.slippageBps as number,
          liquidityPoolId: (r.liquidityPoolId as string) ?? null,
          priority: r.priority as number,
        }),
      );

      candidates.sort(
        (a, b) =>
          scoreRoute(b, request.inputAmount) -
          scoreRoute(a, request.inputAmount),
      );

      return {
        success: true,
        routes: candidates,
        inputAmount: request.inputAmount,
        senderCurrency,
        receiverCurrency,
      };
    } catch (error) {
      return {
        success: false,
        routes: [],
        inputAmount: request.inputAmount,
        senderCurrency: request.senderCurrency.toUpperCase(),
        receiverCurrency: request.receiverCurrency.toUpperCase(),
        error:
          error instanceof Error
            ? error.message
            : "Failed to find optimal routes",
      };
    }
  }

  async createRoute(params: RouteCreateParams): Promise<RouteCandidate> {
    const route = await prisma.paymentRoute.create({
      data: {
        senderCurrency: params.senderCurrency.toUpperCase(),
        receiverCurrency: params.receiverCurrency.toUpperCase(),
        sourceAsset: params.sourceAsset,
        targetRail: params.targetRail.toUpperCase(),
        provider: params.provider,
        rate: params.rate,
        fee: params.fee,
        estimatedAmount: params.estimatedAmount,
        slippageBps: params.slippageBps ?? 0,
        liquidityPoolId: params.liquidityPoolId ?? null,
        priority: params.priority ?? 0,
      },
    });

    return {
      id: route.id,
      senderCurrency: route.senderCurrency,
      receiverCurrency: route.receiverCurrency,
      sourceAsset: route.sourceAsset,
      targetRail: route.targetRail,
      provider: route.provider,
      rate: Number(route.rate),
      fee: Number(route.fee),
      estimatedAmount: Number(route.estimatedAmount),
      slippageBps: route.slippageBps,
      liquidityPoolId: route.liquidityPoolId,
      priority: route.priority,
    };
  }

  async getRouteById(routeId: string): Promise<RouteCandidate | null> {
    const route = await prisma.paymentRoute.findUnique({
      where: { id: routeId },
    });

    if (!route) return null;

    return {
      id: route.id,
      senderCurrency: route.senderCurrency,
      receiverCurrency: route.receiverCurrency,
      sourceAsset: route.sourceAsset,
      targetRail: route.targetRail,
      provider: route.provider,
      rate: Number(route.rate),
      fee: Number(route.fee),
      estimatedAmount: Number(route.estimatedAmount),
      slippageBps: route.slippageBps,
      liquidityPoolId: route.liquidityPoolId,
      priority: route.priority,
    };
  }

  async updateRouteStatus(
    routeId: string,
    status: "ACTIVE" | "PAUSED" | "RETIRED",
  ): Promise<RouteCandidate | null> {
    const route = await prisma.paymentRoute.update({
      where: { id: routeId },
      data: { status },
    });

    return {
      id: route.id,
      senderCurrency: route.senderCurrency,
      receiverCurrency: route.receiverCurrency,
      sourceAsset: route.sourceAsset,
      targetRail: route.targetRail,
      provider: route.provider,
      rate: Number(route.rate),
      fee: Number(route.fee),
      estimatedAmount: Number(route.estimatedAmount),
      slippageBps: route.slippageBps,
      liquidityPoolId: route.liquidityPoolId,
      priority: route.priority,
    };
  }

  async listRoutes(filters?: {
    senderCurrency?: string;
    receiverCurrency?: string;
    targetRail?: string;
    status?: string;
  }): Promise<RouteCandidate[]> {
    const where: Record<string, unknown> = {};

    if (filters?.senderCurrency) {
      where.senderCurrency = filters.senderCurrency.toUpperCase();
    }
    if (filters?.receiverCurrency) {
      where.receiverCurrency = filters.receiverCurrency.toUpperCase();
    }
    if (filters?.targetRail) {
      where.targetRail = filters.targetRail.toUpperCase();
    }
    if (filters?.status) {
      where.status = filters.status.toUpperCase();
    } else {
      where.status = "ACTIVE";
    }

    const dbRoutes = await prisma.paymentRoute.findMany({
      where,
      orderBy: [{ priority: "desc" }, { rate: "desc" }],
    });

    return dbRoutes.map((r: Record<string, unknown>) => ({
      id: r.id as string,
      senderCurrency: r.senderCurrency as string,
      receiverCurrency: r.receiverCurrency as string,
      sourceAsset: r.sourceAsset as string,
      targetRail: r.targetRail as string,
      provider: r.provider as string,
      rate: Number(r.rate),
      fee: Number(r.fee),
      estimatedAmount: Number(r.estimatedAmount),
      slippageBps: r.slippageBps as number,
      liquidityPoolId: (r.liquidityPoolId as string) ?? null,
      priority: r.priority as number,
    }));
  }
}
