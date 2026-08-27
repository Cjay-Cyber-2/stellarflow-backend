export declare const PRICE_REVIEW_WINDOW_MS: number;
export declare const PRICE_CHANGE_THRESHOLD = 0.15;
export interface PriceMovementEvaluation {
    isAnomalous: boolean;
    changePercent: number;
    reason?: string | undefined;
}
export declare function evaluatePriceMovement(params: {
    currentRate: number;
    baselineRate: number;
    currency: string;
    threshold?: number;
}): PriceMovementEvaluation;
//# sourceMappingURL=priceProtection.d.ts.map