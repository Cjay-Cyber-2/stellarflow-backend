import type { MarketRate } from "./marketRate/types";
export declare const REVIEWABLE_CURRENCIES: Set<string>;
type ReviewStatus = "AUTO_APPROVED" | "PENDING" | "APPROVED" | "REJECTED";
type ContractStatus = "NOT_SUBMITTED" | "SUBMITTED" | "SKIPPED";
export interface PendingPriceReview {
    id: number;
    currency: string;
    rate: number;
    source: string;
    fetchedAt: Date;
    reviewStatus: ReviewStatus;
    contractStatus: ContractStatus;
    reason: string | null;
    notes: string | null;
    baselineRate: number | null;
    baselineTimestamp: Date | null;
    changePercent: number | null;
    memoId: string | null;
    stellarTxHash: string | null;
    reviewedAt: Date | null;
    reviewedBy: string | null;
    createdAt: Date;
    updatedAt: Date;
}
export interface PriceAssessment {
    reviewRecordId: number;
    manualReviewRequired: boolean;
    reason?: string | undefined;
    changePercent?: number | undefined;
    comparisonRate?: number | undefined;
    comparisonTimestamp?: Date | undefined;
}
export declare class PriceReviewService {
    private schemaReadyPromise;
    private ensureSchema;
    assessRate(rate: MarketRate): Promise<PriceAssessment>;
    markContractSubmitted(reviewRecordId: number, memoId: string, stellarTxHash: string): Promise<void>;
    getPendingReviews(): Promise<PendingPriceReview[]>;
    getPendingReviewById(reviewId: number): Promise<PendingPriceReview | null>;
    approveReview(params: {
        reviewId: number;
        reviewedBy?: string;
        reviewNotes?: string;
        memoId: string;
        stellarTxHash: string;
    }): Promise<PendingPriceReview>;
    rejectReview(params: {
        reviewId: number;
        reviewedBy?: string;
        reviewNotes?: string;
    }): Promise<PendingPriceReview>;
    private getLatestSubmittedBaseline;
}
export declare const priceReviewService: PriceReviewService;
export {};
//# sourceMappingURL=priceReviewService.d.ts.map