import { Router, Request, Response } from "express";
import { sendApiError } from "../lib/apiError.js";
import { feeEstimationService } from "../services/feeEstimationService";

const router = Router();

/**
 * GET /api/v1/tx/fee-estimate
 *
 * Returns dynamic transaction fee estimates based on recent Stellar ledger
 * statistics. Provides base fee, priority tier recommendations (low, medium,
 * urgent), network congestion level, and ledger capacity usage.
 *
 * Designed for wallet transaction signing wrappers to select an appropriate
 * fee based on desired confirmation priority.
 */
router.get("/fee-estimate", async (_req: Request, res: Response) => {
  try {
    const estimate = await feeEstimationService.getFeeEstimate();

    res.json({
      success: true,
      data: estimate,
    });
  } catch (error: any) {
    console.error("[API] Fee estimate fetch failed:", error);
    sendApiError(
      res,
      503,
      "SERVICE_UNAVAILABLE",
      "Unable to fetch fee estimates from the Stellar network",
    );
  }
});

export default router;
