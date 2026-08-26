import { Router, Request, Response } from "express";
import { votingPowerService } from "../services/votingPowerService";
import { sendApiError } from "../lib/apiError";

const router = Router();

/**
 * @route GET /api/v1/governance/voting-weight
 * @desc Get voting weight for an account at a specific ledger sequence
 * @access Public (or authenticated depending on frontend requirements)
 */
router.get("/voting-weight", async (req: Request, res: Response) => {
  try {
    const { account, ledgerSequence } = req.query;
    
    if (!account || typeof account !== 'string') {
      return sendApiError(res, 400, "BAD_REQUEST", "Account is required");
    }
    
    if (!ledgerSequence || isNaN(Number(ledgerSequence))) {
      return sendApiError(res, 400, "BAD_REQUEST", "Valid ledgerSequence is required");
    }

    const data = await votingPowerService.getVotingWeightAtLedger(
      account, 
      Number(ledgerSequence)
    );
    
    res.json({
      success: true,
      data
    });
  } catch (error) {
    sendApiError(res, 500, "INTERNAL_SERVER_ERROR", (error as Error).message);
  }
});

/**
 * @route GET /api/v1/governance/voting-power/:account
 * @desc Calculate and return user's voting power snapshot based on current lock durations
 * @access Public
 */
router.get("/voting-power/:account", async (req: Request, res: Response) => {
  try {
    const { account } = req.params;
    
    if (!account) {
      return sendApiError(res, 400, "BAD_REQUEST", "Account is required");
    }

    const snapshot = await votingPowerService.getUserVotingPowerSnapshot(
      String(account),
    );
    
    res.json({
      success: true,
      data: snapshot
    });
  } catch (error) {
    sendApiError(res, 500, "INTERNAL_SERVER_ERROR", (error as Error).message);
  }
});

export default router;
