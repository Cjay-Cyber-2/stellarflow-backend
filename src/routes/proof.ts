import { Router, Request, Response } from "express";
import { proofVerificationService } from "../services/proofVerificationService";

const router = Router();

router.post("/verify", async (req: Request, res: Response) => {
  const { proof, simulateContract } = req.body;

  if (!proof || !proof.proof_hex || !proof.public_inputs) {
    return res.status(400).json({
      success: false,
      message: "proof.proof_hex and proof.public_inputs are required",
    });
  }

  const response = await proofVerificationService.verifyProof({
    proof,
    simulate_contract: simulateContract ?? false,
  });

  if (!response.success) {
    return res.status(500).json(response);
  }

  return res.json(response);
});

router.post("/verify-batch", async (req: Request, res: Response) => {
  const { requests } = req.body;

  if (!Array.isArray(requests) || requests.length === 0) {
    return res.status(400).json({
      success: false,
      message: "requests must be a non-empty array",
    });
  }

  const response = await proofVerificationService.verifyProofBatch(requests);
  return res.json(response);
});

router.get("/health", async (req: Request, res: Response) => {
  const healthy = await proofVerificationService.checkHealth();
  return res.json({
    success: healthy,
    proofServiceAvailable: healthy,
  });
});

export default router;
