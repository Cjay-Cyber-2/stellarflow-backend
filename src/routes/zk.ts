import { Router } from "express";
import { verifyMerkleRoot } from "../services/merkleRootCache";

const router = Router();

router.get("/verify-root/:root_hash", async (req, res) => {
  const valid = await verifyMerkleRoot(req.params.root_hash);
  res.json({ valid });
});

export default router;
