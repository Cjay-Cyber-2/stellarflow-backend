import { Request, Response, Router } from "express";
import { sendApiError } from "../lib/apiError.js";
import { orderDepthAggregatorService } from "../services/orderDepthAggregatorService";

const router = Router();

router.get("/depth", async (req: Request, res: Response) => {
  const market = typeof req.query.market === "string" ? req.query.market : "";
  const tickSize = typeof req.query.tickSize === "string" ? req.query.tickSize : "";

  if (!market || !tickSize) {
    sendApiError(
      res,
      400,
      "VALIDATION_ERROR",
      "market and tickSize query parameters are required.",
    );
    return;
  }

  try {
    const depth = await orderDepthAggregatorService.getDepth(market, tickSize);
    res.json({ success: true, data: depth });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : "Unable to build order depth";
    const status = message.includes("Redis") ? 503 : 400;
    sendApiError(res, status, status === 503 ? "SERVICE_UNAVAILABLE" : "VALIDATION_ERROR", message);
  }
});

export default router;