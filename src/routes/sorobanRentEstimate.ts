import { Router, Request, Response } from "express";
import { sendApiError } from "../lib/apiError.js";
import { sorobanRentEstimatorService } from "../services/sorobanRentEstimatorService";
import type {
  StorageType,
  RentEstimateRequest,
} from "../services/sorobanRentEstimatorService";

const router = Router();

/**
 * POST /api/v1/soroban/rent/estimate-instruction-fee
 *
 * Estimate the non-refundable instruction fee for a given number of CPU
 * instructions.
 *
 * @swagger
 * /api/v1/soroban/rent/estimate-instruction-fee:
 *   post:
 *     tags: [Soroban Rent]
 *     summary: Estimate instruction fee
 *     description: >
 *       Computes the non-refundable CPU instruction fee in stroops and XLM for
 *       a given number of Soroban host instructions.
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               instructions:
 *                 type: integer
 *                 description: Number of CPU instructions to estimate.
 *                 example: 100000
 *     responses:
 *       200:
 *         description: Instruction fee estimate.
 */
router.post(
  "/estimate-instruction-fee",
  async (req: Request, res: Response) => {
    try {
      const instructions = Number(req.body?.instructions);
      if (!Number.isFinite(instructions) || instructions < 0) {
        sendApiError(
          res,
          400,
          "VALIDATION_ERROR",
          "A non-negative 'instructions' field is required.",
        );
        return;
      }

      const estimate =
        sorobanRentEstimatorService.computeInstructionFee(instructions);

      res.json({ success: true, data: estimate });
    } catch (error: any) {
      sendApiError(res, 500, "INTERNAL_SERVER_ERROR", error.message);
    }
  },
);

/**
 * POST /api/v1/soroban/rent/estimate-storage-rent
 *
 * Estimate the rent fee for a Soroban ledger entry of a given size and
 * duration.
 *
 * @swagger
 * /api/v1/soroban/rent/estimate-storage-rent:
 *   post:
 *     tags: [Soroban Rent]
 *     summary: Estimate storage rent
 *     description: >
 *       Computes the refundable rent fee in stroops and XLM for a contract data
 *       entry of a given size (bytes) and rental duration (ledgers).
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               entrySizeBytes:
 *                 type: integer
 *                 description: Size of the ledger entry in bytes.
 *                 example: 256
 *               rentLedgers:
 *                 type: integer
 *                 description: Number of ledgers to rent for.
 *                 example: 6307200
 *               storageType:
 *                 type: string
 *                 enum: [temporary, persistent, instance]
 *                 description: Storage type.
 *                 default: persistent
 *     responses:
 *       200:
 *         description: Storage rent estimate.
 */
router.post("/estimate-storage-rent", async (req: Request, res: Response) => {
  try {
    const entrySizeBytes = Number(req.body?.entrySizeBytes);
    const rentLedgers = Number(req.body?.rentLedgers);
    const storageType: StorageType = req.body?.storageType ?? "persistent";

    if (!Number.isFinite(entrySizeBytes) || entrySizeBytes < 0) {
      sendApiError(
        res,
        400,
        "VALIDATION_ERROR",
        "A non-negative 'entrySizeBytes' field is required.",
      );
      return;
    }

    if (
      !Number.isFinite(rentLedgers) ||
      rentLedgers < 0 ||
      !Number.isInteger(rentLedgers)
    ) {
      sendApiError(
        res,
        400,
        "VALIDATION_ERROR",
        "A non-negative integer 'rentLedgers' field is required.",
      );
      return;
    }

    if (!["temporary", "persistent", "instance"].includes(storageType)) {
      sendApiError(
        res,
        400,
        "VALIDATION_ERROR",
        "'storageType' must be one of: temporary, persistent, instance.",
      );
      return;
    }

    const estimate = sorobanRentEstimatorService.computeStorageRent(
      entrySizeBytes,
      rentLedgers,
      storageType,
    );

    res.json({ success: true, data: estimate });
  } catch (error: any) {
    sendApiError(res, 500, "INTERNAL_SERVER_ERROR", error.message);
  }
});

/**
 * POST /api/v1/soroban/rent/estimate-entry-costs
 *
 * Calculate the full cost breakdown for read/write entries and bytes.
 *
 * @swagger
 * /api/v1/soroban/rent/estimate-entry-costs:
 *   post:
 *     tags: [Soroban Rent]
 *     summary: Estimate ledger entry I/O costs
 *     description: >
 *       Computes the full cost breakdown for read/write ledger entries and
 *       byte I/O operations in stroops and XLM.
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               readEntries:
 *                 type: integer
 *                 default: 0
 *               writeEntries:
 *                 type: integer
 *                 default: 0
 *               readBytes:
 *                 type: integer
 *                 default: 0
 *               writeBytes:
 *                 type: integer
 *                 default: 0
 *     responses:
 *       200:
 *         description: Entry cost breakdown.
 */
router.post("/estimate-entry-costs", async (req: Request, res: Response) => {
  try {
    const readEntries = Number(req.body?.readEntries ?? 0);
    const writeEntries = Number(req.body?.writeEntries ?? 0);
    const readBytes = Number(req.body?.readBytes ?? 0);
    const writeBytes = Number(req.body?.writeBytes ?? 0);

    for (const [label, val] of [
      ["readEntries", readEntries],
      ["writeEntries", writeEntries],
      ["readBytes", readBytes],
      ["writeBytes", writeBytes],
    ] as const) {
      if (!Number.isFinite(val) || val < 0 || !Number.isInteger(val)) {
        sendApiError(
          res,
          400,
          "VALIDATION_ERROR",
          `'${label}' must be a non-negative integer.`,
        );
        return;
      }
    }

    const breakdown = sorobanRentEstimatorService.computeEntryCosts(
      readEntries,
      writeEntries,
      readBytes,
      writeBytes,
    );

    res.json({ success: true, data: breakdown });
  } catch (error: any) {
    sendApiError(res, 500, "INTERNAL_SERVER_ERROR", error.message);
  }
});

/**
 * POST /api/v1/soroban/rent/estimate
 *
 * Full rent estimation gateway. Given a contract ID and optional storage
 * parameters, returns a complete cost estimate including instruction fees,
 * storage rent, entry costs, and optional simulation results.
 *
 * @swagger
 * /api/v1/soroban/rent/estimate:
 *   post:
 *     tags: [Soroban Rent]
 *     summary: Full rent estimation gateway
 *     description: >
 *       Performs a comprehensive rent estimation for a Soroban contract
 *       including instruction fees, storage rent, entry costs, and optionally
 *       simulates a contract call to get live resource consumption data.
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               contractId:
 *                 type: string
 *                 description: Deployed contract ID (hex or C... address).
 *               storageType:
 *                 type: string
 *                 enum: [temporary, persistent, instance]
 *                 default: persistent
 *               entrySizeBytes:
 *                 type: integer
 *                 description: Size of the ledger entry in bytes.
 *               rentLedgers:
 *                 type: integer
 *                 description: Number of ledgers to estimate rent for. Defaults to ~20 years.
 *               storageKey:
 *                 type: string
 *                 description: Hex-encoded storage key to look up existing contract data.
 *     responses:
 *       200:
 *         description: Comprehensive rent estimate.
 */
router.post("/estimate", async (req: Request, res: Response) => {
  try {
    const contractId = req.body?.contractId;
    if (!contractId || typeof contractId !== "string") {
      sendApiError(
        res,
        400,
        "VALIDATION_ERROR",
        "'contractId' is required and must be a string.",
      );
      return;
    }

    const request: RentEstimateRequest = {
      contractId,
      storageType: req.body?.storageType,
      entrySizeBytes: req.body?.entrySizeBytes,
      rentLedgers: req.body?.rentLedgers,
      storageKey: req.body?.storageKey,
    };

    const result = await sorobanRentEstimatorService.estimateRent(request);

    res.json({ success: true, data: result });
  } catch (error: any) {
    sendApiError(res, 500, "INTERNAL_SERVER_ERROR", error.message);
  }
});

/**
 * GET /api/v1/soroban/rent/storage-info/:contractId/:storageKey
 *
 * Read the current storage information for a contract data entry from the
 * ledger.
 *
 * @swagger
 * /api/v1/soroban/rent/storage-info/{contractId}/{storageKey}:
 *   get:
 *     tags: [Soroban Rent]
 *     summary: Get contract storage info
 *     description: >
 *       Reads the current storage state of a contract data entry and returns
 *       its size, TTL, and estimated rent paid.
 *     parameters:
 *       - in: path
 *         name: contractId
 *         required: true
 *         schema:
 *           type: string
 *         description: Contract ID (hex or C... address).
 *       - in: path
 *         name: storageKey
 *         required: true
 *         schema:
 *           type: string
 *         description: Hex-encoded ScVal storage key.
 *     responses:
 *       200:
 *         description: Contract storage information.
 */
router.get(
  "/storage-info/:contractId/:storageKey",
  async (req: Request, res: Response) => {
    try {
      const contractId = String(req.params.contractId ?? "");
      const storageKey = String(req.params.storageKey ?? "");

      if (!contractId || !storageKey) {
        sendApiError(
          res,
          400,
          "VALIDATION_ERROR",
          "Both 'contractId' and 'storageKey' are required.",
        );
        return;
      }

      const storageInfo =
        await sorobanRentEstimatorService.getContractStorageInfo(
          contractId,
          storageKey,
        );

      res.json({ success: true, data: storageInfo });
    } catch (error: any) {
      sendApiError(res, 500, "INTERNAL_SERVER_ERROR", error.message);
    }
  },
);

export default router;
