/**
 * Governance Proposals Routes
 *
 * GET /api/v1/governance/proposals        – list all governance proposals (filterable by status)
 * GET /api/v1/governance/proposals/:id   – fetch a single proposal with its raw timelock events
 */
import { Router, Request, Response } from "express";
import prisma from "../lib/prisma";
import { sendApiError } from "../lib/apiError";

const router = Router();

/**
 * @swagger
 * /api/v1/governance/proposals:
 *   get:
 *     tags:
 *       - Governance
 *     summary: List governance proposals
 *     description: >
 *       Returns all indexed governance proposals.
 *       Use the `status` query parameter to filter by proposal status
 *       (Queued | Executed | Cancelled).
 *     parameters:
 *       - in: query
 *         name: status
 *         schema:
 *           type: string
 *           enum: [Queued, Executed, Cancelled]
 *         description: Filter proposals by status
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           default: 50
 *           maximum: 200
 *         description: Maximum number of proposals to return
 *       - in: query
 *         name: offset
 *         schema:
 *           type: integer
 *           default: 0
 *         description: Pagination offset
 *     responses:
 *       '200':
 *         description: List of governance proposals
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 total:
 *                   type: integer
 *                 data:
 *                   type: array
 *                   items:
 *                     $ref: '#/components/schemas/GovernanceProposal'
 *       '500':
 *         description: Internal server error
 */
router.get("/proposals", async (req: Request, res: Response) => {
  try {
    const rawStatus = req.query["status"];
    const status =
      typeof rawStatus === "string" && rawStatus.length > 0
        ? rawStatus
        : undefined;

    const limit = Math.min(Number(req.query["limit"] ?? 50), 200);
    const offset = Math.max(Number(req.query["offset"] ?? 0), 0);

    const where = status ? { status } : {};

    const [total, proposals] = await Promise.all([
      prisma.governanceProposal.count({ where }),
      prisma.governanceProposal.findMany({
        where,
        orderBy: { expiresAt: "asc" },
        take: limit,
        skip: offset,
        select: {
          id: true,
          proposalId: true,
          contractId: true,
          status: true,
          expiresAt: true,
          queuedAt: true,
          timelockActionSource: true,
          notificationCount: true,
          executionReadyNotifiedAt: true,
          transactionHash: true,
          executedAt: true,
          createdAt: true,
          updatedAt: true,
        },
      }),
    ]);

    res.json({ success: true, total, data: proposals });
  } catch (err) {
    console.error("[GovernanceRoute] Failed to list proposals:", err);
    sendApiError(res, 500, "INTERNAL_SERVER_ERROR");
  }
});

/**
 * @swagger
 * /api/v1/governance/proposals/{id}:
 *   get:
 *     tags:
 *       - Governance
 *     summary: Get a single governance proposal
 *     description: >
 *       Returns a governance proposal record including all raw indexed
 *       TimelockEvents (ProposalQueued and TimelockActionExecuted) associated
 *       with this proposal.
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: The proposalId (string, as stored on-chain)
 *     responses:
 *       '200':
 *         description: Governance proposal with associated timelock events
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 success:
 *                   type: boolean
 *                 data:
 *                   $ref: '#/components/schemas/GovernanceProposalDetail'
 *       '404':
 *         description: Proposal not found
 *       '500':
 *         description: Internal server error
 */
router.get("/proposals/:id", async (req: Request, res: Response) => {
  const { id } = req.params;

  if (!id || typeof id !== "string" || id.trim().length === 0) {
    sendApiError(res, 400, "VALIDATION_ERROR");
    return;
  }

  try {
    const proposal = await prisma.governanceProposal.findUnique({
      where: { proposalId: id.trim() },
      include: {
        timelockEvents: {
          orderBy: { ledgerSeq: "asc" },
          select: {
            id: true,
            eventType: true,
            ledgerSeq: true,
            txHash: true,
            topics: true,
            value: true,
            indexedAt: true,
          },
        },
      },
    });

    if (!proposal) {
      sendApiError(res, 404, "NOT_FOUND");
      return;
    }

    res.json({ success: true, data: proposal });
  } catch (err) {
    console.error("[GovernanceRoute] Failed to fetch proposal:", err);
    sendApiError(res, 500, "INTERNAL_SERVER_ERROR");
  }
});

export default router;
