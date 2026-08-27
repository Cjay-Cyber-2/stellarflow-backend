import { Router } from "express";
import {
  listTimelocks,
  getTimelockById,
  getTimelockSummary,
  cancelTimelock,
} from "../controllers/timelockController.js";

const router = Router();

/**
 * @swagger
 * /api/v1/admin/timelocks:
 *   get:
 *     tags:
 *       - Admin
 *       - Timelocks
 *     summary: List timelock actions with ETA countdowns
 *     description: >
 *       Returns queued, executed, and cancelled timelock actions from the
 *       governance execution queue.  Queued entries include a detailed ETA
 *       countdown indicating when the transaction becomes releaseable.
 *       Supports filtering by status, target contract address, and action type.
 *     parameters:
 *       - in: query
 *         name: status
 *         schema:
 *           type: string
 *           enum: [Queued, Executed, Cancelled]
 *         description: Filter by timelock action status.
 *       - in: query
 *         name: contractId
 *         schema:
 *           type: string
 *         description: Filter by target contract address.
 *       - in: query
 *         name: actionType
 *         schema:
 *           type: string
 *         description: Filter by action type (e.g. HALT, UPGRADE, PRICE_UPDATE).
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           default: 50
 *           minimum: 1
 *           maximum: 200
 *         description: Maximum number of entries to return.
 *       - in: query
 *         name: offset
 *         schema:
 *           type: integer
 *           default: 0
 *           minimum: 0
 *         description: Pagination offset.
 *     responses:
 *       '200':
 *         description: Timelock actions returned successfully
 *       '400':
 *         description: Invalid filter parameters
 *       '500':
 *         description: Internal server error
 */
router.get("/", listTimelocks);

/**
 * @swagger
 * /api/v1/admin/timelocks/summary:
 *   get:
 *     tags:
 *       - Admin
 *       - Timelocks
 *     summary: Get timelock queue status summary
 *     description: >
 *       Returns aggregate counts of queued, executed, and cancelled
 *       governance timelock actions.
 *     responses:
 *       '200':
 *         description: Summary retrieved successfully
 *       '500':
 *         description: Internal server error
 */
router.get("/summary", getTimelockSummary);

/**
 * @swagger
 * /api/v1/admin/timelocks/{id}:
 *   get:
 *     tags:
 *       - Admin
 *       - Timelocks
 *     summary: Get a single timelock action by ID
 *     description: >
 *       Returns detailed information about a specific timelock action,
 *       including ETA countdown for queued entries.
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: The timelock action ID.
 *     responses:
 *       '200':
 *         description: Timelock action retrieved successfully
 *       '400':
 *         description: Invalid ID
 *       '404':
 *         description: Timelock action not found
 *       '500':
 *         description: Internal server error
 */
router.get("/:id", getTimelockById);

/**
 * @swagger
 * /api/v1/admin/timelocks/{id}/cancel:
 *   post:
 *     tags:
 *       - Admin
 *       - Timelocks
 *     summary: Cancel a queued timelock action
 *     description: >
 *       Cancels a queued governance timelock action, preventing it from
 *       being executed when the timelock expires.  Only actions in
 *       "Queued" status can be cancelled.
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: integer
 *         description: The timelock action ID.
 *     responses:
 *       '200':
 *         description: Timelock action cancelled successfully
 *       '400':
 *         description: Invalid ID
 *       '404':
 *         description: Timelock action not found or not cancellable
 *       '500':
 *         description: Internal server error
 */
router.post("/:id/cancel", cancelTimelock);

export default router;
