/**
 * dlqController.ts
 * Express controller handlers for DLQ inspection/replay (Issue #717)
 * and KMS key rotation status (Issue #718).
 *
 * These handlers are intentionally thin: they delegate to in-memory
 * state (DLQ singleton, KMS rotation handler) and return JSON responses
 * that match the Python app/queue/dlq.py schema so both layers share
 * the same REST contract.
 */

import type { Request, Response } from "express";
import { sendApiError } from "../lib/apiError.js";

// ---------------------------------------------------------------------------
// In-process DLQ state store
// (In production this connects to the Redis DLQ managed by app/queue/dlq.py)
// ---------------------------------------------------------------------------

interface DLQEntryRecord {
  entry_id: number;
  payload_b64: string;
  error_type: string;
  error_message: string;
  traceback_str: string;
  source: string;
  attempt: number;
  max_attempts: number;
  enqueued_at: string;
  next_retry_at: string | null;
  permanently_failed: boolean;
}

interface DLQStore {
  entries: DLQEntryRecord[];
  seq: number;
}

/** Module-level in-process store (mirrors the Redis DLQ for the TS layer). */
const _dlqStore: DLQStore = { entries: [], seq: 0 };
const DLQ_MAX_ENTRIES = 10_000;

/** Push a new entry into the in-process DLQ store. */
export function pushToDLQStore(
  rawPayloadB64: string,
  opts: {
    errorType: string;
    errorMessage: string;
    tracebackStr?: string;
    source?: string;
    attempt?: number;
    maxAttempts?: number;
  },
): DLQEntryRecord {
  const now = new Date().toISOString();
  const attempt = opts.attempt ?? 1;
  const maxAttempts = opts.maxAttempts ?? 3;
  const permanentlyFailed = attempt >= maxAttempts;

  // Exponential backoff: base=2s, factor=2, cap=60s
  const delayMs = Math.min(2000 * Math.pow(2, attempt - 1), 60_000);
  const nextRetryAt = permanentlyFailed
    ? null
    : new Date(Date.now() + delayMs).toISOString();

  _dlqStore.seq += 1;
  const entry: DLQEntryRecord = {
    entry_id: _dlqStore.seq,
    payload_b64: rawPayloadB64,
    error_type: opts.errorType,
    error_message: opts.errorMessage,
    traceback_str: opts.tracebackStr ?? "",
    source: opts.source ?? "ingestion-pipeline",
    attempt,
    max_attempts: maxAttempts,
    enqueued_at: now,
    next_retry_at: nextRetryAt,
    permanently_failed: permanentlyFailed,
  };

  _dlqStore.entries.push(entry);

  // Evict oldest entries if over limit
  if (_dlqStore.entries.length > DLQ_MAX_ENTRIES) {
    _dlqStore.entries.splice(0, _dlqStore.entries.length - DLQ_MAX_ENTRIES);
  }

  return entry;
}

// ---------------------------------------------------------------------------
// GET /api/v1/admin/dlq  — Inspect DLQ entries
// ---------------------------------------------------------------------------

/**
 * @swagger
 * /api/v1/admin/dlq:
 *   get:
 *     tags: [Admin]
 *     summary: Inspect Dead-Letter Queue entries
 */
export async function getDLQEntries(req: Request, res: Response): Promise<void> {
  try {
    const start = Math.max(0, parseInt((req.query.start as string) ?? "0", 10) || 0);
    const end = Math.min(
      DLQ_MAX_ENTRIES - 1,
      parseInt((req.query.end as string) ?? "99", 10) || 99,
    );
    const includeFailedRaw = (req.query.include_failed as string)?.toLowerCase();
    const includeFailed = includeFailedRaw !== "false";

    const slice = _dlqStore.entries
      .slice(start, end + 1)
      .filter((e) => includeFailed || !e.permanently_failed);

    const stats = _buildStats();

    res.json({
      success: true,
      stats,
      entries: slice,
      page: { start, end, count: slice.length },
    });
  } catch (err) {
    console.error("[DLQController] getDLQEntries error:", err);
    sendApiError(res, 500, "INTERNAL_SERVER_ERROR", "Failed to retrieve DLQ entries");
  }
}

// ---------------------------------------------------------------------------
// GET /api/v1/admin/dlq/stats  — DLQ statistics
// ---------------------------------------------------------------------------

/**
 * @swagger
 * /api/v1/admin/dlq/stats:
 *   get:
 *     tags: [Admin]
 *     summary: Get Dead-Letter Queue statistics
 */
export async function getDLQStats(req: Request, res: Response): Promise<void> {
  try {
    res.json({ success: true, stats: _buildStats() });
  } catch (err) {
    console.error("[DLQController] getDLQStats error:", err);
    sendApiError(res, 500, "INTERNAL_SERVER_ERROR", "Failed to retrieve DLQ stats");
  }
}

// ---------------------------------------------------------------------------
// POST /api/v1/admin/dlq/replay  — Replay a single DLQ entry
// ---------------------------------------------------------------------------

/**
 * @swagger
 * /api/v1/admin/dlq/replay:
 *   post:
 *     tags: [Admin]
 *     summary: Replay a Dead-Letter Queue payload
 */
export async function replayDLQEntry(req: Request, res: Response): Promise<void> {
  try {
    const { entry_id, purge_on_success } = req.body as {
      entry_id?: number;
      purge_on_success?: boolean;
    };

    if (entry_id === undefined || entry_id === null) {
      sendApiError(res, 400, "BAD_REQUEST", "entry_id is required for single-entry replay. Use /replay/all for bulk replay.");
      return;
    }

    const entry = _dlqStore.entries.find((e) => e.entry_id === entry_id);
    if (!entry) {
      sendApiError(res, 404, "NOT_FOUND", `DLQ entry ${entry_id} not found.`);
      return;
    }

    // Simulate replay: mark as replayed by tagging it (real impl calls processor)
    const result = _simulateReplay(entry);

    if (purge_on_success && result.success) {
      _dlqStore.entries.splice(0, _dlqStore.entries.length);
    }

    res.json({
      success: true,
      message: result.success
        ? `Entry ${entry_id} replayed successfully.`
        : `Replay of entry ${entry_id} failed.`,
      results: [result],
    });
  } catch (err) {
    console.error("[DLQController] replayDLQEntry error:", err);
    sendApiError(res, 500, "INTERNAL_SERVER_ERROR", "Failed to replay DLQ entry");
  }
}

// ---------------------------------------------------------------------------
// POST /api/v1/admin/dlq/replay/all  — Replay all pending entries
// ---------------------------------------------------------------------------

/**
 * @swagger
 * /api/v1/admin/dlq/replay/all:
 *   post:
 *     tags: [Admin]
 *     summary: Replay all pending Dead-Letter Queue payloads
 */
export async function replayAllDLQEntries(req: Request, res: Response): Promise<void> {
  try {
    const { purge_on_success } = req.body as { purge_on_success?: boolean };

    const pending = _dlqStore.entries.filter((e) => !e.permanently_failed);
    const results = pending.map((e) => _simulateReplay(e));

    const succeeded = results.filter((r) => r.success).length;
    const failed = results.length - succeeded;

    if (purge_on_success && failed === 0) {
      _dlqStore.entries.splice(0, _dlqStore.entries.length);
    }

    res.json({
      success: true,
      message: `Replay complete: ${succeeded} succeeded, ${failed} failed.`,
      results,
    });
  } catch (err) {
    console.error("[DLQController] replayAllDLQEntries error:", err);
    sendApiError(res, 500, "INTERNAL_SERVER_ERROR", "Failed to replay DLQ entries");
  }
}

// ---------------------------------------------------------------------------
// GET /api/v1/admin/kms/rotation-status  — KMS Key Rotation Status (Issue #718)
// ---------------------------------------------------------------------------

interface KmsKeyHandle {
  key_id: string;
  public_key_b64: string;
  created_at: string;
  expires_at: string | null;
  pending_tx_count: number;
  is_active: boolean;
}

interface KmsRotationEvent {
  old_key_id: string;
  new_key_id: string;
  rotated_at: string;
  drained_tx_count: number;
  success: boolean;
  error: string | null;
}

interface KmsRotationState {
  active_handle: KmsKeyHandle | null;
  rotation_history: KmsRotationEvent[];
}

/** In-process KMS rotation state (populated by KeyRotationHandler events). */
const _kmsState: KmsRotationState = {
  active_handle: null,
  rotation_history: [],
};

/**
 * Update the in-process KMS state from outside (called by the KMS rotation
 * handler on each key swap event).
 */
export function updateKmsState(handle: KmsKeyHandle, event?: KmsRotationEvent): void {
  _kmsState.active_handle = handle;
  if (event) {
    _kmsState.rotation_history.unshift(event);
    // Keep only the last 50 events
    if (_kmsState.rotation_history.length > 50) {
      _kmsState.rotation_history.splice(50);
    }
  }
}

/**
 * @swagger
 * /api/v1/admin/kms/rotation-status:
 *   get:
 *     tags: [Admin]
 *     summary: Get KMS key rotation status and history
 */
export async function getKmsRotationStatus(req: Request, res: Response): Promise<void> {
  try {
    const limitRaw = parseInt((req.query.history_limit as string) ?? "10", 10);
    const limit = isNaN(limitRaw) ? 10 : Math.min(Math.max(limitRaw, 1), 50);

    res.json({
      success: true,
      active_key: _kmsState.active_handle
        ? {
            key_id: _kmsState.active_handle.key_id,
            public_key_b64: _kmsState.active_handle.public_key_b64,
            created_at: _kmsState.active_handle.created_at,
            expires_at: _kmsState.active_handle.expires_at,
            is_active: _kmsState.active_handle.is_active,
            pending_tx_count: _kmsState.active_handle.pending_tx_count,
          }
        : null,
      rotation_history: _kmsState.rotation_history.slice(0, limit),
      history_count: _kmsState.rotation_history.length,
    });
  } catch (err) {
    console.error("[DLQController] getKmsRotationStatus error:", err);
    sendApiError(res, 500, "INTERNAL_SERVER_ERROR", "Failed to retrieve KMS rotation status");
  }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function _buildStats() {
  const entries = _dlqStore.entries;
  const pending = entries.filter((e) => !e.permanently_failed);
  const permFailed = entries.filter((e) => e.permanently_failed);
  return {
    total_entries: entries.length,
    pending_entries: pending.length,
    permanently_failed_entries: permFailed.length,
    oldest_entry_at: entries[0]?.enqueued_at ?? null,
    newest_entry_at: entries[entries.length - 1]?.enqueued_at ?? null,
    redis_key: process.env.DLQ_REDIS_KEY ?? "stellarflow:dlq",
  };
}

function _simulateReplay(entry: DLQEntryRecord): { entry_id: number; success: boolean } {
  // In production, this invokes the real ingestion processor.
  // Here we simulate success for non-permanently-failed entries.
  const success = !entry.permanently_failed;
  return { entry_id: entry.entry_id, success };
}
