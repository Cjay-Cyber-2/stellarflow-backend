/**
 * VoterHistoryService
 *
 * Provides three data-access methods for the governance voter profile endpoint:
 *   - getVoteHistory      – paginated past votes with proposal details
 *   - getDelegationTree   – recursive inbound / outbound delegation graph (CTE)
 *   - getWeightTrend      – per-day average voting weight over a rolling window
 *
 * Also exports `ingestGovernanceVoteEvent` which is called by SorobanEventListener
 * to write on-chain GovernanceVoted events into PostgreSQL.
 */

import prisma from "../lib/prisma.js";
import { logger } from "../utils/logger.js";

// ─── Types ────────────────────────────────────────────────────────────────────

export interface VoteHistoryOptions {
  from?: Date;
  to?: Date;
  /** Max rows to return (already validated: 1–200) */
  limit: number;
  /** Cursor-based pagination: last vote id from the previous page */
  cursor?: number;
}

export interface VoteRecord {
  voteId: number;
  proposalId: string;
  proposalTitle: string | null;
  proposalStatus: string;
  choice: string;
  weight: string;
  votedAt: string;
  txHash: string | null;
}

export interface VoteHistoryResult {
  total: number;
  nextCursor: number | null;
  items: VoteRecord[];
}

export interface DelegationNode {
  delegator: string;
  delegate: string;
  weight: string;
  depth: number;
  expiresAt: string | null;
}

export interface DelegationTreeResult {
  outbound: DelegationNode[];
  inbound: DelegationNode[];
  totalDelegatedInboundWeight: string;
}

export interface WeightTrendPoint {
  date: string;
  avgWeight: string;
  voteCount: number;
}

/** Payload written by SorobanEventListener for each GovernanceVoted event */
export interface GovernanceVoteEventPayload {
  accountId: string;
  proposalId: string;
  choice: string;
  weight: string;
  txHash: string | null;
  votedAt: Date;
}

// ─── Ingestion (called by SorobanEventListener) ───────────────────────────────

/**
 * Upserts a GovernanceVote row from a Soroban event.
 * Silently skips the row if the referenced proposal doesn't exist yet — the
 * proposal record should be created by a separate proposal-tracking flow.
 */
export async function ingestGovernanceVoteEvent(
  payload: GovernanceVoteEventPayload,
): Promise<void> {
  const { accountId, proposalId, choice, weight, txHash, votedAt } = payload;

  // Ensure the proposal exists (create a stub if not yet tracked)
  await prisma.governanceProposal.upsert({
    where: { proposalId },
    create: {
      proposalId,
      contractId: process.env.GOVERNANCE_CONTRACT_ID ?? process.env.CONTRACT_ID ?? "",
      status: "Pending",
      expiresAt: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000), // 7 day default
    },
    update: {},
  });

  await prisma.governanceVote.upsert({
    where: { accountId_proposalId: { accountId, proposalId } },
    create: { accountId, proposalId, choice, weight, txHash, votedAt },
    update: { choice, weight, txHash, votedAt },
  });

  logger.info(
    `[VoterHistoryService] Ingested vote: account=${accountId} proposal=${proposalId} choice=${choice}`,
  );
}

// ─── Query: Vote History ──────────────────────────────────────────────────────

export async function getVoteHistory(
  accountId: string,
  options: VoteHistoryOptions,
): Promise<VoteHistoryResult> {
  const { from, to, limit, cursor } = options;

  const where: Record<string, unknown> = { accountId };
  if (from || to) {
    where.votedAt = {
      ...(from ? { gte: from } : {}),
      ...(to   ? { lte: to  } : {}),
    };
  }
  if (cursor) {
    // Keyset pagination: return rows with id < cursor (desc order)
    where.id = { lt: cursor };
  }

  const [total, rows] = await Promise.all([
    prisma.governanceVote.count({ where: { accountId } }),
    prisma.governanceVote.findMany({
      where,
      orderBy: { id: "desc" },
      take: limit + 1, // fetch one extra to detect next page
      include: {
        proposal: {
          select: { title: true, status: true },
        },
      },
    }),
  ]);

  const hasNextPage = rows.length > limit;
  if (hasNextPage) rows.pop();

  const items: VoteRecord[] = rows.map((v: any) => ({
    voteId:          v.id,
    proposalId:      v.proposalId,
    proposalTitle:   v.proposal?.title ?? null,
    proposalStatus:  v.proposal?.status ?? "Unknown",
    choice:          v.choice,
    weight:          v.weight.toString(),
    votedAt:         v.votedAt.toISOString(),
    txHash:          v.txHash ?? null,
  }));

  return {
    total,
    nextCursor: hasNextPage ? (rows[rows.length - 1] as any).id : null,
    items,
  };
}

// ─── Query: Delegation Tree (Recursive CTE) ───────────────────────────────────

/**
 * Walks the GovernanceDelegation table in both directions using a PostgreSQL
 * recursive CTE. Depth is capped at 10 to prevent infinite loops from cycles.
 */
export async function getDelegationTree(
  accountId: string,
): Promise<DelegationTreeResult> {
  type RawRow = {
    direction: "outbound" | "inbound";
    delegator: string;
    delegate: string;
    weight: string;
    depth: number;
    expires_at: Date | null;
  };

  const rows = await prisma.$queryRaw<RawRow[]>`
    WITH RECURSIVE outbound AS (
      SELECT
        'outbound'::text      AS direction,
        "delegator",
        "delegate",
        "weight"::text        AS weight,
        1                     AS depth,
        "expiresAt"           AS expires_at
      FROM "GovernanceDelegation"
      WHERE "delegator" = ${accountId}
        AND "isActive" = true
        AND ("expiresAt" IS NULL OR "expiresAt" > NOW())
      UNION ALL
      SELECT
        'outbound'::text,
        d."delegator",
        d."delegate",
        d."weight"::text,
        o.depth + 1,
        d."expiresAt"
      FROM "GovernanceDelegation" d
      JOIN outbound o ON d."delegator" = o."delegate"
      WHERE d."isActive" = true
        AND (d."expiresAt" IS NULL OR d."expiresAt" > NOW())
        AND o.depth < 10
    ),
    inbound AS (
      SELECT
        'inbound'::text       AS direction,
        "delegator",
        "delegate",
        "weight"::text        AS weight,
        1                     AS depth,
        "expiresAt"           AS expires_at
      FROM "GovernanceDelegation"
      WHERE "delegate" = ${accountId}
        AND "isActive" = true
        AND ("expiresAt" IS NULL OR "expiresAt" > NOW())
      UNION ALL
      SELECT
        'inbound'::text,
        d."delegator",
        d."delegate",
        d."weight"::text,
        i.depth + 1,
        d."expiresAt"
      FROM "GovernanceDelegation" d
      JOIN inbound i ON d."delegate" = i."delegator"
      WHERE d."isActive" = true
        AND (d."expiresAt" IS NULL OR d."expiresAt" > NOW())
        AND i.depth < 10
    )
    SELECT * FROM outbound
    UNION ALL
    SELECT * FROM inbound;
  `;

  const outbound: DelegationNode[] = [];
  const inbound:  DelegationNode[] = [];
  let totalInboundWeight = 0;

  for (const row of rows) {
    const node: DelegationNode = {
      delegator:  row.delegator,
      delegate:   row.delegate,
      weight:     row.weight,
      depth:      Number(row.depth),
      expiresAt:  row.expires_at ? row.expires_at.toISOString() : null,
    };
    if (row.direction === "outbound") {
      outbound.push(node);
    } else {
      inbound.push(node);
      // Sum direct-depth-1 inbound weights only (avoid double-counting)
      if (node.depth === 1) totalInboundWeight += parseFloat(row.weight);
    }
  }

  return {
    outbound,
    inbound,
    totalDelegatedInboundWeight: totalInboundWeight.toFixed(7),
  };
}

// ─── Query: Weight Trend ──────────────────────────────────────────────────────

/**
 * Returns per-day average voting weight for `accountId` over the last `days`.
 * Reads from the `governance_voter_weight_trend` view (see governance_view.sql).
 */
export async function getWeightTrend(
  accountId: string,
  days: number,
): Promise<WeightTrendPoint[]> {
  type TrendRow = { vote_day: Date; avg_weight: string; vote_count: bigint };

  const since = new Date();
  since.setDate(since.getDate() - days);

  const rows = await prisma.$queryRaw<TrendRow[]>`
    SELECT vote_day, avg_weight, vote_count
    FROM governance_voter_weight_trend
    WHERE "accountId" = ${accountId}
      AND vote_day >= ${since}
    ORDER BY vote_day ASC;
  `;

  return rows.map((r) => ({
    date:       r.vote_day.toISOString().split("T")[0]!,
    avgWeight:  r.avg_weight,
    voteCount:  Number(r.vote_count),
  }));
}
