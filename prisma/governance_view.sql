-- Migration: governance_voter_weight_trend view
-- Run after the Prisma migration that creates GovernanceVote

CREATE OR REPLACE VIEW governance_voter_weight_trend AS
SELECT
  "accountId",
  DATE_TRUNC('day', "votedAt")         AS vote_day,
  AVG("weight")::NUMERIC(20, 7)        AS avg_weight,
  COUNT(*)                              AS vote_count
FROM "GovernanceVote"
GROUP BY "accountId", DATE_TRUNC('day', "votedAt");
