CREATE TYPE "RebalancingSwapStatus" AS ENUM (
  'QUEUED',
  'PROCESSING',
  'COMPLETED',
  'FAILED',
  'CANCELLED'
);

CREATE TABLE "RebalancingSwap" (
  "id" TEXT NOT NULL,
  "poolKey" VARCHAR(100) NOT NULL,
  "anchorAccount" VARCHAR(56) NOT NULL,
  "fromCurrency" VARCHAR(12) NOT NULL,
  "toCurrency" VARCHAR(12) NOT NULL,
  "fromAmount" DECIMAL(30,7) NOT NULL,
  "estimatedToAmount" DECIMAL(30,7) NOT NULL,
  "normalizedVolume" DECIMAL(30,7) NOT NULL,
  "fromReserveRatio" DECIMAL(8,7) NOT NULL,
  "toReserveRatio" DECIMAL(8,7) NOT NULL,
  "status" "RebalancingSwapStatus" NOT NULL DEFAULT 'QUEUED',
  "managerAccounts" TEXT[] NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "RebalancingSwap_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "RebalancingSwap_poolKey_status_idx"
  ON "RebalancingSwap"("poolKey", "status");

CREATE INDEX "RebalancingSwap_status_createdAt_idx"
  ON "RebalancingSwap"("status", "createdAt");

-- Prevent separate application instances from queuing the same pool twice.
CREATE UNIQUE INDEX "RebalancingSwap_one_pending_per_pool_idx"
  ON "RebalancingSwap"("poolKey")
  WHERE "status" IN ('QUEUED', 'PROCESSING');
