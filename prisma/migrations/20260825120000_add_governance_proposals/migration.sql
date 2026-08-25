CREATE TABLE "GovernanceProposal" (
  "id" SERIAL NOT NULL,
  "proposalId" TEXT NOT NULL,
  "contractId" TEXT NOT NULL,
  "expiresAt" TIMESTAMP(3) NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'Queued',
  "transactionHash" TEXT,
  "executedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "GovernanceProposal_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "GovernanceProposal_proposalId_key" ON "GovernanceProposal"("proposalId");
CREATE INDEX "GovernanceProposal_status_expiresAt_idx" ON "GovernanceProposal"("status", "expiresAt");