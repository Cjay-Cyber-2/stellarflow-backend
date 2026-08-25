CREATE TABLE "OpenOrder" (
    "id" TEXT NOT NULL,
    "orderId" TEXT NOT NULL,
    "totalAmount" DECIMAL(38,18) NOT NULL,
    "filledAmount" DECIMAL(38,18) NOT NULL DEFAULT 0,
    "status" TEXT NOT NULL DEFAULT 'open',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    CONSTRAINT "OpenOrder_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "OpenOrder_orderId_key" ON "OpenOrder"("orderId");
CREATE INDEX "OpenOrder_status_idx" ON "OpenOrder"("status");

CREATE TABLE "OrderFilledEvent" (
    "id" TEXT NOT NULL,
    "orderId" TEXT NOT NULL,
    "fillAmount" DECIMAL(38,18) NOT NULL,
    "txHash" TEXT NOT NULL,
    "ledgerSeq" INTEGER NOT NULL,
    "eventIndex" INTEGER NOT NULL,
    "payload" JSONB NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "OrderFilledEvent_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "OrderFilledEvent_txHash_eventIndex_key" ON "OrderFilledEvent"("txHash", "eventIndex");
CREATE INDEX "OrderFilledEvent_orderId_ledgerSeq_idx" ON "OrderFilledEvent"("orderId", "ledgerSeq");