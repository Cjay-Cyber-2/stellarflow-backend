import prisma from "../lib/prisma";
import stellarProvider from "../lib/stellarProvider";
import { StellarService } from "./stellarService";

interface TimelockedProposal {
  id: number;
  proposalId: string;
  contractId: string;
  expiresAt: Date;
}

export class GovernanceTimelockService {
  private readonly pollIntervalMs: number;
  private readonly stellarService: StellarService;
  private timer: ReturnType<typeof setInterval> | null = null;
  private running = false;

  constructor(
    pollIntervalMs = Number(process.env.GOVERNANCE_POLL_INTERVAL_MS) || 15_000,
    stellarService = new StellarService(),
  ) {
    this.pollIntervalMs = pollIntervalMs;
    this.stellarService = stellarService;
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    await this.checkAndExecute();
    this.timer = setInterval(() => {
      this.checkAndExecute().catch((error) =>
        console.error("[GovernanceTimelockService] Polling failed:", error),
      );
    }, this.pollIntervalMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.running = false;
  }

  private async checkAndExecute(): Promise<void> {
    const contractId =
      process.env.GOVERNANCE_CONTRACT_ID || process.env.CONTRACT_ID;
    if (!contractId) return;

    const ledger = await stellarProvider
      .getServer()
      .ledgers()
      .order("desc")
      .limit(1)
      .call();
    const ledgerTimestamp = new Date(ledger.records[0]!.closed_at);
    const proposals = await prisma.$queryRaw<TimelockedProposal[]>`
      SELECT "id", "proposalId", "contractId", "expiresAt"
      FROM "GovernanceProposal"
      WHERE "status" = 'Queued' AND "expiresAt" <= ${ledgerTimestamp}
      ORDER BY "expiresAt" ASC
    `;

    for (const proposal of proposals) {
      try {
        const transactionHash =
          await this.stellarService.executeGovernanceProposal(
            proposal.contractId || contractId,
            proposal.proposalId,
          );
        await prisma.$executeRaw`
          UPDATE "GovernanceProposal"
          SET "status" = 'Executed', "transactionHash" = ${transactionHash}, "executedAt" = NOW(), "updatedAt" = NOW()
          WHERE "id" = ${proposal.id} AND "status" = 'Queued'
        `;
      } catch (error) {
        console.error(
          `[GovernanceTimelockService] Failed to execute proposal ${proposal.proposalId}:`,
          error,
        );
      }
    }
  }
}

export const governanceTimelockService = new GovernanceTimelockService();
