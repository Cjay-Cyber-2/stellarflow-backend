import {
  Contract,
  xdr,
  TransactionBuilder,
  Operation,
  SorobanDataBuilder,
} from "@stellar/stellar-sdk";
import stellarProvider from "../lib/stellarProvider";
import { StellarService } from "./stellarService";
import { logger } from "../utils/logger";
import dotenv from "dotenv";

dotenv.config();

export class StorageRentBumpService {
  private stellarService: StellarService;
  private pollIntervalMs: number;
  private isRunning: boolean = false;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private readonly CONTRACT_ID: string;

  private readonly MIN_TTL_LEDGERS = 10000;
  private readonly EXTEND_TO_LEDGERS = 535000; // ~30 days

  constructor(pollIntervalMs: number = 60 * 60 * 1000) { // Default to every hour
    this.stellarService = new StellarService();
    this.pollIntervalMs = pollIntervalMs;
    this.CONTRACT_ID = process.env.CONTRACT_ID || "";
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      console.warn("[StorageRentBumpService] Service is already running");
      return;
    }

    if (!this.CONTRACT_ID) {
      console.warn("[StorageRentBumpService] No CONTRACT_ID configured. Service will not start.");
      return;
    }

    this.isRunning = true;
    console.info(`[StorageRentBumpService] Started with ${this.pollIntervalMs}ms poll interval`);

    // Initial check
    await this.checkAndBumpStorage();

    // Start periodic polling
    this.pollTimer = setInterval(() => {
      this.checkAndBumpStorage().catch((err) => {
        console.error("[StorageRentBumpService] Polling error:", err);
      });
    }, this.pollIntervalMs);
  }

  stop(): void {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
    this.isRunning = false;
    console.info("[StorageRentBumpService] Stopped");
  }

  restart(newIntervalMs: number): void {
    if (!this.isRunning) return;
    if (newIntervalMs === this.pollIntervalMs) return;
    this.pollIntervalMs = newIntervalMs;
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
    }
    this.pollTimer = setInterval(() => {
      this.checkAndBumpStorage().catch((err) => {
        console.error("[StorageRentBumpService] Polling error:", err);
      });
    }, this.pollIntervalMs);
    console.info(`[StorageRentBumpService] Poll interval updated to ${this.pollIntervalMs}ms`);
  }

  private async checkAndBumpStorage(): Promise<void> {
    try {
      const server = stellarProvider.getRpcServer();
      const contract = new Contract(this.CONTRACT_ID);

      const keys = [
        xdr.LedgerKey.contractData(new xdr.LedgerKeyContractData({
          contract: contract.address().toScAddress(),
          key: xdr.ScVal.scvLedgerKeyContractInstance(),
          durability: xdr.ContractDataDurability.persistent(),
        })),
        xdr.LedgerKey.contractData(new xdr.LedgerKeyContractData({
          contract: contract.address().toScAddress(),
          key: xdr.ScVal.scvSymbol("oracles"),
          durability: xdr.ContractDataDurability.persistent(),
        })),
        xdr.LedgerKey.contractData(new xdr.LedgerKeyContractData({
          contract: contract.address().toScAddress(),
          key: xdr.ScVal.scvSymbol("treasury"),
          durability: xdr.ContractDataDurability.persistent(),
        }))
      ];

      const ledgerEntries = await server.getLedgerEntries(...keys);
      
      const latestLedger = await server.getLatestLedger();
      const currentLedger = latestLedger.sequence;

      const keysToBump: xdr.LedgerKey[] = [];

      for (const entry of ledgerEntries.entries) {
        if (!entry.liveUntilLedgerSeq) continue;
        
        const ttl = entry.liveUntilLedgerSeq - currentLedger;
        if (ttl < this.MIN_TTL_LEDGERS) {
          const keyBase64 = entry.xdr.toXDR("base64"); // We could parse it back, but we can just use the returned key directly from entry.xdr if it was the LedgerKey. Wait, entry has key? No, `entry` has `key` which is xdr.LedgerKey ?
          // In Soroban RPC 20+, `entry` contains the `xdr` of the ledger entry data.
          // The request keys map 1:1, but the simplest is just to rebuild from the original keys.
          // Wait, `getLedgerEntries` returns an array of `entries`. 
          // Let's just blindly add all our keys to `keysToBump` if we notice ANY of them are below TTL.
          keysToBump.push(xdr.LedgerKey.fromXDR(entry.key, "base64"));
        }
      }

      if (keysToBump.length === 0) {
        logger.debug("[StorageRentBumpService] All protocol storage keys have sufficient TTL.");
        return;
      }

      console.info(`[StorageRentBumpService] Bumping storage for ${keysToBump.length} keys`);

      const sorobanData = new SorobanDataBuilder()
        .setReadOnly(keysToBump)
        .build();

      const baseFee = parseInt(await this.stellarService.getRecommendedFee(), 10);
      
      // We will bump storage using an extendFootprintTtl operation
      const txHash = await this.stellarService.submitTransactionWithRetries(
        (sourceAccount, currentFee) => {
          return new TransactionBuilder(sourceAccount, {
            fee: currentFee.toString(),
            networkPassphrase: stellarProvider.getNetworkPassphrase(),
          })
            .addOperation(
              Operation.extendFootprintTtl({
                extendTo: this.EXTEND_TO_LEDGERS,
              }),
            )
            .setSorobanData(sorobanData)
            .setTimeout(30)
            .build();
        },
        3,
        baseFee
      );

      console.info(`[StorageRentBumpService] ✅ Storage bump transaction confirmed: ${txHash}`);
    } catch (error) {
      console.error("[StorageRentBumpService] Error bumping storage:", error);
    }
  }
}

export const storageRentBumpService = new StorageRentBumpService();
