import { xdr } from "@stellar/stellar-sdk";
import stellarProvider from "../lib/stellarProvider";
import { logger } from "../utils/logger";

// ─── Constants ───────────────────────────────────────────────────────────────
// Source: https://developers.stellar.org/docs/learn/fundamentals/fees-resource-limits-metering

/** Cost per 10,000 CPU instructions in stroops. */
const FEE_RATE_PER_INSTRUCTION_INCREMENT = 25;
const INSTRUCTION_INCREMENT = 10_000;

/** Cost per read entry in stroops. */
const FEE_RATE_PER_READ_ENTRY = 6_250;

/** Cost per write entry in stroops. */
const FEE_RATE_PER_WRITE_ENTRY = 10_000;

/** Cost per KB of read bytes in stroops. */
const FEE_RATE_PER_READ_BYTE_KB = 1_786;

/** Cost per KB of write bytes in stroops. */
const FEE_RATE_PER_WRITE_BYTE_KB = 11_800;

/** Base size of a TTL ledger entry in bytes — minimum for rent calculation. */
const TTL_ENTRY_SIZE = 48;

/** Bytes per 1 KB. */
const BYTES_PER_KB = 1_024;

const STROOPS_PER_XLM = 10_000_000;

// ─── Types ───────────────────────────────────────────────────────────────────

export type StorageType = "temporary" | "persistent" | "instance";

export interface InstructionFeeEstimate {
  /** Raw CPU instructions consumed. */
  instructions: number;
  /** Fee in stroops. */
  feeStroops: number;
  /** Fee in XLM. */
  feeXlm: string;
}

export interface StorageRentEstimate {
  /** Storage type: temporary, persistent, or instance. */
  storageType: StorageType;
  /** Size of the ledger entry in bytes. */
  entrySizeBytes: number;
  /** Number of ledgers the entry is rented for. */
  rentLedgers: number;
  /** Total rent fee in stroops. */
  rentFeeStroops: number;
  /** Total rent fee in XLM. */
  rentFeeXlm: string;
  /** Duration of one ledger in seconds (approximate). */
  ledgerDurationSeconds: number;
  /** Approximate total rental duration in seconds. */
  estimatedDurationSeconds: number;
}

export interface EntryCostBreakdown {
  /** Number of entries read. */
  readEntries: number;
  /** Number of entries written. */
  writeEntries: number;
  /** Total bytes read. */
  readBytes: number;
  /** Total bytes written. */
  writeBytes: number;
  /** Read entry cost in stroops. */
  readEntryFeeStroops: number;
  /** Write entry cost in stroops. */
  writeEntryFeeStroops: number;
  /** Read bytes cost in stroops. */
  readBytesFeeStroops: number;
  /** Write bytes cost in stroops. */
  writeBytesFeeStroops: number;
  /** Total cost in stroops. */
  totalStroops: number;
  /** Total cost in XLM. */
  totalXlm: string;
}

export interface ContractStorageInfo {
  contractId: string;
  /** The key used to look up the data. */
  storageKey: string;
  /** Size in bytes of the entry. */
  entrySizeBytes: number;
  /** The TTL ledger number. */
  liveUntilLedgerSeq: number;
  /** Current ledger sequence. */
  currentLedgerSeq: number;
  /** Ledgers remaining before expiration. */
  ledgersUntilExpiration: number;
  /** Storage type. */
  storageType: StorageType;
  /** Estimated current rent paid (stroops). */
  currentRentFeeStroops: number;
  /** Estimated current rent paid (XLM). */
  currentRentFeeXlm: string;
}

export interface RentEstimateRequest {
  contractId: string;
  /** Storage type for rent calculation. Defaults to "persistent". */
  storageType?: StorageType;
  /** Entry size in bytes. If omitted, a default will be used. */
  entrySizeBytes?: number;
  /** Number of ledgers to estimate rent for. Defaults to 6307200 (~20 years at 5s/ledger). */
  rentLedgers?: number;
  /** Optional hex-encoded storage key to look up existing contract data. */
  storageKey?: string;
}

// ─── Service ─────────────────────────────────────────────────────────────────

export class SorobanRentEstimatorService {
  /** Approximate ledger close time in seconds. */
  private readonly ledgerCloseTimeSeconds = 5;

  // ─── Instruction Fee Estimation ──────────────────────────────────────────

  /**
   * Compute the non-refundable instruction fee for a given number of CPU
   * instructions.
   *
   * Formula: ceil(instructions / 10_000) * 25 stroops
   */
  computeInstructionFee(instructions: number): InstructionFeeEstimate {
    const increments = Math.ceil(instructions / INSTRUCTION_INCREMENT);
    const feeStroops = increments * FEE_RATE_PER_INSTRUCTION_INCREMENT;

    return {
      instructions,
      feeStroops,
      feeXlm: this.stroopsToXlm(feeStroops),
    };
  }

  // ─── Storage Rent Estimation ─────────────────────────────────────────────

  /**
   * Estimate rent for a contract data entry of a given size and duration.
   *
   * The rent formula for Soroban ledger entries:
   *   rentFee = ceil( max(entrySize, 48) * 11_800 * rentLedgers / (10_000 * 1_024) )
   *
   * This produces the refundable rent fee in stroops.
   */
  computeStorageRent(
    entrySizeBytes: number,
    rentLedgers: number,
    storageType: StorageType = "persistent",
  ): StorageRentEstimate {
    const effectiveSize = Math.max(entrySizeBytes, TTL_ENTRY_SIZE);

    // Soroban rent fee formula (stroops):
    // fee = ceil(effectiveSize * 11_800 * rentLedgers / (10_000 * 1_024))
    const rentFeeStroops = Math.ceil(
      (effectiveSize * FEE_RATE_PER_WRITE_BYTE_KB * rentLedgers) /
        (INSTRUCTION_INCREMENT * BYTES_PER_KB),
    );

    const estimatedDurationSeconds = rentLedgers * this.ledgerCloseTimeSeconds;

    return {
      storageType,
      entrySizeBytes,
      rentLedgers,
      rentFeeStroops,
      rentFeeXlm: this.stroopsToXlm(rentFeeStroops),
      ledgerDurationSeconds: this.ledgerCloseTimeSeconds,
      estimatedDurationSeconds,
    };
  }

  // ─── Entry Cost Breakdown ────────────────────────────────────────────────

  /**
   * Calculate a full cost breakdown for read/write entries and bytes.
   */
  computeEntryCosts(
    readEntries: number,
    writeEntries: number,
    readBytes: number,
    writeBytes: number,
  ): EntryCostBreakdown {
    const readEntryFeeStroops = readEntries * FEE_RATE_PER_READ_ENTRY;
    const writeEntryFeeStroops = writeEntries * FEE_RATE_PER_WRITE_ENTRY;

    const readBytesFeeStroops = Math.ceil(
      (readBytes * FEE_RATE_PER_READ_BYTE_KB) / BYTES_PER_KB,
    );
    const writeBytesFeeStroops = Math.ceil(
      (writeBytes * FEE_RATE_PER_WRITE_BYTE_KB) / BYTES_PER_KB,
    );

    const totalStroops =
      readEntryFeeStroops +
      writeEntryFeeStroops +
      readBytesFeeStroops +
      writeBytesFeeStroops;

    return {
      readEntries,
      writeEntries,
      readBytes,
      writeBytes,
      readEntryFeeStroops,
      writeEntryFeeStroops,
      readBytesFeeStroops,
      writeBytesFeeStroops,
      totalStroops,
      totalXlm: this.stroopsToXlm(totalStroops),
    };
  }

  // ─── RPC: Read Contract Storage Info ─────────────────────────────────────

  /**
   * Read contract data entry from the ledger and return its storage info.
   */
  async getContractStorageInfo(
    contractId: string,
    storageKeyHex: string,
  ): Promise<ContractStorageInfo> {
    const rpc = stellarProvider.getRpcServer();

    // Decode the hex-encoded key into an ScVal
    const keyBuffer = Buffer.from(storageKeyHex, "hex");
    const scVal = xdr.ScVal.fromXDR(keyBuffer);

    const ledgerEntry = await rpc.getContractData(contractId, scVal);
    const currentLedger = await rpc.getLatestLedger();

    const liveUntilLedgerSeq = ledgerEntry.liveUntilLedgerSeq ?? 0;
    const currentLedgerSeq = currentLedger.sequence;
    const ledgersUntilExpiration = Math.max(
      0,
      liveUntilLedgerSeq - currentLedgerSeq,
    );

    // Estimate entry size from the XDR
    const entryDataXdr = ledgerEntry.val.toXDR();
    const entrySizeBytes = entryDataXdr.length;

    // Estimate current rent fee
    const totalRentedLedgers = liveUntilLedgerSeq;
    const rent = this.computeStorageRent(
      entrySizeBytes,
      totalRentedLedgers,
      "persistent",
    );

    return {
      contractId,
      storageKey: storageKeyHex,
      entrySizeBytes,
      liveUntilLedgerSeq,
      currentLedgerSeq,
      ledgersUntilExpiration,
      storageType: "persistent",
      currentRentFeeStroops: rent.rentFeeStroops,
      currentRentFeeXlm: rent.rentFeeXlm,
    };
  }

  // ─── High-Level Gateway ──────────────────────────────────────────────────

  /**
   * Full rent estimation gateway: given a contract and storage parameters,
   * returns a complete cost estimate including instruction fees, storage rent,
   * and entry costs.
   */
  async estimateRent(request: RentEstimateRequest): Promise<{
    instructionEstimate: InstructionFeeEstimate;
    storageEstimate: StorageRentEstimate;
    entryCost: EntryCostBreakdown;
    storageInfo: ContractStorageInfo | null;
  }> {
    const storageType = request.storageType ?? "persistent";
    const rentLedgers = request.rentLedgers ?? 6_307_200; // ~20 years
    const entrySizeBytes = request.entrySizeBytes ?? 0;

    // Compute instruction fee estimate (baseline for a 100k instruction call)
    const instructionEstimate = this.computeInstructionFee(100_000);

    // Compute storage rent estimate
    const storageEstimate = this.computeStorageRent(
      entrySizeBytes || TTL_ENTRY_SIZE,
      rentLedgers,
      storageType,
    );

    // Compute entry cost breakdown for a typical read+write operation
    const entryCost = this.computeEntryCosts(1, 1, 256, 256);

    // Attempt to read existing storage info if a key is provided
    let storageInfo: ContractStorageInfo | null = null;

    if (request.storageKey) {
      try {
        storageInfo = await this.getContractStorageInfo(
          request.contractId,
          request.storageKey,
        );
      } catch (error: unknown) {
        const msg = error instanceof Error ? error.message : String(error);
        logger.warn(
          `[SorobanRentEstimator] Could not read contract storage for key ${request.storageKey}: ${msg}`,
        );
      }
    }

    return {
      instructionEstimate,
      storageEstimate,
      entryCost,
      storageInfo,
    };
  }

  // ─── Private Helpers ─────────────────────────────────────────────────────

  private stroopsToXlm(stroops: number): string {
    return (stroops / STROOPS_PER_XLM).toFixed(7);
  }
}

export const sorobanRentEstimatorService = new SorobanRentEstimatorService();
