import { createHash } from "crypto";
import { getRedisClient } from "../lib/redis";
import { createFetcherLogger } from "../utils/logger";

const NONCE_TTL_SECONDS = 300;

export class CryptographicNonceStore {
  private readonly logger = createFetcherLogger("CryptographicNonceStore");

  constructor(private readonly ttlSeconds: number = NONCE_TTL_SECONDS) {}

  private normalizeClientId(clientId: string): string {
    const value = (clientId ?? "").trim();
    if (!value) {
      throw new Error("Client ID is required to validate a nonce");
    }
    return value;
  }

  private normalizeNonce(nonce: string): string {
    const value = (nonce ?? "").trim();
    if (!value) {
      throw new Error("Nonce value is required");
    }
    return value;
  }

  private hashNonce(clientId: string, nonce: string): string {
    return createHash("sha256")
      .update(`${this.normalizeClientId(clientId)}:${this.normalizeNonce(nonce)}`)
      .digest("hex");
  }

  private getRedisKey(clientId: string, nonce: string): string {
    return `anti-replay:${this.normalizeClientId(clientId)}:${this.hashNonce(clientId, nonce)}`;
  }

  /**
   * Atomically reserves a nonce for a client using Redis SET NX with TTL.
   * Returns true only if the nonce has not been seen before within the TTL window.
   */
  async consume(clientId: string, nonce: string): Promise<boolean> {
    const redis = getRedisClient();
    if (!redis) {
      return true;
    }

    const key = this.getRedisKey(clientId, nonce);
    const result = await redis.set(key, "1", {
      NX: true,
      EX: this.ttlSeconds,
    });

    if (result === "OK") {
      return true;
    }

    this.logger.warn("Rejected replayed request nonce", {
      clientId: this.normalizeClientId(clientId),
      nonceHash: this.hashNonce(clientId, nonce),
      ttlSeconds: this.ttlSeconds,
    });

    return false;
  }

  /**
   * Convenience alias for callers that want the same anti-replay check with a more descriptive name.
   */
  async verifyAndStore(clientId: string, nonce: string): Promise<boolean> {
    return this.consume(clientId, nonce);
  }

  /**
   * Explicitly check whether a nonce is already present without consuming it.
   */
  async hasSeenNonce(clientId: string, nonce: string): Promise<boolean> {
    const redis = getRedisClient();
    if (!redis) {
      return false;
    }

    const key = this.getRedisKey(clientId, nonce);
    return (await redis.exists(key)) === 1;
  }

  /**
   * Remove a nonce from the current anti-replay set when an operation must be retried or explicitly cancelled.
   */
  async invalidate(clientId: string, nonce: string): Promise<void> {
    const redis = getRedisClient();
    if (!redis) {
      return;
    }

    await redis.del(this.getRedisKey(clientId, nonce));
  }
}

export const cryptographicNonceStore = new CryptographicNonceStore();
export default cryptographicNonceStore;
