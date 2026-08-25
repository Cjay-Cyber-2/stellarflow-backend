import { getRedisClient } from "../lib/redis";

const ROOTS_KEY = "stellarflow:zk:merkle-roots";
const ROOT_ORDER_KEY = "stellarflow:zk:merkle-roots:order";
const ROOT_MARKER_PREFIX = "stellarflow:zk:merkle-root:";
const MAX_ROOTS = 100;
const MAX_AGE_SECONDS = Number(process.env.ZK_MAX_ROOT_AGE_SECONDS ?? 3600);

async function removeExpiredRoots(now: number): Promise<void> {
  const redis = getRedisClient();
  if (!redis?.isOpen) return;

  const expired = await redis.zRangeByScore(
    ROOT_ORDER_KEY,
    "-inf",
    String(now - MAX_AGE_SECONDS),
  );

  if (expired.length > 0) {
    await redis.sRem(ROOTS_KEY, expired);
    await redis.zRem(ROOT_ORDER_KEY, expired);
  }
}

export async function recordMerkleRoot(rootHash: string): Promise<void> {
  if (!rootHash) return;

  const redis = getRedisClient();
  if (!redis?.isOpen) return;

  try {
    const now = Math.floor(Date.now() / 1000);
    await removeExpiredRoots(now);
    await redis.sAdd(ROOTS_KEY, rootHash);
    await redis.zAdd(ROOT_ORDER_KEY, { score: now, value: rootHash });
    await redis.setEx(`${ROOT_MARKER_PREFIX}${rootHash}`, MAX_AGE_SECONDS, "1");

    const oldest = await redis.zRange(ROOT_ORDER_KEY, 0, -MAX_ROOTS - 1);
    if (oldest.length > 0) {
      await redis.sRem(ROOTS_KEY, oldest);
      await redis.zRem(ROOT_ORDER_KEY, oldest);
      await redis.del(oldest.map((root) => `${ROOT_MARKER_PREFIX}${root}`));
    }
  } catch (error) {
    console.error("[MerkleRootCache] Store error:", error);
  }
}

export async function verifyMerkleRoot(rootHash: string): Promise<boolean> {
  if (!rootHash) return false;

  const redis = getRedisClient();
  if (!redis?.isOpen) return false;

  try {
    await removeExpiredRoots(Math.floor(Date.now() / 1000));
    return (
      (await redis.sIsMember(ROOTS_KEY, rootHash)) === 1 &&
      (await redis.exists(`${ROOT_MARKER_PREFIX}${rootHash}`)) === 1
    );
  } catch (error) {
    console.error("[MerkleRootCache] Verify error:", error);
    return false;
  }
}
