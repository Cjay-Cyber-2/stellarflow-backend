import type { LiquidityAsset, LiquidityPoolConfig } from "./types";

function isAsset(value: unknown): value is LiquidityAsset {
  if (!value || typeof value !== "object") return false;
  const asset = value as Record<string, unknown>;
  return (
    typeof asset.code === "string" &&
    asset.code.length > 0 &&
    (asset.issuer === undefined || typeof asset.issuer === "string")
  );
}

export function loadLiquidityPoolConfig(
  raw = process.env.ANCHOR_LIQUIDITY_POOLS,
): LiquidityPoolConfig[] {
  if (!raw?.trim()) return [];

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("ANCHOR_LIQUIDITY_POOLS must be valid JSON");
  }

  if (!Array.isArray(parsed)) {
    throw new Error("ANCHOR_LIQUIDITY_POOLS must be a JSON array");
  }

  const keys = new Set<string>();
  return parsed.map((value, index) => {
    if (!value || typeof value !== "object") {
      throw new Error(`Liquidity pool at index ${index} must be an object`);
    }

    const pool = value as Record<string, unknown>;
    if (typeof pool.key !== "string" || !pool.key.trim()) {
      throw new Error(`Liquidity pool at index ${index} requires a key`);
    }
    if (keys.has(pool.key)) {
      throw new Error(`Duplicate liquidity pool key: ${pool.key}`);
    }
    keys.add(pool.key);

    if (typeof pool.anchorAccount !== "string" || !pool.anchorAccount.trim()) {
      throw new Error(`Liquidity pool ${pool.key} requires an anchorAccount`);
    }
    if (
      !Array.isArray(pool.assets) ||
      pool.assets.length !== 2 ||
      !pool.assets.every(isAsset)
    ) {
      throw new Error(`Liquidity pool ${pool.key} must contain exactly two assets`);
    }
    if (
      !Array.isArray(pool.managerAccounts) ||
      pool.managerAccounts.length === 0 ||
      !pool.managerAccounts.every((account) => typeof account === "string")
    ) {
      throw new Error(`Liquidity pool ${pool.key} requires managerAccounts`);
    }
    if (
      pool.alertWebhookUrl !== undefined &&
      typeof pool.alertWebhookUrl !== "string"
    ) {
      throw new Error(`Liquidity pool ${pool.key} has an invalid alertWebhookUrl`);
    }

    return {
      key: pool.key,
      anchorAccount: pool.anchorAccount,
      assets: pool.assets as [LiquidityAsset, LiquidityAsset],
      managerAccounts: pool.managerAccounts as string[],
      ...(typeof pool.alertWebhookUrl === "string" && {
        alertWebhookUrl: pool.alertWebhookUrl,
      }),
    };
  });
}
