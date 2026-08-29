import { Request, Response, NextFunction } from "express";
import { getRedisClient } from "../lib/redis";

export interface RateLimitOptions {
  windowMs?: number;
  maxPublicRequests?: number;
  maxAuthenticatedRequests?: number;
}

/**
 * Sliding window rate-limiting middleware using Redis.
 * Limits: 60 requests/minute for public routes, 300 requests/minute for authenticated clients.
 * Returns HTTP 429 (Too Many Requests) with Retry-After header.
 */
export function rateLimitMiddleware(options?: RateLimitOptions) {
  const windowMs = options?.windowMs ?? 60 * 1000;
  const maxPublic = options?.maxPublicRequests ?? 60;
  const maxAuth = options?.maxAuthenticatedRequests ?? 300;

  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const redis = getRedisClient();
    const now = Date.now();
    const windowKey = Math.floor(now / windowMs);
    
    // Determine client identifier and authentication status
    const isAuthenticated = Boolean(
      req.headers.authorization || (req as any).user || req.headers["x-api-key"]
    );
    const limit = isAuthenticated ? maxAuth : maxPublic;
    const identifier = (
      (req as any).user?.id ||
      req.headers["x-api-key"] ||
      req.ip ||
      req.socket.remoteAddress ||
       "anonymous"
    ).toString();

    const key = `ratelimit:${isAuthenticated ? "auth" : "public"}:${identifier}:${windowKey}`;

    if (!redis || !redis.isOpen) {
      // Fallback if Redis is unavailable
      return next();
    }

    try {
      const multi = redis.multi();
      multi.incr(key);
      multi.pExpire(key, windowMs);
      const results = await multi.exec();
      const currentCount = results && results[0] ? Number(results[0]) : 1;

      if (currentCount > limit) {
        const retryAfterSeconds = Math.ceil(windowMs / 1000);
        res.setHeader("Retry-After", String(retryAfterSeconds));
        res.status(429).json({
          error: "Too Many Requests",
          message: `Rate limit exceeded. Maximum allowed is ${limit} requests per ${windowMs / 1000} seconds.`,
          retryAfter: retryAfterSeconds,
        });
        return;
      }

      res.setHeader("X-RateLimit-Limit", String(limit));
      res.setHeader("X-RateLimit-Remaining", String(Math.max(0, limit - currentCount)));
      next();
    } catch (error) {
      console.error("[RateLimit] Redis error during rate limiting:", error);
      // Fail open to prevent service disruption if Redis fails
      next();
    }
  };
}
