import { prisma } from "../lib/prisma.js";
import {
  generateToken,
  verifyPassword,
  createUserSession,
  invalidateSession,
  generateRefreshToken,
  verifyRefreshToken,
  isRefreshTokenBlacklisted,
  blacklistRefreshToken,
} from "../utils/jwt.js";
import {
  logLoginSuccess,
  logLoginFailed,
  logLogout,
} from "../services/userAuditService.js";
import {
  bruteForceGuard,
  recordFailedAttempt,
  clearBruteForceRecord,
} from "../middleware/bruteForceMiddleware.js";
import express from "express";
import crypto from "crypto";
import { sendApiError } from "../lib/apiError.js";
import { storeEncryptedSession, revokeSessionByToken } from "../utils/jwt.js";

const router = express.Router();

router.post(
  "/login",
  bruteForceGuard,
  async (
    req: express.Request,
    res: express.Response,
  ): Promise<void> => {
    try {
      const { email, password } = req.body as { email?: string; password?: string };

      if (!email || !password) {
        res.status(400).json({
          success: false,
          error: {
            code: "MISSING_CREDENTIALS",
            message: "Email and password are required",
          },
        });
        return;
      }

      const relayer = await prisma.relayer.findUnique({
        where: { email },
      });

      const clientIp = req.ip || "unknown";

      if (!relayer || !relayer.passwordHash) {
        recordFailedAttempt(clientIp);
        await logLoginFailed(
          email,
          clientIp,
          req.headers["user-agent"] || "unknown",
          "User not found or no password set",
        );
        res.status(401).json({
          success: false,
          error: {
            code: "INVALID_CREDENTIALS",
            message: "Invalid email or password",
          },
        });
        return;
      }

      if (!relayer.isActive) {
        recordFailedAttempt(clientIp);
        await logLoginFailed(
          email,
          clientIp,
          req.headers["user-agent"] || "unknown",
          "Account deactivated",
        );
        res.status(403).json({
          success: false,
          error: {
            code: "ACCOUNT_DISABLED",
            message: "Account is disabled",
          },
        });
        return;
      }

      const isValid = await verifyPassword(password, relayer.passwordHash);

      if (!isValid) {
        recordFailedAttempt(clientIp);
        await logLoginFailed(
          email,
          clientIp,
          req.headers["user-agent"] || "unknown",
          "Invalid password",
        );
        res.status(401).json({
          success: false,
          error: {
            code: "INVALID_CREDENTIALS",
            message: "Invalid email or password",
          },
        });
        return;
      }

      // Successful auth — clear any brute-force counters for this IP
      clearBruteForceRecord(clientIp);

      const sessionId = crypto.randomUUID();
      const token = generateToken({
        userId: relayer.id,
        email: relayer.email!,
        role: relayer.role || "VIEWER",
        sid: sessionId,
      }, "15m");

      const refreshTokenData = generateRefreshToken(relayer.id);
      const sessionUserAgent = req.headers["user-agent"] || "unknown";

      await storeEncryptedSession({
        userId: relayer.id,
        email: relayer.email!,
        role: relayer.role || "VIEWER",
        sid: sessionId,
        ipAddress: clientIp,
        userAgent: sessionUserAgent,
        expiresAt: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
        exp: Math.floor((Date.now() + 15 * 60 * 1000) / 1000),
      }, 15 * 60);

      await createUserSession(
        relayer.id,
        token,
        clientIp,
        sessionUserAgent,
      );

      await prisma.relayer.update({
        where: { id: relayer.id },
        data: { lastLoginAt: new Date() },
      });

      await logLoginSuccess(
        relayer.id,
        clientIp,
        req.headers["user-agent"] || "unknown",
      );

      res.json({
        success: true,
        data: {
          token,
          refreshToken: refreshTokenData.token,
          user: {
            id: relayer.id,
            email: relayer.email,
            name: relayer.name,
            role: relayer.role,
            lastLoginAt: relayer.lastLoginAt,
          },
        },
      });
    } catch (error) {
      console.error("[AUTH] Login error:", error);
      res.status(500).json({
        success: false,
        error: {
          code: "INTERNAL_ERROR",
          message: "An error occurred during login",
        },
      });
    }
  },
);

router.post(
  "/logout",
  async (
    req: express.Request,
    res: express.Response,
  ): Promise<void> => {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader?.startsWith("Bearer ")) {
        res.status(401).json({
          success: false,
          error: {
            code: "MISSING_TOKEN",
            message: "Authorization token required",
          },
        });
        return;
      }

      const token = authHeader.substring(7);

      await invalidateSession(token);
      await revokeSessionByToken(token);

      const userId = (req as any).user?.userId;

      if (userId) {
        await logLogout(
          userId,
          req.ip || "unknown",
          req.headers["user-agent"] || "unknown",
        );
      }

      res.json({
        success: true,
        message: "Logged out successfully",
      });
    } catch (error) {
      console.error("[AUTH] Logout error:", error);
      res.status(500).json({
        success: false,
        error: {
          code: "INTERNAL_ERROR",
          message: "An error occurred during logout",
        },
      });
    }
  },
);

router.post(
  "/refresh",
  async (req: express.Request, res: express.Response): Promise<void> => {
    try {
      const { refreshToken } = req.body as { refreshToken?: string };
      if (!refreshToken) {
        res.status(400).json({
          success: false,
          error: { code: "MISSING_TOKEN", message: "Refresh token is required" },
        });
        return;
      }

      const decoded = verifyRefreshToken(refreshToken);
      if (!decoded) {
        res.status(401).json({
          success: false,
          error: { code: "INVALID_TOKEN", message: "Invalid or expired refresh token" },
        });
        return;
      }

      const isBlacklisted = await isRefreshTokenBlacklisted(decoded.jti);
      if (isBlacklisted) {
        res.status(401).json({
          success: false,
          error: { code: "TOKEN_REVOKED", message: "Refresh token has been revoked" },
        });
        return;
      }

      const relayer = await prisma.relayer.findUnique({
        where: { id: decoded.userId },
      });

      if (!relayer || !relayer.isActive) {
        res.status(401).json({
          success: false,
          error: { code: "USER_INVALID", message: "User not found or disabled" },
        });
        return;
      }

      const expiresInSec = decoded.exp ? decoded.exp - Math.floor(Date.now() / 1000) : 7 * 24 * 60 * 60;
      if (expiresInSec > 0) {
        await blacklistRefreshToken(decoded.jti, expiresInSec);
      }

      const accessToken = generateToken({
        userId: relayer.id,
        email: relayer.email!,
        role: relayer.role || "VIEWER",
      }, "15m");

      const newRefreshTokenData = generateRefreshToken(relayer.id);

      res.json({
        success: true,
        data: {
          accessToken,
          refreshToken: newRefreshTokenData.token,
        },
      });

    } catch (error) {
      console.error("[AUTH] Refresh error:", error);
      res.status(500).json({
        success: false,
        error: { code: "INTERNAL_ERROR", message: "An error occurred during token refresh" },
      });
    }
  }
);

export default router;