import dotenv from "dotenv";

import express from "express";

import morgan from "morgan";

import swaggerUi from "swagger-ui-express";

import cacheMetricsRouter from "./cache/CacheMetrics";

import { specs } from "./lib/swagger";

import { adminMiddleware } from "./middleware/adminMiddleware";
import { adminRateLimitMiddleware } from "./middleware/adminRateLimitMiddleware";

import { apiKeyMiddleware } from "./middleware/apiKeyMiddleware";

import { originGuard } from "./middleware/corsMiddleware";

import { applyHttpSecurity } from "./middleware/httpSecurity";

import { inspectHeadersMiddleware } from "./middleware/securityMiddleware";

import { latencyValidationMiddleware } from "./middleware/latencyGuardMiddleware";

import { signatureVerificationMiddleware } from "./middleware/signatureVerificationMiddleware";
import { maintenanceMiddleware } from "./middleware/maintenanceMiddleware";

import { rateLimitMiddleware } from "./middleware/rateLimitMiddleware";

import {
  tracingMiddleware,
  axiosTracingMiddleware,
} from "./middleware/tracingMiddleware";
import { jwtMiddleware } from "./middleware/jwtMiddleware";
import adminRouter from "./routes/admin";

import authRouter from "./routes/auth";
import assetsRouter from "./routes/assets";

import derivedAssetsRouter from "./routes/derivedAssets";

import historyRouter from "./routes/history";

import intelligenceRouter from "./routes/intelligence";

import marketRatesRouter from "./routes/marketRates";

import priceUpdatesRouter from "./routes/priceUpdates";

import sanityCheckRouter from "./routes/sanityCheck";

import statsRouter from "./routes/stats";

import statusRouter from "./routes/status";
import systemControlRouter from "./routes/systemControl";
import systemFailoverRouter from "./routes/systemFailover";
import analyticsRouter from "./routes/analytics";
import zkRouter from "./routes/zk";
import { sendApiError } from "./lib/apiError.js";

dotenv.config();

const app = express();

app.use(morgan("dev"));

// Issue #792 – Security headers + strict CORS allowlist. Registered before
// everything else so the headers reach every response, including short-circuit
// replies such as CORS 403s, preflight 204s and maintenance 503s.
applyHttpSecurity(app);

// Maintenance mode middleware: must be early in the chain

app.use(maintenanceMiddleware);

app.use(inspectHeadersMiddleware);

app.use(express.json());

// Add tracing middleware early in the stack
app.use(tracingMiddleware);
app.use(axiosTracingMiddleware);

// Issue #792 – Reject API traffic with no usable Origin unless it identifies as
// a non-browser client. Mounted ahead of the auth router and the API key layer
// so credential-bearing browser endpoints are covered and blocked requests
// never reach a database lookup.
app.use("/api", originGuard);

app.use("/api/v1/docs", swaggerUi.serve);

app.get(
  "/api/v1/docs",

  swaggerUi.setup(specs, {
    swaggerOptions: {
      persistAuthorization: true,
    },

    customCss: `

    .topbar { display: none; }

    .swagger-ui .api-info { margin-bottom: 20px; }

  `,

    customSiteTitle: "StellarFlow API Documentation",
  }),
);

app.use("/api/v1/auth", authRouter);

app.use("/api", rateLimitMiddleware);

app.use("/api", apiKeyMiddleware);

app.use("/api", jwtMiddleware);

// Ed25519 signature verification for relayer payloads (Issue #225)
app.use("/api/v1/price-updates", signatureVerificationMiddleware);

// Latency validation for relayer payloads - validates timestamps to prevent stale data

app.use("/api/v1/price-updates", latencyValidationMiddleware);

app.use("/api/admin", adminMiddleware, adminRateLimitMiddleware, adminRouter);

app.use(
  "/api/admin/system",
  adminMiddleware,
  adminRateLimitMiddleware,
  systemControlRouter,
);
app.use(
  "/api/v1/system",
  adminMiddleware,
  adminRateLimitMiddleware,
  systemFailoverRouter,
);
app.use("/api/v1/market-rates", marketRatesRouter);

app.use("/api/v1/history", historyRouter);

app.use("/api/v1/stats", statsRouter);

app.use("/api/v1/intelligence", intelligenceRouter);

app.use("/api/v1/price-updates", priceUpdatesRouter);

app.use("/api/v1/assets", assetsRouter);

app.use("/api/v1/status", statusRouter);

app.use("/api/v1/derived-assets", derivedAssetsRouter);

app.use("/api/v1/sanity-check", sanityCheckRouter);

app.use("/api/v1/cache", cacheMetricsRouter);

// Issue #208 – Analytics / OHLC time-series endpoint
app.use("/api/v1/analytics", analyticsRouter);

app.use("/api/v1/zk", zkRouter);

app.get("/", (req, res) => {
  res.json({
    success: true,

    message: "StellarFlow Backend API",

    version: "1.0.0",

    endpoints: {
      health: "/health",

      marketRates: {
        allRates: "/api/v1/market-rates/rates",

        singleRate: "/api/v1/market-rates/rate/:currency",

        health: "/api/v1/market-rates/health",

        currencies: "/api/v1/market-rates/currencies",

        cache: "/api/v1/market-rates/cache",

        clearCache: "POST /api/v1/market-rates/cache/clear",
      },

      stats: {
        volume: "/api/v1/stats/volume?date=YYYY-MM-DD",
      },

      history: {
        assetHistory: "/api/v1/history/:asset?range=1d|7d|30d|90d",
      },

      intelligence: {
        hourlyVolatility: "/api/v1/intelligence/hourly-volatility",

        priceChange: "/api/v1/intelligence/price-change/:currency",

        staleCurrencies: "/api/v1/intelligence/stale",
      },

      derivedAssets: {
        crossRate: "/api/v1/derived-assets/rate/:base/:quote",

        ngnGhs: "/api/v1/derived-assets/ngn-ghs",
      },

      admin: {
        lockdown: "POST /api/admin/lockdown",

        reportSummary:
          "/api/admin/reports/summary?format=html|pdf&month=YYYY-MM",

        rateLimit: {
          getConfig: "GET /api/admin/rate-limit",
          updateConfig: "PUT /api/admin/rate-limit",
          refreshWhitelist: "POST /api/admin/rate-limit/whitelist/refresh",
        },
      },
    },
  });
});

app.use(
  (
    err: Error,

    req: express.Request,

    res: express.Response,

    _next: express.NextFunction,
  ) => {
    console.error("Unhandled error:", err);

    sendApiError(res, 500, "INTERNAL_SERVER_ERROR");
  },
);

app.use((req, res) => {
  sendApiError(res, 404, "ENDPOINT_NOT_FOUND");
});

export default app;
