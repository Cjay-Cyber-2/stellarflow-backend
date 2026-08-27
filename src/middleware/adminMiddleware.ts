import { NextFunction, Request, Response } from "express";
import { sendApiError } from "../lib/apiError.js";

let hasWarnedAboutMissingAdminControls = false;

function getHeaderValue(
  value: string | string[] | undefined,
): string | undefined {
  if (Array.isArray(value)) {
    return value[0];
  }

  return value;
}

function matchesAdminIp(requestIp: string | undefined, adminIp: string): boolean {
  if (!requestIp) {
    return false;
  }

  return requestIp === adminIp || requestIp === `::ffff:${adminIp}`;
}

export const adminMiddleware = (
  req: Request,
  res: Response,
  next: NextFunction,
) => {
  const configuredAdminKey = process.env.ADMIN_API_KEY;
  const requestAdminKey = getHeaderValue(req.headers["x-admin-key"]);

  if (configuredAdminKey && requestAdminKey !== configuredAdminKey) {
    return sendApiError(res, 403, "INVALID_ADMIN_KEY");
  }

  const configuredAdminIp = process.env.ADMIN_IP;
  if (configuredAdminIp && !matchesAdminIp(req.ip, configuredAdminIp)) {
    return sendApiError(res, 403, "ADMIN_IP_DENIED");
  }

  if (
    !configuredAdminKey &&
    !configuredAdminIp &&
    !hasWarnedAboutMissingAdminControls
  ) {
    hasWarnedAboutMissingAdminControls = true;
    console.warn(
      "[AdminMiddleware] ADMIN_API_KEY and ADMIN_IP are not configured. Admin routes are protected only by the shared API key.",
    );
  }

  next();
};
