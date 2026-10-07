/**
 * Runtime configuration of the Next.js server, read from the environment (not inlined at
 * build time: nothing here is NEXT_PUBLIC_, so one image works in every environment).
 *
 * - API_INTERNAL_URL (default http://api:8000): the FastAPI gateway as the Next server
 *   reaches it. The browser never calls it directly.
 * - API_TIMEOUT_MS (default 8000): per-request timeout for those calls. Keep it above the
 *   API's SEARCH_TIMEOUT_SECONDS so the API's own 504 reaches the page.
 * - SESSION_COOKIE_SECURE (default auto): auto marks the session cookie Secure when the
 *   request came in over https (X-Forwarded-Proto from nginx, or the URL); always/never
 *   force it.
 *
 * `readServerEnv` throws on an invalid value; instrumentation.ts calls it at startup so a
 * misconfigured server refuses to start instead of failing on the first request.
 */

export type CookieSecureMode = "auto" | "always" | "never";

export interface ServerEnv {
  apiInternalUrl: string;
  apiTimeoutMs: number;
  cookieSecure: CookieSecureMode;
}

const DEFAULTS: ServerEnv = {
  apiInternalUrl: "http://api:8000",
  apiTimeoutMs: 8000,
  cookieSecure: "auto",
};

export function readServerEnv(env: Record<string, string | undefined> = process.env): ServerEnv {
  const problems: string[] = [];

  let apiInternalUrl = DEFAULTS.apiInternalUrl;
  const rawUrl = env.API_INTERNAL_URL?.trim();
  if (rawUrl) {
    try {
      const url = new URL(rawUrl);
      if (url.protocol !== "http:" && url.protocol !== "https:") {
        problems.push("API_INTERNAL_URL must be an http:// or https:// URL");
      } else if (url.username || url.password || url.search || url.hash) {
        problems.push("API_INTERNAL_URL must not contain credentials, a query or a fragment");
      } else {
        apiInternalUrl = `${url.origin}${url.pathname}`.replace(/\/+$/, "");
      }
    } catch {
      problems.push("API_INTERNAL_URL is not a valid URL");
    }
  }

  let apiTimeoutMs = DEFAULTS.apiTimeoutMs;
  const rawTimeout = env.API_TIMEOUT_MS?.trim();
  if (rawTimeout) {
    const value = Number(rawTimeout);
    if (!Number.isInteger(value) || value < 500 || value > 60000) {
      problems.push("API_TIMEOUT_MS must be an integer between 500 and 60000");
    } else {
      apiTimeoutMs = value;
    }
  }

  let cookieSecure = DEFAULTS.cookieSecure;
  const rawSecure = env.SESSION_COOKIE_SECURE?.trim().toLowerCase();
  if (rawSecure) {
    if (rawSecure === "auto" || rawSecure === "always" || rawSecure === "never") {
      cookieSecure = rawSecure;
    } else {
      problems.push("SESSION_COOKIE_SECURE must be auto, always or never");
    }
  }

  if (problems.length > 0) {
    throw new Error(`Invalid UI server configuration: ${problems.join("; ")}`);
  }
  return { apiInternalUrl, apiTimeoutMs, cookieSecure };
}

let cached: ServerEnv | undefined;

export function serverEnv(): ServerEnv {
  cached ??= readServerEnv();
  return cached;
}
