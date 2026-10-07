// Runs once when a Next.js server instance starts, before it handles requests.
export async function register() {
  if (process.env.NEXT_RUNTIME === "nodejs") {
    const { readServerEnv } = await import("./lib/env");
    // Throws on an invalid API_INTERNAL_URL / API_TIMEOUT_MS / SESSION_COOKIE_SECURE, so a
    // misconfigured server stops at startup with the reason instead of failing per request.
    readServerEnv();
  }
}
