// Server-side calls from the Next.js server to the FastAPI gateway (API_INTERNAL_URL).
// Only imported by server components and route handlers: the browser never talks to
// the API through this module.
import { serverEnv } from "@/lib/env";

export type ApiResult<T> =
  | { ok: true; status: number; data: T }
  | { ok: false; status: number; error: string };

interface ApiOptions {
  method?: "GET" | "POST";
  token?: string;
  body?: unknown;
  searchParams?: Record<string, string | number | undefined>;
}

/** Call the API with a timeout. Never throws: network failures come back as status 0. */
export async function apiFetch<T>(
  path: string,
  validate: (value: unknown) => value is T,
  options: ApiOptions = {},
): Promise<ApiResult<T>> {
  const env = serverEnv();
  const url = new URL(`${env.apiInternalUrl}${path}`);
  for (const [name, value] of Object.entries(options.searchParams ?? {})) {
    if (value !== undefined && value !== "") url.searchParams.set(name, String(value));
  }
  const headers: Record<string, string> = { Accept: "application/json" };
  if (options.token) headers.Authorization = `Bearer ${options.token}`;
  if (options.body !== undefined) headers["Content-Type"] = "application/json";

  let response: Response;
  try {
    response = await fetch(url, {
      method: options.method ?? "GET",
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
      cache: "no-store",
      signal: AbortSignal.timeout(env.apiTimeoutMs),
    });
  } catch (error) {
    const timedOut = error instanceof DOMException && error.name === "TimeoutError";
    return { ok: false, status: 0, error: timedOut ? "The API did not answer in time." : "The API is unreachable." };
  }

  let payload: unknown = null;
  try {
    payload = await response.json();
  } catch {
    // Non-JSON body (e.g. a proxy error page): handled below by status.
  }
  if (!response.ok) {
    const detail =
      payload && typeof payload === "object" && "detail" in payload && typeof payload.detail === "string"
        ? payload.detail
        : `The API answered ${response.status}.`;
    return { ok: false, status: response.status, error: detail };
  }
  if (!validate(payload)) {
    return { ok: false, status: 502, error: "The API returned an unexpected response." };
  }
  return { ok: true, status: response.status, data: payload };
}
