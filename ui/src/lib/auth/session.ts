import { cache } from "react";
import { cookies } from "next/headers";
import { apiFetch } from "@/lib/api/server";
import { isAdminUser, type AdminUser } from "@/lib/api/types";

/**
 * The admin session: the API's signed access token (POST /api/v1/auth/login), kept in an
 * httpOnly cookie. The UI never trusts the cookie's content: every request that needs the
 * user asks the API (GET /api/v1/auth/me), which verifies the signature and expiry.
 */
export const SESSION_COOKIE = "pgs_session";

export async function getSessionToken(): Promise<string | null> {
  const store = await cookies();
  return store.get(SESSION_COOKIE)?.value || null;
}

/** The signed-in admin, or null. Cached per request (React cache). */
export const getSessionUser = cache(async (): Promise<AdminUser | null> => {
  const token = await getSessionToken();
  if (!token) return null;
  const result = await apiFetch("/api/v1/auth/me", isAdminUser, { token });
  return result.ok ? result.data : null;
});
