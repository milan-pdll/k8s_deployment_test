import { NextResponse, type NextRequest } from "next/server";
import { apiFetch } from "@/lib/api/server";
import { isLoginResponse } from "@/lib/api/types";
import { SESSION_COOKIE } from "@/lib/auth/session";
import { serverEnv } from "@/lib/env";

function cookieIsSecure(request: NextRequest): boolean {
  const mode = serverEnv().cookieSecure;
  if (mode !== "auto") return mode === "always";
  const forwarded = request.headers.get("x-forwarded-proto")?.split(",")[0]?.trim();
  return (forwarded ?? request.nextUrl.protocol.replace(":", "")) === "https";
}

export async function POST(request: NextRequest) {
  const body = await request.json().catch(() => null);
  const login = typeof body?.login === "string" ? body.login.trim() : "";
  const password = typeof body?.password === "string" ? body.password : "";
  if (!login || !password) {
    return NextResponse.json({ error: "Enter your username or email and password." }, { status: 400 });
  }

  const credentials = login.includes("@") ? { email: login, password } : { username: login, password };
  const result = await apiFetch("/api/v1/auth/login", isLoginResponse, {
    method: "POST",
    body: credentials,
  });
  if (!result.ok) {
    const status = result.status === 401 ? 401 : result.status === 503 ? 503 : 502;
    const error =
      status === 401
        ? "Invalid username/email or password."
        : status === 503
          ? "Sign-in is not available (the API has no API_AUTH_SECRET)."
          : "Sign-in failed; try again later.";
    return NextResponse.json({ error }, { status });
  }

  const response = NextResponse.json({ user: result.data.user });
  response.cookies.set(SESSION_COOKIE, result.data.access_token, {
    httpOnly: true,
    sameSite: "lax",
    secure: cookieIsSecure(request),
    path: "/",
    maxAge: result.data.expires_in,
  });
  return response;
}
