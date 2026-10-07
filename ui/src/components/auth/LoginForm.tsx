"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useState } from "react";
import { Loader2, Lock, Mail } from "lucide-react";

export function LoginForm() {
  const router = useRouter();
  const searchParams = useSearchParams();
  // Only same-site paths: an absolute ?next= would be an open redirect.
  const requested = searchParams.get("next") ?? "";
  const next = requested.startsWith("/") && !requested.startsWith("//") ? requested : "/dashboard";

  const [login, setLogin] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    setLoading(true);
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ login, password }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setError(data.error ?? "Sign in failed.");
        return;
      }
      router.push(next);
      router.refresh();
    } finally {
      setLoading(false);
    }
  }

  return (
    <form onSubmit={handleSubmit} className="w-full max-w-sm space-y-4">
      <div>
        <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
          Username or email
        </label>
        <div className="flex items-center gap-2 rounded-lg border border-slate-200 px-3 py-2 dark:border-slate-700">
          <Mail className="h-4 w-4 text-slate-400" />
          <input
            type="text"
            autoComplete="username"
            required
            value={login}
            onChange={(e) => setLogin(e.target.value)}
            className="w-full bg-transparent text-sm outline-none"
          />
        </div>
      </div>

      <div>
        <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">Password</label>
        <div className="flex items-center gap-2 rounded-lg border border-slate-200 px-3 py-2 dark:border-slate-700">
          <Lock className="h-4 w-4 text-slate-400" />
          <input
            type="password"
            autoComplete="current-password"
            required
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="w-full bg-transparent text-sm outline-none"
          />
        </div>
      </div>

      {error && <p className="text-sm text-rose-600">{error}</p>}

      <button
        type="submit"
        disabled={loading}
        className="flex w-full items-center justify-center gap-2 rounded-lg bg-blue-600 py-2.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-60"
      >
        {loading && <Loader2 className="h-4 w-4 animate-spin" />}
        Sign in
      </button>

      <p className="rounded-lg bg-slate-50 px-3 py-2 text-xs text-slate-500 dark:bg-slate-800 dark:text-slate-400">
        Admin accounts are created by an operator (
        <code>python scripts/create_admin.py</code> in <code>database/</code>).
      </p>
    </form>
  );
}
