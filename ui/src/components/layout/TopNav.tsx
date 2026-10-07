import Link from "next/link";
import { LayoutDashboard, MapPinned } from "lucide-react";
import { getSessionUser } from "@/lib/auth/session";

export async function TopNav() {
  const user = await getSessionUser();

  return (
    <header className="flex items-center justify-between gap-4 px-6 py-4 text-sm">
      <Link href="/" className="font-medium text-slate-600 dark:text-slate-300">
        PGS Search
      </Link>
      <nav className="flex items-center gap-5 text-slate-600 dark:text-slate-300">
        <Link href="/map" className="flex items-center gap-1.5 hover:underline">
          <MapPinned className="h-4 w-4" />
          Geo Explorer
        </Link>
        {user ? (
          <Link href="/dashboard" className="flex items-center gap-1.5 hover:underline">
            <LayoutDashboard className="h-4 w-4" />
            Dashboard
          </Link>
        ) : null}
        {user ? (
          <span className="rounded-full bg-slate-800 px-3 py-1.5 text-xs font-medium text-white dark:bg-slate-100 dark:text-slate-900">
            {user.username}
          </span>
        ) : (
          <Link
            href="/login"
            className="rounded-full bg-blue-600 px-4 py-1.5 font-medium text-white hover:bg-blue-700"
          >
            Sign in
          </Link>
        )}
      </nav>
    </header>
  );
}
