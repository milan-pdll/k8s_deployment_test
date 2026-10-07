import { redirect } from "next/navigation";
import { Sidebar } from "@/components/dashboard/Sidebar";
import { SignOutButton } from "@/components/dashboard/SignOutButton";
import { getSessionUser } from "@/lib/auth/session";

export default async function DashboardLayout({ children }: { children: React.ReactNode }) {
  // proxy.ts only checks that a session cookie exists; the API verifies the token here.
  const user = await getSessionUser();
  if (!user) redirect("/login?next=/dashboard");

  return (
    <div className="flex min-h-screen bg-slate-50 dark:bg-slate-950">
      <Sidebar />
      <div className="flex-1">
        <header className="flex items-center justify-between border-b border-slate-200 bg-white px-6 py-3 dark:border-slate-800 dark:bg-slate-900">
          <div>
            <h1 className="text-sm font-semibold text-slate-900 dark:text-slate-100">
              PGS Search Engine · Dashboard
            </h1>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Signed in as {user.username} ({user.role})
            </p>
          </div>
          <SignOutButton />
        </header>
        <main className="p-6">{children}</main>
      </div>
    </div>
  );
}
