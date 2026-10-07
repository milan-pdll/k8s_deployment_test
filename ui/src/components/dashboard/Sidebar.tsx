"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { ArrowUpRight, MapPinned, Search } from "lucide-react";
import { PgsLogo } from "@/components/home/PgsLogo";
import { cn } from "@/lib/cn";
import { SECTIONS } from "@/components/dashboard/sections";

const LINKS = [
  { label: "Search", href: "/", icon: Search },
  { label: "Geo Explorer", href: "/map", icon: MapPinned },
];

/** The section currently in view, so the nav can highlight it. */
function useActiveSection() {
  const [active, setActive] = useState(SECTIONS[0].id);

  useEffect(() => {
    const elements = SECTIONS.map((s) => document.getElementById(s.id)).filter((el): el is HTMLElement => !!el);
    const visible = new Map<string, boolean>();
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) visible.set(entry.target.id, entry.isIntersecting);
        const first = SECTIONS.find((s) => visible.get(s.id));
        if (first) setActive(first.id);
      },
      // A section counts as "current" once it reaches the upper part of the screen.
      { rootMargin: "-25% 0px -65% 0px" }
    );
    elements.forEach((el) => observer.observe(el));
    return () => observer.disconnect();
  }, []);

  return [active, setActive] as const;
}

/** Desktop sidebar (large screens). */
export function Sidebar() {
  const [active, setActive] = useActiveSection();

  return (
    <aside className="sticky top-0 hidden h-screen w-60 shrink-0 flex-col border-r border-slate-200 bg-white px-3 py-5 dark:border-slate-800 dark:bg-slate-900 lg:flex">
      <Link
        href="/"
        aria-label="PGS Search home"
        className="block rounded-md px-2 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
      >
        <PgsLogo size="small" />
      </Link>

      <nav aria-label="Dashboard sections" className="mt-8">
        <p className="px-3 text-[11px] font-semibold uppercase tracking-wider text-slate-400">Monitoring</p>
        <ul className="mt-2 space-y-0.5">
          {SECTIONS.map((section) => {
            const isActive = active === section.id;
            return (
              <li key={section.id}>
                <a
                  href={`#${section.id}`}
                  onClick={() => setActive(section.id)}
                  aria-current={isActive ? "location" : undefined}
                  className={cn(
                    "flex items-center gap-2.5 rounded-lg px-3 py-2 text-sm transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500",
                    isActive
                      ? "bg-blue-50 font-medium text-blue-700 dark:bg-blue-500/10 dark:text-blue-300"
                      : "text-slate-600 hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-slate-100"
                  )}
                >
                  <section.icon className="h-4 w-4 shrink-0" aria-hidden />
                  {section.label}
                </a>
              </li>
            );
          })}
        </ul>
      </nav>

      <nav aria-label="Other pages" className="mt-8">
        <p className="px-3 text-[11px] font-semibold uppercase tracking-wider text-slate-400">Go to</p>
        <ul className="mt-2 space-y-0.5">
          {LINKS.map((link) => (
            <li key={link.href}>
              <Link
                href={link.href}
                className="group flex items-center gap-2.5 rounded-lg px-3 py-2 text-sm text-slate-600 hover:bg-slate-100 hover:text-slate-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-slate-100"
              >
                <link.icon className="h-4 w-4 shrink-0" aria-hidden />
                {link.label}
                <ArrowUpRight className="ml-auto h-3.5 w-3.5 opacity-0 transition-opacity group-hover:opacity-100" aria-hidden />
              </Link>
            </li>
          ))}
        </ul>
      </nav>
    </aside>
  );
}

/** Horizontal section bar for phones and tablets (the sidebar is hidden there). */
export function SectionNav() {
  const [active, setActive] = useActiveSection();
  const barRef = useRef<HTMLDivElement>(null);

  // Keep the current section's chip visible as you scroll the page.
  useEffect(() => {
    const bar = barRef.current;
    const chip = bar?.querySelector<HTMLElement>(`[data-section="${active}"]`);
    if (!bar || !chip) return;
    bar.scrollTo({ left: chip.offsetLeft - (bar.clientWidth - chip.offsetWidth) / 2, behavior: "smooth" });
  }, [active]);

  return (
    <nav aria-label="Dashboard sections" className="lg:hidden">
      <div
        ref={barRef}
        className="flex gap-1 overflow-x-auto px-4 pb-2 [scrollbar-width:none] sm:px-6 [&::-webkit-scrollbar]:hidden"
      >
        {SECTIONS.map((section) => {
          const isActive = active === section.id;
          return (
            <a
              key={section.id}
              href={`#${section.id}`}
              data-section={section.id}
              onClick={() => setActive(section.id)}
              aria-current={isActive ? "location" : undefined}
              className={cn(
                "shrink-0 rounded-full px-3 py-1.5 text-sm transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500",
                isActive
                  ? "bg-slate-900 font-medium text-white dark:bg-slate-100 dark:text-slate-900"
                  : "text-slate-600 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-800"
              )}
            >
              {section.label}
            </a>
          );
        })}
      </div>
    </nav>
  );
}
