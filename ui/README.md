# PGS UI (Next.js)

## What is implemented

- `/` search box, `/search?q=&page=` results from the API (`GET /api/v1/search`, called by
  the Next server -- `src/lib/api/server.ts`), with real counts, errors and pagination.
- `/map`: the interactive Nepal map (`src/components/map/`, GeoJSON in `public/data/`); a
  region's popup links to the search for that place.
- `/login` and `/dashboard`: admin login through `POST /api/v1/auth/login` (the API's signed
  token in an httpOnly, SameSite=Lax cookie, Secure over https); the dashboard layout
  verifies the token with `GET /api/v1/auth/me` on every request and shows
  `GET /api/v1/admin/summary`. `src/proxy.ts` (Next 16's middleware) only redirects requests
  without a session cookie.
- Server configuration (runtime, validated at startup in `src/lib/env.ts`):
  `API_INTERNAL_URL` (default `http://api:8000`), `API_TIMEOUT_MS`, `SESSION_COOKIE_SECURE`
  (auto/always/never). Nothing is inlined at build time; browsers never call the API
  directly except through the same-origin nginx routes.

```bash
npm ci && API_INTERNAL_URL=http://localhost:8000 npm run dev
npm run lint && npx tsc --noEmit && npm run build
```

Not implemented from the specification below: i18n, the cluster/node metrics, domain manager,
storage, logs and malware-audit admin pages, and per-region news feeds.

---

# Original specification (target design)

# System Architecture & Implementation Specification

## Next.js Frontend Application (User Portal & Admin Dashboard)

---

## 1. Executive Summary

This document specifies the architecture, file structure, state management, and implementation details for the **Next.js Frontend Application**.

The frontend provides two distinct portals within a unified Next.js App Router codebase:

1. **User Section (`/` and `/search`):** A high-performance, bilingual (English & Nepali) search portal featuring traditional keyword search, dynamic document/PDF viewers, and an interactive vector map of Nepal for geo-spatial administrative filtering (down to the Municipality / Rural Municipality level).
2. **Admin Section (`/admin/*`):** A secure, JWT-authenticated operations dashboard for real-time cluster monitoring, crawl domain management, data pipeline queue inspection (DFS raw files), error logging, and malware quarantine auditing.

---

## 2. Technology Stack & Core Libraries

```
┌────────────────────────────────────────────────────────────────────────┐
│                        NEXT.JS FRONTEND ARCHITECTURE                   │
├───────────────────┬────────────────────────────────────────────────────┤
│ Framework         │ Next.js 14+ (App Router, Server & Client Comps)    │
│ Language          │ TypeScript 5.x (Strict Typing)                     │
│ Styling           │ Tailwind CSS + Shadcn UI (Component Primitives)     │
│ State Management  │ Zustand (Global App State) + TanStack Query (Server)│
│ Interactive Map   │ D3.js / Leaflet.js + GeoJSON (Nepal Boundaries)   │
│ Charts / Graphics │ Recharts (Admin Monitoring Dashboard)              │
│ Auth / Security   │ Next.js Middleware + HTTP-Only Cookies / Local     │
└───────────────────┴────────────────────────────────────────────────────┘

```

---

## 3. Project Directory Structure

```text
nepal-search-frontend/
├── app/
│   ├── (user)/                   # User Portal Route Group (Layout A)
│   │   ├── page.tsx              # Home Page (Search Box + Interactive Map)
│   │   ├── search/
│   │   │   └── page.tsx          # SERP Page (Results, Filters, Regional Cards)
│   │   └── document/
│   │       └── [id]/
│   │           └── page.tsx      # Document Viewer & DFS Download Proxy
│   ├── admin/                    # Admin Portal Route Group (Layout B)
│   │   ├── login/
│   │   │   └── page.tsx          # JWT Auth Login Page
│   │   ├── dashboard/
│   │   │   └── page.tsx          # High-Level System Summary Metrics
│   │   ├── nodes/
│   │   │   └── page.tsx          # K8s Node & Cluster Metric Graphs
│   │   ├── domains/
│   │   │   └── page.tsx          # Seed Domain & Child Link Crawl Manager
│   │   ├── storage/
│   │   │   └── page.tsx          # MinIO DFS Raw File Queue Inspection
│   │   └── logs/
│   │       └── page.tsx          # Error Logs & ClamAV Quarantine Audit
│   ├── api/                      # Next.js Internal API Routes (BFF/Proxy)
│   ├── layout.tsx                # Root App Layout
│   └── globals.css               # Global Tailwind CSS & Devanagari Fonts
├── components/
│   ├── user/                     # User Components
│   │   ├── SearchBar.tsx         # Auto-complete Search Input
│   │   ├── NepalVectorMap.tsx    # Interactive GeoJSON Map (753 Local Bodies)
│   │   ├── RegionalCard.tsx      # Municipality/District Information Card
│   │   ├── ResultCard.tsx        # Standard Web Result Card
│   │   ├── PdfCard.tsx           # Document & PDF Download Card
│   │   └── LanguageSwitcher.tsx  # EN/NE Language Toggle
│   ├── admin/                    # Admin Components
│   │   ├── Sidebar.tsx           # Admin Navigation
│   │   ├── MetricWidget.tsx      # Summary Statistic Counter
│   │   ├── NodeCpuChart.tsx      # Recharts Real-Time Cluster Graph
│   │   ├── DomainTable.tsx       # Paginated Crawl Management Table
│   │   └── LogViewer.tsx         # Live Stream Error Terminal Component
│   └── ui/                       # Shadcn UI Base Primitives (Buttons, Dialogs)
├── hooks/
│   ├── useSearch.ts              # TanStack Query Hook for Search API
│   ├── useAdminMetrics.ts        # Polling Hook for Admin Metrics
│   └── useLanguage.ts           # i18n Context Hook
├── lib/
│   ├── api-client.ts             # Axios Instance with JWT Interceptors
│   ├── geojson/                  # Nepal Admin Boundary Spatial Data
│   │   ├── nepal-provinces.json
│   │   ├── nepal-districts.json
│   │   └── nepal-municipalities.json
│   └── utils.ts                  # Devanagari Normalizer & Formatters
├── middleware.ts                 # Route Guard (Protects /admin/* routes)
└── types/                        # TypeScript Interfaces & API Schemas
    ├── api.ts
    ├── geo.ts
    └── admin.ts

```

---

## 4. User Section Implementation

### 4.1 Pages & Functionality

#### 1. Home Page (`/app/(user)/page.tsx`)

* **Features:** Prominent Google-style search bar, language switcher (`EN` / `नेपाली`), and a full interactive vector map of Nepal.
* **Behavior:** Typing a query redirects to `/search?q=...`. Clicking a municipality/district on the vector map directly navigates to `/search?municipality_id=...` or `/search?district_code=...`.

#### 2. Search Engine Results Page (`/app/(user)/search/page.tsx`)

* **Features:**
* **Top Search Header:** Search bar with auto-suggestions and language toggle.
* **Left Sidebar / Floating Drawer:** Interactive SVG map of Nepal for dynamic geo-filtering. Clicking a region immediately filters the current query by that administrative boundary without reloading the page.
* **Main Content Area:**
* **Regional Knowledge Card:** Displayed when a specific region is selected or searched. Displays official municipality/district links, phone numbers, email, and direct ward portal links.
* **Result List:** Tabbed view (`All Results`, `Documents / PDFs`, `Official Portals`). Renders standard web cards and binary document cards with direct DFS download capability.





#### 3. Bilingual Support (i18n)

* Supports full Devanagari script rendering with system fonts (`Mukta`, `Kalimati`, `Noto Sans Devanagari`).
* Toggle dynamically translates UI labels (e.g., `"Search"` $\rightarrow$ `"खोज्नुहोस्"`, `"Documents"` $\rightarrow$ `"कागजातहरू"`).

---

### 4.2 Interactive Nepal Vector Map (`NepalVectorMap.tsx`)

The map component renders a lightweight SVG/GeoJSON vector map supporting three drill-down levels: **Province (7) $\rightarrow$ District (77) $\rightarrow$ Municipality/Rural Municipality (753)**.

```tsx
// Sample Conceptual Hook Usage inside NepalVectorMap.tsx
"use client";

import React, { useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import municipalitiesGeoJson from "@/lib/geojson/nepal-municipalities.json";

export default function NepalVectorMap() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [hoveredRegion, setHoveredRegion] = useState<string | null>(null);

  const handleMunicipalityClick = (muniId: string, muniNameEn: string) => {
    const params = new URLSearchParams(searchParams.toString());
    params.set("municipality_id", muniId);
    // Preserves active text query if user typed something beforehand
    router.push(`/search?${params.toString()}`);
  };

  return (
    <div className="relative w-full h-[400px] border rounded-lg bg-slate-50 p-2">
      <svg viewBox="0 0 800 500" className="w-full h-full">
        {municipalitiesGeoJson.features.map((feature) => (
          <path
            key={feature.properties.local_body_id}
            d={/* Convert GeoJSON coordinates to SVG Path */}""
            className="fill-slate-200 stroke-white hover:fill-blue-500 transition-colors cursor-pointer"
            onMouseEnter={() => setHoveredRegion(feature.properties.local_body_name_en)}
            onClick={() => handleMunicipalityClick(
              feature.properties.local_body_id,
              feature.properties.local_body_name_en
            )}
          />
        ))}
      </svg>
      {hoveredRegion && (
        <div className="absolute bottom-2 left-2 bg-slate-900 text-white px-3 py-1 rounded text-sm">
          {hoveredRegion}
        </div>
      )}
    </div>
  );
}

```

---

## 5. Admin Section Implementation

### 5.1 Route Protection & Authentication Flow

The Admin section uses a secure **JWT Auth Token Lifecycle** with Next.js Middleware.

```text
[ Admin User ] ──► Navigates to /admin/dashboard
                          │
                          ▼
                [ Next.js Middleware ]
                          │
         ┌────────────────┴────────────────┐
         │ Is Valid JWT Cookie Present?    │
         ├─────────────────────────────────┤
         │ YES ──► Render /admin/dashboard  │
         │ NO  ──► Redirect to /admin/login│
         └─────────────────────────────────┘

```

#### Middleware Guard (`middleware.ts`)

```typescript
import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

export function middleware(request: NextRequest) {
  const token = request.cookies.get("admin_jwt_token")?.value;
  const isAdminRoute = request.nextUrl.pathname.startsWith("/admin");
  const isLoginPage = request.nextUrl.pathname === "/admin/login";

  if (isAdminRoute && !isLoginPage && !token) {
    return NextResponse.redirect(new URL("/admin/login", request.url));
  }

  if (isLoginPage && token) {
    return NextResponse.redirect(new URL("/admin/dashboard", request.url));
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/admin/:path*"],
};

```

---

### 5.2 Admin Pages & Monitoring Specs

#### 1. Login Page (`/app/admin/login/page.tsx`)

* Standard secure login form. On `200 OK` from `POST /api/v1/auth/login`, stores the JWT token in an `HTTP-Only Cookie` and redirects to `/admin/dashboard`.

#### 2. System Overview Dashboard (`/app/admin/dashboard/page.tsx`)

* **Real-time Counters:**
* Total Registered Seed Domains.
* Discovered Child Links Count.
* Unprocessed Raw Files in Storage vs. Processing Queue vs. Processed.
* Active Temporal Scraper Workflows.


* **System Status Indicators:** Visual status badges (`GREEN`/`YELLOW`/`RED`) for Kafka, Spark, MinIO, Redis Bloom Filter, and OpenSearch.

#### 3. Cluster & Node Metrics (`/app/admin/nodes/page.tsx`)

* Charts rendered using **Recharts**. Shows CPU %, RAM usage, and Disk I/O across K8s node pools (Go Scraper Workers, Spark Executors, Kafka Brokers).

#### 4. Domain & Link Crawl Manager (`/app/admin/domains/page.tsx`)

* **Domain Table:** Displays domain name, category, status (`CRAWLING`, `PAUSED`, `FAILED`), child links count, and last crawl timestamp.
* **Actions:** Add new seed domains via dynamic modal, Pause/Resume crawls, or force full Re-Crawl.

#### 5. Storage & Processing Queue (`/app/admin/storage/page.tsx`)

* Inspects MinIO DFS storage statistics. Lists raw HTML/PDF file count, storage space utilized (GB/TB), and processing lag.

#### 6. Error Logs & Malware Audit (`/app/admin/logs/page.tsx`)

* **Live Error Log Feed:** Streaming error terminal showing scraper connection failures and rate-limiting blocks.
* **Quarantine Audit Table:** Lists infected files blocked by ClamAV, displaying the threat signature, source URL, and options to permanently erase infected binaries.

---

## 6. API Communication & Data Fetching Layer

### 6.1 API Client Setup (`lib/api-client.ts`)

The application uses an Axios client pre-configured with interceptors to automatically append JWT tokens for admin endpoints and handle errors globally.

```typescript
import axios from "axios";
import Cookies from "js-cookie";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8000";

export const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    "Content-Type": "application/json",
  },
  timeout: 10000,
});

// Interceptor for Admin JWT Auth Injection
apiClient.interceptors.request.use((config) => {
  if (config.url?.startsWith("/api/v1/admin")) {
    const token = Cookies.get("admin_jwt_token");
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
  }
  return config;
});

// Interceptor for 401 Unauthorized handling
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401 && typeof window !== "undefined") {
      if (window.location.pathname.startsWith("/admin")) {
        Cookies.remove("admin_jwt_token");
        window.location.href = "/admin/login";
      }
    }
    return Promise.reject(error);
  }
);

```

---

### 6.2 Data Fetching Strategy Hooks

#### 1. User Search Hook (`hooks/useSearch.ts`)

Uses TanStack Query (`useQuery`) for automatic caching, debouncing, and refetching during search parameter changes.

```typescript
import { useQuery } from "@tanstack/react-query";
import { apiClient } from "@/lib/api-client";
import { SearchResponse } from "@/types/api";

interface SearchParams {
  q?: string;
  province_code?: string;
  district_code?: string;
  municipality_id?: string;
  page?: number;
}

export function useSearch(params: SearchParams) {
  return useQuery<SearchResponse>({
    queryKey: ["search", params],
    queryFn: async () => {
      const response = await apiClient.get("/api/v1/user/search", { params });
      return response.data;
    },
    enabled: Boolean(params.q || params.municipality_id || params.district_code || params.province_code),
    staleTime: 1000 * 60 * 5, // Cache results for 5 minutes
  });
}

```

#### 2. Admin Metrics Polling Hook (`hooks/useAdminMetrics.ts`)

Polls the API Gateway every 5 seconds to update cluster health dashboards in real time.

```typescript
import { useQuery } from "@tanstack/react-query";
import { apiClient } from "@/lib/api-client";
import { AdminSummaryResponse } from "@/types/admin";

export function useAdminSummary() {
  return useQuery<AdminSummaryResponse>({
    queryKey: ["adminSummary"],
    queryFn: async () => {
      const response = await apiClient.get("/api/v1/admin/dashboard/summary");
      return response.data;
    },
    refetchInterval: 5000, // Real-time polling every 5 seconds
  });
}

```

---

## 7. Frontend UI Wireframes & Layout Mockups

### 7.1 Search Engine Results Page (User Portal)

```text
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  [ LOGO ]  🔍 [ lok sewa syllabus 2080                     ] [Search]       [ EN | नेपाली ]            │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ TABS: [ All ]  [ Documents (PDF) ]  [ Official Portals ]                                              │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                        │
│ ┌──────────────────────────────────────────────┐ ┌───────────────────────────────────────────────────┐ │
│ │ INTERACTIVE NEPAL VECTOR MAP                 │ │ REGIONAL KNOWLEDGE CARD                           │ │
│ │                                              │ │ Pokhara Metropolitan City (पोखरा महानगरपालिका)     │ │
│ │ ┌──────────────────────────────────────────┐ │ │ District: Kaski | Province: Gandaki               │ │
│ │ │                                          │ │ │ Official Portal: https://pokharamun.gov.np         │ │
│ │ │           [ Gandaki (Active) ]           │ │ │ 📞 +977-61-521105 | ✉️ info@pokharamun.gov.np     │ │
│ │ │                                          │ │ │ ------------------------------------------------- │ │
│ │ └──────────────────────────────────────────┘ │ │ Direct Links: Wards 1-33 | DAO Kaski              │ │
│ │ Click region to filter results               │ └───────────────────────────────────────────────────┘ │
│ └──────────────────────────────────────────────┘                                                       │
│                                                                                                        │
│ ┌────────────────────────────────────────────────────────────────────────────────────────────────────┐ │
│ │ SEARCH RESULTS (184 hits found in 0.024s)                                                          │ │
│ ├────────────────────────────────────────────────────────────────────────────────────────────────────┤ │
│ │ 1. Pokhara Metropolitan City - Official Portal                                                     │ │
│ │    https://pokharamun.gov.np                                                                       │ │
│ │    Official website of Pokhara Municipality presenting notices, budgets, and ward directories...   │ │
│ │                                                                                                    │ │
│ │ 2. [PDF] Pokhara_Annual_Budget_2080.pdf                                                            │ │
│ │    Source: pokharamun.gov.np • Size: 3.1 MB                                                        │ │
│ │    [ Download PDF File ]                                                                           │ │
│ └────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘

```

### 7.2 System Administration Dashboard (Admin Portal)

```text
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  ADMIN CONTROL PANEL | Search Engine Infrastructure               User: admin_operator [ Logout ]     │
├───────────────┬────────────────────────────────────────────────────────────────────────────────────────┤
│ SIDEBAR       │ SYSTEM METRICS SUMMARY                                                                 │
│               ├────────────────────┬────────────────────┬────────────────────┬─────────────────────────┤
│ 📊 Dashboard  │ Registered Domains │ Child Links Found  │ DFS Unprocessed    │ K8s Worker Pods         │
│ 🖥️ Cluster    │ 12,450             │ 14,250,000         │ 125,000 files      │ 48 Active               │
│ 🌐 Domains    ├────────────────────┴────────────────────┴────────────────────┴─────────────────────────┤
│ 💾 Storage    │ INFRASTRUCTURE CPU / RAM USAGE                                                         │
│ ⚠️ Log Audits │ ┌────────────────────────────────────────────────────────────────────────────────────┐ │
│ 🛡️ Security   │ │ [Line Chart: Spark Executor CPU Load (87%) & Scraper Fleet Network I/O]            │ │
│               │ └────────────────────────────────────────────────────────────────────────────────────┘ │
│               │ SERVICE HEALTH STATUS                                                                  │
│               │ [ Temporal: UP ]   [ Kafka: UP ]   [ MinIO: UP ]   [ Spark: UP ]   [ OpenSearch: GREEN ]   │
└───────────────┴────────────────────────────────────────────────────────────────────────────────────────┘

```