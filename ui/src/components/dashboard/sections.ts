import { Globe, LayoutDashboard, Search, ShieldAlert, Workflow, type LucideIcon } from "lucide-react";

// The dashboard's sections, in page order. The sidebar and the mobile section
// bar both link to these ids.
export const SECTIONS: { id: string; label: string; icon: LucideIcon }[] = [
  { id: "overview", label: "Overview", icon: LayoutDashboard },
  { id: "pipeline", label: "Pipeline", icon: Workflow },
  { id: "websites", label: "Websites", icon: Globe },
  { id: "search", label: "Search traffic", icon: Search },
  { id: "security", label: "Errors & security", icon: ShieldAlert },
];
