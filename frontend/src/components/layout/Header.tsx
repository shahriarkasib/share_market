import { NavLink } from "react-router-dom";
import { useState, useEffect } from "react";
import { Activity, BarChart3, Search, Eye, Briefcase, Grid3X3, PieChart, Table2, Menu, X, LineChart, TrendingUp, Target } from "lucide-react";
import { clsx } from "clsx";
import { useMarketStore } from "../../store/marketStore.ts";
import SymbolSearch from "../search/SymbolSearch.tsx";

const links = [
  { to: "/", label: "Dashboard", icon: BarChart3 },
  { to: "/heatmap", label: "Heatmap", icon: Grid3X3 },
  { to: "/sectors", label: "Sectors", icon: PieChart },
  { to: "/matrix", label: "Matrix", icon: Table2 },
  { to: "/screener", label: "Screener", icon: Search },
  { to: "/portfolio", label: "Portfolio", icon: Briefcase },
  { to: "/watchlist", label: "Watchlist", icon: Eye },
  { to: "/chart", label: "Chart", icon: LineChart },
  { to: "/radar", label: "Radar", icon: Target },
  { to: "/analysis", label: "Analysis", icon: TrendingUp },
] as const;

export default function Header() {
  const marketSummary = useMarketStore((s) => s.marketSummary);
  const lastUpdated = useMarketStore((s) => s.lastUpdated);
  const [mobileOpen, setMobileOpen] = useState(false);

  const statusText = marketSummary?.market_status ?? "---";
  const isOpen = statusText.toLowerCase().includes("open");

  // Close mobile menu on route change
  useEffect(() => {
    setMobileOpen(false);
  }, []);

  // "/" shortcut focuses search
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "/" && !["INPUT", "TEXTAREA", "SELECT"].includes((e.target as Element)?.tagName)) {
        e.preventDefault();
        document.querySelector<HTMLInputElement>("[data-global-search] input")?.focus();
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, []);

  return (
    <header className="sticky top-0 z-50 bg-[var(--surface)] border-b border-[var(--border)]">
      <div className="max-w-[1440px] mx-auto px-3 sm:px-4 lg:px-8 flex items-center h-12">
        {/* Logo */}
        <NavLink to="/" className="flex items-center gap-2 mr-4 sm:mr-8 shrink-0">
          <Activity className="h-5 w-5 text-blue-500" />
          <span className="text-sm font-bold text-[var(--text)] tracking-tight hidden sm:inline">
            DSE Trading
          </span>
        </NavLink>

        {/* Desktop navigation */}
        <nav className="hidden md:flex items-center gap-1">
          {links.map(({ to, label, icon: Icon }) => (
            <NavLink
              key={to}
              to={to}
              end={to === "/"}
              className={({ isActive }) =>
                clsx(
                  "flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors",
                  isActive
                    ? "bg-[var(--surface-active)] text-[var(--text)]"
                    : "text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)]",
                )
              }
            >
              <Icon className="h-3.5 w-3.5 shrink-0" />
              <span>{label}</span>
            </NavLink>
          ))}
        </nav>

        {/* Global search (desktop) */}
        <div className="ml-auto mr-3 hidden sm:block w-56 lg:w-72" data-global-search>
          <SymbolSearch placeholder="Search stock... ( / )" compact />
        </div>

        {/* Right side */}
        <div className="ml-auto sm:ml-0 flex items-center gap-2 sm:gap-3">
          <span
            className={clsx(
              "hidden sm:flex items-center gap-1.5 text-xs font-medium",
              isOpen ? "text-green-500" : "text-[var(--text-dim)]",
            )}
          >
            <span
              className={clsx(
                "h-1.5 w-1.5 rounded-full",
                isOpen ? "bg-green-500" : "bg-[var(--text-dim)]",
              )}
            />
            {statusText}
          </span>
          {lastUpdated && (
            <span className="text-[10px] text-[var(--text-dim)] hidden lg:block tabular-nums">
              {lastUpdated.toLocaleTimeString()}
            </span>
          )}

          {/* Mobile hamburger */}
          <button
            onClick={() => setMobileOpen(!mobileOpen)}
            className="md:hidden p-1.5 rounded-md text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)] transition-colors"
          >
            {mobileOpen ? <X className="h-4 w-4" /> : <Menu className="h-4 w-4" />}
          </button>
        </div>
      </div>

      {/* Mobile nav drawer */}
      {mobileOpen && (
        <nav className="md:hidden border-t border-[var(--border)] bg-[var(--surface)] px-3 pb-3 pt-2 space-y-1">
          {/* Mobile search */}
          <div className="mb-2 sm:hidden" data-global-search>
            <SymbolSearch placeholder="Search stock..." compact />
          </div>
          {links.map(({ to, label, icon: Icon }) => (
            <NavLink
              key={to}
              to={to}
              end={to === "/"}
              onClick={() => setMobileOpen(false)}
              className={({ isActive }) =>
                clsx(
                  "flex items-center gap-2.5 px-3 py-2.5 rounded-md text-sm font-medium transition-colors",
                  isActive
                    ? "bg-[var(--surface-active)] text-[var(--text)]"
                    : "text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)]",
                )
              }
            >
              <Icon className="h-4 w-4 shrink-0" />
              {label}
            </NavLink>
          ))}
          {/* Mobile market status */}
          <div className="flex items-center gap-2 px-3 pt-2 border-t border-[var(--border)] mt-2">
            <span
              className={clsx(
                "h-1.5 w-1.5 rounded-full",
                isOpen ? "bg-green-500" : "bg-[var(--text-dim)]",
              )}
            />
            <span className={clsx("text-xs", isOpen ? "text-green-500" : "text-[var(--text-dim)]")}>
              {statusText}
            </span>
          </div>
        </nav>
      )}
    </header>
  );
}
