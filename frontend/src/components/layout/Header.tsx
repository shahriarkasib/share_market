import { NavLink } from "react-router-dom";
import { useEffect } from "react";
import { Activity, BarChart3, Search, Eye, Briefcase, Grid3X3, PieChart, Table2, Sun, Moon } from "lucide-react";
import { clsx } from "clsx";
import { useMarketStore } from "../../store/marketStore.ts";
import { useThemeStore } from "../../store/themeStore.ts";
import SymbolSearch from "../search/SymbolSearch.tsx";

const links = [
  { to: "/", label: "Dashboard", icon: BarChart3 },
  { to: "/heatmap", label: "Heatmap", icon: Grid3X3 },
  { to: "/sectors", label: "Sectors", icon: PieChart },
  { to: "/matrix", label: "Matrix", icon: Table2 },
  { to: "/screener", label: "Screener", icon: Search },
  { to: "/portfolio", label: "Portfolio", icon: Briefcase },
  { to: "/watchlist", label: "Watchlist", icon: Eye },
] as const;

export default function Header() {
  const marketSummary = useMarketStore((s) => s.marketSummary);
  const lastUpdated = useMarketStore((s) => s.lastUpdated);
  const { theme, toggleTheme } = useThemeStore();

  const statusText = marketSummary?.market_status ?? "---";
  const isOpen = statusText.toLowerCase().includes("open");

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
      <div className="max-w-[1440px] mx-auto px-4 lg:px-8 flex items-center h-12">
        {/* Logo */}
        <NavLink to="/" className="flex items-center gap-2 mr-8 shrink-0">
          <Activity className="h-5 w-5 text-blue-500" />
          <span className="text-sm font-bold text-[var(--text)] tracking-tight hidden sm:inline">
            DSE Trading
          </span>
        </NavLink>

        {/* Navigation tabs */}
        <nav className="flex items-center gap-1">
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
              <span className="hidden sm:inline">{label}</span>
            </NavLink>
          ))}
        </nav>

        {/* Global search */}
        <div className="ml-auto mr-3 hidden sm:block w-56 lg:w-72" data-global-search>
          <SymbolSearch placeholder="Search stock... ( / )" compact />
        </div>

        {/* Right side: theme toggle + market status + last updated */}
        <div className="flex items-center gap-3">
          {/* Theme toggle */}
          <button
            onClick={toggleTheme}
            className="p-1.5 rounded-md text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)] transition-colors"
            title={`Switch to ${theme === "light" ? "dark" : "light"} mode`}
          >
            {theme === "light" ? (
              <Moon className="h-4 w-4" />
            ) : (
              <Sun className="h-4 w-4" />
            )}
          </button>

          <span
            className={clsx(
              "flex items-center gap-1.5 text-xs font-medium",
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
            <span className="text-[10px] text-[var(--text-dim)] hidden md:block tabular-nums">
              {lastUpdated.toLocaleTimeString()}
            </span>
          )}
        </div>
      </div>
    </header>
  );
}
