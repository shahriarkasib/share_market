/**
 * Seasonality Dashboard -- monthly sector heatmap, stock patterns, and outlook.
 *
 * Three tabs:
 *  1. Monthly Sector Heatmap (avg return per sector per month, color-coded)
 *  2. Stock Patterns (per-stock up_pct by month, filterable/sortable)
 *  3. This Month's Outlook (strong/weak sectors and stocks for the current month)
 */

import React, { useEffect, useState, useMemo, useCallback } from "react";
import { Calendar, Loader2, AlertCircle, ArrowUpDown, Search, Plus, Minus } from "lucide-react";
import { clsx } from "clsx";
import {
  fetchMonthlySectorSeasonality,
  fetchMonthlyStockSeasonality,
  fetchSeasonalOutlook,
  fetchSectorYearlyDetail,
  fetchStockYearlyDetail,
} from "../api/client.ts";
import type {
  SectorSeasonality,
  StockSeasonality,
  SeasonalOutlook,
  SectorYearlyDetail,
  StockYearlyDetail,
} from "../api/client.ts";

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const MONTH_LABELS = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

const TABS = ["Sector Heatmap", "Stock Patterns", "This Month"] as const;
type TabId = (typeof TABS)[number];

const CURRENT_MONTH = new Date().getMonth() + 1; // 1-based

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

/** Return a CSS background color based on return value. */
function returnBg(value: number): string {
  if (value >= 5) return "rgba(34,197,94,0.45)";
  if (value >= 3) return "rgba(34,197,94,0.32)";
  if (value >= 1) return "rgba(34,197,94,0.18)";
  if (value > 0) return "rgba(34,197,94,0.08)";
  if (value > -1) return "rgba(239,68,68,0.08)";
  if (value > -3) return "rgba(239,68,68,0.18)";
  if (value > -5) return "rgba(239,68,68,0.32)";
  return "rgba(239,68,68,0.45)";
}

/** Color for up_pct cells. */
function upPctBg(pct: number): string {
  if (pct >= 70) return "rgba(34,197,94,0.30)";
  if (pct >= 50) return "rgba(34,197,94,0.12)";
  if (pct >= 30) return "rgba(239,68,68,0.12)";
  return "rgba(239,68,68,0.30)";
}

function fmtPct(v: number): string {
  return v.toFixed(1) + "%";
}

/* ------------------------------------------------------------------ */
/*  Loading / Error states                                             */
/* ------------------------------------------------------------------ */

function LoadingState() {
  return (
    <div className="flex items-center justify-center py-24 text-[var(--text-muted)]">
      <Loader2 className="h-5 w-5 animate-spin mr-2" />
      Loading seasonal data...
    </div>
  );
}

function ErrorState({ message }: { message: string }) {
  return (
    <div className="flex items-center justify-center py-24 text-red-400 gap-2 text-sm">
      <AlertCircle className="h-4 w-4" />
      {message}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Tab 1: Monthly Sector Heatmap                                      */
/* ------------------------------------------------------------------ */

function YearSelector({ years, selected, onChange }: {
  years: number[]; selected: number; onChange: (y: number) => void;
}) {
  return (
    <div className="flex items-center gap-1.5 flex-wrap">
      <button
        onClick={() => onChange(0)}
        className={clsx(
          "px-2.5 py-1 text-xs rounded-md border transition-colors",
          selected === 0
            ? "bg-blue-500/15 border-blue-500/40 text-blue-400"
            : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]",
        )}
      >
        Overall
      </button>
      {years.map((y) => (
        <button
          key={y}
          onClick={() => onChange(y)}
          className={clsx(
            "px-2.5 py-1 text-xs rounded-md border transition-colors",
            selected === y
              ? "bg-blue-500/15 border-blue-500/40 text-blue-400"
              : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]",
          )}
        >
          {y}
        </button>
      ))}
    </div>
  );
}

function SectorHeatmapTab() {
  const [sectors, setSectors] = useState<SectorSeasonality[]>([]);
  const [yearlyData, setYearlyData] = useState<SectorYearlyDetail | null>(null);
  const [years, setYears] = useState<number[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedMonths, setExpandedMonths] = useState<Set<number>>(new Set());
  const [expandYears, setExpandYears] = useState(5);

  // Load both overall + yearly detail in parallel
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    Promise.all([
      fetchMonthlySectorSeasonality(),
      fetchSectorYearlyDetail(),
    ])
      .then(([sectorRes, yearlyRes]) => {
        if (cancelled) return;
        setSectors(sectorRes.sectors);
        if (sectorRes.years?.length) setYears(sectorRes.years);
        setYearlyData(yearlyRes);
      })
      .catch((err) => { if (!cancelled) setError(err instanceof Error ? err.message : "Failed to load"); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, []);

  const toggleMonth = useCallback((month: number) => {
    setExpandedMonths((prev) => {
      const next = new Set(prev);
      if (next.has(month)) next.delete(month); else next.add(month);
      return next;
    });
  }, []);

  if (loading) return <LoadingState />;
  if (error) return <ErrorState message={error} />;
  if (!sectors.length) return <ErrorState message="No sector seasonality data available" />;

  const visibleYears = years.slice(0, expandYears);

  return (
    <div className="space-y-3">
      {/* Controls */}
      <div className="flex items-center gap-3 flex-wrap text-xs">
        <span className="text-[var(--text-muted)]">Click month headers to expand yearly breakdown</span>
        <span className="text-[var(--text-dim)]">|</span>
        <label className="flex items-center gap-1.5 text-[var(--text-muted)]">
          Expand years:
          <select
            value={expandYears}
            onChange={(e) => setExpandYears(Number(e.target.value))}
            className="px-1.5 py-0.5 rounded border border-[var(--border)] bg-[var(--bg)] text-[var(--text)] text-xs"
          >
            <option value={3}>3</option>
            <option value={5}>5</option>
            <option value={7}>7</option>
            <option value={12}>All</option>
          </select>
        </label>
        {expandedMonths.size > 0 && (
          <button
            onClick={() => setExpandedMonths(new Set())}
            className="text-blue-400 hover:text-blue-300"
          >
            Collapse all
          </button>
        )}
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr>
              <th className="text-left px-2 py-2 text-[var(--text-muted)] font-medium sticky left-0 bg-[var(--surface)] z-10 min-w-[140px]">
                Sector
              </th>
              {MONTH_LABELS.map((m, i) => {
                const month = i + 1;
                const isExpanded = expandedMonths.has(month);
                return (
                  <th
                    key={m}
                    colSpan={isExpanded ? 1 + visibleYears.length : 1}
                    className={clsx(
                      "px-1 py-2 text-center font-medium cursor-pointer select-none",
                      month === CURRENT_MONTH
                        ? "text-blue-400 border-b-2 border-blue-500"
                        : "text-[var(--text-muted)]",
                    )}
                    onClick={() => toggleMonth(month)}
                  >
                    <span className="inline-flex items-center gap-0.5">
                      {isExpanded ? <Minus className="h-3 w-3" /> : <Plus className="h-3 w-3" />}
                      {m}
                    </span>
                  </th>
                );
              })}
            </tr>
            {/* Sub-header row for expanded year columns */}
            {expandedMonths.size > 0 && (
              <tr>
                <th className="sticky left-0 bg-[var(--surface)] z-10" />
                {MONTH_LABELS.map((_, i) => {
                  const month = i + 1;
                  if (!expandedMonths.has(month)) {
                    return <th key={i} className="text-[10px] text-[var(--text-dim)] px-1 py-0.5">Avg</th>;
                  }
                  return (
                    <React.Fragment key={i}>
                      <th className="text-[10px] text-[var(--text-dim)] px-1 py-0.5">Avg</th>
                      {visibleYears.map((y) => (
                        <th key={`${i}-${y}`} className="text-[10px] text-[var(--text-dim)] px-1 py-0.5 min-w-[48px]">
                          {String(y).slice(2)}
                        </th>
                      ))}
                    </React.Fragment>
                  );
                })}
              </tr>
            )}
          </thead>
          <tbody>
            {sectors.map((sector) => {
              const monthMap = new Map(sector.months.map((md) => [md.month, md]));
              const sectorYearly = yearlyData?.sectors?.[sector.name] ?? {};

              return (
                <tr key={sector.name} className="border-t border-[var(--border)]">
                  <td className="px-2 py-1.5 text-[var(--text)] font-medium sticky left-0 bg-[var(--surface)] z-10 whitespace-nowrap">
                    {sector.name}
                  </td>
                  {Array.from({ length: 12 }, (_, i) => {
                    const month = i + 1;
                    const md = monthMap.get(month);
                    const isExpanded = expandedMonths.has(month);

                    // Overall avg cell
                    const avgCell = !md ? (
                      <td key={i} className="px-1 py-1.5 text-center text-[var(--text-dim)]">--</td>
                    ) : (
                      <td
                        key={i}
                        className={clsx(
                          "px-1 py-1.5 text-center tabular-nums",
                          month === CURRENT_MONTH && !isExpanded && "ring-1 ring-blue-500/40 rounded",
                        )}
                        style={{ background: returnBg(md.avg_return * 100) }}
                        title={`Win: ${(md.win_rate * 100).toFixed(0)}% | Samples: ${md.sample_size}`}
                      >
                        {md.avg_return >= 0 ? "+" : ""}{(md.avg_return * 100).toFixed(1)}%
                      </td>
                    );

                    if (!isExpanded) return avgCell;

                    // Expanded: avg + per-year cells
                    return (
                      <React.Fragment key={`exp-${i}`}>
                        {avgCell}
                        {visibleYears.map((y) => {
                          const val = sectorYearly[String(y)]?.[String(month)];
                          if (val === undefined) {
                            return (
                              <td key={`${i}-${y}`} className="px-1 py-1.5 text-center text-[var(--text-dim)] text-[10px]">
                                --
                              </td>
                            );
                          }
                          const pct = val * 100;
                          return (
                            <td
                              key={`${i}-${y}`}
                              className="px-1 py-1.5 text-center tabular-nums text-[10px]"
                              style={{ background: returnBg(pct) }}
                            >
                              {pct >= 0 ? "+" : ""}{pct.toFixed(1)}%
                            </td>
                          );
                        })}
                      </React.Fragment>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Tab 2: Stock Patterns                                              */
/* ------------------------------------------------------------------ */

type SortKey = "symbol" | "sector" | number;

function StockPatternsTab() {
  const [stocks, setStocks] = useState<StockSeasonality[]>([]);
  const [yearlyData, setYearlyData] = useState<StockYearlyDetail | null>(null);
  const [sectorList, setSectorList] = useState<string[]>([]);
  const [selectedSector, setSelectedSector] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("symbol");
  const [sortAsc, setSortAsc] = useState(true);
  const [expandedMonths, setExpandedMonths] = useState<Set<number>>(new Set());
  const [expandYears, setExpandYears] = useState(5);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    Promise.all([
      fetchMonthlyStockSeasonality(
        "A",
        undefined,
        selectedSector || undefined,
      ),
      fetchStockYearlyDetail("A"),
    ])
      .then(([stockRes, yearlyRes]) => {
        if (cancelled) return;
        setStocks(stockRes.stocks);
        if (stockRes.sectors?.length) setSectorList(stockRes.sectors);
        setYearlyData(yearlyRes);
      })
      .catch((err) => { if (!cancelled) setError(err instanceof Error ? err.message : "Failed to load"); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [selectedSector]);

  const handleSort = useCallback(
    (key: SortKey) => {
      if (sortKey === key) {
        setSortAsc((prev) => !prev);
      } else {
        setSortKey(key);
        setSortAsc(key === "symbol" || key === "sector");
      }
    },
    [sortKey],
  );

  const toggleMonth = useCallback((month: number) => {
    setExpandedMonths((prev) => {
      const next = new Set(prev);
      if (next.has(month)) next.delete(month); else next.add(month);
      return next;
    });
  }, []);

  const filtered = useMemo(() => {
    let list = stocks;
    if (search) {
      const q = search.toUpperCase();
      list = list.filter(
        (s) => s.symbol.includes(q) || (s.sector || "").toUpperCase().includes(q),
      );
    }
    const sorted = [...list].sort((a, b) => {
      if (sortKey === "symbol") return a.symbol.localeCompare(b.symbol);
      if (sortKey === "sector") return (a.sector || "").localeCompare(b.sector || "");
      // month number
      const aMonth = a.months.find((m) => m.month === sortKey);
      const bMonth = b.months.find((m) => m.month === sortKey);
      return (aMonth?.up_pct ?? 0) - (bMonth?.up_pct ?? 0);
    });
    return sortAsc ? sorted : sorted.reverse();
  }, [stocks, search, sortKey, sortAsc]);

  if (loading && !stocks.length) return <LoadingState />;
  if (error) return <ErrorState message={error} />;

  const allYears = yearlyData?.years ?? [];
  const visibleYears = allYears.slice(0, expandYears);

  return (
    <div className="space-y-3">
      {/* Sector filter + search + expand controls */}
      <div className="flex items-center gap-3 flex-wrap">
        <select
          value={selectedSector}
          onChange={(e) => setSelectedSector(e.target.value)}
          className="px-2 py-1.5 text-xs rounded-md border border-[var(--border)] bg-[var(--bg)] text-[var(--text)] focus:outline-none focus:ring-1 focus:ring-blue-500"
        >
          <option value="">All Sectors</option>
          {sectorList.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>

        <div className="relative max-w-xs flex-1">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-[var(--text-dim)]" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Filter by symbol..."
            className="w-full pl-8 pr-3 py-1.5 text-xs rounded-md bg-[var(--bg)] border border-[var(--border)] text-[var(--text)] placeholder:text-[var(--text-dim)] focus:outline-none focus:ring-1 focus:ring-blue-500"
          />
        </div>

        <span className="text-[var(--text-dim)] text-xs">|</span>

        <label className="flex items-center gap-1.5 text-[var(--text-muted)] text-xs">
          Expand years:
          <select
            value={expandYears}
            onChange={(e) => setExpandYears(Number(e.target.value))}
            className="px-1.5 py-0.5 rounded border border-[var(--border)] bg-[var(--bg)] text-[var(--text)] text-xs"
          >
            <option value={3}>3</option>
            <option value={5}>5</option>
            <option value={7}>7</option>
            <option value={12}>All</option>
          </select>
        </label>
        {expandedMonths.size > 0 && (
          <button
            onClick={() => setExpandedMonths(new Set())}
            className="text-blue-400 hover:text-blue-300 text-xs"
          >
            Collapse all
          </button>
        )}
      </div>

      <div className="text-[10px] text-[var(--text-dim)]">
        {filtered.length} stocks{selectedSector ? ` in ${selectedSector}` : ""} (overall) | Click month headers to expand yearly breakdown
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr>
              <th
                className="text-left px-2 py-2 text-[var(--text-muted)] font-medium sticky left-0 bg-[var(--surface)] z-10 min-w-[80px] cursor-pointer select-none"
                onClick={() => handleSort("symbol")}
              >
                <span className="inline-flex items-center gap-0.5">
                  Symbol
                  {sortKey === "symbol" && <ArrowUpDown className="h-3 w-3 text-blue-400" />}
                </span>
              </th>
              <th
                className="px-2 py-2 text-left text-[var(--text-muted)] font-medium cursor-pointer select-none whitespace-nowrap"
                onClick={() => handleSort("sector")}
              >
                <span className="inline-flex items-center gap-0.5">
                  Sector
                  {sortKey === "sector" && <ArrowUpDown className="h-3 w-3 text-blue-400" />}
                </span>
              </th>
              {MONTH_LABELS.map((m, i) => {
                const month = i + 1;
                const isExpanded = expandedMonths.has(month);
                return (
                  <th
                    key={m}
                    colSpan={isExpanded ? 1 + visibleYears.length : 1}
                    className={clsx(
                      "px-1 py-2 text-center font-medium cursor-pointer select-none min-w-[48px]",
                      month === CURRENT_MONTH
                        ? "text-blue-400 border-b-2 border-blue-500"
                        : "text-[var(--text-muted)]",
                    )}
                    onClick={() => toggleMonth(month)}
                  >
                    <span className="inline-flex items-center gap-0.5">
                      {isExpanded ? <Minus className="h-3 w-3" /> : <Plus className="h-3 w-3" />}
                      {m}
                    </span>
                  </th>
                );
              })}
            </tr>
            {/* Sub-header row for expanded year columns */}
            {expandedMonths.size > 0 && (
              <tr>
                <th className="sticky left-0 bg-[var(--surface)] z-10" />
                <th />
                {MONTH_LABELS.map((_, i) => {
                  const month = i + 1;
                  if (!expandedMonths.has(month)) {
                    return <th key={i} className="text-[10px] text-[var(--text-dim)] px-1 py-0.5">Avg</th>;
                  }
                  return (
                    <React.Fragment key={i}>
                      <th className="text-[10px] text-[var(--text-dim)] px-1 py-0.5">Avg</th>
                      {visibleYears.map((y) => (
                        <th key={`${i}-${y}`} className="text-[10px] text-[var(--text-dim)] px-1 py-0.5 min-w-[48px]">
                          {String(y).slice(2)}
                        </th>
                      ))}
                    </React.Fragment>
                  );
                })}
              </tr>
            )}
          </thead>
          <tbody>
            {filtered.map((stock) => {
              const monthMap = new Map(stock.months.map((m) => [m.month, m]));
              const stockYearly = yearlyData?.stocks?.[stock.symbol] ?? {};

              return (
                <tr key={stock.symbol} className="border-t border-[var(--border)] hover:bg-[var(--hover)]">
                  <td className="px-2 py-1.5 font-medium text-[var(--text)] sticky left-0 bg-[var(--surface)] z-10 whitespace-nowrap">
                    {stock.symbol}
                  </td>
                  <td className="px-2 py-1.5 text-[var(--text-muted)] whitespace-nowrap">
                    {stock.sector}
                  </td>
                  {Array.from({ length: 12 }, (_, i) => {
                    const month = i + 1;
                    const md = monthMap.get(month);
                    const isExpanded = expandedMonths.has(month);

                    // Overall avg cell (up_pct)
                    const avgCell = !md ? (
                      <td key={i} className="px-1 py-1.5 text-center text-[var(--text-dim)]">--</td>
                    ) : (
                      <td
                        key={i}
                        className={clsx(
                          "px-1 py-1.5 text-center tabular-nums",
                          month === CURRENT_MONTH && !isExpanded && "ring-1 ring-blue-500/40 rounded",
                        )}
                        style={{ background: upPctBg(md.up_pct * 100) }}
                        title={`Up ${md.years_up}/${md.years_total} years | Avg return: ${(md.avg_return * 100).toFixed(1)}%`}
                      >
                        {(md.up_pct * 100).toFixed(0)}%
                      </td>
                    );

                    if (!isExpanded) return avgCell;

                    // Expanded: avg + per-year cells
                    return (
                      <React.Fragment key={`exp-${i}`}>
                        {avgCell}
                        {visibleYears.map((y) => {
                          const val = stockYearly[String(y)]?.[String(month)];
                          if (val === undefined) {
                            return (
                              <td key={`${i}-${y}`} className="px-1 py-1.5 text-center text-[var(--text-dim)] text-[10px]">
                                --
                              </td>
                            );
                          }
                          const pct = val * 100;
                          return (
                            <td
                              key={`${i}-${y}`}
                              className="px-1 py-1.5 text-center tabular-nums text-[10px]"
                              style={{ background: returnBg(pct) }}
                            >
                              {pct >= 0 ? "+" : ""}{pct.toFixed(1)}%
                            </td>
                          );
                        })}
                      </React.Fragment>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function SortableHeader({
  label,
  sortKey,
  current,
  asc,
  onClick,
  sticky,
  highlight,
}: {
  label: string;
  sortKey: SortKey;
  current: SortKey;
  asc: boolean;
  onClick: (key: SortKey) => void;
  sticky?: boolean;
  highlight?: boolean;
}) {
  const active = current === sortKey;
  return (
    <th
      className={clsx(
        "px-2 py-2 text-center font-medium cursor-pointer select-none whitespace-nowrap",
        sticky && "text-left sticky left-0 bg-[var(--surface)] z-10 min-w-[80px]",
        highlight ? "text-blue-400 border-b-2 border-blue-500" : "text-[var(--text-muted)]",
        !sticky && "min-w-[48px]",
      )}
      onClick={() => onClick(sortKey)}
    >
      <span className="inline-flex items-center gap-0.5">
        {label}
        {active && (
          <ArrowUpDown className="h-3 w-3 text-blue-400" />
        )}
      </span>
    </th>
  );
}

/* ------------------------------------------------------------------ */
/*  Tab 3: This Month's Outlook                                        */
/* ------------------------------------------------------------------ */

const MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

function OutlookTab() {
  const now = new Date();
  const thisMonth = now.getMonth() + 1;
  const nextMonth = thisMonth === 12 ? 1 : thisMonth + 1;

  const [selectedMonth, setSelectedMonth] = useState(thisMonth);
  const [outlook, setOutlook] = useState<SeasonalOutlook | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetchSeasonalOutlook(selectedMonth)
      .then((res) => { if (!cancelled) setOutlook(res); })
      .catch((err) => { if (!cancelled) setError(err instanceof Error ? err.message : "Failed to load"); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [selectedMonth]);

  if (loading) return <LoadingState />;
  if (error) return <ErrorState message={error} />;
  if (!outlook) return <ErrorState message="No outlook data available" />;

  const topSectors = outlook.top_sectors || [];
  const bottomSectors = outlook.bottom_sectors || [];
  const topStocks = outlook.top_stocks || [];
  const bottomStocks = outlook.bottom_stocks || [];
  const yearly = outlook.yearly || [];

  return (
    <div className="space-y-5">
      {/* Month selector */}
      <div className="flex items-center gap-2 flex-wrap">
        <button
          onClick={() => setSelectedMonth(thisMonth)}
          className={clsx(
            "px-3 py-1.5 text-xs rounded-md border transition-colors",
            selectedMonth === thisMonth
              ? "bg-blue-500/15 border-blue-500/40 text-blue-400"
              : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]",
          )}
        >
          This Month ({MONTH_NAMES[thisMonth - 1]})
        </button>
        <button
          onClick={() => setSelectedMonth(nextMonth)}
          className={clsx(
            "px-3 py-1.5 text-xs rounded-md border transition-colors",
            selectedMonth === nextMonth
              ? "bg-blue-500/15 border-blue-500/40 text-blue-400"
              : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]",
          )}
        >
          Next Month ({MONTH_NAMES[nextMonth - 1]})
        </button>
        <select
          value={selectedMonth}
          onChange={(e) => setSelectedMonth(Number(e.target.value))}
          className="px-2 py-1.5 text-xs rounded-md border border-[var(--border)] bg-[var(--bg)] text-[var(--text)] focus:outline-none focus:ring-1 focus:ring-blue-500"
        >
          {MONTH_NAMES.map((m, i) => (
            <option key={i} value={i + 1}>{m}</option>
          ))}
        </select>
      </div>

      <h2 className="text-sm font-semibold text-[var(--text)]">
        Seasonal Outlook for {outlook.month_name}
      </h2>

      {/* Yearly breakdown */}
      {yearly.length > 0 && (
        <div>
          <h3 className="text-xs font-semibold text-[var(--text-muted)] mb-2">
            How {outlook.month_name} Actually Performed Each Year
          </h3>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-2">
            {yearly.map((y) => {
              const pct = y.avg_return * 100;
              const isUp = pct > 0;
              return (
                <div
                  key={y.year}
                  className={clsx(
                    "rounded-lg border px-3 py-2 text-center",
                    isUp ? "border-green-500/20 bg-green-500/5" : "border-red-500/20 bg-red-500/5",
                  )}
                >
                  <div className="text-xs font-semibold text-[var(--text)]">{y.year}</div>
                  <div className={clsx("text-sm font-bold tabular-nums", isUp ? "text-green-400" : "text-red-400")}>
                    {isUp ? "+" : ""}{pct.toFixed(1)}%
                  </div>
                  <div className="text-[10px] text-[var(--text-dim)] mt-0.5">
                    {y.stocks_up}↑ {y.stocks_down}↓
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Strong Sectors */}
      <Section title={`Strong Sectors in ${outlook.month_name}`} variant="green">
        {topSectors.length === 0 ? (
          <p className="text-xs text-[var(--text-dim)]">No strong sectors identified</p>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
            {topSectors.map((s) => (
              <div
                key={s.sector}
                className="rounded-lg border border-green-500/20 bg-green-500/5 px-3 py-2"
              >
                <div className="text-xs font-medium text-[var(--text)]">{s.sector}</div>
                <div className="flex items-center gap-3 mt-1 text-[10px]">
                  <span className="text-green-400">Avg: +{(s.avg_return * 100).toFixed(1)}%</span>
                  <span className="text-[var(--text-muted)]">Win: {(s.win_rate * 100).toFixed(0)}%</span>
                </div>
              </div>
            ))}
          </div>
        )}
      </Section>

      {/* Weak Sectors */}
      <Section title={`Weak Sectors in ${outlook.month_name}`} variant="red">
        {bottomSectors.length === 0 ? (
          <p className="text-xs text-[var(--text-dim)]">No weak sectors identified</p>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
            {bottomSectors.map((s) => (
              <div
                key={s.sector}
                className="rounded-lg border border-red-500/20 bg-red-500/5 px-3 py-2"
              >
                <div className="text-xs font-medium text-[var(--text)]">{s.sector}</div>
                <div className="flex items-center gap-3 mt-1 text-[10px]">
                  <span className="text-red-400">Avg: {(s.avg_return * 100).toFixed(1)}%</span>
                  <span className="text-[var(--text-muted)]">Win: {(s.win_rate * 100).toFixed(0)}%</span>
                </div>
              </div>
            ))}
          </div>
        )}
      </Section>

      {/* Strong Stocks — scrollable */}
      <Section title={`Strong Stocks in ${outlook.month_name} (Top ${topStocks.length})`} variant="green">
        {topStocks.length === 0 ? (
          <p className="text-xs text-[var(--text-dim)]">No data</p>
        ) : (
          <div className="overflow-y-auto max-h-[400px]">
            <table className="w-full text-xs">
              <thead className="sticky top-0 bg-[var(--surface)]">
                <tr className="text-[var(--text-muted)]">
                  <th className="text-left px-2 py-1.5 font-medium">#</th>
                  <th className="text-left px-2 py-1.5 font-medium">Symbol</th>
                  <th className="text-left px-2 py-1.5 font-medium">Sector</th>
                  <th className="text-right px-2 py-1.5 font-medium">Avg Return</th>
                  <th className="text-right px-2 py-1.5 font-medium">Win %</th>
                  <th className="text-right px-2 py-1.5 font-medium">Years</th>
                </tr>
              </thead>
              <tbody>
                {topStocks.map((s, i) => (
                  <tr key={s.symbol} className="border-t border-[var(--border)] hover:bg-[var(--hover)]">
                    <td className="px-2 py-1.5 text-[var(--text-dim)]">{i + 1}</td>
                    <td className="px-2 py-1.5 font-medium text-[var(--text)]">{s.symbol}</td>
                    <td className="px-2 py-1.5 text-[var(--text-muted)]">{s.sector || "—"}</td>
                    <td className="px-2 py-1.5 text-right tabular-nums text-green-400">
                      +{(s.avg_return * 100).toFixed(1)}%
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums text-[var(--text-muted)]">
                      {(s.win_rate * 100).toFixed(0)}%
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums text-[var(--text-dim)]">
                      {s.sample_size}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Section>

      {/* Weak Stocks — scrollable */}
      <Section title={`Weak Stocks in ${outlook.month_name} (Bottom ${bottomStocks.length})`} variant="red">
        {bottomStocks.length === 0 ? (
          <p className="text-xs text-[var(--text-dim)]">No data</p>
        ) : (
          <div className="overflow-y-auto max-h-[400px]">
            <table className="w-full text-xs">
              <thead className="sticky top-0 bg-[var(--surface)]">
                <tr className="text-[var(--text-muted)]">
                  <th className="text-left px-2 py-1.5 font-medium">#</th>
                  <th className="text-left px-2 py-1.5 font-medium">Symbol</th>
                  <th className="text-left px-2 py-1.5 font-medium">Sector</th>
                  <th className="text-right px-2 py-1.5 font-medium">Avg Return</th>
                  <th className="text-right px-2 py-1.5 font-medium">Win %</th>
                  <th className="text-right px-2 py-1.5 font-medium">Years</th>
                </tr>
              </thead>
              <tbody>
                {bottomStocks.map((s, i) => (
                  <tr key={s.symbol} className="border-t border-[var(--border)] hover:bg-[var(--hover)]">
                    <td className="px-2 py-1.5 text-[var(--text-dim)]">{i + 1}</td>
                    <td className="px-2 py-1.5 font-medium text-[var(--text)]">{s.symbol}</td>
                    <td className="px-2 py-1.5 text-[var(--text-muted)]">{s.sector || "—"}</td>
                    <td className="px-2 py-1.5 text-right tabular-nums text-red-400">
                      {(s.avg_return * 100).toFixed(1)}%
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums text-[var(--text-muted)]">
                      {(s.win_rate * 100).toFixed(0)}%
                    </td>
                    <td className="px-2 py-1.5 text-right tabular-nums text-[var(--text-dim)]">
                      {s.sample_size}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Section>
    </div>
  );
}

function Section({
  title,
  variant,
  children,
}: {
  title: string;
  variant: "green" | "red";
  children: React.ReactNode;
}) {
  return (
    <div>
      <h3
        className={clsx(
          "text-xs font-semibold mb-2",
          variant === "green" ? "text-green-400" : "text-red-400",
        )}
      >
        {title}
      </h3>
      {children}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Main Page                                                          */
/* ------------------------------------------------------------------ */

export default function Seasonality() {
  const [tab, setTab] = useState<TabId>("Sector Heatmap");

  return (
    <div className="space-y-4">
      {/* Page header */}
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <h1 className="text-sm font-semibold text-[var(--text)] flex items-center gap-2">
          <Calendar className="h-4 w-4 text-blue-500" />
          Seasonality Dashboard
        </h1>
      </div>

      {/* Tabs */}
      <div className="flex items-center gap-1 border-b border-[var(--border)] pb-px">
        {TABS.map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={clsx(
              "px-3 py-1.5 text-xs font-medium rounded-t-md transition-colors",
              tab === t
                ? "bg-[var(--surface)] text-[var(--text)] border border-[var(--border)] border-b-transparent -mb-px"
                : "text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)]",
            )}
          >
            {t}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div className="bg-[var(--surface)] rounded-lg border border-[var(--border)] p-4">
        {tab === "Sector Heatmap" && <SectorHeatmapTab />}
        {tab === "Stock Patterns" && <StockPatternsTab />}
        {tab === "This Month" && <OutlookTab />}
      </div>
    </div>
  );
}
