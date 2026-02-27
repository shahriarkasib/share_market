/**
 * Sector Performance page.
 *
 * Displays a CSS-only horizontal bar chart of sector avg_change_pct,
 * then a grid of sector cards with expand-to-detail behavior.
 */

import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { clsx } from "clsx";
import { TrendingUp, Loader2, AlertCircle, ChevronDown, ChevronRight, ArrowRight } from "lucide-react";
import type { SectorPerformance, StockPrice } from "../types/index.ts";
import { fetchSectorPerformance, fetchSectorDetail } from "../api/client.ts";
import { formatCompact, formatPct, colorBySign } from "../lib/format.ts";

export default function SectorPerformancePage() {
  const navigate = useNavigate();

  const [sectors, setSectors] = useState<SectorPerformance[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Only one sector expanded at a time
  const [expandedSector, setExpandedSector] = useState<string | null>(null);
  // Cache fetched sector details so we don't re-fetch on collapse/expand
  const [sectorStocks, setSectorStocks] = useState<Record<string, StockPrice[]>>({});
  const [detailLoading, setDetailLoading] = useState(false);

  /* ---- Fetch sector list on mount ---- */
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    fetchSectorPerformance()
      .then((data) => {
        if (!cancelled) setSectors(data);
      })
      .catch((err) => {
        if (!cancelled)
          setError(err instanceof Error ? err.message : "Failed to load sector data");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, []);

  /* ---- Toggle expand / collapse for a sector card ---- */
  function handleToggleSector(sectorName: string) {
    if (expandedSector === sectorName) {
      setExpandedSector(null);
      return;
    }

    setExpandedSector(sectorName);

    // Fetch detail if not already cached
    if (!sectorStocks[sectorName]) {
      setDetailLoading(true);
      fetchSectorDetail(sectorName)
        .then((result) => {
          setSectorStocks((prev) => ({ ...prev, [sectorName]: result.stocks }));
        })
        .catch(() => {
          // On error, store empty array so we don't retry endlessly
          setSectorStocks((prev) => ({ ...prev, [sectorName]: [] }));
        })
        .finally(() => setDetailLoading(false));
    }
  }

  /* ---- Derived: sorted sectors & max abs value for bar scaling ---- */
  const sorted = [...sectors].sort((a, b) => b.avg_change_pct - a.avg_change_pct);
  const maxAbs = Math.max(...sorted.map((s) => Math.abs(s.avg_change_pct)), 0.01);
  const totalStocks = sectors.reduce((sum, s) => sum + s.stock_count, 0);

  /* ---- Loading / error states ---- */
  if (loading) {
    return (
      <div className="flex items-center justify-center gap-2 py-24">
        <Loader2 className="h-5 w-5 animate-spin text-blue-500" />
        <span className="text-xs text-[var(--text-muted)]">Loading sectors...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-900/20 border border-red-800/40 rounded-lg px-4 py-2.5 text-xs text-red-400 flex items-center gap-2">
        <AlertCircle className="h-3.5 w-3.5 shrink-0" />
        {error}
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* ========== Header ========== */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-sm font-semibold text-[var(--text)] flex items-center gap-2">
          <TrendingUp className="h-4 w-4 text-blue-500" />
          Sector Performance
          <span className="ml-1 rounded-full bg-blue-600/20 text-blue-400 text-[10px] font-medium px-2 py-0.5">
            {totalStocks} stocks
          </span>
        </h1>
        <Link
          to="/heatmap"
          className="text-xs text-blue-400 hover:text-blue-300 flex items-center gap-1 transition-colors"
        >
          View as Heatmap <ArrowRight className="h-3 w-3" />
        </Link>
      </div>

      {/* ========== Horizontal Bar Chart ========== */}
      <div className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4 space-y-1.5">
        {sorted.map((s) => {
          const pct = Math.abs(s.avg_change_pct);
          const barWidth = (pct / maxAbs) * 50; // max 50%
          const isPositive = s.avg_change_pct >= 0;

          return (
            <div key={s.sector} className="flex items-center gap-3 text-xs">
              <span className="w-36 shrink-0 truncate text-[var(--text-muted)] text-right">
                {s.sector}
              </span>
              <div className="flex-1 h-5 relative">
                <div
                  className={clsx(
                    "h-full rounded-sm transition-all",
                    isPositive ? "bg-green-500/70" : "bg-red-500/70",
                  )}
                  style={{ width: `${Math.max(barWidth, 0.5)}%` }}
                />
              </div>
              <span
                className={clsx(
                  "w-16 text-right tabular-nums font-medium shrink-0",
                  colorBySign(s.avg_change_pct),
                )}
              >
                {formatPct(s.avg_change_pct)}
              </span>
            </div>
          );
        })}
      </div>

      {/* ========== Sector Cards Grid ========== */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {sorted.map((s) => {
          const isExpanded = expandedSector === s.sector;
          const stocks = sectorStocks[s.sector];
          const total = s.advances + s.declines + s.unchanged;
          const advPct = total > 0 ? (s.advances / total) * 100 : 0;
          const decPct = total > 0 ? (s.declines / total) * 100 : 0;
          const uncPct = total > 0 ? (s.unchanged / total) * 100 : 0;

          return (
            <div
              key={s.sector}
              className="rounded-lg border border-[var(--border)] bg-[var(--surface)] overflow-hidden"
            >
              {/* Card header - clickable */}
              <button
                type="button"
                onClick={() => handleToggleSector(s.sector)}
                className="w-full text-left px-4 py-3 hover:bg-[var(--hover)] transition-colors"
              >
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    {isExpanded ? (
                      <ChevronDown className="h-3.5 w-3.5 text-[var(--text-muted)]" />
                    ) : (
                      <ChevronRight className="h-3.5 w-3.5 text-[var(--text-muted)]" />
                    )}
                    <span className="text-xs font-bold text-[var(--text)]">{s.sector}</span>
                    <span className="rounded-full bg-[var(--surface-active)] text-[var(--text-dim)] text-[10px] px-1.5 py-0.5">
                      {s.stock_count}
                    </span>
                  </div>
                  <span
                    className={clsx(
                      "text-xs font-semibold tabular-nums",
                      colorBySign(s.avg_change_pct),
                    )}
                  >
                    {formatPct(s.avg_change_pct)}
                  </span>
                </div>

                {/* Stacked horizontal bar: advances | declines | unchanged */}
                <div className="flex h-2 rounded-full overflow-hidden mb-2">
                  {advPct > 0 && (
                    <div
                      className="bg-green-500 transition-all"
                      style={{ width: `${advPct}%` }}
                      title={`Advances: ${s.advances}`}
                    />
                  )}
                  {decPct > 0 && (
                    <div
                      className="bg-red-500 transition-all"
                      style={{ width: `${decPct}%` }}
                      title={`Declines: ${s.declines}`}
                    />
                  )}
                  {uncPct > 0 && (
                    <div
                      className="bg-slate-500 transition-all"
                      style={{ width: `${uncPct}%` }}
                      title={`Unchanged: ${s.unchanged}`}
                    />
                  )}
                </div>
                <div className="flex items-center gap-3 text-[10px] text-[var(--text-dim)]">
                  <span className="flex items-center gap-1">
                    <span className="h-1.5 w-1.5 rounded-full bg-green-500" />
                    {s.advances} adv
                  </span>
                  <span className="flex items-center gap-1">
                    <span className="h-1.5 w-1.5 rounded-full bg-red-500" />
                    {s.declines} dec
                  </span>
                  <span className="flex items-center gap-1">
                    <span className="h-1.5 w-1.5 rounded-full bg-slate-500" />
                    {s.unchanged} unc
                  </span>
                </div>

                {/* Stats row */}
                <div className="flex items-center justify-between mt-2 text-[10px]">
                  <span className="text-[var(--text-dim)]">
                    Turnover: <span className="text-[var(--text-muted)]">{formatCompact(s.total_turnover)}</span>
                  </span>
                  <div className="flex items-center gap-3">
                    {s.top_gainer && (
                      <span className="text-green-400">
                        {s.top_gainer.symbol} {formatPct(s.top_gainer.change_pct)}
                      </span>
                    )}
                    {s.top_loser && (
                      <span className="text-red-400">
                        {s.top_loser.symbol} {formatPct(s.top_loser.change_pct)}
                      </span>
                    )}
                  </div>
                </div>
              </button>

              {/* Top gainer / loser clickable links (outside the button) */}
              <div className="flex items-center gap-4 px-4 pb-2 text-[10px]">
                {s.top_gainer && (
                  <Link
                    to={`/stock/${s.top_gainer.symbol}`}
                    className="text-green-400 hover:underline"
                    onClick={(e) => e.stopPropagation()}
                  >
                    Top Gainer: {s.top_gainer.symbol} {formatPct(s.top_gainer.change_pct)}
                  </Link>
                )}
                {s.top_loser && (
                  <Link
                    to={`/stock/${s.top_loser.symbol}`}
                    className="text-red-400 hover:underline"
                    onClick={(e) => e.stopPropagation()}
                  >
                    Top Loser: {s.top_loser.symbol} {formatPct(s.top_loser.change_pct)}
                  </Link>
                )}
              </div>

              {/* ---- Expanded detail ---- */}
              {isExpanded && (
                <div className="border-t border-[var(--border)] px-4 py-3">
                  {detailLoading && !stocks ? (
                    <div className="flex items-center justify-center gap-2 py-6">
                      <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
                      <span className="text-xs text-[var(--text-muted)]">Loading stocks...</span>
                    </div>
                  ) : stocks && stocks.length > 0 ? (
                    <div className="overflow-x-auto">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="text-[var(--text-dim)] text-left">
                            <th className="pb-1.5 font-medium">Symbol</th>
                            <th className="pb-1.5 font-medium text-right">LTP</th>
                            <th className="pb-1.5 font-medium text-right">Change%</th>
                            <th className="pb-1.5 font-medium text-right">Volume</th>
                          </tr>
                        </thead>
                        <tbody>
                          {stocks.map((stock) => (
                            <tr
                              key={stock.symbol}
                              className="border-t border-[var(--border)] hover:bg-[var(--hover)] cursor-pointer transition-colors"
                              onClick={() => navigate(`/stock/${stock.symbol}`)}
                            >
                              <td className="py-1.5 font-medium text-[var(--text)]">
                                {stock.symbol}
                              </td>
                              <td className="py-1.5 text-right tabular-nums text-[var(--text-muted)]">
                                {stock.ltp.toFixed(2)}
                              </td>
                              <td
                                className={clsx(
                                  "py-1.5 text-right tabular-nums font-medium",
                                  colorBySign(stock.change_pct),
                                )}
                              >
                                {formatPct(stock.change_pct)}
                              </td>
                              <td className="py-1.5 text-right tabular-nums text-[var(--text-muted)]">
                                {formatCompact(stock.volume)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="text-xs text-[var(--text-dim)] text-center py-4">
                      No stock data available for this sector.
                    </p>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
