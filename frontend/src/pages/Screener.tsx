import { useCallback, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Search, Loader2 } from "lucide-react";
import { clsx } from "clsx";
import type { StockSignal } from "../types/index.ts";
import { fetchScreener, type ScreenerParams } from "../api/client.ts";
import { formatNumber, formatPct, colorBySign } from "../lib/format.ts";
import SymbolSearch from "../components/search/SymbolSearch.tsx";

/* ---------- Filter state ---------- */

interface Filters {
  signal_type: string;
  rsi_min: string;
  rsi_max: string;
  price_min: string;
  price_max: string;
  sort_by: string;
  t2_safe: string;
  min_expected_return: string;
  max_risk_score: string;
  trend: string;
}

const defaultFilters: Filters = {
  signal_type: "",
  rsi_min: "",
  rsi_max: "",
  price_min: "",
  price_max: "",
  sort_by: "confidence",
  t2_safe: "",
  min_expected_return: "",
  max_risk_score: "",
  trend: "",
};

const signalOptions = [
  { value: "", label: "All Signals" },
  { value: "STRONG_BUY", label: "Strong Buy" },
  { value: "BUY", label: "Buy" },
  { value: "HOLD", label: "Hold" },
  { value: "SELL", label: "Sell" },
  { value: "STRONG_SELL", label: "Strong Sell" },
];

const sortOptions = [
  { value: "confidence", label: "Confidence" },
  { value: "change_pct", label: "Change %" },
  { value: "risk_reward", label: "R:R Ratio" },
  { value: "ltp", label: "Price" },
  { value: "rsi", label: "RSI" },
];

const trendOptions = [
  { value: "", label: "All" },
  { value: "STRONG_UP", label: "Strong Up" },
  { value: "UP", label: "Up" },
  { value: "SIDEWAYS", label: "Sideways" },
  { value: "DOWN", label: "Down" },
  { value: "STRONG_DOWN", label: "Strong Down" },
];

/* ---------- Signal badge config ---------- */

const signalBadge: Record<string, { label: string; cls: string }> = {
  STRONG_BUY: { label: "Strong Buy", cls: "bg-green-500/20 text-green-300" },
  BUY: { label: "Buy", cls: "bg-emerald-500/20 text-emerald-300" },
  HOLD: { label: "Hold", cls: "bg-yellow-500/20 text-yellow-300" },
  SELL: { label: "Sell", cls: "bg-red-500/20 text-red-300" },
  STRONG_SELL: { label: "Strong Sell", cls: "bg-red-600/20 text-red-200" },
};

/* ================================================================== */

export default function Screener() {
  const navigate = useNavigate();
  const [filters, setFilters] = useState<Filters>(defaultFilters);
  const [results, setResults] = useState<StockSignal[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const doSearch = useCallback(async (f: Filters) => {
    setLoading(true);
    setError(null);
    try {
      const params: ScreenerParams = {
        sort_by: f.sort_by || undefined,
        signal_type: f.signal_type || undefined,
        rsi_min: f.rsi_min ? Number(f.rsi_min) : undefined,
        rsi_max: f.rsi_max ? Number(f.rsi_max) : undefined,
        price_min: f.price_min ? Number(f.price_min) : undefined,
        price_max: f.price_max ? Number(f.price_max) : undefined,
        t2_safe: f.t2_safe === "true" ? true : f.t2_safe === "false" ? false : undefined,
        min_expected_return: f.min_expected_return ? Number(f.min_expected_return) : undefined,
        max_risk_score: f.max_risk_score ? Number(f.max_risk_score) : undefined,
        trend: f.trend || undefined,
        limit: 50,
      };
      const data = await fetchScreener(params);
      setResults(data.stocks);
      setTotalCount(data.total_count);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Screener query failed");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void doSearch(defaultFilters);
  }, [doSearch]);

  const handleChange = (field: keyof Filters, value: string) => {
    setFilters((prev) => ({ ...prev, [field]: value }));
  };

  const handleApply = () => {
    void doSearch(filters);
  };

  const handleReset = () => {
    setFilters(defaultFilters);
    void doSearch(defaultFilters);
  };

  const inputCls =
    "bg-[var(--surface)] border border-[var(--border)] rounded-md px-2.5 py-1.5 text-xs text-[var(--text)] placeholder-[var(--text-dim)] focus:outline-none focus:border-blue-500 tabular-nums";

  return (
    <div className="space-y-4">
      {/* Page title + quick search */}
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <h1 className="text-sm font-semibold text-[var(--text)] flex items-center gap-2">
          <Search className="h-4 w-4 text-blue-500" />
          Stock Screener
        </h1>
        <div className="w-56">
          <SymbolSearch placeholder="Quick stock lookup..." compact />
        </div>
      </div>

      {/* Horizontal filter bar */}
      <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg px-4 py-3">
        <div className="flex items-end gap-3 flex-wrap">
          {/* Signal type */}
          <div className="flex flex-col gap-1">
            <label className="text-[10px] text-[var(--text-muted)] uppercase tracking-wider">Signal</label>
            <select
              value={filters.signal_type}
              onChange={(e) => handleChange("signal_type", e.target.value)}
              className={clsx(inputCls, "w-32 appearance-none cursor-pointer")}
            >
              {signalOptions.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>

          {/* RSI range */}
          <div className="flex flex-col gap-1">
            <label className="text-[10px] text-[var(--text-muted)] uppercase tracking-wider">RSI</label>
            <div className="flex items-center gap-1">
              <input
                type="number"
                placeholder="0"
                min="0"
                max="100"
                value={filters.rsi_min}
                onChange={(e) => handleChange("rsi_min", e.target.value)}
                className={clsx(inputCls, "w-16")}
              />
              <span className="text-[var(--text-dim)] text-[10px]">-</span>
              <input
                type="number"
                placeholder="100"
                min="0"
                max="100"
                value={filters.rsi_max}
                onChange={(e) => handleChange("rsi_max", e.target.value)}
                className={clsx(inputCls, "w-16")}
              />
            </div>
          </div>

          {/* Price range */}
          <div className="flex flex-col gap-1">
            <label className="text-[10px] text-[var(--text-muted)] uppercase tracking-wider">Price (BDT)</label>
            <div className="flex items-center gap-1">
              <input
                type="number"
                placeholder="Min"
                min="0"
                value={filters.price_min}
                onChange={(e) => handleChange("price_min", e.target.value)}
                className={clsx(inputCls, "w-20")}
              />
              <span className="text-[var(--text-dim)] text-[10px]">-</span>
              <input
                type="number"
                placeholder="Max"
                min="0"
                value={filters.price_max}
                onChange={(e) => handleChange("price_max", e.target.value)}
                className={clsx(inputCls, "w-20")}
              />
            </div>
          </div>

          {/* Sort by */}
          <div className="flex flex-col gap-1">
            <label className="text-[10px] text-[var(--text-muted)] uppercase tracking-wider">Sort By</label>
            <select
              value={filters.sort_by}
              onChange={(e) => handleChange("sort_by", e.target.value)}
              className={clsx(inputCls, "w-32 appearance-none cursor-pointer")}
            >
              {sortOptions.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>

          {/* T+2 Safe */}
          <div className="flex flex-col gap-1">
            <label className="text-[10px] text-[var(--text-muted)] uppercase tracking-wider">T+2 Safe</label>
            <select
              value={filters.t2_safe}
              onChange={(e) => handleChange("t2_safe", e.target.value)}
              className={clsx(inputCls, "w-24 appearance-none cursor-pointer")}
            >
              <option value="">All</option>
              <option value="true">Safe</option>
              <option value="false">Risky</option>
            </select>
          </div>

          {/* Min Return */}
          <div className="flex flex-col gap-1">
            <label className="text-[10px] text-[var(--text-muted)] uppercase tracking-wider">Min Return%</label>
            <input
              type="number"
              placeholder="0"
              step="0.5"
              value={filters.min_expected_return}
              onChange={(e) => handleChange("min_expected_return", e.target.value)}
              className={clsx(inputCls, "w-20")}
            />
          </div>

          {/* Max Risk */}
          <div className="flex flex-col gap-1">
            <label className="text-[10px] text-[var(--text-muted)] uppercase tracking-wider">Max Risk</label>
            <input
              type="number"
              placeholder="100"
              min="0"
              max="100"
              value={filters.max_risk_score}
              onChange={(e) => handleChange("max_risk_score", e.target.value)}
              className={clsx(inputCls, "w-20")}
            />
          </div>

          {/* Trend */}
          <div className="flex flex-col gap-1">
            <label className="text-[10px] text-[var(--text-muted)] uppercase tracking-wider">Trend</label>
            <select
              value={filters.trend}
              onChange={(e) => handleChange("trend", e.target.value)}
              className={clsx(inputCls, "w-32 appearance-none cursor-pointer")}
            >
              {trendOptions.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          </div>

          {/* Buttons */}
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={handleApply}
              disabled={loading}
              className="bg-blue-600 hover:bg-blue-500 disabled:opacity-50 text-white text-xs font-medium px-4 py-1.5 rounded-md transition-colors flex items-center gap-1.5"
            >
              {loading ? (
                <Loader2 className="h-3 w-3 animate-spin" />
              ) : (
                <Search className="h-3 w-3" />
              )}
              Search
            </button>
            <button
              type="button"
              onClick={handleReset}
              className="bg-[var(--surface-active)] hover:bg-[var(--surface-elevated)] text-[var(--text-muted)] text-xs font-medium px-3 py-1.5 rounded-md transition-colors"
            >
              Reset
            </button>
          </div>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="bg-red-900/20 border border-red-800/40 rounded-lg px-4 py-2.5 text-xs text-red-400">
          {error}
        </div>
      )}

      {/* Results table */}
      <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg overflow-hidden">
        <div className="px-4 py-2.5 border-b border-[var(--border)] flex items-center justify-between">
          <span className="text-xs text-[var(--text-muted)]">
            Showing {results.length} of {totalCount} stocks
          </span>
        </div>

        <div className="divide-y divide-[var(--border)]">
          {loading && results.length === 0 ? (
            <div className="flex items-center justify-center gap-2 py-12">
              <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
              <span className="text-xs text-[var(--text-muted)]">Searching...</span>
            </div>
          ) : results.length === 0 ? (
            <div className="text-center text-[var(--text-dim)] py-12 text-xs">
              No results match your filters
            </div>
          ) : (
            results.map((s) => {
              const badge = signalBadge[s.signal_type] ?? {
                label: s.signal_type,
                cls: "bg-[var(--surface-elevated)] text-[var(--text)]",
              };
              return (
                <div
                  key={s.symbol}
                  onClick={() => navigate(`/stock/${s.symbol}`)}
                  className="hover:bg-[var(--hover)] cursor-pointer transition-colors px-4 py-2.5"
                >
                  {/* Row 1: Symbol, Signal badge, LTP, Change%, T+2, Exp return, Hold, Risk */}
                  <div className="flex items-center gap-2.5 mb-1.5 flex-wrap">
                    <span className="font-medium text-sm text-[var(--text)] w-24 shrink-0">
                      {s.symbol}
                    </span>
                    <span
                      className={clsx(
                        "inline-block px-2 py-0.5 rounded text-[10px] font-medium",
                        badge.cls,
                      )}
                    >
                      {badge.label}
                    </span>
                    <span className="text-xs text-[var(--text)] tabular-nums">
                      {formatNumber(s.ltp)}
                    </span>
                    <span
                      className={clsx(
                        "text-xs font-medium tabular-nums",
                        colorBySign(s.change_pct),
                      )}
                    >
                      {formatPct(s.change_pct)}
                    </span>

                    <div className="ml-auto flex items-center gap-2.5">
                      {s.t2_safe != null && (
                        <span
                          className={clsx(
                            "inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium",
                            s.t2_safe
                              ? "bg-green-500/15 text-green-400 border border-green-500/30"
                              : "bg-red-500/15 text-red-400 border border-red-500/30",
                          )}
                        >
                          T+2 {s.t2_safe ? "Safe" : "Risky"}
                        </span>
                      )}
                      {s.expected_return_pct != null && (
                        <span
                          className={clsx(
                            "text-xs font-bold tabular-nums",
                            colorBySign(s.expected_return_pct),
                          )}
                        >
                          {s.expected_return_pct > 0 ? "+" : ""}
                          {s.expected_return_pct.toFixed(1)}%
                        </span>
                      )}
                      {s.hold_days != null && (
                        <span className="text-[10px] text-[var(--text-muted)] tabular-nums">
                          {s.hold_days}d
                        </span>
                      )}
                      {s.risk_score != null && (
                        <span
                          className={clsx(
                            "text-[10px] tabular-nums font-medium",
                            s.risk_score < 30
                              ? "text-green-400"
                              : s.risk_score <= 60
                                ? "text-yellow-400"
                                : "text-red-400",
                          )}
                        >
                          Risk {s.risk_score.toFixed(0)}
                        </span>
                      )}
                    </div>
                  </div>

                  {/* Row 2: Entry + Exit strategies */}
                  <div className="flex items-start gap-3 ml-0">
                    {s.entry_strategy && (
                      <div className="flex items-start gap-1.5 flex-1 min-w-0">
                        <span className="text-[10px] font-semibold uppercase shrink-0 mt-px text-emerald-500">
                          Entry:
                        </span>
                        <span className="text-[11px] text-[var(--text-muted)] truncate">
                          {s.entry_strategy}
                        </span>
                      </div>
                    )}
                    {s.exit_strategy && (
                      <div className="flex items-start gap-1.5 flex-1 min-w-0">
                        <span className="text-[10px] font-semibold uppercase shrink-0 mt-px text-amber-500">
                          Exit:
                        </span>
                        <span className="text-[11px] text-[var(--text-muted)] truncate">
                          {s.exit_strategy}
                        </span>
                      </div>
                    )}
                  </div>

                  {/* Row 3: Compact metrics */}
                  <div className="flex items-center gap-3 mt-1">
                    <span className="text-[10px] text-[var(--text-dim)]">
                      Target <span className="text-green-500 tabular-nums">{formatNumber(s.target_price)}</span>
                    </span>
                    <span className="text-[10px] text-[var(--text-dim)]">
                      Stop <span className="text-red-500 tabular-nums">{formatNumber(s.stop_loss)}</span>
                    </span>
                    <span className="text-[10px] text-[var(--text-dim)]">
                      R:R <span className="text-blue-400 tabular-nums">{s.risk_reward_ratio.toFixed(1)}</span>
                    </span>
                    <span className="text-[10px] text-[var(--text-dim)]">
                      RSI <span className="text-[var(--text-muted)] tabular-nums">{s.indicators.rsi?.toFixed(0) ?? "--"}</span>
                    </span>
                    <span className="text-[10px] text-[var(--text-dim)]">
                      Conf <span className="text-[var(--text-muted)] tabular-nums">{(s.confidence * 100).toFixed(0)}%</span>
                    </span>
                  </div>
                </div>
              );
            })
          )}
        </div>
      </div>
    </div>
  );
}
