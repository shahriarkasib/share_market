import { useState, useEffect, useMemo, useRef, useCallback } from "react";
import { Link } from "react-router-dom";
import { clsx } from "clsx";
import {
  TrendingUp,
  TrendingDown,
  Clock,
  Shield,
  AlertTriangle,
  Download,
  RefreshCw,
  ChevronDown,
  ChevronUp,
  Target,
  CalendarDays,
  BarChart3,
  Loader2,
  Search,
  X,
  Filter,
} from "lucide-react";
import {
  fetchDailyAnalysis,
  fetchAnalysisDates,
  triggerAnalysis,
  fetchAnalysisStatus,
  getAnalysisExcelUrl,
} from "../api/client.ts";
import type { DailyAnalysis, DailyAnalysisResponse } from "../types/index.ts";
import { formatNumber, formatPct, colorBySign } from "../lib/format.ts";

/* ── action config ─────────────────────────────────────────── */

const ACTION_CONFIG: Record<string, { color: string; bg: string; border: string; icon: typeof TrendingUp; label: string }> = {
  BUY:                              { color: "text-green-400", bg: "bg-green-500/10", border: "border-green-500/30", icon: TrendingUp,    label: "BUY" },
  "BUY on dip":                     { color: "text-emerald-400", bg: "bg-emerald-500/10", border: "border-emerald-500/30", icon: Target,       label: "BUY on Dip" },
  "BUY (wait for MACD cross)":      { color: "text-amber-400", bg: "bg-amber-500/10", border: "border-amber-500/30", icon: Clock,        label: "Wait MACD" },
  "HOLD/WAIT":                      { color: "text-blue-400", bg: "bg-blue-500/10", border: "border-blue-500/30", icon: Shield,       label: "Hold/Wait" },
  "SELL/AVOID":                     { color: "text-red-400", bg: "bg-red-500/10", border: "border-red-500/30", icon: AlertTriangle, label: "Sell/Avoid" },
  AVOID:                            { color: "text-red-400", bg: "bg-red-500/10", border: "border-red-500/30", icon: TrendingDown,  label: "Avoid" },
};

const TAB_ORDER = ["BUY", "BUY on dip", "BUY (wait for MACD cross)", "HOLD/WAIT", "SELL/AVOID", "AVOID"];

function getActionCfg(action: string) {
  return ACTION_CONFIG[action] ?? { color: "text-[var(--text-muted)]", bg: "bg-[var(--surface)]", border: "border-[var(--border)]", icon: BarChart3, label: action };
}

/* ── main page ─────────────────────────────────────────────── */

export default function DailyAnalysisPage() {
  const [data, setData] = useState<DailyAnalysisResponse | null>(null);
  const [dates, setDates] = useState<string[]>([]);
  const [selectedDate, setSelectedDate] = useState<string>("");
  const [activeTab, setActiveTab] = useState<string>("BUY");
  const [loading, setLoading] = useState(true);
  const [triggering, setTriggering] = useState(false);

  // Filters
  const [searchQuery, setSearchQuery] = useState("");
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [sectorFilter, setSectorFilter] = useState("");
  const [categoryFilter, setCategoryFilter] = useState("");
  const [highlightSymbol, setHighlightSymbol] = useState<string | null>(null);

  const searchRef = useRef<HTMLDivElement>(null);
  const cardRefs = useRef<Record<string, HTMLDivElement | null>>({});

  // Close suggestions on click outside
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  // Load available dates on mount
  useEffect(() => {
    fetchAnalysisDates().then((r) => {
      const sorted = r.dates.sort().reverse();
      setDates(sorted);
      if (sorted.length > 0 && !selectedDate) {
        setSelectedDate(sorted[0]);
      }
    }).catch(() => {});
  }, []);

  // Load analysis when date changes
  useEffect(() => {
    if (!selectedDate) return;
    setLoading(true);
    setHighlightSymbol(null);
    fetchDailyAnalysis(selectedDate)
      .then((r) => {
        setData(r);
        const firstWithData = TAB_ORDER.find((t) =>
          r.analysis.some((a) => a.action === t),
        );
        if (firstWithData) setActiveTab(firstWithData);
      })
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [selectedDate]);

  // All unique sectors and categories from current data
  const { sectors, categories } = useMemo(() => {
    if (!data?.analysis) return { sectors: [] as string[], categories: [] as string[] };
    const sSet = new Set<string>();
    const cSet = new Set<string>();
    for (const item of data.analysis) {
      if (item.sector) sSet.add(item.sector);
      if (item.category) cSet.add(item.category);
    }
    return {
      sectors: [...sSet].sort(),
      categories: [...cSet].sort(),
    };
  }, [data]);

  // Symbol search suggestions — searches across ALL tabs
  const suggestions = useMemo(() => {
    if (!data?.analysis || !searchQuery || searchQuery.length < 1) return [];
    const q = searchQuery.toUpperCase();
    return data.analysis
      .filter((s) => s.symbol.includes(q))
      .slice(0, 10)
      .map((s) => ({ symbol: s.symbol, action: s.action, ltp: s.ltp, sector: s.sector }));
  }, [data, searchQuery]);

  // Group stocks by action
  const grouped = useMemo(() => {
    if (!data?.analysis) return {};
    const g: Record<string, DailyAnalysis[]> = {};
    for (const item of data.analysis) {
      g[item.action] = g[item.action] || [];
      g[item.action].push(item);
    }
    return g;
  }, [data]);

  // Filtered stocks for active tab (apply sector + category filters)
  const filteredStocks = useMemo(() => {
    let stocks = grouped[activeTab] || [];
    if (sectorFilter) {
      stocks = stocks.filter((s) => s.sector === sectorFilter);
    }
    if (categoryFilter) {
      stocks = stocks.filter((s) => s.category === categoryFilter);
    }
    return stocks;
  }, [grouped, activeTab, sectorFilter, categoryFilter]);

  // Tab counts (respecting sector/category filters)
  const tabCounts = useMemo(() => {
    if (!data?.analysis) return {} as Record<string, number>;
    const counts: Record<string, number> = {};
    for (const item of data.analysis) {
      if (sectorFilter && item.sector !== sectorFilter) continue;
      if (categoryFilter && item.category !== categoryFilter) continue;
      counts[item.action] = (counts[item.action] || 0) + 1;
    }
    return counts;
  }, [data, sectorFilter, categoryFilter]);

  // Navigate to a symbol: switch tab, scroll, highlight
  const navigateToSymbol = useCallback((symbol: string, action: string) => {
    setSearchQuery(symbol);
    setShowSuggestions(false);
    setActiveTab(action);
    setHighlightSymbol(symbol);

    // Scroll to card after render
    setTimeout(() => {
      const el = cardRefs.current[symbol];
      if (el) {
        el.scrollIntoView({ behavior: "smooth", block: "center" });
      }
    }, 100);

    // Remove highlight after 3s
    setTimeout(() => setHighlightSymbol(null), 3000);
  }, []);

  const clearSearch = () => {
    setSearchQuery("");
    setShowSuggestions(false);
    setHighlightSymbol(null);
  };

  const clearAllFilters = () => {
    setSearchQuery("");
    setSectorFilter("");
    setCategoryFilter("");
    setHighlightSymbol(null);
    setShowSuggestions(false);
  };

  const hasFilters = sectorFilter || categoryFilter;

  const handleTrigger = async () => {
    setTriggering(true);
    try {
      await triggerAnalysis();
      const poll = setInterval(async () => {
        const s = await fetchAnalysisStatus();
        if (!s.running) {
          clearInterval(poll);
          setTriggering(false);
          const r = await fetchDailyAnalysis(selectedDate);
          setData(r);
          const d = await fetchAnalysisDates();
          setDates(d.dates.sort().reverse());
        }
      }, 5000);
    } catch {
      setTriggering(false);
    }
  };

  return (
    <div className="space-y-4">
      {/* Header bar */}
      <div className="flex flex-col sm:flex-row sm:items-center gap-3">
        <div className="flex items-center gap-2">
          <BarChart3 className="h-5 w-5 text-blue-500" />
          <h1 className="text-base font-bold text-[var(--text)]">Daily Analysis</h1>
        </div>

        <div className="flex items-center gap-2 sm:ml-auto flex-wrap">
          {/* Date picker */}
          <div className="flex items-center gap-1.5">
            <CalendarDays className="h-3.5 w-3.5 text-[var(--text-dim)]" />
            <select
              value={selectedDate}
              onChange={(e) => setSelectedDate(e.target.value)}
              className="bg-[var(--surface)] border border-[var(--border)] rounded-md px-2 py-1 text-xs text-[var(--text)] focus:outline-none focus:ring-1 focus:ring-blue-500"
            >
              {dates.map((d) => (
                <option key={d} value={d}>{d}</option>
              ))}
              {dates.length === 0 && <option value="">No data</option>}
            </select>
          </div>

          {/* Excel download */}
          <a
            href={getAnalysisExcelUrl(selectedDate)}
            download
            className="flex items-center gap-1 px-2.5 py-1 rounded-md text-xs font-medium bg-[var(--surface)] border border-[var(--border)] text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)] transition-colors"
          >
            <Download className="h-3.5 w-3.5" />
            Excel
          </a>

          {/* Trigger */}
          <button
            onClick={handleTrigger}
            disabled={triggering}
            className={clsx(
              "flex items-center gap-1 px-2.5 py-1 rounded-md text-xs font-medium transition-colors",
              triggering
                ? "bg-amber-500/10 text-amber-400 border border-amber-500/30"
                : "bg-blue-500/10 text-blue-400 border border-blue-500/30 hover:bg-blue-500/20",
            )}
          >
            {triggering ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <RefreshCw className="h-3.5 w-3.5" />
            )}
            {triggering ? "Running..." : "Run Analysis"}
          </button>
        </div>
      </div>

      {/* Filter bar */}
      {data && data.count > 0 && (
        <div className="flex flex-wrap items-center gap-2">
          {/* Symbol search with autocomplete */}
          <div ref={searchRef} className="relative">
            <div className="relative">
              <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-[var(--text-dim)]" />
              <input
                type="text"
                placeholder="Search symbol..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setShowSuggestions(true);
                  setHighlightSymbol(null);
                }}
                onFocus={() => searchQuery && setShowSuggestions(true)}
                className="bg-[var(--surface)] border border-[var(--border)] rounded-md pl-7 pr-7 py-1 text-xs text-[var(--text)] w-40 focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
              {searchQuery && (
                <button
                  onClick={clearSearch}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-[var(--text-dim)] hover:text-[var(--text)]"
                >
                  <X className="h-3 w-3" />
                </button>
              )}
            </div>

            {/* Suggestions dropdown */}
            {showSuggestions && suggestions.length > 0 && (
              <div className="absolute z-50 top-full mt-1 w-64 bg-[var(--surface)] border border-[var(--border)] rounded-lg shadow-xl overflow-hidden">
                {suggestions.map((s) => {
                  const cfg = getActionCfg(s.action);
                  return (
                    <button
                      key={s.symbol}
                      onClick={() => navigateToSymbol(s.symbol, s.action)}
                      className="w-full px-3 py-2 flex items-center gap-2 hover:bg-[var(--hover)] transition-colors text-left"
                    >
                      <span className="text-xs font-bold text-[var(--text)]">{s.symbol}</span>
                      <span className={clsx("text-[10px] px-1.5 py-0.5 rounded font-medium border", cfg.bg, cfg.color, cfg.border)}>
                        {cfg.label}
                      </span>
                      {s.sector && (
                        <span className="text-[10px] text-[var(--text-dim)] ml-auto truncate max-w-20">{s.sector}</span>
                      )}
                    </button>
                  );
                })}
              </div>
            )}
          </div>

          {/* Sector filter */}
          <div className="flex items-center gap-1">
            <select
              value={sectorFilter}
              onChange={(e) => setSectorFilter(e.target.value)}
              className="bg-[var(--surface)] border border-[var(--border)] rounded-md px-2 py-1 text-xs text-[var(--text)] focus:outline-none focus:ring-1 focus:ring-blue-500 max-w-40"
            >
              <option value="">All Sectors</option>
              {sectors.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
          </div>

          {/* Category filter */}
          <div className="flex items-center gap-1">
            <select
              value={categoryFilter}
              onChange={(e) => setCategoryFilter(e.target.value)}
              className="bg-[var(--surface)] border border-[var(--border)] rounded-md px-2 py-1 text-xs text-[var(--text)] focus:outline-none focus:ring-1 focus:ring-blue-500"
            >
              <option value="">All Categories</option>
              {categories.map((c) => (
                <option key={c} value={c}>{c}</option>
              ))}
            </select>
          </div>

          {/* Clear filters */}
          {hasFilters && (
            <button
              onClick={clearAllFilters}
              className="flex items-center gap-1 px-2 py-1 rounded-md text-[10px] font-medium text-[var(--text-dim)] hover:text-[var(--text)] hover:bg-[var(--hover)] transition-colors"
            >
              <Filter className="h-3 w-3" />
              Clear filters
            </button>
          )}
        </div>
      )}

      {/* Tab bar */}
      {data && data.count > 0 && (
        <div className="flex flex-wrap gap-2">
          {TAB_ORDER.map((action) => {
            const count = tabCounts[action] ?? 0;
            if (count === 0 && !hasFilters) return null;
            const cfg = getActionCfg(action);
            return (
              <button
                key={action}
                onClick={() => setActiveTab(action)}
                className={clsx(
                  "flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors",
                  activeTab === action
                    ? `${cfg.bg} ${cfg.color} ${cfg.border}`
                    : "bg-[var(--surface)] border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]",
                  count === 0 && "opacity-40",
                )}
              >
                <cfg.icon className="h-3.5 w-3.5" />
                {cfg.label}
                <span className={clsx(
                  "ml-0.5 px-1.5 py-0.5 rounded-full text-[10px] font-bold",
                  activeTab === action ? `${cfg.bg} ${cfg.color}` : "bg-[var(--surface-active)] text-[var(--text-dim)]",
                )}>
                  {count}
                </span>
              </button>
            );
          })}
        </div>
      )}

      {/* Content */}
      {loading ? (
        <div className="flex items-center justify-center py-20">
          <Loader2 className="h-6 w-6 animate-spin text-[var(--text-dim)]" />
        </div>
      ) : !data || data.count === 0 ? (
        <div className="text-center py-20">
          <BarChart3 className="h-10 w-10 text-[var(--text-dim)] mx-auto mb-3" />
          <p className="text-sm text-[var(--text-muted)]">No analysis data for this date</p>
          <p className="text-xs text-[var(--text-dim)] mt-1">Click "Run Analysis" to generate today's picks</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
          {filteredStocks.map((stock) => (
            <AnalysisCard
              key={stock.symbol}
              stock={stock}
              highlight={highlightSymbol === stock.symbol}
              ref={(el) => { cardRefs.current[stock.symbol] = el; }}
            />
          ))}
          {filteredStocks.length === 0 && (
            <p className="col-span-full text-center text-sm text-[var(--text-dim)] py-8">
              No stocks match your filters
            </p>
          )}
        </div>
      )}
    </div>
  );
}

/* ── stock analysis card ───────────────────────────────────── */

import { forwardRef } from "react";

const AnalysisCard = forwardRef<HTMLDivElement, { stock: DailyAnalysis; highlight?: boolean }>(
  function AnalysisCard({ stock, highlight }, ref) {
    const [expanded, setExpanded] = useState(false);
    const cfg = getActionCfg(stock.action);

    // Parse scenarios (backend stores {name, steps[]})
    let scenarios: { name: string; steps: string[] }[] = [];
    if (stock.scenarios_json) {
      try {
        const parsed = typeof stock.scenarios_json === "string"
          ? JSON.parse(stock.scenarios_json)
          : stock.scenarios_json;
        if (Array.isArray(parsed)) scenarios = parsed;
      } catch { /* ignore */ }
    }

    const riskReward = stock.risk_pct && stock.reward_pct
      ? Math.abs(stock.reward_pct / stock.risk_pct)
      : 0;

    return (
      <div
        ref={ref}
        className={clsx(
          "bg-[var(--surface)] border rounded-lg overflow-hidden transition-all duration-300",
          cfg.border,
          highlight && "ring-2 ring-blue-500 ring-offset-1 ring-offset-[var(--bg)]",
        )}
      >
        {/* Card header */}
        <div className="px-3 py-2.5 flex items-center gap-2">
          <Link
            to={`/stock/${stock.symbol}`}
            className="text-sm font-bold text-[var(--text)] hover:text-blue-400 transition-colors"
          >
            {stock.symbol}
          </Link>
          <span className={clsx(
            "px-1.5 py-0.5 rounded text-[10px] font-semibold border",
            cfg.bg, cfg.color, cfg.border,
          )}>
            {cfg.label}
          </span>
          {stock.sector && (
            <span className="text-[10px] text-[var(--text-dim)] truncate max-w-24 hidden sm:inline" title={stock.sector}>
              {stock.sector}
            </span>
          )}
          {stock.category && (
            <span className="text-[9px] px-1 py-0.5 rounded bg-[var(--hover)] text-[var(--text-dim)] font-medium hidden sm:inline">
              {stock.category}
            </span>
          )}
          <span className="ml-auto text-sm font-semibold tabular-nums text-[var(--text)]">
            {formatNumber(stock.ltp)}
          </span>
        </div>

        {/* Entry/Exit row */}
        <div className="px-3 py-2 border-t border-[var(--border)] grid grid-cols-4 gap-2 text-center">
          <div>
            <div className="text-[10px] text-[var(--text-dim)]">Entry</div>
            <div className="text-xs font-medium tabular-nums text-[var(--text)]">
              {formatNumber(stock.entry_low)}–{formatNumber(stock.entry_high)}
            </div>
          </div>
          <div>
            <div className="text-[10px] text-[var(--text-dim)]">SL</div>
            <div className="text-xs font-medium tabular-nums text-red-400">
              {formatNumber(stock.sl)}
            </div>
          </div>
          <div>
            <div className="text-[10px] text-[var(--text-dim)]">T1</div>
            <div className="text-xs font-medium tabular-nums text-green-400">
              {formatNumber(stock.t1)}
            </div>
          </div>
          <div>
            <div className="text-[10px] text-[var(--text-dim)]">T2</div>
            <div className="text-xs font-medium tabular-nums text-green-400">
              {formatNumber(stock.t2)}
            </div>
          </div>
        </div>

        {/* Indicators row */}
        <div className="px-3 py-2 border-t border-[var(--border)] flex flex-wrap gap-x-3 gap-y-1 text-[11px]">
          <span className="text-[var(--text-dim)]">
            RSI <span className={clsx("font-medium", stock.rsi < 30 ? "text-green-400" : stock.rsi > 70 ? "text-red-400" : "text-[var(--text)]")}>
              {stock.rsi?.toFixed(1)}
            </span>
          </span>
          <span className="text-[var(--text-dim)]">
            StochRSI <span className="font-medium text-[var(--text)]">{stock.stoch_rsi?.toFixed(1)}</span>
          </span>
          <span className="text-[var(--text-dim)]">
            MACD <span className={clsx(
              "font-medium",
              stock.macd_status?.toLowerCase().includes("bullish") ? "text-green-400" :
              stock.macd_status?.toLowerCase().includes("bearish") ? "text-red-400" : "text-amber-400",
            )}>
              {stock.macd_status}
            </span>
          </span>
          <span className="text-[var(--text-dim)]">
            Risk <span className={clsx("font-medium", colorBySign(-(stock.risk_pct ?? 0)))}>
              {formatPct(stock.risk_pct)}
            </span>
          </span>
          <span className="text-[var(--text-dim)]">
            Reward <span className={clsx("font-medium", colorBySign(stock.reward_pct ?? 0))}>
              {formatPct(stock.reward_pct)}
            </span>
          </span>
          {riskReward > 0 && (
            <span className="text-[var(--text-dim)]">
              R:R <span className={clsx("font-medium", riskReward >= 2 ? "text-green-400" : riskReward >= 1 ? "text-amber-400" : "text-red-400")}>
                1:{riskReward.toFixed(1)}
              </span>
            </span>
          )}
        </div>

        {/* Reasoning */}
        {stock.reasoning && (
          <div className="px-3 py-2 border-t border-[var(--border)]">
            <p className="text-[11px] text-[var(--text-muted)] leading-relaxed">
              {expanded ? stock.reasoning : stock.reasoning.slice(0, 120) + (stock.reasoning.length > 120 ? "..." : "")}
            </p>
          </div>
        )}

        {/* Wait days + expand */}
        <div className="px-3 py-1.5 border-t border-[var(--border)] flex items-center justify-between">
          {stock.wait_days && (
            <span className="text-[10px] text-[var(--text-dim)] flex items-center gap-1">
              <Clock className="h-3 w-3" />
              {stock.wait_days}
            </span>
          )}
          <button
            onClick={() => setExpanded(!expanded)}
            className="ml-auto flex items-center gap-0.5 text-[10px] text-[var(--text-dim)] hover:text-[var(--text)] transition-colors"
          >
            {expanded ? "Less" : "More"}
            {expanded ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
          </button>
        </div>

        {/* Expanded detail */}
        {expanded && (
          <div className="px-3 py-2 border-t border-[var(--border)] space-y-2">
            {/* Support/Resistance */}
            <div className="flex gap-4 text-[11px]">
              <span className="text-[var(--text-dim)]">Support: <span className="text-[var(--text)] font-medium">{formatNumber(stock.support)}</span></span>
              <span className="text-[var(--text-dim)]">Resistance: <span className="text-[var(--text)] font-medium">{formatNumber(stock.resistance)}</span></span>
              <span className="text-[var(--text-dim)]">ATR: <span className="text-[var(--text)] font-medium">{formatNumber(stock.atr)} ({formatPct(stock.atr_pct)})</span></span>
            </div>
            <div className="flex gap-4 text-[11px]">
              <span className="text-[var(--text-dim)]">50d Trend: <span className={clsx("font-medium", colorBySign(stock.trend_50d))}>{formatPct(stock.trend_50d)}</span></span>
              <span className="text-[var(--text-dim)]">Vol ratio: <span className="text-[var(--text)] font-medium">{stock.vol_ratio?.toFixed(1)}x</span></span>
              <span className="text-[var(--text-dim)]">Max DD: <span className="text-red-400 font-medium">{formatPct(stock.max_dd)}</span></span>
            </div>

            {/* Sector / Category (shown in expanded) */}
            {(stock.sector || stock.category) && (
              <div className="flex gap-4 text-[11px]">
                {stock.sector && <span className="text-[var(--text-dim)]">Sector: <span className="text-[var(--text)] font-medium">{stock.sector}</span></span>}
                {stock.category && <span className="text-[var(--text-dim)]">Category: <span className="text-[var(--text)] font-medium">{stock.category}</span></span>}
              </div>
            )}

            {/* Scenarios */}
            {scenarios.length > 0 && (
              <div className="space-y-1.5">
                <div className="text-[10px] font-semibold text-[var(--text-dim)] uppercase tracking-wider">Scenarios</div>
                {scenarios.map((sc, i) => (
                  <div key={i} className="text-[11px]">
                    <span className={clsx(
                      "font-medium",
                      (sc.name || "").toLowerCase().includes("dip") ? "text-green-400" :
                      (sc.name || "").toLowerCase().includes("gap down") ? "text-red-400" : "text-amber-400",
                    )}>{sc.name}</span>
                    {sc.steps && (
                      <ul className="ml-3 mt-0.5 space-y-0.5 text-[var(--text-muted)]">
                        {sc.steps.map((step, j) => (
                          <li key={j} className="list-disc list-inside">{step}</li>
                        ))}
                      </ul>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    );
  },
);
