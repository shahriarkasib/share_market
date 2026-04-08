import { useState, useEffect, useCallback } from "react";
import { clsx } from "clsx";
import { Loader2, RefreshCw, Zap, TrendingUp, TrendingDown, Minus } from "lucide-react";
import { fetchLiveSignals } from "../api/client.ts";
import type { LiveSignal } from "../api/client.ts";

const MOMENTUM_COLORS: Record<string, string> = {
  STRONG_BULLISH: "text-emerald-400 bg-emerald-400/10",
  BULLISH: "text-emerald-400",
  GAP_FILL_UP: "text-emerald-300",
  NEUTRAL: "text-gray-400",
  GAP_FADE: "text-amber-400",
  BEARISH: "text-red-400",
  STRONG_BEARISH: "text-red-400 bg-red-400/10",
};

const MOMENTUM_LABELS: Record<string, string> = {
  STRONG_BULLISH: "Strong Bullish",
  BULLISH: "Bullish",
  GAP_FILL_UP: "Gap Filling Up",
  NEUTRAL: "Neutral",
  GAP_FADE: "Gap Fading",
  BEARISH: "Bearish",
  STRONG_BEARISH: "Strong Bearish",
};

function MomentumBadge({ momentum }: { momentum: string }) {
  const color = MOMENTUM_COLORS[momentum] || "text-gray-400";
  const label = MOMENTUM_LABELS[momentum] || momentum;
  return <span className={clsx("px-1.5 py-0.5 rounded text-xs font-bold", color)}>{label}</span>;
}

function VolBadge({ signal, ratio }: { signal: string; ratio: number }) {
  const color = signal === "VERY_HIGH" ? "text-emerald-400" : signal === "HIGH" ? "text-blue-400" : signal === "LOW" ? "text-red-400" : "text-gray-400";
  return <span className={clsx("text-xs font-mono", color)}>{ratio.toFixed(1)}x</span>;
}

function GapBadge({ type, pct }: { type: string; pct: number }) {
  if (type === "FLAT") return null;
  const color = type === "GAP_UP" ? "text-emerald-400" : "text-red-400";
  return <span className={clsx("text-xs font-mono", color)}>{type === "GAP_UP" ? "+" : ""}{pct.toFixed(1)}% gap</span>;
}

function ShadowBadge({ signal }: { signal: string | null }) {
  if (!signal) return null;
  const map: Record<string, { label: string; color: string }> = {
    SELLING_PRESSURE: { label: "Selling Pressure", color: "text-red-400" },
    BUYING_SUPPORT: { label: "Buyer Support", color: "text-emerald-400" },
    INDECISION: { label: "Indecision", color: "text-amber-400" },
  };
  const info = map[signal];
  if (!info) return null;
  return <span className={clsx("text-xs", info.color)}>{info.label}</span>;
}

type FilterType = "ALL" | "STRONG_BULLISH" | "BULLISH" | "BEARISH" | "GAP_UP" | "GAP_DOWN" | "HIGH_VOLUME";

export default function LiveSignals() {
  const [signals, setSignals] = useState<LiveSignal[]>([]);
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState<string>("");
  const [filter, setFilter] = useState<FilterType>("ALL");
  const [search, setSearch] = useState("");

  const loadData = useCallback(async () => {
    try {
      const res = await fetchLiveSignals();
      setSignals(res.signals);
      setLastUpdate(new Date().toLocaleTimeString("en-GB"));
    } catch {
      // silent fail on refresh
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
    // Auto-refresh every 2 minutes during market hours
    const interval = setInterval(loadData, 120_000);
    return () => clearInterval(interval);
  }, [loadData]);

  const filtered = signals.filter((s) => {
    if (search && !s.symbol.toLowerCase().includes(search.toLowerCase())) return false;
    switch (filter) {
      case "STRONG_BULLISH": return s.momentum === "STRONG_BULLISH";
      case "BULLISH": return s.momentum === "BULLISH" || s.momentum === "STRONG_BULLISH";
      case "BEARISH": return s.momentum === "BEARISH" || s.momentum === "STRONG_BEARISH";
      case "GAP_UP": return s.gap_type === "GAP_UP";
      case "GAP_DOWN": return s.gap_type === "GAP_DOWN";
      case "HIGH_VOLUME": return s.vol_signal === "VERY_HIGH" || s.vol_signal === "HIGH";
      default: return true;
    }
  });

  // Counts
  const bullishCount = signals.filter(s => s.momentum === "STRONG_BULLISH" || s.momentum === "BULLISH").length;
  const bearishCount = signals.filter(s => s.momentum === "BEARISH" || s.momentum === "STRONG_BEARISH").length;
  const gapUpCount = signals.filter(s => s.gap_type === "GAP_UP").length;
  const highVolCount = signals.filter(s => s.vol_signal === "VERY_HIGH" || s.vol_signal === "HIGH").length;

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="animate-spin text-blue-400" size={32} />
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Zap size={18} className="text-amber-400" />
          <h2 className="text-lg font-bold">Live Signals</h2>
          <span className="text-xs text-[var(--text-dim)]">Updated {lastUpdate}</span>
        </div>
        <button onClick={loadData} className="p-1.5 rounded hover:bg-[var(--hover)]">
          <RefreshCw size={14} />
        </button>
      </div>

      {/* Summary bar */}
      <div className="flex flex-wrap gap-3 text-sm">
        <span className="text-emerald-400">{bullishCount} Bullish</span>
        <span className="text-red-400">{bearishCount} Bearish</span>
        <span className="text-blue-400">{gapUpCount} Gap Up</span>
        <span className="text-amber-400">{highVolCount} High Vol</span>
      </div>

      {/* Filters */}
      <div className="flex items-center gap-1 overflow-x-auto pb-1">
        {([
          ["ALL", `All (${signals.length})`],
          ["BULLISH", `Bullish (${bullishCount})`],
          ["BEARISH", `Bearish (${bearishCount})`],
          ["GAP_UP", `Gap Up (${gapUpCount})`],
          ["HIGH_VOLUME", `High Vol (${highVolCount})`],
        ] as [FilterType, string][]).map(([key, label]) => (
          <button
            key={key}
            onClick={() => setFilter(key)}
            className={clsx(
              "px-2.5 py-1 rounded text-xs font-medium whitespace-nowrap",
              filter === key ? "bg-blue-500/20 text-blue-400" : "text-[var(--text-muted)] hover:bg-[var(--hover)]"
            )}
          >
            {label}
          </button>
        ))}
        <input
          type="text"
          placeholder="Search..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="ml-auto px-2 py-1 rounded bg-[var(--surface)] border border-[var(--border)] text-xs w-24 focus:outline-none focus:border-blue-400"
        />
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="text-[var(--text-dim)] border-b border-[var(--border)]">
              <th className="text-left py-1.5 px-1">Symbol</th>
              <th className="text-right px-1">LTP</th>
              <th className="text-right px-1">Chg%</th>
              <th className="text-center px-1">Momentum</th>
              <th className="text-center px-1">Gap</th>
              <th className="text-center px-1">Candle</th>
              <th className="text-right px-1">Vol</th>
              <th className="text-center px-1">Pivot</th>
              <th className="text-center px-1">Structure</th>
              <th className="text-right px-1">P</th>
              <th className="text-right px-1">R1</th>
              <th className="text-right px-1">S1</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((s) => (
              <tr key={s.symbol} className="border-b border-[var(--border)]/30 hover:bg-[var(--hover)]">
                <td className="py-1.5 px-1 font-bold">{s.symbol}</td>
                <td className="text-right px-1 font-mono">{s.ltp.toFixed(1)}</td>
                <td className={clsx("text-right px-1 font-mono", s.change_pct > 0 ? "text-emerald-400" : s.change_pct < 0 ? "text-red-400" : "")}>
                  {s.change_pct > 0 ? "+" : ""}{s.change_pct.toFixed(1)}%
                </td>
                <td className="text-center px-1"><MomentumBadge momentum={s.momentum} /></td>
                <td className="text-center px-1"><GapBadge type={s.gap_type} pct={s.gap_pct} /></td>
                <td className="text-center px-1">
                  <span className={clsx("text-xs", s.body === "BULLISH" ? "text-emerald-400" : s.body === "BEARISH" ? "text-red-400" : "text-gray-400")}>
                    {s.body === "BULLISH" ? <TrendingUp size={12} className="inline" /> : s.body === "BEARISH" ? <TrendingDown size={12} className="inline" /> : <Minus size={12} className="inline" />}
                  </span>
                  {s.shadow_signal && <ShadowBadge signal={s.shadow_signal} />}
                </td>
                <td className="text-right px-1"><VolBadge signal={s.vol_signal} ratio={s.vol_ratio} /></td>
                <td className="text-center px-1">
                  {s.pivot_position && (
                    <span className={clsx("text-xs",
                      s.pivot_position === "ABOVE_R1" ? "text-emerald-400" :
                      s.pivot_position === "ABOVE_PIVOT" ? "text-blue-400" :
                      s.pivot_position === "BELOW_S1" ? "text-red-400" : "text-amber-400"
                    )}>
                      {s.pivot_position.replace("_", " ").replace("ABOVE ", ">").replace("BELOW ", "<")}
                    </span>
                  )}
                </td>
                <td className="text-center px-1">
                  <span className={clsx("text-xs",
                    s.swing_structure === "UPTREND" ? "text-emerald-400" :
                    s.swing_structure === "DOWNTREND" ? "text-red-400" :
                    s.swing_structure === "HIGHER_LOWS" ? "text-blue-400" : "text-gray-400"
                  )}>
                    {s.swing_structure || "–"}
                  </span>
                </td>
                <td className="text-right px-1 font-mono text-[var(--text-dim)]">{s.pivot_p?.toFixed(1) || "–"}</td>
                <td className="text-right px-1 font-mono text-emerald-400/60">{s.pivot_r1?.toFixed(1) || "–"}</td>
                <td className="text-right px-1 font-mono text-red-400/60">{s.pivot_s1?.toFixed(1) || "–"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {filtered.length === 0 && (
        <div className="text-center text-[var(--text-dim)] py-8">
          {signals.length === 0 ? "Market may be closed. Signals appear during trading hours." : "No stocks match this filter."}
        </div>
      )}
    </div>
  );
}
