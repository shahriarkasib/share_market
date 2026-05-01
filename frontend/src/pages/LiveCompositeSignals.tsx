import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { RefreshCw, TrendingUp, AlertTriangle, Clock } from "lucide-react";
import { fetchLiveCompositeSignals, type LiveCompositeSignal } from "../api/client";

type StatusFilter = "active" | "hit_t1" | "all" | "closed";

const REFRESH_MS = 5 * 60 * 1000; // 5 min

function timeAgo(iso: string): string {
  const ms = Date.now() - new Date(iso).getTime();
  const min = Math.floor(ms / 60000);
  if (min < 1) return "just now";
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  return `${Math.floor(hr / 24)}d ago`;
}

function levelColor(level: string): string {
  if (level === "STRONG_BUY") return "text-emerald-500 bg-emerald-500/15 border-emerald-500/40";
  if (level === "BUY") return "text-emerald-400 bg-emerald-500/10 border-emerald-500/30";
  if (level === "WATCH") return "text-amber-500 bg-amber-500/10 border-amber-500/30";
  return "text-gray-500 bg-gray-500/10 border-gray-500/30";
}

function statusColor(status: string): string {
  if (status === "active") return "text-emerald-500";
  if (status === "hit_t1") return "text-blue-500";
  if (status === "completed") return "text-emerald-600";
  if (status === "stopped_out") return "text-red-500";
  if (status === "invalidated") return "text-orange-500";
  return "text-gray-500";
}

function riskBars(risk: number): string {
  return "█".repeat(risk) + "░".repeat(5 - risk);
}

export default function LiveCompositeSignals() {
  const [signals, setSignals] = useState<LiveCompositeSignal[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<StatusFilter>("active");
  const [minScore, setMinScore] = useState(60);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchLiveCompositeSignals(filter, minScore);
      setSignals(data);
      setLastRefresh(new Date());
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, [filter, minScore]);

  // Auto-refresh every 5 min
  useEffect(() => {
    const id = window.setInterval(() => void load(), REFRESH_MS);
    return () => window.clearInterval(id);
  }, [filter, minScore]);

  const grouped = useMemo(() => {
    const strong = signals.filter((s) => s.signal_level === "STRONG_BUY");
    const buy = signals.filter((s) => s.signal_level === "BUY");
    const watch = signals.filter((s) => s.signal_level === "WATCH");
    return { strong, buy, watch };
  }, [signals]);

  return (
    <div className="max-w-[1440px] mx-auto px-3 sm:px-4 lg:px-8 py-6">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <div>
          <h1 className="text-xl font-bold flex items-center gap-2">
            Live Composite Signals
            <span className="text-xs font-normal text-[var(--text-muted)]">
              9 strategies × all stocks · refreshes every 5 min
            </span>
          </h1>
          {lastRefresh && (
            <p className="text-xs text-[var(--text-muted)] mt-0.5">
              Last refresh: {lastRefresh.toLocaleTimeString()} ·{" "}
              <span className="text-emerald-500">auto-refreshing</span>
            </p>
          )}
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded text-xs border border-[var(--border)] hover:bg-[var(--hover)] disabled:opacity-50"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      <div className="flex flex-wrap items-center gap-2 mb-4">
        <span className="text-xs text-[var(--text-muted)]">Status:</span>
        {(["active", "hit_t1", "closed", "all"] as StatusFilter[]).map((f) => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`px-2.5 py-1 rounded text-xs border transition ${
              filter === f
                ? "bg-[var(--surface-active)] border-[var(--border)] text-[var(--text)]"
                : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]"
            }`}
          >
            {f.toUpperCase()}
          </button>
        ))}
        <span className="text-xs text-[var(--text-muted)] ml-4">Min score:</span>
        {[50, 60, 70, 80].map((s) => (
          <button
            key={s}
            onClick={() => setMinScore(s)}
            className={`px-2.5 py-1 rounded text-xs border transition ${
              minScore === s
                ? "bg-[var(--surface-active)] border-[var(--border)] text-[var(--text)]"
                : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]"
            }`}
          >
            {s}+
          </button>
        ))}
      </div>

      {error && (
        <div className="rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-500 mb-3">
          {error}
        </div>
      )}

      {grouped.strong.length === 0 && grouped.buy.length === 0 && grouped.watch.length === 0 && !loading && (
        <div className="rounded border border-[var(--border)] px-4 py-8 text-center text-[var(--text-muted)] text-sm">
          No signals matching this filter. Lower min-score or change status to see more.
        </div>
      )}

      {(["strong", "buy", "watch"] as const).map((tier) => {
        const list = grouped[tier];
        if (list.length === 0) return null;
        const tierLabel =
          tier === "strong" ? "🔥 STRONG BUY (≥80)" :
          tier === "buy" ? "✅ BUY (65-79)" :
          "👀 WATCH (50-64)";
        return (
          <div key={tier} className="mb-6">
            <div className="text-sm font-bold mb-2 text-[var(--text)]">
              {tierLabel} <span className="text-[var(--text-muted)] font-normal">({list.length})</span>
            </div>
            <div className="rounded-lg border border-[var(--border)] overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-[var(--surface-active)] text-[var(--text-muted)] text-xs">
                  <tr>
                    <th className="text-left px-3 py-2 font-medium">Symbol</th>
                    <th className="text-right px-3 py-2 font-medium">Score</th>
                    <th className="text-left px-3 py-2 font-medium">Status</th>
                    <th className="text-left px-3 py-2 font-medium">Triggered</th>
                    <th className="text-right px-3 py-2 font-medium">Entry</th>
                    <th className="text-right px-3 py-2 font-medium">Stop</th>
                    <th className="text-right px-3 py-2 font-medium">T1</th>
                    <th className="text-right px-3 py-2 font-medium">Risk</th>
                    <th className="text-left px-3 py-2 font-medium">Reasons</th>
                  </tr>
                </thead>
                <tbody>
                  {list.map((s) => (
                    <tr key={s.id} className="border-t border-[var(--border)] hover:bg-[var(--hover)]">
                      <td className="px-3 py-2">
                        <Link
                          to={`/smc-chart/${s.symbol}`}
                          className="font-mono font-bold text-blue-500 hover:underline"
                        >
                          {s.symbol}
                        </Link>
                      </td>
                      <td className="px-3 py-2 text-right">
                        <span className={`inline-block px-2 py-0.5 rounded text-xs border ${levelColor(s.signal_level)}`}>
                          {s.composite_score}
                        </span>
                      </td>
                      <td className={`px-3 py-2 text-xs font-medium ${statusColor(s.status)}`}>
                        {s.status === "active" && <TrendingUp className="inline h-3.5 w-3.5 mr-1" />}
                        {s.status === "stopped_out" && <AlertTriangle className="inline h-3.5 w-3.5 mr-1" />}
                        {s.status.replace("_", " ")}
                        {s.pl_pct !== null && s.pl_pct !== undefined && (
                          <span className={`ml-2 ${s.pl_pct >= 0 ? "text-emerald-500" : "text-red-500"}`}>
                            {s.pl_pct > 0 ? "+" : ""}{s.pl_pct.toFixed(1)}%
                          </span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-xs text-[var(--text-muted)]">
                        <Clock className="inline h-3 w-3 mr-1" />
                        {timeAgo(s.first_triggered)}
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-xs">
                        {s.entry !== null ? `৳${s.entry.toFixed(1)}` : "—"}
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-xs text-red-500/80">
                        {s.stop_loss !== null ? `৳${s.stop_loss.toFixed(1)}` : "—"}
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-xs text-emerald-500/80">
                        {s.target1 !== null ? `৳${s.target1.toFixed(1)}` : "—"}
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-xs">
                        <span title={`Risk ${s.risk_score}/5`} className={
                          s.risk_score <= 2 ? "text-emerald-500" :
                          s.risk_score === 3 ? "text-amber-500" :
                          "text-red-500"
                        }>
                          {riskBars(s.risk_score)}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-xs">
                        <div className="flex flex-wrap gap-1">
                          {s.active_signals?.slice(0, 6).map((sig, i) => (
                            <span key={i} className="px-1.5 py-0.5 rounded bg-[var(--surface-active)] text-[10px] text-[var(--text-muted)]">
                              {sig}
                            </span>
                          ))}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );
      })}
    </div>
  );
}
