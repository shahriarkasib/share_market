import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { RefreshCw } from "lucide-react";
import {
  fetchLiveCompositeSignals,
  fetchNasdaqLiveSignals,
  type LiveCompositeSignal,
} from "../api/client";

interface Props {
  market?: "dse" | "nasdaq";
}

const REFRESH_MS = 5 * 60 * 1000; // 5 min

function fmtDateTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  const datePart = d.toLocaleDateString("en-GB", { day: "2-digit", month: "short" });
  const timePart = d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", hour12: false });
  return `${datePart} ${timePart}`;
}

function pctClass(pct: number | null | undefined): string {
  if (pct == null) return "text-[var(--text-muted)]";
  if (pct > 0.1) return "text-emerald-500";
  if (pct < -0.1) return "text-red-500";
  return "text-[var(--text-muted)]";
}

function fmtPct(pct: number | null | undefined): string {
  if (pct == null) return "—";
  return `${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%`;
}

function fmtPrice(p: number | null | undefined, cur: string): string {
  if (p == null || p === 0) return "—";
  return `${cur}${p.toFixed(1)}`;
}

function verdictColor(v: string | null | undefined): string {
  if (!v) return "text-[var(--text-muted)]";
  if (v === "STRONG_BUY") return "text-emerald-500 font-semibold";
  if (v === "BUY") return "text-emerald-400";
  if (v === "WATCH") return "text-amber-500";
  if (v === "NEUTRAL") return "text-gray-400";
  if (v === "AVOID") return "text-orange-500";
  return "text-red-500";
}

function statusBadge(status: string | null | undefined): { text: string; cls: string } {
  if (status === "active") return { text: "active", cls: "text-emerald-500 bg-emerald-500/10 border-emerald-500/30" };
  if (status === "hit_t1") return { text: "hit T1", cls: "text-blue-500 bg-blue-500/10 border-blue-500/30" };
  if (status === "completed") return { text: "completed", cls: "text-emerald-600 bg-emerald-600/10 border-emerald-600/30" };
  if (status === "stopped_out") return { text: "stopped", cls: "text-red-500 bg-red-500/10 border-red-500/30" };
  if (status === "invalidated") return { text: "invalid", cls: "text-orange-500 bg-orange-500/10 border-orange-500/30" };
  if (status === "expired") return { text: "expired", cls: "text-gray-500 bg-gray-500/10 border-gray-500/30" };
  return { text: status || "—", cls: "text-gray-500 bg-gray-500/10 border-gray-500/30" };
}

export default function LiveCompositeSignals({ market = "dse" }: Props = {}) {
  const isNasdaq = market === "nasdaq";
  const cur = isNasdaq ? "$" : "৳";
  const chartBase = isNasdaq ? "/nasdaq/smc-chart/" : "/smc-chart/";

  const [signals, setSignals] = useState<LiveCompositeSignal[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      // Show ALL signals (no BUY-only filter), but keep quality filter
      // to exclude insurance/MF/banks/BATBC noise.
      const data = isNasdaq
        ? await fetchNasdaqLiveSignals("all", 0)
        : await fetchLiveCompositeSignals("all", 0, { buy_only: false, quality_filter: true });
      // Sort by first_triggered DESC (latest first)
      data.sort((a, b) => {
        const ta = a.first_triggered ? new Date(a.first_triggered).getTime() : 0;
        const tb = b.first_triggered ? new Date(b.first_triggered).getTime() : 0;
        return tb - ta;
      });
      // Dedupe: keep only the MOST RECENT trigger per symbol (data is already
      // sorted DESC by first_triggered so the first occurrence wins).
      const seen = new Set<string>();
      const deduped = data.filter((s) => {
        if (seen.has(s.symbol)) return false;
        seen.add(s.symbol);
        return true;
      });
      setSignals(deduped);
      setLastRefresh(new Date());
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  useEffect(() => {
    const id = window.setInterval(() => void load(), REFRESH_MS);
    return () => window.clearInterval(id);
  }, []);

  const exportCSV = () => {
    const cols = [
      "symbol", "first_triggered", "actual_trigger_price", "current_price", "pl_pct",
      "t1_close", "t2_close", "t5_close",
      "verdict", "analyst_score", "composite_score",
      "status", "bucket", "bias", "regime", "action_type",
      "stop_loss", "target1", "target2", "rvol"
    ];
    const rows = signals.map((s) => {
      const verdict = s.analyst_verdict?.verdict || "";
      const tp = s.actual_trigger_price ?? null;
      const cp = s.current_price ?? null;
      const pl = (tp && cp && tp > 0) ? ((cp - tp) / tp * 100).toFixed(2) : "";
      return [
        s.symbol,
        s.first_triggered ? `"${s.first_triggered}"` : "",
        tp ?? "",
        cp ?? "",
        pl,
        s.t1_close ?? "",
        s.t2_close ?? "",
        s.t5_close ?? "",
        verdict,
        s.analyst_score ?? "",
        s.composite_score ?? "",
        s.status ?? "",
        s.bucket ?? "",
        s.bias ?? "",
        s.regime ?? "",
        s.action_type ?? "",
        s.stop_loss ?? "",
        s.target1 ?? "",
        s.target2 ?? "",
        s.rvol ?? "",
      ].join(",");
    });
    const csv = cols.join(",") + "\n" + rows.join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `live_signals_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-xl font-semibold">
            {isNasdaq ? "🇺🇸 NASDAQ" : "🇧🇩 DSE"} Live Signals
          </h1>
          <p className="text-xs text-[var(--text-muted)] mt-1">
            {signals.length} signals · sorted by trigger time (newest first)
            {lastRefresh && (
              <> · Last refresh {lastRefresh.toLocaleTimeString()} · <span className="text-emerald-500">auto-refreshing</span></>
            )}
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={exportCSV}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded text-xs border border-[var(--border)] hover:bg-[var(--hover)]"
            title="Export to CSV"
          >
            📥 CSV
          </button>
          <button
            onClick={load}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded text-xs border border-[var(--border)] hover:bg-[var(--hover)] disabled:opacity-50"
          >
            <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-500 mb-3">
          {error}
        </div>
      )}

      <div className="rounded border border-[var(--border)] overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-[var(--surface)] text-[var(--text-muted)] text-xs">
            <tr>
              <th className="text-left px-3 py-2 font-medium">Symbol</th>
              <th className="text-left px-3 py-2 font-medium">Triggered</th>
              <th className="text-right px-3 py-2 font-medium">Trigger ৳</th>
              <th className="text-right px-3 py-2 font-medium">Current ৳</th>
              <th className="text-right px-3 py-2 font-medium">P&L</th>
              <th className="text-right px-3 py-2 font-medium">T+1 Close</th>
              <th className="text-right px-3 py-2 font-medium">T+2 Close</th>
              <th className="text-right px-3 py-2 font-medium">T+5 Close</th>
              <th className="text-left px-3 py-2 font-medium">Verdict</th>
              <th className="text-right px-3 py-2 font-medium">Score</th>
              <th className="text-right px-3 py-2 font-medium">Stop</th>
              <th className="text-right px-3 py-2 font-medium">Target 1</th>
              <th className="text-right px-3 py-2 font-medium">Target 2</th>
              <th className="text-right px-3 py-2 font-medium">RVOL</th>
              <th className="text-left px-3 py-2 font-medium">Status</th>
            </tr>
          </thead>
          <tbody>
            {signals.length === 0 && !loading && (
              <tr>
                <td colSpan={15} className="px-3 py-8 text-center text-[var(--text-muted)]">
                  No signals.
                </td>
              </tr>
            )}
            {signals.map((s) => {
              const verdict = s.analyst_verdict?.verdict;
              const tp = s.actual_trigger_price;
              const cp = s.current_price;
              const pl = (tp && cp && tp > 0) ? (cp - tp) / tp * 100 : null;
              const t1Pct = (tp && s.t1_close && tp > 0 && s.t1_close > 0) ? (s.t1_close - tp) / tp * 100 : null;
              const t2Pct = (tp && s.t2_close && tp > 0 && s.t2_close > 0) ? (s.t2_close - tp) / tp * 100 : null;
              const t5Pct = (tp && s.t5_close && tp > 0 && s.t5_close > 0) ? (s.t5_close - tp) / tp * 100 : null;
              const sb = statusBadge(s.status);
              return (
                <tr key={s.id} className="border-t border-[var(--border)] hover:bg-[var(--hover)]/40">
                  <td className="px-3 py-2">
                    <Link
                      to={`${chartBase}${s.symbol}`}
                      className="font-mono font-bold text-blue-500 hover:underline"
                    >
                      {s.symbol}
                    </Link>
                  </td>
                  <td className="px-3 py-2 text-xs font-mono text-[var(--text-muted)]">
                    {fmtDateTime(s.first_triggered)}
                  </td>
                  <td className="px-3 py-2 text-right font-mono">
                    {fmtPrice(tp, cur)}
                  </td>
                  <td className="px-3 py-2 text-right font-mono">
                    {fmtPrice(cp, cur)}
                  </td>
                  <td className={`px-3 py-2 text-right font-mono text-xs ${pctClass(pl)}`}>
                    {fmtPct(pl)}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-xs">
                    <div>{fmtPrice(s.t1_close, cur)}</div>
                    <div className={`text-[10px] ${pctClass(t1Pct)}`}>{fmtPct(t1Pct)}</div>
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-xs">
                    <div>{fmtPrice(s.t2_close, cur)}</div>
                    <div className={`text-[10px] ${pctClass(t2Pct)}`}>{fmtPct(t2Pct)}</div>
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-xs">
                    <div>{fmtPrice(s.t5_close, cur)}</div>
                    <div className={`text-[10px] ${pctClass(t5Pct)}`}>{fmtPct(t5Pct)}</div>
                  </td>
                  <td className={`px-3 py-2 text-xs ${verdictColor(verdict)}`}>
                    {verdict || "—"}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-xs">
                    {s.analyst_score != null ? (
                      <span className={s.analyst_score >= 25 ? "text-emerald-400" : s.analyst_score <= -25 ? "text-red-400" : "text-[var(--text-muted)]"}>
                        {s.analyst_score >= 0 ? "+" : ""}{s.analyst_score}
                      </span>
                    ) : "—"}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-xs text-[var(--text-muted)]">
                    {fmtPrice(s.stop_loss, cur)}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-xs text-emerald-400/80">
                    {fmtPrice(s.target1, cur)}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-xs text-emerald-500/80">
                    {fmtPrice(s.target2, cur)}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-xs">
                    {s.rvol != null ? `${Number(s.rvol).toFixed(1)}×` : "—"}
                  </td>
                  <td className="px-3 py-2">
                    <span className={`inline-block px-2 py-0.5 rounded border text-[10px] ${sb.cls}`}>
                      {sb.text}
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
