import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { RefreshCw, ClipboardList, CheckCircle2, XCircle, AlertTriangle } from "lucide-react";
import { fetchDailySummary, type DailySummarySignal } from "../api/client";

function fmt(n: number | null | undefined, decimals = 1): string {
  if (n == null) return "—";
  return n.toFixed(decimals);
}

function fmtVol(n: number | null | undefined): string {
  if (n == null) return "—";
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K`;
  return String(n);
}

function verdictPill(v: string | null | undefined): string {
  if (v === "STRONG_BUY") return "bg-emerald-500/20 text-emerald-300 border-emerald-500/50";
  if (v === "BUY") return "bg-emerald-500/15 text-emerald-400 border-emerald-500/40";
  if (v === "WATCH") return "bg-amber-500/15 text-amber-400 border-amber-500/40";
  return "bg-gray-500/15 text-gray-400 border-gray-500/40";
}

export default function DailySummary() {
  const today = new Date().toISOString().slice(0, 10);
  const [date, setDate] = useState(today);
  const [data, setData] = useState<{ date: string; count: number; signals: DailySummarySignal[] } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetchDailySummary(date);
      setData(res);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, [date]);

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-xl font-semibold flex items-center gap-2">
            <ClipboardList className="h-5 w-5" />
            Daily Summary
          </h1>
          <p className="text-xs text-[var(--text-muted)] mt-1">
            End-of-day BUY signals + next-day confirmation rules
            {data && <> · {data.count} signals on {data.date}</>}
          </p>
        </div>
        <div className="flex gap-2">
          <input
            type="date"
            value={date}
            onChange={(e) => setDate(e.target.value)}
            className="px-3 py-1.5 rounded text-xs border border-[var(--border)] bg-[var(--bg)]"
          />
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

      {data && data.signals.length === 0 && !loading && (
        <div className="rounded border border-[var(--border)] px-6 py-12 text-center text-[var(--text-muted)]">
          No BUY-level signals on {data.date}.
        </div>
      )}

      <div className="space-y-3">
        {data?.signals.map((s) => {
          const c = s.confirmation || {};
          const ohlc = c.today_ohlc;
          return (
            <div
              key={s.symbol}
              className="rounded border border-[var(--border)] overflow-hidden"
            >
              {/* Header */}
              <div className="flex items-center justify-between px-4 py-3 bg-[var(--surface)]">
                <div className="flex items-center gap-3">
                  <Link
                    to={`/smc-chart/${s.symbol}`}
                    className="text-base font-bold text-blue-400 hover:underline"
                  >
                    {s.symbol}
                  </Link>
                  <span className={`text-xs px-2 py-0.5 rounded border ${verdictPill(s.verdict)}`}>
                    {s.verdict || "—"}
                  </span>
                  <span className="text-xs text-[var(--text-muted)]">
                    Score {s.analyst_score != null && s.analyst_score >= 0 ? "+" : ""}{s.analyst_score}
                  </span>
                  {s.rvol != null && (
                    <span className="text-xs text-[var(--text-muted)]">
                      RVOL {s.rvol.toFixed(1)}×
                    </span>
                  )}
                </div>
                <div className="text-right text-xs font-mono">
                  <div>Close ৳{fmt(s.close_price)} · Trigger ৳{fmt(s.trigger_price)}</div>
                  <div className="text-[var(--text-muted)]">
                    Stop ৳{fmt(s.stop_loss)} · T1 ৳{fmt(s.target1)} · T2 ৳{fmt(s.target2)}
                  </div>
                </div>
              </div>

              {/* Why it signaled */}
              {s.top_factors && s.top_factors.length > 0 && (
                <div className="px-4 py-2 border-t border-[var(--border)] bg-[var(--bg)]">
                  <div className="text-[10px] uppercase font-semibold text-[var(--text-muted)] mb-1">
                    Why it signaled
                  </div>
                  <div className="flex flex-wrap gap-1.5">
                    {s.top_factors.slice(0, 5).map((f, i) => (
                      <span
                        key={i}
                        className={`text-[11px] px-2 py-0.5 rounded border ${
                          f.score > 0
                            ? "bg-emerald-500/10 border-emerald-500/30 text-emerald-400"
                            : "bg-red-500/10 border-red-500/30 text-red-400"
                        }`}
                        title={f.detail}
                      >
                        {f.score >= 0 ? "+" : ""}{f.score} {f.factor}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {/* Tomorrow's confirmation rules */}
              {ohlc && (
                <div className="px-4 py-3 border-t border-[var(--border)] bg-blue-500/5">
                  <div className="text-[10px] uppercase font-semibold text-blue-400 mb-2">
                    📋 Tomorrow's Confirmation Rules
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-2 text-[12px]">
                    {/* GO conditions */}
                    <div className="space-y-1">
                      <div className="flex items-center gap-1.5 text-emerald-400 font-semibold">
                        <CheckCircle2 className="h-3.5 w-3.5" /> CONFIRM the BUY if:
                      </div>
                      <div className="pl-5 space-y-0.5 text-[var(--text)]">
                        <div>• Opens at <strong>≥ ৳{fmt(c.min_confirm_open)}</strong> (flat or gap up)</div>
                        <div>• Holds ≥ ৳{fmt(c.must_hold_30min)} in <strong>first 30 min</strong></div>
                        {c.min_volume_first_hour && (
                          <div>• First-hour volume <strong>≥ {fmtVol(c.min_volume_first_hour)}</strong></div>
                        )}
                      </div>
                    </div>

                    {/* INVALIDATE */}
                    <div className="space-y-1">
                      <div className="flex items-center gap-1.5 text-red-400 font-semibold">
                        <XCircle className="h-3.5 w-3.5" /> SKIP/CANCEL the BUY if:
                      </div>
                      <div className="pl-5 space-y-0.5 text-[var(--text)]">
                        <div>• Opens below <strong>৳{fmt(c.invalidation_low)}</strong> (gaps down)</div>
                        <div>• Breaks today's low <strong>৳{fmt(ohlc.low)}</strong></div>
                        <div>• First-hour volume dries up (&lt; {fmtVol(c.min_volume_first_hour ? c.min_volume_first_hour * 0.5 : 0)})</div>
                      </div>
                    </div>

                    {/* STRONG signal */}
                    <div className="space-y-1">
                      <div className="flex items-center gap-1.5 text-emerald-500 font-semibold">
                        🚀 STRONG confirmation:
                      </div>
                      <div className="pl-5 space-y-0.5 text-[var(--text)]">
                        <div>• Gap-up open <strong>≥ ৳{fmt(c.strong_confirm_open)}</strong></div>
                        {c.strong_volume_first_hour && (
                          <div>• First-hour vol <strong>≥ {fmtVol(c.strong_volume_first_hour)}</strong></div>
                        )}
                        <div>• Breaks above ৳{fmt(ohlc.high)} (today's high)</div>
                      </div>
                    </div>

                    {/* Entry plan */}
                    <div className="space-y-1">
                      <div className="flex items-center gap-1.5 text-amber-400 font-semibold">
                        <AlertTriangle className="h-3.5 w-3.5" /> Entry Plan:
                      </div>
                      <div className="pl-5 space-y-0.5 text-[var(--text)]">
                        <div>• Buy zone: <strong>৳{fmt(c.buy_zone_low)} – ৳{fmt(c.buy_zone_high)}</strong></div>
                        <div>• First scalp target: <strong>৳{fmt(c.first_target)}</strong> (+2%)</div>
                        <div>• Stop loss: <strong>৳{fmt(s.stop_loss)}</strong></div>
                      </div>
                    </div>
                  </div>

                  {/* Today's OHLC reference */}
                  <div className="mt-2 pt-2 border-t border-[var(--border)] text-[11px] font-mono text-[var(--text-muted)]">
                    Today: O ৳{fmt(ohlc.open)} · H ৳{fmt(ohlc.high)} · L ৳{fmt(ohlc.low)} · C ৳{fmt(ohlc.close)}
                  </div>
                </div>
              )}

              {/* Bid ladder if present */}
              {s.bid_ladder && s.bid_ladder.length > 0 && (
                <div className="px-4 py-2 border-t border-[var(--border)] bg-emerald-500/5">
                  <div className="text-[10px] uppercase font-semibold text-emerald-400 mb-1">
                    💰 Suggested Bid Placement
                  </div>
                  <div className="flex flex-wrap gap-2 text-[11px]">
                    {s.bid_ladder.map((b, i) => (
                      <span
                        key={i}
                        className="px-2 py-1 rounded border border-emerald-500/30 bg-emerald-500/10 text-emerald-400"
                      >
                        <strong>{b.size_pct}%</strong> @ ৳{b.price.toFixed(1)} — {b.label}
                      </span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
