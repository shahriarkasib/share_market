import { Fragment, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { RefreshCw } from "lucide-react";
import { fetchMethodSignals, fetchMethodCounts } from "../api/client";
import type { MethodSignal, MethodKey, MethodBucketCounts } from "../api/client";

const METHODS: Array<{ key: MethodKey; label: string; emoji: string; tip: string }> = [
  { key: "SMC", label: "SMC", emoji: "📐", tip: "Smart Money Concepts: FVG + BOS/ChoCh + Order Blocks + Premium/Discount" },
  { key: "ORDER_FLOW", label: "Order Flow", emoji: "🌊", tip: "Volume Profile + VWAP + Volume Delta + Absorption + Bid/Ask" },
  { key: "VSA", label: "VSA", emoji: "📊", tip: "Volume Spread Analysis (Tom Williams): No Demand, No Supply, Stopping Volume" },
  { key: "WYCKOFF", label: "Wyckoff", emoji: "🏛️", tip: "Wyckoff method: Spring, SOS, accumulation phases" },
  { key: "HARMONIC", label: "Harmonic", emoji: "🦋", tip: "Harmonic patterns: Gartley, Butterfly, Bat, Crab (XABCD with Fib)" },
  { key: "FIBONACCI", label: "Fibonacci", emoji: "🌀", tip: "Fib retracement: 61.8% / 78.6% Golden Pocket of dealing range" },
  { key: "ELLIOTT", label: "Elliott Wave", emoji: "🌊", tip: "Elliott Wave: contracting triangle, wave 4 complete" },
  { key: "ICHIMOKU", label: "Ichimoku", emoji: "☁️", tip: "Above cloud + Tenkan/Kijun cross + chikou span" },
  { key: "RSI_MACD", label: "RSI / MACD", emoji: "📈", tip: "RSI oversold/divergence + MACD histogram turning up" },
  { key: "BOLLINGER", label: "Bollinger", emoji: "🎯", tip: "Squeeze breakout, mean reversion, BB walk" },
  { key: "CHART_PATTERN", label: "Chart Patterns", emoji: "📉", tip: "H&S, double bottom, cup & handle, ascending triangle" },
  { key: "CANDLE_PATTERN", label: "Candles", emoji: "🕯️", tip: "Hammer, Engulfing, Morning Star at support" },
  { key: "MOVING_AVG", label: "Moving Avg", emoji: "📏", tip: "EMA stack bullish (20>50>200) + pullback to EMA20" },
  { key: "SUPPORT_RESISTANCE", label: "S/R", emoji: "🔵", tip: "Multi-touch support bounce" },
  { key: "OBV_MFI", label: "OBV / MFI", emoji: "💰", tip: "OBV bullish divergence + MFI oversold" },
];

const BUCKETS = [
  { key: "IN_ZONE", label: "🟢 BUY ZONE", desc: "price IN entry range", cls: "bg-emerald-500/15 border-emerald-500/50 text-emerald-500" },
  { key: "WATCHING", label: "👀 WATCHING", desc: "approaching from above", cls: "bg-amber-500/15 border-amber-500/50 text-amber-500" },
  { key: "MISSED", label: "❌ MISSED", desc: "triggered, paid off, didn't buy", cls: "bg-red-500/15 border-red-500/50 text-red-500" },
  { key: "WRONG_TRIGGER", label: "💥 WRONG", desc: "triggered, zone broke", cls: "bg-rose-500/15 border-rose-500/50 text-rose-400" },
  { key: "ALL", label: "📊 ALL", desc: "every signal", cls: "bg-blue-500/15 border-blue-500/50 text-blue-500" },
] as const;

const REFRESH_MS = 5 * 60 * 1000;

export default function MethodSignals() {
  const [method, setMethod] = useState<MethodKey>("SMC");
  const [bucket, setBucket] = useState<typeof BUCKETS[number]["key"]>("IN_ZONE");
  const [signals, setSignals] = useState<MethodSignal[]>([]);
  const [counts, setCounts] = useState<MethodBucketCounts[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const [sigs, cts] = await Promise.all([
        fetchMethodSignals(method),
        fetchMethodCounts(),
      ]);
      setSignals(sigs);
      setCounts(cts);
      setLastRefresh(new Date());
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { void load(); }, [method]);
  useEffect(() => {
    const id = window.setInterval(() => void load(), REFRESH_MS);
    return () => window.clearInterval(id);
  }, [method]);

  const countFor = (m: MethodKey): number => {
    const c = counts.find((x) => x.method === m);
    if (!c) return 0;
    return Object.values(c.buckets).reduce((a, b) => a + (b ?? 0), 0);
  };
  const bucketCountFor = (m: MethodKey, b: string): number => {
    const c = counts.find((x) => x.method === m);
    return c?.buckets[b as keyof typeof c.buckets] ?? 0;
  };

  const filtered = useMemo(() => {
    if (bucket === "ALL") return signals;
    return signals.filter((s) => s.bucket === bucket);
  }, [signals, bucket]);

  return (
    <div className="max-w-[1440px] mx-auto px-3 sm:px-4 lg:px-8 py-6">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <div>
          <h1 className="text-xl font-bold flex items-center gap-2">
            Per-Method Signals
            <span className="text-xs font-normal text-[var(--text-muted)]">
              15 trading methodologies · trade your favorite system
            </span>
          </h1>
          {lastRefresh && (
            <p className="text-xs text-[var(--text-muted)] mt-0.5">
              Last refresh: {lastRefresh.toLocaleTimeString()} · auto-refresh every 5 min
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

      {/* METHOD TABS — 15 methodologies */}
      <div className="mb-3 flex flex-wrap gap-1.5">
        {METHODS.map((m) => {
          const n = countFor(m.key);
          const active = method === m.key;
          return (
            <button
              key={m.key}
              onClick={() => setMethod(m.key)}
              title={m.tip}
              className={`px-2.5 py-1.5 rounded text-xs border transition ${
                active
                  ? "bg-blue-500/15 border-blue-500/50 text-blue-500 font-semibold"
                  : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]"
              }`}
            >
              <span className="mr-1">{m.emoji}</span>{m.label}
              {n > 0 && <span className="ml-1.5 opacity-80">({n})</span>}
            </button>
          );
        })}
      </div>

      {/* BUCKET TABS — within selected methodology */}
      <div className="mb-4 flex flex-wrap gap-2 border-t border-[var(--border)] pt-3">
        {BUCKETS.map((b) => {
          const n = b.key === "ALL" ? countFor(method) : bucketCountFor(method, b.key);
          return (
            <button
              key={b.key}
              onClick={() => setBucket(b.key)}
              title={b.desc}
              className={`px-3 py-1.5 rounded text-xs border transition ${
                bucket === b.key
                  ? b.cls
                  : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]"
              }`}
            >
              <span className="font-semibold">{b.label}</span>
              <span className="ml-1.5 opacity-80">({n})</span>
            </button>
          );
        })}
      </div>

      {error && (
        <div className="rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-500 mb-3">
          {error}
        </div>
      )}

      {filtered.length === 0 && !loading && (
        <div className="rounded border border-[var(--border)] px-4 py-8 text-center text-[var(--text-muted)] text-sm">
          No <strong>{method}</strong> signals in <strong>{bucket}</strong> bucket. Try a different bucket or method.
        </div>
      )}

      {filtered.length > 0 && (
        <div className="rounded-lg border border-[var(--border)] overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-[var(--surface-active)] text-[var(--text-muted)] text-xs">
              <tr>
                <th className="text-left px-3 py-2 font-medium">Symbol</th>
                <th className="text-right px-3 py-2 font-medium">Current</th>
                <th className="text-right px-3 py-2 font-medium">Entry Zone</th>
                <th className="text-right px-3 py-2 font-medium">Stop</th>
                <th className="text-right px-3 py-2 font-medium">T1</th>
                <th className="text-left px-3 py-2 font-medium">Bucket</th>
                <th className="text-left px-3 py-2 font-medium">Reason</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((s) => {
                const cur = s.current_price;
                const zlow = s.entry_zone_low; const zhigh = s.entry_zone_high;
                let dist: number | null = null;
                if (cur != null && zhigh != null) {
                  dist = ((cur - zhigh) / zhigh) * 100;
                }
                const bucketColor =
                  s.bucket === "IN_ZONE" ? "text-emerald-500 font-bold" :
                  s.bucket === "WATCHING" ? "text-amber-500" :
                  s.bucket === "MISSED" ? "text-red-500" :
                  s.bucket === "WRONG_TRIGGER" ? "text-rose-400" :
                  "text-gray-500";
                return (
                  <Fragment key={s.id}>
                    <tr className="border-t border-[var(--border)] hover:bg-[var(--hover)]">
                      <td className="px-3 py-2">
                        <Link
                          to={`/smc-chart/${s.symbol}`}
                          className="font-mono font-bold text-blue-500 hover:underline"
                        >
                          {s.symbol}
                        </Link>
                        {s.confidence && (
                          <div className="text-[10px] text-[var(--text-muted)] mt-0.5">
                            conf: <span className={
                              s.confidence === "HIGH" ? "text-emerald-500" :
                              s.confidence === "MEDIUM" ? "text-amber-500" :
                              "text-gray-500"
                            }>{s.confidence}</span>
                          </div>
                        )}
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-xs">
                        {cur != null ? `৳${cur.toFixed(2)}` : "—"}
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-xs">
                        {zlow != null && zhigh != null ? (
                          <>
                            ৳{zlow.toFixed(2)}–{zhigh.toFixed(2)}
                            {dist != null && (
                              <div className="text-[10px] opacity-80">
                                {dist > 1 ? `${dist.toFixed(1)}% above` :
                                 dist < -1 ? `${Math.abs(dist).toFixed(1)}% below` :
                                 "in zone"}
                              </div>
                            )}
                          </>
                        ) : "—"}
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-xs text-red-500/80">
                        {s.stop_loss != null ? `৳${s.stop_loss.toFixed(2)}` : "—"}
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-xs text-emerald-500/80">
                        {s.target1 != null ? `৳${s.target1.toFixed(2)}` : "—"}
                      </td>
                      <td className={`px-3 py-2 text-xs ${bucketColor}`}>
                        {s.bucket}
                      </td>
                      <td className="px-3 py-2 text-xs text-[var(--text-muted)]">
                        {s.reason}
                      </td>
                    </tr>
                    {(s.bucket === "MISSED" || s.bucket === "WRONG_TRIGGER") && s.trigger_date && (
                      <tr className="border-t border-[var(--border)]/30">
                        <td colSpan={7} className="px-3 py-1.5 text-[11px] bg-blue-500/5">
                          <span className="text-blue-400 font-semibold">📅 Triggered: </span>
                          <strong>{s.trigger_date}</strong>
                          {s.bars_since_trigger != null && <> ({s.bars_since_trigger}d ago)</>}
                          {s.max_profit_since_pct != null && (
                            <> — max profit since: <strong className={
                              s.max_profit_since_pct >= 0 ? "text-emerald-500" : "text-red-500"
                            }>{s.max_profit_since_pct >= 0 ? "+" : ""}{s.max_profit_since_pct.toFixed(1)}%</strong></>
                          )}
                          {s.max_drawdown_since_pct != null && s.max_drawdown_since_pct < 0 && (
                            <> · max drawdown: <strong className="text-red-500">{s.max_drawdown_since_pct.toFixed(1)}%</strong></>
                          )}
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
