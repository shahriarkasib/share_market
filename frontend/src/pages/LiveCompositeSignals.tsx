import { Fragment, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { RefreshCw, TrendingUp, AlertTriangle, Clock } from "lucide-react";
import {
  fetchLiveCompositeSignals,
  fetchNasdaqLiveSignals,
  type LiveCompositeSignal,
} from "../api/client";

interface Props {
  market?: "dse" | "nasdaq";
}

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

export default function LiveCompositeSignals({ market = "dse" }: Props = {}) {
  const isNasdaq = market === "nasdaq";
  const cur = isNasdaq ? "$" : "৳";
  const fmtPrice = (n: number) => isNasdaq ? n.toFixed(2) : n.toFixed(1);
  const chartBase = isNasdaq ? "/nasdaq/smc-chart/" : "/smc-chart/";
  const [signals, setSignals] = useState<LiveCompositeSignal[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<StatusFilter>("active");
  const [minScore, setMinScore] = useState(60);
  const [tPlusTwoOnly, setTPlusTwoOnly] = useState(false);
  const [minAgreement, setMinAgreement] = useState(0);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = isNasdaq
        ? await fetchNasdaqLiveSignals(filter, minScore)
        : await fetchLiveCompositeSignals(filter, minScore);
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

  const [bucket, setBucket] = useState<"IN_ZONE" | "JUST_BOUNCED" | "WATCHING" | "MISSED" | "WRONG_TRIGGER" | "ALL">("IN_ZONE");

  const filteredAll = useMemo(() => signals.filter((s) => {
    if (tPlusTwoOnly && !s.t_plus_2_friendly) return false;
    if (minAgreement > 0 && (s.buy_votes ?? 0) < minAgreement) return false;
    return true;
  }), [signals, tPlusTwoOnly, minAgreement]);

  const deriveBucket = (s: LiveCompositeSignal): "IN_ZONE" | "JUST_BOUNCED" | "WATCHING" | "MISSED" | "WRONG_TRIGGER" | "STALE" => {
    if (s.bucket) return s.bucket;
    const cp = s.current_price;
    if (cp == null) return "STALE";
    const t1l = s.aggressive_entry_zone_low; const t1h = s.aggressive_entry_zone_high;
    const t2l = s.entry_zone_low; const t2h = s.entry_zone_high;
    const barsAgo = s.primary_trigger_bars_ago ?? 0;
    const maxProfit = s.primary_trigger_max_profit_pct ?? 0;
    const maxDrawdown = s.primary_trigger_max_drawdown_pct ?? 0;
    const triggeredInPast = barsAgo >= 2;
    const deliveredProfit = maxProfit >= 3.0;
    const zoneBroke = maxDrawdown < -3.0;
    const recentTouch = barsAgo >= 0 && barsAgo <= 5;
    const bouncedUp = maxProfit >= 1.0;

    if ((t1l != null && t1h != null && cp >= t1l && cp <= t1h)
      || (t2l != null && t2h != null && cp >= t2l && cp <= t2h)) return "IN_ZONE";
    if ((t1l != null && cp < t1l) || (t2l != null && cp < t2l)) {
      if (triggeredInPast && zoneBroke) return "WRONG_TRIGGER";
      return "IN_ZONE";
    }
    const highs = [t1h, t2h].filter((x): x is number => x != null);
    if (!highs.length) {
      if (triggeredInPast && deliveredProfit) return "MISSED";
      return "STALE";
    }
    const closest = Math.max(...highs);
    const pctAbove = ((cp - closest) / closest) * 100;
    if (recentTouch && bouncedUp && pctAbove <= 6.0 && !zoneBroke) return "JUST_BOUNCED";
    if (triggeredInPast && deliveredProfit) return "MISSED";
    if (pctAbove <= 1.5) return "IN_ZONE";
    if (pctAbove <= 8) return "WATCHING";
    return "MISSED";
  };

  const bucketed = useMemo(() => {
    const byBucket: Record<string, LiveCompositeSignal[]> = {
      IN_ZONE: [], JUST_BOUNCED: [], WATCHING: [], MISSED: [], WRONG_TRIGGER: [], STALE: [],
    };
    filteredAll.forEach((s) => {
      const b = deriveBucket(s);
      byBucket[b].push(s);
    });
    return byBucket;
  }, [filteredAll]);

  // Accuracy stats — for past triggers (>T+2 = 2 trading days), avg max-profit
  const accuracy = useMemo(() => {
    const triggered = filteredAll.filter(
      (s) => (s.primary_trigger_bars_ago ?? 0) >= 2
        && s.primary_trigger_max_profit_pct != null
    );
    if (!triggered.length) return null;
    const profits = triggered.map((s) => s.primary_trigger_max_profit_pct as number);
    const avgProfit = profits.reduce((a, b) => a + b, 0) / profits.length;
    const hits = profits.filter((p) => p >= 5).length;  // ≥5% gain = hit
    const hitRate = (hits / profits.length) * 100;
    return {
      total: triggered.length,
      avgProfit: Math.round(avgProfit * 10) / 10,
      hitRate: Math.round(hitRate),
      best: Math.round(Math.max(...profits) * 10) / 10,
      worst: Math.round(Math.min(...profits) * 10) / 10,
    };
  }, [filteredAll]);

  // For the active bucket, group by signal_level for finer ranking
  const list = useMemo(() => {
    if (bucket === "ALL") return filteredAll;
    return bucketed[bucket] || [];
  }, [bucket, bucketed, filteredAll]);

  const grouped = useMemo(() => {
    const strong = list.filter((s) => s.signal_level === "STRONG_BUY");
    const buy = list.filter((s) => s.signal_level === "BUY");
    const watch = list.filter((s) => s.signal_level === "WATCH");
    return { strong, buy, watch };
  }, [list]);

  return (
    <div className="max-w-[1440px] mx-auto px-3 sm:px-4 lg:px-8 py-6">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <div>
          <h1 className="text-xl font-bold flex items-center gap-2">
            {isNasdaq ? "NASDAQ Live Composite Signals" : "Live Composite Signals"}
            <span className="text-xs font-normal text-[var(--text-muted)]">
              9 strategies × {isNasdaq ? "halal NASDAQ" : "all DSE"} stocks · refreshes every 5 min
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
        <span className="text-xs text-[var(--text-muted)] ml-4">Agreement:</span>
        {[0, 4, 5, 6].map((a) => (
          <button
            key={a}
            onClick={() => setMinAgreement(a)}
            className={`px-2.5 py-1 rounded text-xs border transition ${
              minAgreement === a
                ? "bg-[var(--surface-active)] border-[var(--border)] text-[var(--text)]"
                : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]"
            }`}
            title={a === 0 ? "any agreement" : `at least ${a}/9 strategies say BUY`}
          >
            {a === 0 ? "any" : `${a}+/9`}
          </button>
        ))}
        <button
          onClick={() => setTPlusTwoOnly((v) => !v)}
          className={`ml-4 px-3 py-1 rounded text-xs border transition font-semibold ${
            tPlusTwoOnly
              ? "bg-blue-500/15 border-blue-500/50 text-blue-500"
              : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]"
          }`}
          title="Show only signals likely to resolve in 1-3 days (BUY_NOW + ADX>=25 + not extreme premium + no overhead supply)"
        >
          {tPlusTwoOnly ? "✅ T+2 ONLY" : "T+2 mode"}
        </button>
      </div>

      {error && (
        <div className="rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-500 mb-3">
          {error}
        </div>
      )}

      {/* BUCKET TABS — 3 sections: BUY ZONE / WATCHING / MISSED */}
      <div className="mb-3 flex flex-wrap gap-2">
        {([
          { key: "IN_ZONE", label: "🟢 BUY ZONE", desc: "price IN entry range — actionable now",
            cls: "bg-emerald-500/15 border-emerald-500/50 text-emerald-500" },
          { key: "JUST_BOUNCED", label: "🚀 JUST BOUNCED", desc: "touched zone in last 5 bars + bounced ≥1% — support CONFIRMED, momentum bullish",
            cls: "bg-cyan-500/15 border-cyan-500/50 text-cyan-400" },
          { key: "WATCHING", label: "👀 WATCHING", desc: "above zone, no recent touch — wait for first pullback",
            cls: "bg-amber-500/15 border-amber-500/50 text-amber-500" },
          { key: "MISSED", label: "❌ MISSED", desc: "triggered ≥2d ago, delivered ≥3% profit, didn't buy",
            cls: "bg-red-500/15 border-red-500/50 text-red-500" },
          { key: "WRONG_TRIGGER", label: "💥 WRONG TRIGGER", desc: "triggered, but price went BELOW zone — our zone was wrong",
            cls: "bg-rose-500/15 border-rose-500/50 text-rose-400" },
          { key: "ALL", label: "📊 ALL", desc: "all signals",
            cls: "bg-blue-500/15 border-blue-500/50 text-blue-500" },
        ] as const).map((b) => {
          const count = b.key === "ALL" ? filteredAll.length : (bucketed[b.key]?.length ?? 0);
          return (
            <button
              key={b.key}
              onClick={() => setBucket(b.key as typeof bucket)}
              title={b.desc}
              className={`px-3 py-1.5 rounded text-xs border transition flex items-center gap-2 ${
                bucket === b.key
                  ? b.cls
                  : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]"
              }`}
            >
              <span className="font-semibold">{b.label}</span>
              <span className="opacity-80">({count})</span>
            </button>
          );
        })}
      </div>

      {/* ACCURACY BANNER — what past triggers actually delivered */}
      {accuracy && (
        <div className="mb-4 rounded border border-purple-500/30 bg-purple-500/5 px-3 py-2 text-xs">
          <span className="font-semibold text-purple-400">Past trigger accuracy</span>
          <span className="text-[var(--text-muted)]"> ({accuracy.total} triggers ≥2d old):</span>
          {" "}
          hit rate (≥5% gain) <strong className="text-emerald-500">{accuracy.hitRate}%</strong>
          {" · "}avg max profit <strong className="text-emerald-500">+{accuracy.avgProfit}%</strong>
          {" · "}best <strong className="text-emerald-500">+{accuracy.best}%</strong>
          {" · "}worst drawdown <strong className="text-red-500">{accuracy.worst}%</strong>
        </div>
      )}

      <div className="mb-4 rounded border border-[var(--border)] px-3 py-2 text-xs text-[var(--text-muted)] flex flex-wrap gap-x-4 gap-y-1">
        <span className="font-semibold text-[var(--text)]">Legend:</span>
        <span><span className="text-emerald-500 font-bold">BUY NOW</span> = today tagged FVG + green close</span>
        <span><span className="text-emerald-400">RECENT TRIGGER</span> = tagged 1-3d ago, re-entry possible</span>
        <span><span className="text-blue-400">MISSED ENTRY</span> = triggered, price moved past zone</span>
        <span><span className="text-purple-400">RUNNING</span> = tagged &gt;7d ago, trend continuing</span>
        <span><span className="text-amber-500">BUY LIMIT</span> = pullback to entry pending</span>
        <span><span className="text-gray-500">STALE</span> = entry &gt;12% away</span>
      </div>

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
                    <th className="text-right px-3 py-2 font-medium" title="Tier-1 = closer support (recent swing low / equilibrium / Fib). Lower edge but realistic fill.">Tier-1 (agg.)</th>
                    <th className="text-right px-3 py-2 font-medium" title="Tier-2 = high-edge confluence (FVG + multi-touch support). May not fill.">Tier-2 (patient)</th>
                    <th className="text-right px-3 py-2 font-medium">Stop</th>
                    <th className="text-right px-3 py-2 font-medium">T1</th>
                    <th className="text-right px-3 py-2 font-medium">Risk</th>
                    <th className="text-left px-3 py-2 font-medium">Status / Verdict</th>
                    <th className="text-left px-3 py-2 font-medium">Strategies (vote)</th>
                  </tr>
                </thead>
                <tbody>
                  {list.map((s) => {
                    const eStatus = s.entry_status;
                    const tier1 = s.aggressive_entry;
                    const tier1Dist = s.aggressive_entry_distance_pct;
                    const tier2 = s.entry;
                    const tier2Dist = s.entry_distance_pct;
                    // Color logic: AT_ENTRY/DISCOUNT_TRIGGERED → green; WAIT_PULLBACK → amber; TOO_FAR → red
                    const tierColor = (dist: number | null | undefined) => {
                      if (dist === null || dist === undefined) return "text-[var(--text-muted)]";
                      if (dist <= 2 && dist >= -2) return "text-emerald-500 font-semibold";
                      if (dist > 2 && dist <= 8) return "text-amber-500";
                      if (dist > 8) return "text-red-500";
                      if (dist < -2) return "text-emerald-400 font-semibold"; // discount triggered
                      return "text-[var(--text-muted)]";
                    };
                    const statusBadge =
                      eStatus === "AT_ENTRY" ? { text: "🟢 BUY NOW", cls: "text-emerald-500 font-bold" } :
                      eStatus === "DISCOUNT_TRIGGERED" ? { text: "🟢 DISCOUNT", cls: "text-emerald-400 font-bold" } :
                      eStatus === "WAIT_PULLBACK" ? { text: "🟡 BUY LIMIT", cls: "text-amber-500 font-medium" } :
                      eStatus === "TOO_FAR" ? { text: "🔴 DON'T CHASE", cls: "text-red-500 font-medium" } :
                      null;
                    return (
                    <Fragment key={s.id}>
                    <tr className="border-t border-[var(--border)] hover:bg-[var(--hover)]">
                      <td className="px-3 py-2">
                        <Link
                          to={`${chartBase}${s.symbol}`}
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
                        <div className="text-[10px] text-[var(--text-muted)] mt-0.5">
                          <Clock className="inline h-3 w-3 mr-0.5" />
                          {timeAgo(s.first_triggered)}
                        </div>
                      </td>
                      <td className={`px-3 py-2 text-right font-mono text-xs ${tierColor(tier1Dist)}`}>
                        {tier1 !== null && tier1 !== undefined ? (
                          <>
                            {cur}{fmtPrice(tier1)}
                            {tier1Dist !== null && tier1Dist !== undefined && (
                              <div className="text-[10px] opacity-80">
                                {tier1Dist > 0 ? `${tier1Dist.toFixed(1)}% below` :
                                 tier1Dist < -1 ? `${Math.abs(tier1Dist).toFixed(1)}% triggered` :
                                 "at entry"}
                              </div>
                            )}
                          </>
                        ) : "—"}
                      </td>
                      <td className={`px-3 py-2 text-right font-mono text-xs ${tierColor(tier2Dist)}`}>
                        {tier2 !== null && tier2 !== undefined ? (
                          <>
                            {cur}{fmtPrice(tier2)}
                            {tier2Dist !== null && tier2Dist !== undefined && (
                              <div className="text-[10px] opacity-80">
                                {tier2Dist > 0 ? `${tier2Dist.toFixed(1)}% below` :
                                 tier2Dist < -1 ? `${Math.abs(tier2Dist).toFixed(1)}% triggered` :
                                 "at entry"}
                              </div>
                            )}
                          </>
                        ) : "—"}
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-xs text-red-500/80">
                        {s.stop_loss !== null ? `${cur}${fmtPrice(s.stop_loss)}` : "—"}
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-xs text-emerald-500/80">
                        {s.target1 !== null ? `${cur}${fmtPrice(s.target1)}` : "—"}
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
                        <div className="flex flex-col gap-0.5">
                          {statusBadge ? (
                            <span className={statusBadge.cls}>{statusBadge.text}</span>
                          ) : (
                            <span className={
                              s.action_type === "BUY_NOW" ? "text-emerald-500 font-bold" :
                              s.action_type === "RECENT_TRIGGER" ? "text-emerald-400 font-medium" :
                              s.action_type === "BUY_LIMIT" ? "text-amber-500" :
                              s.action_type === "MISSED_ENTRY" ? "text-blue-400" :
                              s.action_type === "RUNNING" ? "text-purple-400" :
                              s.action_type === "SETUP_DEEP" ? "text-blue-300/70" :
                              s.action_type === "STALE" ? "text-gray-500" :
                              s.action_type === "AVOID" ? "text-red-500" :
                              "text-[var(--text-muted)]"
                            }>
                              {s.action_type?.replace(/_/g, " ")}
                            </span>
                          )}
                          {s.hedge_fund_verdict && (
                            <span className="text-[10px] text-[var(--text-muted)]" title={s.hedge_fund_verdict}>
                              {s.hedge_fund_verdict.length > 38
                                ? s.hedge_fund_verdict.slice(0, 38) + "…"
                                : s.hedge_fund_verdict}
                            </span>
                          )}
                          {s.htf_bias && s.htf_bias.bias && (
                            <span className={`text-[10px] ${
                              s.htf_bias.bias === "BULLISH" ? "text-emerald-500" :
                              s.htf_bias.bias === "BEARISH" ? "text-red-500" :
                              "text-amber-500"
                            }`}>
                              HTF {s.htf_bias.bias.toLowerCase()}
                              {s.htf_bias.trend_pct !== null && s.htf_bias.trend_pct !== undefined &&
                                ` (${s.htf_bias.trend_pct >= 0 ? "+" : ""}${s.htf_bias.trend_pct.toFixed(0)}%)`}
                            </span>
                          )}
                          {s.regime && !statusBadge && (
                            <span className="text-[10px] text-[var(--text-muted)]">
                              {s.regime.replace("_", " ").toLowerCase()}
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-3 py-2 text-xs">
                        <div className="flex flex-wrap gap-0.5">
                          {s.votes && Object.entries(s.votes)
                            .sort(([, a], [, b]) => b.weight_in_regime - a.weight_in_regime)
                            .slice(0, 6)
                            .map(([name, v]) => (
                              <span
                                key={name}
                                className={`px-1.5 py-0.5 rounded text-[10px] font-mono ${
                                  v.vote === "BUY"
                                    ? "bg-emerald-500/15 text-emerald-500 border border-emerald-500/30"
                                    : v.vote === "AVOID"
                                    ? "bg-red-500/15 text-red-500 border border-red-500/30"
                                    : "bg-gray-500/10 text-gray-500 border border-gray-500/30"
                                }`}
                                title={`${name}: ${v.vote} (score ${v.score}, weight ${v.weight_in_regime}%)`}
                              >
                                {name}:{v.score}
                              </span>
                            ))}
                        </div>
                      </td>
                    </tr>
                    {s.chase_warning && (
                      <tr className="border-t border-[var(--border)]/30">
                        <td colSpan={9} className="px-3 py-1.5 text-[11px] bg-red-500/5 text-red-400/90 italic">
                          {s.chase_warning}
                        </td>
                      </tr>
                    )}
                    {/* Trigger info — shown for JUST_BOUNCED, MISSED, WRONG_TRIGGER */}
                    {(s.primary_trigger_date || s.tier1_trigger_date || s.tier2_trigger_date) &&
                     (bucket === "JUST_BOUNCED" || bucket === "MISSED" || bucket === "WRONG_TRIGGER") && (
                      <tr className="border-t border-[var(--border)]/30">
                        <td colSpan={9} className="px-3 py-1.5 text-[11px] bg-blue-500/5">
                          <span className="text-blue-400 font-semibold">📅 Triggered: </span>
                          {s.tier1_trigger_date && (
                            <span className="mr-3">
                              Tier-1 zone hit on <strong>{s.tier1_trigger_date}</strong>
                              {s.tier1_trigger_bars_ago !== null && s.tier1_trigger_bars_ago !== undefined && (
                                <> ({s.tier1_trigger_bars_ago}d ago)</>
                              )}
                              {s.tier1_max_profit_pct !== null && s.tier1_max_profit_pct !== undefined && (
                                <> — max profit since: <strong className={
                                  s.tier1_max_profit_pct >= 0 ? "text-emerald-500" : "text-red-500"
                                }>{s.tier1_max_profit_pct >= 0 ? "+" : ""}{s.tier1_max_profit_pct.toFixed(1)}%</strong></>
                              )}
                            </span>
                          )}
                          {s.tier2_trigger_date && (
                            <span>
                              Tier-2 zone hit on <strong>{s.tier2_trigger_date}</strong>
                              {s.tier2_trigger_bars_ago !== null && s.tier2_trigger_bars_ago !== undefined && (
                                <> ({s.tier2_trigger_bars_ago}d ago)</>
                              )}
                              {s.tier2_max_profit_pct !== null && s.tier2_max_profit_pct !== undefined && (
                                <> — would-be max profit: <strong className={
                                  s.tier2_max_profit_pct >= 0 ? "text-emerald-500" : "text-red-500"
                                }>{s.tier2_max_profit_pct >= 0 ? "+" : ""}{s.tier2_max_profit_pct.toFixed(1)}%</strong></>
                              )}
                            </span>
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
          </div>
        );
      })}
    </div>
  );
}
