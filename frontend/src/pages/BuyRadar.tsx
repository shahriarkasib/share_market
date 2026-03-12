import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { clsx } from "clsx";
import {
  Loader2,
  Target,
  ArrowUpRight,
  ArrowDownRight,
  RefreshCw,
  Sparkles,
  Clock,
  TrendingUp,
  TrendingDown,
  Minus,
  ChevronDown,
  ChevronUp,
  XCircle,
  Brain,
  AlertTriangle,
  Lightbulb,
  ShieldAlert,
} from "lucide-react";
import { fetchBuyRadar } from "../api/client.ts";
import type { BuyRadarStock, BuyRadarResponse, RemovedRadarStock, MarketContext } from "../types/index.ts";

/* ── Stage config ── */
const STAGES = [
  { key: "ENTRY_ZONE", label: "Entry Zone", desc: "5+ signals, accumulating" },
  { key: "READY", label: "Ready", desc: "4+ signals, MACD converging" },
  { key: "APPROACHING", label: "Approaching", desc: "3+ signals building" },
  { key: "BUILDING", label: "Building", desc: "Accumulation starting" },
  { key: "WATCHING", label: "Watching", desc: "On radar" },
] as const;

const STAGE_STYLES: Record<string, { bg: string; border: string; badge: string; text: string; dot: string }> = {
  ENTRY_ZONE:  { bg: "bg-emerald-500/8",  border: "border-emerald-500/30", badge: "bg-emerald-500/20 text-emerald-400", text: "text-emerald-400", dot: "bg-emerald-400" },
  READY:       { bg: "bg-green-500/8",     border: "border-green-500/25",   badge: "bg-green-500/20 text-green-400",     text: "text-green-400",   dot: "bg-green-400" },
  APPROACHING: { bg: "bg-amber-500/8",     border: "border-amber-500/25",   badge: "bg-amber-500/20 text-amber-400",     text: "text-amber-400",   dot: "bg-amber-400" },
  BUILDING:    { bg: "bg-blue-500/8",      border: "border-blue-500/25",    badge: "bg-blue-500/20 text-blue-400",       text: "text-blue-400",    dot: "bg-blue-400" },
  WATCHING:    { bg: "bg-slate-500/5",     border: "border-[var(--border)]", badge: "bg-slate-500/15 text-slate-400",    text: "text-slate-400",   dot: "bg-slate-400" },
};

const STAGE_ORDER: Record<string, number> = { WATCHING: 0, BUILDING: 1, APPROACHING: 2, READY: 3, ENTRY_ZONE: 4 };

/* ── Layer config ── */
const LAYER_CONFIG = [
  { key: "leading",    label: "Leading",    icon: "bolt",   desc: "StochRSI, MFI, W%R" },
  { key: "confirming", label: "Confirm",    icon: "check",  desc: "MACD, ADX, EMA" },
  { key: "money_flow", label: "Money",      icon: "dollar", desc: "CMF, OBV, Volume" },
  { key: "positioning",label: "Position",   icon: "target", desc: "RSI, BB%, VWAP" },
  { key: "ai_verdict", label: "AI",         icon: "brain",  desc: "LLM + Judge verdict" },
] as const;

/* ── Readiness ring SVG ── */
function ReadinessRing({ value, size = 40 }: { value: number; size?: number }) {
  const r = (size - 6) / 2;
  const circ = 2 * Math.PI * r;
  const offset = circ - (value / 100) * circ;
  const color =
    value >= 75 ? "stroke-emerald-400" :
    value >= 50 ? "stroke-amber-400" :
    value >= 30 ? "stroke-blue-400" :
    "stroke-slate-500";

  return (
    <svg width={size} height={size} className="shrink-0">
      <circle cx={size/2} cy={size/2} r={r} fill="none" stroke="var(--border)" strokeWidth={3} />
      <circle
        cx={size/2} cy={size/2} r={r} fill="none"
        className={color} strokeWidth={3} strokeLinecap="round"
        strokeDasharray={circ} strokeDashoffset={offset}
        transform={`rotate(-90 ${size/2} ${size/2})`}
      />
      <text x={size/2} y={size/2} textAnchor="middle" dominantBaseline="central"
        className="fill-[var(--text)] text-[9px] font-bold">
        {Math.round(value)}
      </text>
    </svg>
  );
}

/* ── Layer bar ── */
function LayerBar({ label, pct, desc }: { label: string; pct: number; desc: string }) {
  const barColor =
    pct >= 70 ? "bg-emerald-400" :
    pct >= 50 ? "bg-green-400" :
    pct >= 30 ? "bg-amber-400" :
    pct >= 15 ? "bg-blue-400" :
    "bg-slate-500";

  return (
    <div className="flex items-center gap-1.5" title={desc}>
      <span className="text-[9px] text-[var(--text-dim)] w-10 text-right shrink-0">{label}</span>
      <div className="flex-1 h-2 bg-[var(--border)] rounded-full overflow-hidden min-w-[40px]">
        <div className={clsx("h-full rounded-full transition-all duration-500", barColor)}
          style={{ width: `${Math.max(pct, 2)}%` }} />
      </div>
      <span className={clsx("text-[9px] w-7 text-right shrink-0 font-semibold",
        pct >= 60 ? "text-emerald-400" : pct >= 40 ? "text-amber-400" : "text-[var(--text-dim)]"
      )}>{pct}%</span>
    </div>
  );
}

/* ── Stage progression dots ── */
function StageProgression({ history }: { history: string[] }) {
  if (history.length <= 1) return null;
  // Deduplicate consecutive same stages
  const dedups: string[] = [];
  for (const s of history) {
    if (dedups[dedups.length - 1] !== s) dedups.push(s);
  }
  if (dedups.length <= 1) return null;

  return (
    <div className="flex items-center gap-0.5">
      {dedups.map((stage, i) => {
        const style = STAGE_STYLES[stage] || STAGE_STYLES.WATCHING;
        return (
          <div key={i} className="flex items-center gap-0.5">
            {i > 0 && <span className="text-[8px] text-[var(--text-dim)]">&rarr;</span>}
            <div className={clsx("w-1.5 h-1.5 rounded-full", style.dot)} title={stage} />
          </div>
        );
      })}
    </div>
  );
}

/* ── Trend icon ── */
function TrendBadge({ trend }: { trend: string }) {
  if (trend === "IMPROVING") return <TrendingUp className="h-3 w-3 text-emerald-400" />;
  if (trend === "DETERIORATING") return <TrendingDown className="h-3 w-3 text-red-400" />;
  return <Minus className="h-3 w-3 text-[var(--text-dim)]" />;
}

/* ── Market context banner ── */
const REGIME_STYLES: Record<string, { bg: string; border: string; text: string; label: string; advice: string }> = {
  OVERSOLD:   { bg: "bg-emerald-500/10", border: "border-emerald-500/25", text: "text-emerald-400", label: "Oversold", advice: "Market cheap — easier buy signals, good accumulation window" },
  WEAK:       { bg: "bg-blue-500/10",    border: "border-blue-500/25",    text: "text-blue-400",    label: "Weak",     advice: "Market soft — selective buying, focus on strong fundamentals" },
  NEUTRAL:    { bg: "bg-slate-500/10",   border: "border-[var(--border)]", text: "text-[var(--text-muted)]", label: "Neutral", advice: "Market balanced — standard criteria apply" },
  HEATED:     { bg: "bg-amber-500/10",   border: "border-amber-500/25",   text: "text-amber-400",   label: "Heated",   advice: "Market stretched — only high-conviction picks, tighter stops" },
  OVERBOUGHT: { bg: "bg-red-500/10",    border: "border-red-500/25",     text: "text-red-400",     label: "Overbought", advice: "Market expensive — avoid new buys, take profits on winners" },
};

function MarketContextBanner({ ctx }: { ctx: MarketContext }) {
  const rs = REGIME_STYLES[ctx.regime] || REGIME_STYLES.NEUTRAL;
  return (
    <div className={clsx("mb-4 rounded-lg border px-3 py-2 flex items-center gap-3 flex-wrap", rs.bg, rs.border)}>
      <div className="flex items-center gap-2">
        <AlertTriangle className={clsx("h-3.5 w-3.5 shrink-0", rs.text)} />
        <span className={clsx("text-xs font-bold", rs.text)}>DSEX {rs.label}</span>
        <span className="text-[10px] text-[var(--text-dim)]">RSI {ctx.dsex_rsi}</span>
        <span className="text-[10px] text-[var(--text-dim)]">({ctx.dsex.toFixed(0)})</span>
      </div>
      <span className="text-[9px] text-[var(--text-muted)]">{rs.advice}</span>
      <span className={clsx("text-[9px] font-mono px-1.5 py-0.5 rounded", rs.bg, rs.text)}>
        {ctx.adjustment > 1 ? "+" : ""}{((ctx.adjustment - 1) * 100).toFixed(0)}% adj
      </span>
    </div>
  );
}

/* ── Stock card ── */
function StockCard({ stock }: { stock: BuyRadarStock }) {
  const navigate = useNavigate();
  const style = STAGE_STYLES[stock.stage];
  const ind = stock.indicators;

  // Price since entry: green if down (getting cheaper = good), red if up (moving away)
  const priceDir = stock.price_change_pct <= 0 ? "getting-cheaper" : "moving-away";

  return (
    <div
      onClick={() => navigate(`/stock/${stock.symbol}`)}
      className={clsx(
        "rounded-lg border p-2.5 cursor-pointer transition-all hover:scale-[1.01] hover:shadow-md",
        style.bg, style.border
      )}
    >
      {/* Header row: ring + info + badges */}
      <div className="flex items-start gap-2 mb-1.5">
        <ReadinessRing value={stock.overall_readiness} size={36} />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1.5 flex-wrap">
            <span className="text-xs font-bold text-[var(--text)] truncate">{stock.symbol}</span>

            {/* NEW or Day N badge */}
            {stock.is_new ? (
              <span className="text-[8px] px-1 py-0.5 rounded bg-yellow-500/20 text-yellow-400 font-bold flex items-center gap-0.5">
                <Sparkles className="h-2.5 w-2.5" /> NEW
              </span>
            ) : (
              <span className="text-[8px] px-1 py-0.5 rounded bg-[var(--surface)] text-[var(--text-dim)] font-medium flex items-center gap-0.5">
                <Clock className="h-2.5 w-2.5" /> Day {stock.days_on_radar}
              </span>
            )}

            {/* Trend */}
            <TrendBadge trend={stock.trend} />
          </div>

          {/* Price + sector + 5d return */}
          <div className="flex items-center gap-2 text-[10px] mt-0.5">
            <span className="font-semibold text-[var(--text-muted)]">{stock.price.toFixed(1)}</span>
            <span className={clsx(
              "text-[9px] font-medium flex items-center gap-0.5",
              stock.ret_5d >= 0 ? "text-green-400" : "text-red-400"
            )}>
              {stock.ret_5d >= 0 ? <ArrowUpRight className="h-2.5 w-2.5" /> : <ArrowDownRight className="h-2.5 w-2.5" />}
              {stock.ret_5d > 0 ? "+" : ""}{stock.ret_5d}%
              <span className="text-[var(--text-dim)] ml-0.5">5d</span>
            </span>
            {stock.sector && (
              <span className="text-[var(--text-dim)] truncate">{stock.sector}</span>
            )}
          </div>
        </div>
      </div>

      {/* Entry price tracking (if not new) */}
      {!stock.is_new && stock.days_on_radar > 1 && (
        <div className={clsx(
          "text-[9px] px-2 py-1 rounded mb-1.5 flex items-center gap-1.5",
          priceDir === "getting-cheaper"
            ? "bg-green-500/10 text-green-400"
            : "bg-red-500/10 text-red-400"
        )}>
          <span className="text-[var(--text-dim)]">Since Day 1 ({stock.entry_price.toFixed(1)}):</span>
          <span className="font-bold">
            {stock.price_change_pct > 0 ? "+" : ""}{stock.price_change_pct}%
          </span>
          {priceDir === "getting-cheaper" ? (
            <span className="text-[8px] text-green-300">cheaper!</span>
          ) : (
            <span className="text-[8px] text-red-300">moved up</span>
          )}
        </div>
      )}

      {/* Stage progression */}
      {stock.stage_history.length > 1 && (
        <div className="flex items-center gap-1.5 mb-1">
          <span className="text-[8px] text-[var(--text-dim)]">Journey:</span>
          <StageProgression history={stock.stage_history} />
        </div>
      )}

      {/* Layer bars */}
      <div className="space-y-0.5">
        {LAYER_CONFIG.map(({ key, label, desc }) => (
          <LayerBar key={key} label={label} pct={stock.layers[key as keyof typeof stock.layers] ?? 0} desc={desc} />
        ))}
      </div>

      {/* AI Insight card (if we have AI data) */}
      {stock.ai_action && (
        <div className="mt-1.5 rounded-md bg-purple-500/8 border border-purple-500/20 px-2 py-1.5">
          <div className="flex items-center gap-1.5 mb-1">
            <Brain className="h-3 w-3 text-purple-400 shrink-0" />
            <span className={clsx("text-[9px] font-bold",
              stock.ai_action.toUpperCase().includes("BUY") ? "text-emerald-400" :
              stock.ai_action.toUpperCase().includes("SELL") ? "text-red-400" :
              "text-amber-400"
            )}>
              {stock.ai_action}
            </span>
            {stock.ai_confidence && (
              <span className={clsx("text-[8px] px-1 py-0.5 rounded font-medium",
                stock.ai_confidence.toUpperCase() === "HIGH"
                  ? "bg-emerald-500/15 text-emerald-400"
                  : "bg-amber-500/15 text-amber-400"
              )}>
                {stock.ai_confidence}
              </span>
            )}
          </div>

          {/* AI reasoning (truncated) */}
          {stock.ai_reasoning && (
            <p className="text-[8px] text-[var(--text-muted)] leading-relaxed line-clamp-2 mb-1">
              {stock.ai_reasoning}
            </p>
          )}

          {/* How to buy + wait for */}
          <div className="flex flex-wrap gap-x-3 gap-y-0.5 text-[8px]">
            {stock.ai_how_to_buy && (
              <span className="text-blue-400 flex items-center gap-0.5">
                <Lightbulb className="h-2.5 w-2.5 shrink-0" />
                {stock.ai_how_to_buy.slice(0, 60)}{stock.ai_how_to_buy.length > 60 ? "..." : ""}
              </span>
            )}
            {stock.ai_key_risk && (
              <span className="text-red-400 flex items-center gap-0.5">
                <ShieldAlert className="h-2.5 w-2.5 shrink-0" />
                {stock.ai_key_risk.slice(0, 50)}{stock.ai_key_risk.length > 50 ? "..." : ""}
              </span>
            )}
          </div>

          {/* Catalysts */}
          {stock.ai_catalysts?.length > 0 && (
            <div className="mt-0.5 flex flex-wrap gap-1">
              {stock.ai_catalysts.map((c, i) => (
                <span key={i} className="text-[7px] px-1 py-0.5 rounded bg-purple-500/10 text-purple-300">
                  {c.length > 40 ? c.slice(0, 40) + "..." : c}
                </span>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Key signals */}
      {stock.signals.length > 0 && (
        <div className="mt-1.5 flex flex-wrap gap-1">
          {stock.signals.slice(0, 3).map((sig, i) => (
            <span key={i} className="text-[8px] px-1 py-0.5 rounded bg-[var(--surface)] text-[var(--text-muted)] border border-[var(--border)]">
              {sig}
            </span>
          ))}
        </div>
      )}

      {/* Red flags */}
      {stock.red_flags.length > 0 && (
        <div className="mt-1 flex flex-wrap gap-1">
          {stock.red_flags.map((flag, i) => (
            <span key={i} className="text-[8px] px-1 py-0.5 rounded bg-red-500/10 text-red-400 border border-red-500/20">
              {flag}
            </span>
          ))}
        </div>
      )}

      {/* Entry zone + targets */}
      {stock.entry_low && stock.entry_high && (
        <div className="mt-1.5 flex items-center gap-2 text-[9px] text-[var(--text-dim)]">
          <span>Entry: {stock.entry_low.toFixed(1)}-{stock.entry_high.toFixed(1)}</span>
          {stock.t1 && <span>T1: {stock.t1.toFixed(1)}</span>}
          {stock.sl && <span className="text-red-400">SL: {stock.sl.toFixed(1)}</span>}
        </div>
      )}

      {/* Layer count + volume badges */}
      <div className="mt-1 flex items-center gap-1">
        <span className={clsx("text-[8px] px-1 py-0.5 rounded font-medium", style.badge)}>
          {stock.ready_count}/5 layers
        </span>
        {stock.vol_ratio >= 1.5 && (
          <span className="text-[8px] px-1 py-0.5 rounded bg-blue-500/15 text-blue-400 font-medium">
            Vol {stock.vol_ratio}x
          </span>
        )}
      </div>
    </div>
  );
}

/* ── Removed stocks section ── */
function RemovedSection({ removed }: { removed: RemovedRadarStock[] }) {
  const [open, setOpen] = useState(false);
  if (removed.length === 0) return null;

  return (
    <div className="mt-6 border-t border-[var(--border)] pt-4">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-2 text-xs font-medium text-[var(--text-dim)] hover:text-[var(--text)] transition-colors"
      >
        <XCircle className="h-3.5 w-3.5 text-red-400" />
        Recently Removed ({removed.length})
        {open ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
      </button>

      {open && (
        <div className="mt-2 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-2">
          {removed.map((r) => (
            <div key={r.symbol} className="rounded-md border border-red-500/15 bg-red-500/5 px-3 py-2">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-[var(--text)]">{r.symbol}</span>
                <span className="text-[8px] px-1.5 py-0.5 rounded bg-red-500/15 text-red-400 font-medium">
                  {r.reason}
                </span>
              </div>
              <div className="flex items-center gap-3 mt-1 text-[9px] text-[var(--text-dim)]">
                <span>Last: {r.last_price.toFixed(1)}</span>
                <span>Stage: {r.last_stage}</span>
                {r.days_tracked > 0 && <span>Tracked {r.days_tracked}d</span>}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/* ── Main BuyRadar page ── */
export default function BuyRadar() {
  const [data, setData] = useState<BuyRadarResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [view, setView] = useState<"pipeline" | "list">("pipeline");

  const load = () => {
    setLoading(true);
    setError("");
    fetchBuyRadar()
      .then(setData)
      .catch((e) => setError(e.message || "Failed to load"))
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 gap-2 text-[var(--text-dim)]">
        <Loader2 className="h-5 w-5 animate-spin" />
        <span className="text-sm">Computing buy radar...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-center py-12">
        <p className="text-red-400 text-sm mb-2">{error}</p>
        <button onClick={load} className="text-xs text-blue-400 hover:underline">Retry</button>
      </div>
    );
  }

  if (!data) return null;

  // Group stocks by stage
  const byStage: Record<string, BuyRadarStock[]> = {};
  for (const s of STAGES) byStage[s.key] = [];
  for (const stock of data.stocks) {
    if (byStage[stock.stage]) byStage[stock.stage].push(stock);
  }

  return (
    <div className="max-w-[1440px] mx-auto px-3 sm:px-4 lg:px-8 py-4">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <Target className="h-5 w-5 text-amber-400" />
          <h1 className="text-lg font-bold text-[var(--text)]">Buy Radar</h1>
          <span className="text-xs text-[var(--text-dim)]">
            {data.count} stocks &middot; {data.date}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <div className="flex rounded-md border border-[var(--border)] overflow-hidden">
            <button onClick={() => setView("pipeline")}
              className={clsx("px-2.5 py-1 text-[10px] font-medium transition-colors",
                view === "pipeline" ? "bg-[var(--surface-active)] text-[var(--text)]" : "text-[var(--text-dim)] hover:text-[var(--text)]")}>
              Pipeline
            </button>
            <button onClick={() => setView("list")}
              className={clsx("px-2.5 py-1 text-[10px] font-medium transition-colors",
                view === "list" ? "bg-[var(--surface-active)] text-[var(--text)]" : "text-[var(--text-dim)] hover:text-[var(--text)]")}>
              List
            </button>
          </div>
          <button onClick={load}
            className="p-1.5 rounded-md hover:bg-[var(--hover)] text-[var(--text-dim)] hover:text-[var(--text)] transition-colors">
            <RefreshCw className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>

      {/* Stage summary bar */}
      <div className="flex gap-2 mb-4 overflow-x-auto pb-1">
        {STAGES.map(({ key, label, desc }) => {
          const count = byStage[key]?.length || 0;
          const style = STAGE_STYLES[key];
          return (
            <div key={key} className={clsx("flex items-center gap-2 px-3 py-1.5 rounded-lg border shrink-0", style.bg, style.border)}>
              <span className={clsx("text-lg font-bold", style.text)}>{count}</span>
              <div>
                <div className={clsx("text-[10px] font-semibold", style.text)}>{label}</div>
                <div className="text-[8px] text-[var(--text-dim)]">{desc}</div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Market context banner */}
      {data.market_ctx && (
        <MarketContextBanner ctx={data.market_ctx} />
      )}

      {/* How to read guide */}
      <div className="mb-4 bg-[var(--surface)] rounded-lg border border-[var(--border)] px-3 py-2 space-y-1">
        <div className="flex flex-wrap gap-x-4 gap-y-1 text-[9px] text-[var(--text-dim)]">
          <span><b className="text-purple-400">Leading</b> = StochRSI, MFI, W%R (signals early)</span>
          <span><b className="text-blue-400">Confirm</b> = MACD, ADX, EMA (validates trend)</span>
          <span><b className="text-amber-400">Money</b> = CMF, OBV, Volume (is it real?)</span>
          <span><b className="text-green-400">Position</b> = RSI, BB%, VWAP (price zone)</span>
          <span><b className="text-fuchsia-400">AI</b> = LLM + Judge verdict (news, context)</span>
        </div>
        <div className="flex flex-wrap gap-x-4 gap-y-1 text-[9px] text-[var(--text-dim)]">
          <span><Sparkles className="h-2.5 w-2.5 inline text-yellow-400" /> <b>NEW</b> = just appeared</span>
          <span><Clock className="h-2.5 w-2.5 inline" /> <b>Day N</b> = tracked N days</span>
          <span className="text-green-400">Price down = getting cheaper</span>
          <span className="text-red-400">Red flags = hard blockers</span>
          <span><TrendingUp className="h-2.5 w-2.5 inline text-emerald-400" /> improving stages</span>
        </div>
      </div>

      {/* Pipeline view */}
      {view === "pipeline" && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-3">
          {STAGES.map(({ key, label }) => {
            const stocks = byStage[key] || [];
            const style = STAGE_STYLES[key];
            return (
              <div key={key} className="min-w-0">
                <div className={clsx("text-[10px] font-bold mb-2 px-1 flex items-center gap-1.5", style.text)}>
                  <span className={clsx("w-2 h-2 rounded-full", style.dot)} />
                  {label}
                  <span className="text-[var(--text-dim)] font-normal">({stocks.length})</span>
                </div>
                <div className="space-y-2">
                  {stocks.length === 0 && (
                    <p className="text-[10px] text-[var(--text-dim)] italic px-1">No stocks yet</p>
                  )}
                  {stocks.map((s) => <StockCard key={s.symbol} stock={s} />)}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* List view */}
      {view === "list" && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
          {data.stocks.map((s) => <StockCard key={s.symbol} stock={s} />)}
        </div>
      )}

      {/* Removed stocks */}
      <RemovedSection removed={data.removed} />
    </div>
  );
}
