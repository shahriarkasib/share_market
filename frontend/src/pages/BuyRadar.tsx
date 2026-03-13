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
  HelpCircle,
  DollarSign,
  BarChart3,
} from "lucide-react";
import { fetchBuyRadar } from "../api/client.ts";
import type { BuyRadarStock, BuyRadarResponse, RemovedRadarStock, MarketContext, DsexForecast, DsexDailyPrediction } from "../types/index.ts";

/* ── Stage config ── */
const STAGES = [
  { key: "ENTRY_ZONE", label: "Entry Zone", desc: "Buy NOW — best price is here" },
  { key: "READY", label: "Ready", desc: "1-2 days — one trigger away" },
  { key: "APPROACHING", label: "Approaching", desc: "5-10 days — setup forming" },
  { key: "BUILDING", label: "Building", desc: "2-3 weeks — early accumulation" },
  { key: "WATCHING", label: "Watching", desc: "On radar — not actionable yet" },
] as const;

const STAGE_STYLES: Record<string, { bg: string; border: string; badge: string; text: string; dot: string }> = {
  ENTRY_ZONE:  { bg: "bg-emerald-500/8",  border: "border-emerald-500/30", badge: "bg-emerald-500/20 text-emerald-400", text: "text-emerald-400", dot: "bg-emerald-400" },
  READY:       { bg: "bg-green-500/8",     border: "border-green-500/25",   badge: "bg-green-500/20 text-green-400",     text: "text-green-400",   dot: "bg-green-400" },
  APPROACHING: { bg: "bg-amber-500/8",     border: "border-amber-500/25",   badge: "bg-amber-500/20 text-amber-400",     text: "text-amber-400",   dot: "bg-amber-400" },
  BUILDING:    { bg: "bg-blue-500/8",      border: "border-blue-500/25",    badge: "bg-blue-500/20 text-blue-400",       text: "text-blue-400",    dot: "bg-blue-400" },
  WATCHING:    { bg: "bg-slate-500/5",     border: "border-[var(--border)]", badge: "bg-slate-500/15 text-slate-400",    text: "text-slate-400",   dot: "bg-slate-400" },
};

/* ── Layer config ── */
const LAYER_CONFIG = [
  { key: "leading",    label: "Leading",    desc: "StochRSI, MFI, W%R" },
  { key: "confirming", label: "Confirm",    desc: "MACD, ADX, EMA" },
  { key: "money_flow", label: "Money",      desc: "CMF, OBV, Volume" },
  { key: "positioning",label: "Position",   desc: "RSI, BB%, VWAP" },
  { key: "ai_verdict", label: "AI",         desc: "LLM + Judge verdict" },
] as const;

/* ── Readiness ring SVG ── */
function ReadinessRing({ value, size = 44 }: { value: number; size?: number }) {
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
        className="fill-[var(--text)] text-[10px] font-bold">
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
      <span className="text-[10px] text-[var(--text-dim)] w-12 text-right shrink-0">{label}</span>
      <div className="flex-1 h-2.5 bg-[var(--border)] rounded-full overflow-hidden min-w-[40px]">
        <div className={clsx("h-full rounded-full transition-all duration-500", barColor)}
          style={{ width: `${Math.max(pct, 2)}%` }} />
      </div>
      <span className={clsx("text-[10px] w-8 text-right shrink-0 font-semibold",
        pct >= 60 ? "text-emerald-400" : pct >= 40 ? "text-amber-400" : "text-[var(--text-dim)]"
      )}>{pct}%</span>
    </div>
  );
}

/* ── Stage progression dots ── */
function StageProgression({ history }: { history: string[] }) {
  if (history.length <= 1) return null;
  const dedups: string[] = [];
  for (const s of history) {
    if (dedups[dedups.length - 1] !== s) dedups.push(s);
  }
  if (dedups.length <= 1) return null;

  return (
    <div className="flex items-center gap-1">
      {dedups.map((stage, i) => {
        const style = STAGE_STYLES[stage] || STAGE_STYLES.WATCHING;
        return (
          <div key={i} className="flex items-center gap-1">
            {i > 0 && <span className="text-[9px] text-[var(--text-dim)]">&rarr;</span>}
            <div className={clsx("w-2 h-2 rounded-full", style.dot)} title={stage} />
          </div>
        );
      })}
    </div>
  );
}

/* ── Trend icon ── */
function TrendBadge({ trend }: { trend: string }) {
  if (trend === "IMPROVING") return <TrendingUp className="h-3.5 w-3.5 text-emerald-400" />;
  if (trend === "DETERIORATING") return <TrendingDown className="h-3.5 w-3.5 text-red-400" />;
  return <Minus className="h-3.5 w-3.5 text-[var(--text-dim)]" />;
}

/* ── Market context banner ── */
const REGIME_STYLES: Record<string, { bg: string; border: string; text: string; label: string }> = {
  OVERSOLD:   { bg: "bg-emerald-500/10", border: "border-emerald-500/25", text: "text-emerald-400", label: "Oversold" },
  WEAK:       { bg: "bg-blue-500/10",    border: "border-blue-500/25",    text: "text-blue-400",    label: "Weak" },
  NEUTRAL:    { bg: "bg-slate-500/10",   border: "border-[var(--border)]", text: "text-[var(--text-muted)]", label: "Neutral" },
  HEATED:     { bg: "bg-amber-500/10",   border: "border-amber-500/25",   text: "text-amber-400",   label: "Heated" },
  OVERBOUGHT: { bg: "bg-red-500/10",    border: "border-red-500/25",     text: "text-red-400",     label: "Overbought" },
};

const VOL_STYLES: Record<string, { text: string; label: string }> = {
  VERY_LOW:  { text: "text-red-400",    label: "Very Low" },
  LOW:       { text: "text-amber-400",  label: "Low" },
  NORMAL:    { text: "text-[var(--text-muted)]", label: "Normal" },
  HIGH:      { text: "text-green-400",  label: "High" },
  VERY_HIGH: { text: "text-emerald-400", label: "Very High" },
};

function MarketContextBanner({ ctx }: { ctx: MarketContext }) {
  const rs = REGIME_STYLES[ctx.regime] || REGIME_STYLES.NEUTRAL;
  const vs = VOL_STYLES[ctx.volume_verdict] || VOL_STYLES.NORMAL;
  const breadthColor = (ctx.breadth_pct ?? 50) >= 60 ? "text-green-400" : (ctx.breadth_pct ?? 50) <= 40 ? "text-red-400" : "text-[var(--text-muted)]";

  return (
    <div className={clsx("mb-4 rounded-lg border px-4 py-3 space-y-2", rs.bg, rs.border)}>
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex items-center gap-1.5">
          <BarChart3 className={clsx("h-4 w-4 shrink-0", rs.text)} />
          <span className={clsx("text-sm font-bold", rs.text)}>Market: {rs.label}</span>
        </div>
        <span className="text-xs text-[var(--text-muted)]">
          DSEX {ctx.dsex?.toFixed(0)} ({ctx.dsex_change > 0 ? "+" : ""}{ctx.dsex_change?.toFixed(1)})
        </span>
        <span className="text-xs text-[var(--text-muted)]">RSI {ctx.dsex_rsi}</span>
      </div>

      <div className="flex items-center gap-4 flex-wrap text-xs">
        <span className="flex items-center gap-1">
          <span className="text-[var(--text-dim)]">Turnover:</span>
          <span className={clsx("font-semibold", vs.text)}>{ctx.total_value_cr?.toFixed(0)} Cr ({vs.label})</span>
        </span>
        <span className="flex items-center gap-1">
          <span className="text-[var(--text-dim)]">Breadth:</span>
          <span className={clsx("font-semibold", breadthColor)}>
            {ctx.advances}A / {ctx.declines}D ({ctx.breadth_pct}%)
          </span>
        </span>
      </div>

      {ctx.signal && (
        <div className="text-xs text-[var(--text-muted)] flex items-center gap-1.5">
          <Brain className="h-3.5 w-3.5 text-purple-400 shrink-0" />
          {ctx.signal}
        </div>
      )}
    </div>
  );
}

/* ── DSEX Forecast banner ── */
const SENTIMENT_STYLES: Record<string, { bg: string; border: string; text: string; icon: typeof TrendingUp }> = {
  BULLISH: { bg: "bg-green-500/5", border: "border-green-500/20", text: "text-green-400", icon: TrendingUp },
  BEARISH: { bg: "bg-red-500/5", border: "border-red-500/20", text: "text-red-400", icon: TrendingDown },
  CAUTIOUS: { bg: "bg-yellow-500/5", border: "border-yellow-500/20", text: "text-yellow-400", icon: AlertTriangle },
  NEUTRAL: { bg: "bg-[var(--surface)]", border: "border-[var(--border)]", text: "text-[var(--text-muted)]", icon: Minus },
};

const DAY_DIR_STYLE: Record<string, { bg: string; text: string; icon: typeof TrendingUp }> = {
  UP:   { bg: "bg-green-500/10 border-green-500/20", text: "text-green-400", icon: TrendingUp },
  DOWN: { bg: "bg-red-500/10 border-red-500/20",     text: "text-red-400",   icon: TrendingDown },
  FLAT: { bg: "bg-slate-500/10 border-[var(--border)]", text: "text-[var(--text-muted)]", icon: Minus },
};

function DayPredictionCard({ pred, label }: { pred: DsexDailyPrediction; label: string }) {
  const [showReason, setShowReason] = useState(false);
  const ds = DAY_DIR_STYLE[pred.direction] || DAY_DIR_STYLE.FLAT;
  const DIcon = ds.icon;

  return (
    <div className={clsx("rounded-lg border p-2.5 flex flex-col gap-1", ds.bg)}>
      <div className="flex items-center justify-between">
        <span className="text-[10px] font-bold text-[var(--text-dim)] uppercase">{label}</span>
        <DIcon className={clsx("h-3.5 w-3.5", ds.text)} />
      </div>
      <div className={clsx("text-sm font-bold", ds.text)}>{pred.direction}</div>
      <div className="text-[10px] text-[var(--text-muted)]">
        {pred.range_low?.toFixed(0)} – {pred.range_high?.toFixed(0)}
      </div>
      {pred.reasoning && (
        <button
          onClick={() => setShowReason(!showReason)}
          className="text-[10px] text-[var(--text-dim)] hover:text-[var(--text)] text-left mt-0.5"
        >
          {showReason ? "Hide reason" : "Why?"}
        </button>
      )}
      {showReason && pred.reasoning && (
        <p className="text-[10px] text-[var(--text-muted)] leading-relaxed mt-1">
          {pred.reasoning}
        </p>
      )}
    </div>
  );
}

function DsexForecastBanner({ forecast }: { forecast: DsexForecast }) {
  const [showDetails, setShowDetails] = useState(false);
  const style = SENTIMENT_STYLES[forecast.sentiment] || SENTIMENT_STYLES.NEUTRAL;
  const DirIcon = style.icon;
  const days = forecast.daily_predictions || [];

  return (
    <div className={clsx("mb-4 rounded-lg border", style.bg, style.border)}>
      {/* Header — always visible */}
      <div className="px-4 py-3">
        <div className="flex items-center gap-2 flex-wrap">
          <DirIcon className={clsx("h-5 w-5", style.text)} />
          <span className={clsx("text-sm font-bold", style.text)}>
            DSEX Forecast: {forecast.sentiment}
          </span>
          <span className={clsx("text-xs font-medium", style.text)}>
            {forecast.expected_direction}
          </span>
          <span className="text-xs text-[var(--text-dim)]">
            Support {forecast.support?.toFixed(0)} | Resistance {forecast.resistance?.toFixed(0)} | Confidence: {forecast.confidence}
          </span>
        </div>

        {/* Key factors — always visible */}
        {forecast.key_factors && (
          <p className="text-xs text-[var(--text-muted)] mt-2 leading-relaxed">{forecast.key_factors}</p>
        )}
      </div>

      {/* 5-day predictions — always visible */}
      {days.length > 0 && (
        <div className="px-4 pb-3">
          <div className="text-[10px] font-bold uppercase text-[var(--text-dim)] mb-2">Next 5 Trading Days</div>
          <div className="grid grid-cols-5 gap-2">
            {days.map((d) => (
              <DayPredictionCard
                key={d.day}
                pred={d}
                label={d.day === 1 ? "Tomorrow" : `Day ${d.day}`}
              />
            ))}
          </div>
        </div>
      )}

      {/* Expand for full analysis + scenarios */}
      <div className="border-t border-[var(--border)]">
        <button
          onClick={() => setShowDetails(!showDetails)}
          className="w-full px-4 py-2 text-left flex items-center gap-1.5 text-xs text-[var(--text-dim)] hover:text-[var(--text)]"
        >
          <Brain className="h-3.5 w-3.5" />
          <span className="font-medium">Full Analysis & Scenarios</span>
          {showDetails ? <ChevronUp className="h-3.5 w-3.5 ml-auto" /> : <ChevronDown className="h-3.5 w-3.5 ml-auto" />}
        </button>

        {showDetails && (
          <div className="px-4 pb-4 space-y-3">
            <p className="text-xs text-[var(--text-muted)] leading-relaxed whitespace-pre-line">
              {forecast.forecast}
            </p>

            <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
              {forecast.scenario_base && (
                <div className="bg-blue-500/5 border border-blue-500/15 rounded p-2">
                  <div className="text-[10px] text-blue-400 font-medium mb-0.5">Most Likely (60%+)</div>
                  <p className="text-xs text-blue-300/80 leading-relaxed">{forecast.scenario_base}</p>
                </div>
              )}
              {forecast.scenario_bull && (
                <div className="bg-green-500/5 border border-green-500/15 rounded p-2">
                  <div className="text-[10px] text-green-400 font-medium mb-0.5">Bull Case</div>
                  <p className="text-xs text-green-300/80 leading-relaxed">{forecast.scenario_bull}</p>
                </div>
              )}
              {forecast.scenario_bear && (
                <div className="bg-red-500/5 border border-red-500/15 rounded p-2">
                  <div className="text-[10px] text-red-400 font-medium mb-0.5">Bear Case</div>
                  <p className="text-xs text-red-300/80 leading-relaxed">{forecast.scenario_bear}</p>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Section label helper ── */
function SectionLabel({ icon: Icon, label, color }: { icon: typeof Brain; label: string; color: string }) {
  return (
    <div className={clsx("flex items-center gap-1.5 mb-1.5 mt-3 first:mt-0", color)}>
      <Icon className="h-3.5 w-3.5 shrink-0" />
      <span className="text-[11px] font-bold uppercase tracking-wide">{label}</span>
    </div>
  );
}

/* ── Stock card — expandable with full AI analysis ── */
function StockCard({ stock }: { stock: BuyRadarStock }) {
  const navigate = useNavigate();
  const [expanded, setExpanded] = useState(false);
  const style = STAGE_STYLES[stock.stage];
  const priceDir = stock.price_change_pct <= 0 ? "getting-cheaper" : "moving-away";

  return (
    <div className={clsx("rounded-lg border transition-all", style.bg, style.border)}>
      {/* ── Compact header (always visible) ── */}
      <div
        className="p-3 cursor-pointer"
        onClick={(e) => { e.stopPropagation(); setExpanded(!expanded); }}
      >
        <div className="flex items-start gap-3">
          <ReadinessRing value={stock.overall_readiness} size={44} />
          <div className="flex-1 min-w-0">
            {/* Symbol + badges */}
            <div className="flex items-center gap-2 flex-wrap">
              <span
                className="text-sm font-bold text-[var(--text)] hover:text-blue-400 cursor-pointer"
                onClick={(e) => { e.stopPropagation(); navigate(`/stock/${stock.symbol}`); }}
              >
                {stock.symbol}
              </span>
              <span className={clsx("text-[10px] px-1.5 py-0.5 rounded font-bold", style.badge)}>
                {stock.stage.replace("_", " ")}
              </span>
              {stock.is_new && (
                <span className="text-[9px] px-1.5 py-0.5 rounded bg-yellow-500/20 text-yellow-400 font-bold flex items-center gap-0.5">
                  <Sparkles className="h-3 w-3" /> NEW
                </span>
              )}
              {!stock.is_new && stock.days_on_radar > 1 && (
                <span className="text-[9px] px-1.5 py-0.5 rounded bg-[var(--surface)] text-[var(--text-dim)] flex items-center gap-0.5">
                  <Clock className="h-3 w-3" /> Day {stock.days_on_radar}
                </span>
              )}
              <TrendBadge trend={stock.trend} />
            </div>

            {/* Price + return + sector */}
            <div className="flex items-center gap-3 text-xs mt-1">
              <span className="font-bold text-[var(--text)]">{stock.price.toFixed(1)} BDT</span>
              <span className={clsx("font-medium flex items-center gap-0.5",
                stock.ret_5d >= 0 ? "text-green-400" : "text-red-400"
              )}>
                {stock.ret_5d >= 0 ? <ArrowUpRight className="h-3 w-3" /> : <ArrowDownRight className="h-3 w-3" />}
                {stock.ret_5d > 0 ? "+" : ""}{stock.ret_5d}% (5d)
              </span>
              {stock.sector && <span className="text-[var(--text-dim)]">{stock.sector}</span>}
            </div>

            {/* AI action + confidence (summary) */}
            {stock.ai_action && (
              <div className="flex items-center gap-2 mt-1.5">
                <Brain className="h-3.5 w-3.5 text-purple-400 shrink-0" />
                <span className={clsx("text-xs font-bold",
                  stock.ai_action.toUpperCase().includes("BUY") ? "text-emerald-400" :
                  stock.ai_action.toUpperCase().includes("SELL") ? "text-red-400" :
                  "text-amber-400"
                )}>
                  {stock.ai_action}
                </span>
                {stock.ai_confidence && (
                  <span className={clsx("text-[10px] px-1.5 py-0.5 rounded font-medium",
                    stock.ai_confidence.toUpperCase() === "HIGH"
                      ? "bg-emerald-500/15 text-emerald-400"
                      : stock.ai_confidence.toUpperCase() === "LOW"
                      ? "bg-red-500/15 text-red-400"
                      : "bg-amber-500/15 text-amber-400"
                  )}>
                    {stock.ai_confidence}
                  </span>
                )}
                {stock.ai_wait_for && (
                  <span className="text-[10px] text-[var(--text-dim)] truncate">
                    Wait: {stock.ai_wait_for.slice(0, 40)}
                  </span>
                )}
              </div>
            )}
          </div>

          {/* Expand chevron */}
          <button className="p-1 text-[var(--text-dim)] hover:text-[var(--text)]">
            {expanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
          </button>
        </div>

        {/* Entry zone + targets (compact) */}
        {stock.entry_low != null && stock.entry_high != null && (
          <div className="flex items-center gap-3 mt-2 text-xs">
            <span className="text-emerald-400 font-semibold">
              Entry: {stock.entry_low.toFixed(1)}–{stock.entry_high.toFixed(1)}
            </span>
            {stock.t1 != null && <span className="text-[var(--text-muted)]">T1: {stock.t1.toFixed(1)}</span>}
            {stock.t2 != null && <span className="text-[var(--text-muted)]">T2: {stock.t2.toFixed(1)}</span>}
            {stock.sl != null && <span className="text-red-400">SL: {stock.sl.toFixed(1)}</span>}
          </div>
        )}

        {/* Red flags (always visible) */}
        {stock.red_flags.length > 0 && (
          <div className="mt-2 flex flex-wrap gap-1.5">
            {stock.red_flags.map((flag, i) => (
              <span key={i} className="text-[10px] px-1.5 py-0.5 rounded bg-red-500/10 text-red-400 border border-red-500/20">
                {flag}
              </span>
            ))}
          </div>
        )}
      </div>

      {/* ── Expanded content ── */}
      {expanded && (
        <div className="border-t border-[var(--border)] px-4 py-3 space-y-1">

          {/* Stage reasoning — WHY this stage */}
          {stock.stage_reasoning && (
            <>
              <SectionLabel icon={Target} label="Why this stage" color="text-amber-400" />
              <p className="text-xs text-[var(--text-muted)] leading-relaxed">
                {stock.stage_reasoning}
              </p>
            </>
          )}

          {/* Full AI reasoning */}
          {stock.ai_reasoning && (
            <>
              <SectionLabel icon={Brain} label="AI Analysis" color="text-purple-400" />
              <p className="text-xs text-[var(--text-muted)] leading-relaxed whitespace-pre-line">
                {stock.ai_reasoning}
              </p>
            </>
          )}

          {/* How to buy */}
          {stock.ai_how_to_buy && (
            <>
              <SectionLabel icon={Lightbulb} label="How to Buy" color="text-blue-400" />
              <p className="text-xs text-[var(--text-muted)] leading-relaxed whitespace-pre-line">
                {stock.ai_how_to_buy}
              </p>
            </>
          )}

          {/* Wait for */}
          {stock.ai_wait_for && (
            <>
              <SectionLabel icon={Clock} label="Wait For" color="text-cyan-400" />
              <p className="text-xs text-[var(--text-muted)] leading-relaxed">
                {stock.ai_wait_for}
              </p>
            </>
          )}

          {/* Profit estimation */}
          {(stock.expected_return_1w != null || stock.expected_return_2w != null || stock.expected_return_1m != null) && (
            <div>
              <SectionLabel icon={TrendingUp} label="Profit Estimation" color="text-emerald-400" />
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-xs">
                {stock.expected_return_1w != null && (
                  <div className="bg-[var(--surface)] rounded p-2 text-center border border-[var(--border)]">
                    <div className="text-[var(--text-dim)] text-[10px]">1 Week</div>
                    <div className={clsx("font-bold", stock.expected_return_1w > 0 ? "text-green-400" : "text-red-400")}>
                      {stock.expected_return_1w > 0 ? "+" : ""}{stock.expected_return_1w.toFixed(1)}%
                    </div>
                  </div>
                )}
                {stock.expected_return_2w != null && (
                  <div className="bg-[var(--surface)] rounded p-2 text-center border border-[var(--border)]">
                    <div className="text-[var(--text-dim)] text-[10px]">2 Weeks</div>
                    <div className={clsx("font-bold", stock.expected_return_2w > 0 ? "text-green-400" : "text-red-400")}>
                      {stock.expected_return_2w > 0 ? "+" : ""}{stock.expected_return_2w.toFixed(1)}%
                    </div>
                  </div>
                )}
                {stock.expected_return_1m != null && (
                  <div className="bg-[var(--surface)] rounded p-2 text-center border border-[var(--border)]">
                    <div className="text-[var(--text-dim)] text-[10px]">1 Month</div>
                    <div className={clsx("font-bold", stock.expected_return_1m > 0 ? "text-green-400" : "text-red-400")}>
                      {stock.expected_return_1m > 0 ? "+" : ""}{stock.expected_return_1m.toFixed(1)}%
                    </div>
                  </div>
                )}
                {stock.downside_risk != null && (
                  <div className="bg-[var(--surface)] rounded p-2 text-center border border-red-500/20">
                    <div className="text-[var(--text-dim)] text-[10px]">Downside Risk</div>
                    <div className="font-bold text-red-400">{stock.downside_risk.toFixed(1)}%</div>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* DSEX Impact Analysis */}
          {(stock.dsex_outlook || stock.if_dsex_drops || stock.if_dsex_rises) && (
            <div>
              <SectionLabel icon={BarChart3} label={`DSEX Impact ${stock.dsex_dependency ? `(${stock.dsex_dependency} dependency)` : ""}`} color="text-indigo-400" />
              <div className="space-y-2">
                {stock.dsex_outlook && (
                  <p className="text-xs text-indigo-300/80 leading-relaxed whitespace-pre-line">
                    {stock.dsex_outlook}
                  </p>
                )}
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                  {stock.if_dsex_drops && (
                    <div className="bg-red-500/5 border border-red-500/15 rounded p-2">
                      <div className="text-[10px] text-red-400 font-medium mb-0.5">If DSEX drops 1-2%</div>
                      <p className="text-xs text-red-300/80 leading-relaxed">{stock.if_dsex_drops}</p>
                    </div>
                  )}
                  {stock.if_dsex_rises && (
                    <div className="bg-green-500/5 border border-green-500/15 rounded p-2">
                      <div className="text-[10px] text-green-400 font-medium mb-0.5">If DSEX rises 1-2%</div>
                      <p className="text-xs text-green-300/80 leading-relaxed">{stock.if_dsex_rises}</p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* Risk + Catalysts */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {stock.ai_key_risk && (
              <div>
                <SectionLabel icon={ShieldAlert} label="Key Risk" color="text-red-400" />
                <p className="text-xs text-red-300/80 leading-relaxed">{stock.ai_key_risk}</p>
              </div>
            )}
            {stock.ai_catalysts?.length > 0 && (
              <div>
                <SectionLabel icon={Sparkles} label="Catalysts" color="text-green-400" />
                <ul className="text-xs text-green-300/80 leading-relaxed space-y-0.5">
                  {stock.ai_catalysts.map((c, i) => (
                    <li key={i} className="flex gap-1.5">
                      <span className="text-green-500 shrink-0">+</span>
                      {c}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>

          {stock.ai_risk_factors?.length > 0 && (
            <div>
              <SectionLabel icon={AlertTriangle} label="Risk Factors" color="text-orange-400" />
              <ul className="text-xs text-orange-300/80 leading-relaxed space-y-0.5">
                {stock.ai_risk_factors.map((r, i) => (
                  <li key={i} className="flex gap-1.5">
                    <span className="text-orange-500 shrink-0">!</span>
                    {r}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Layer bars */}
          <SectionLabel icon={BarChart3} label="Signal Layers" color="text-[var(--text-muted)]" />
          <div className="space-y-1">
            {LAYER_CONFIG.map(({ key, label, desc }) => (
              <LayerBar key={key} label={label} pct={stock.layers[key as keyof typeof stock.layers] ?? 0} desc={desc} />
            ))}
          </div>

          {/* Key signals */}
          {stock.signals.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-1.5">
              {stock.signals.map((sig, i) => (
                <span key={i} className="text-[10px] px-1.5 py-0.5 rounded bg-[var(--surface)] text-[var(--text-muted)] border border-[var(--border)]">
                  {sig}
                </span>
              ))}
            </div>
          )}

          {/* Price tracking */}
          {!stock.is_new && stock.days_on_radar > 1 && (
            <div className={clsx(
              "text-xs px-3 py-2 rounded mt-2 flex items-center gap-2",
              priceDir === "getting-cheaper"
                ? "bg-green-500/10 text-green-400"
                : "bg-red-500/10 text-red-400"
            )}>
              <DollarSign className="h-3.5 w-3.5 shrink-0" />
              <span>Since first seen ({stock.entry_price.toFixed(1)}):</span>
              <span className="font-bold">
                {stock.price_change_pct > 0 ? "+" : ""}{stock.price_change_pct}%
              </span>
              <span className="text-[var(--text-dim)]">
                {priceDir === "getting-cheaper" ? "— price came down (good for buying)" : "— price moved up"}
              </span>
            </div>
          )}

          {/* Stage progression */}
          {stock.stage_history.length > 1 && (
            <div className="flex items-center gap-2 mt-2">
              <span className="text-[10px] text-[var(--text-dim)]">Stage journey:</span>
              <StageProgression history={stock.stage_history} />
            </div>
          )}
        </div>
      )}
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
        <XCircle className="h-4 w-4 text-red-400" />
        Recently Removed ({removed.length})
        {open ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
      </button>

      {open && (
        <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-2">
          {removed.map((r) => (
            <div key={r.symbol} className="rounded-md border border-red-500/15 bg-red-500/5 px-3 py-2.5">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold text-[var(--text)]">{r.symbol}</span>
                <span className="text-[10px] px-1.5 py-0.5 rounded bg-red-500/15 text-red-400 font-medium">
                  {r.reason}
                </span>
              </div>
              <div className="flex items-center gap-3 mt-1 text-xs text-[var(--text-dim)]">
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

/* ── Indicator explainer (collapsible) ── */
const INDICATOR_HELP = [
  { name: "StochRSI", what: "A faster version of RSI that catches bottoms earlier", buy: "Below 20 = deeply oversold (strong bounce signal). K crossing above D in oversold zone = early buy." },
  { name: "MFI", what: "Like RSI but counts volume too — more reliable for DSE", buy: "Below 20 = sellers exhausted AND volume confirms it. Much stronger than RSI alone." },
  { name: "Williams %R", what: "Shows where price sits in its recent high-low range", buy: "Below -80 = near the bottom of its range. Below -90 = extreme oversold." },
  { name: "MACD", what: "Shows momentum direction — is the stock speeding up or slowing down?", buy: "Histogram crossing from negative to positive = momentum shifting UP. Converging = about to cross." },
  { name: "ADX / DI", what: "ADX = how strong the trend is. +DI/-DI = which direction", buy: "+DI above -DI with ADX > 20 = uptrend is real and getting stronger." },
  { name: "EMA 9/21", what: "Short-term (9-day) vs medium-term (21-day) average price", buy: "EMA9 crossing above EMA21 = 'golden cross' — short-term trend is turning up." },
  { name: "CMF", what: "Are big players buying or selling? Positive = money flowing IN", buy: "Above +0.05 = accumulation. Above +0.15 = strong buying pressure. Negative = stay away." },
  { name: "OBV", what: "Tracks cumulative volume — rising OBV on flat price = hidden accumulation", buy: "OBV rising while price is flat/down = smart money buying before the move." },
  { name: "RSI", what: "Measures if sellers are exhausted (oversold) or buyers are exhausted (overbought)", buy: "Below 30 = oversold, bounce likely. 40-60 = neutral. Above 70 = overbought, risky." },
  { name: "BB%", what: "Where price sits within its 20-day normal range (0% = bottom, 100% = top)", buy: "Below 10% = at the very bottom of normal range. Very likely to bounce back up." },
  { name: "Volume Ratio", what: "Today's volume compared to the 20-day average", buy: "Above 2x = unusual interest, something is happening. Below 0.5x = dead, avoid." },
  { name: "AI Verdict", what: "Claude AI analyzes the full chart + all indicators + market context", buy: "BUY with HIGH confidence = AI sees strong opportunity across all factors." },
] as const;

function IndicatorHelp() {
  const [open, setOpen] = useState(false);
  return (
    <div className="mb-4">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-1.5 text-xs text-[var(--text-dim)] hover:text-[var(--text)] transition-colors"
      >
        <HelpCircle className="h-4 w-4" />
        <span className="font-medium">What do these indicators mean?</span>
        {open ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
      </button>
      {open && (
        <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2.5">
          {INDICATOR_HELP.map(({ name, what, buy }) => (
            <div key={name} className="rounded-lg border border-[var(--border)] bg-[var(--surface)] px-3 py-2.5">
              <div className="text-xs font-bold text-[var(--text)] mb-1">{name}</div>
              <div className="text-[11px] text-[var(--text-muted)] mb-1.5">{what}</div>
              <div className="text-[11px] text-emerald-400 flex gap-1">
                <span className="shrink-0">Buy signal:</span>
                <span>{buy}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/* ── Category filter pills ── */
const CATEGORIES = [
  { key: "A", label: "A (Blue Chip)" },
  { key: "B", label: "B (Mid Cap)" },
  { key: "Z", label: "Z (Small/Risky)" },
] as const;

/* ── Main BuyRadar page ── */
export default function BuyRadar() {
  const [data, setData] = useState<BuyRadarResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [view, setView] = useState<"pipeline" | "list">("pipeline");
  const [selectedCats, setSelectedCats] = useState<Set<string>>(new Set(["A"]));

  const toggleCat = (cat: string) => {
    setSelectedCats(prev => {
      const next = new Set(prev);
      if (next.has(cat)) {
        if (next.size > 1) next.delete(cat);
      } else {
        next.add(cat);
      }
      return next;
    });
  };

  const load = () => {
    setLoading(true);
    setError("");
    const cats = Array.from(selectedCats).join(",");
    fetchBuyRadar(cats)
      .then(setData)
      .catch((e) => setError(e.message || "Failed to load"))
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, [selectedCats]);

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
              className={clsx("px-3 py-1.5 text-xs font-medium transition-colors",
                view === "pipeline" ? "bg-[var(--surface-active)] text-[var(--text)]" : "text-[var(--text-dim)] hover:text-[var(--text)]")}>
              Pipeline
            </button>
            <button onClick={() => setView("list")}
              className={clsx("px-3 py-1.5 text-xs font-medium transition-colors",
                view === "list" ? "bg-[var(--surface-active)] text-[var(--text)]" : "text-[var(--text-dim)] hover:text-[var(--text)]")}>
              List
            </button>
          </div>
          <button onClick={load}
            className="p-1.5 rounded-md hover:bg-[var(--hover)] text-[var(--text-dim)] hover:text-[var(--text)] transition-colors">
            <RefreshCw className="h-4 w-4" />
          </button>
        </div>
      </div>

      {/* Category filter */}
      <div className="flex items-center gap-2 mb-4">
        <span className="text-xs text-[var(--text-dim)] font-medium">Category:</span>
        {CATEGORIES.map(({ key, label }) => (
          <button
            key={key}
            onClick={() => toggleCat(key)}
            className={clsx(
              "px-3 py-1.5 rounded-md text-xs font-medium transition-colors border",
              selectedCats.has(key)
                ? "bg-blue-500/15 text-blue-400 border-blue-500/30"
                : "text-[var(--text-dim)] border-[var(--border)] hover:text-[var(--text)] hover:bg-[var(--hover)]"
            )}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Stage summary bar */}
      <div className="flex gap-2 mb-4 overflow-x-auto pb-1">
        {STAGES.map(({ key, label, desc }) => {
          const count = byStage[key]?.length || 0;
          const st = STAGE_STYLES[key];
          return (
            <div key={key} className={clsx("flex items-center gap-2.5 px-3 py-2 rounded-lg border shrink-0", st.bg, st.border)}>
              <span className={clsx("text-xl font-bold", st.text)}>{count}</span>
              <div>
                <div className={clsx("text-xs font-semibold", st.text)}>{label}</div>
                <div className="text-[10px] text-[var(--text-dim)]">{desc}</div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Market context banner */}
      {data.market_ctx && <MarketContextBanner ctx={data.market_ctx} />}

      {/* DSEX Forecast */}
      {data.dsex_forecast && <DsexForecastBanner forecast={data.dsex_forecast} />}

      {/* Indicator explainer */}
      <IndicatorHelp />

      {/* Pipeline view */}
      {view === "pipeline" && (
        <div className="space-y-6">
          {STAGES.map(({ key, label }) => {
            const stocks = byStage[key] || [];
            if (stocks.length === 0) return null;
            const st = STAGE_STYLES[key];
            return (
              <div key={key}>
                <div className={clsx("text-sm font-bold mb-3 flex items-center gap-2", st.text)}>
                  <span className={clsx("w-2.5 h-2.5 rounded-full", st.dot)} />
                  {label}
                  <span className="text-[var(--text-dim)] font-normal text-xs">({stocks.length} stocks)</span>
                </div>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                  {stocks.map((s) => <StockCard key={s.symbol} stock={s} />)}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* List view */}
      {view === "list" && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
          {data.stocks.map((s) => <StockCard key={s.symbol} stock={s} />)}
        </div>
      )}

      {/* Removed stocks */}
      <RemovedSection removed={data.removed} />
    </div>
  );
}
