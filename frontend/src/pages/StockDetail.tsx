import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import {
  ArrowUpRight,
  ArrowDownRight,
  BarChart3,
  Activity,
  Loader2,
  TrendingUp,
  Shield,
} from "lucide-react";
import PriceChart from "../components/chart/PriceChart.tsx";
import TechnicalSummary from "../components/stock/TechnicalSummary.tsx";
import PeerComparison from "../components/stock/PeerComparison.tsx";
import { clsx } from "clsx";
import type { StockPrice, StockSignal } from "../types/index.ts";
import { fetchStockPrice, fetchStockSignal } from "../api/client.ts";
import {
  formatBDT,
  formatPct,
  formatChange,
  formatNumber,
  formatCompact,
  colorBySign,
} from "../lib/format.ts";

export default function StockDetail() {
  const { symbol } = useParams<{ symbol: string }>();
  const [price, setPrice] = useState<StockPrice | null>(null);
  const [signal, setSignal] = useState<StockSignal | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!symbol) return;
    setLoading(true);
    setError(null);

    const fetchData = async () => {
      try {
        const [priceData, signalData] = await Promise.allSettled([
          fetchStockPrice(symbol),
          fetchStockSignal(symbol),
        ]);

        if (priceData.status === "fulfilled") {
          setPrice(priceData.value);
        }
        if (signalData.status === "fulfilled") {
          setSignal(signalData.value);
        }
        if (
          priceData.status === "rejected" &&
          signalData.status === "rejected"
        ) {
          setError("Failed to load stock data. Please try again.");
        }
      } catch {
        setError("An unexpected error occurred.");
      } finally {
        setLoading(false);
      }
    };

    void fetchData();
  }, [symbol]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 gap-2">
        <Loader2 className="h-5 w-5 animate-spin text-blue-500" />
        <span className="text-xs text-[var(--text-muted)]">Loading...</span>
      </div>
    );
  }

  if (error && !price && !signal) {
    return (
      <div className="space-y-4">
        <BackLink />
        <div className="bg-red-900/20 border border-red-800/40 rounded-lg px-4 py-2.5 text-xs text-red-400">
          {error}
        </div>
      </div>
    );
  }

  const changePct = price?.change_pct ?? signal?.change_pct ?? 0;
  const change = price?.change ?? 0;
  const ltp = price?.ltp ?? signal?.ltp ?? 0;
  const isPositive = change >= 0;

  return (
    <div className="space-y-4">
      <BackLink />

      {/* Price header */}
      <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg p-5">
        <div className="flex items-start justify-between flex-wrap gap-4">
          <div>
            <h1 className="text-xl font-bold text-[var(--text)]">{symbol}</h1>
            {price?.company_name && (
              <p className="text-xs text-[var(--text-muted)] mt-0.5">
                {price.company_name}
              </p>
            )}
          </div>

          <div className="text-right">
            <div className="text-3xl font-bold text-[var(--text)] tabular-nums">
              {formatBDT(ltp)}
            </div>
            <div
              className={clsx(
                "flex items-center justify-end gap-1 text-sm font-medium mt-1",
                colorBySign(change),
              )}
            >
              {isPositive ? (
                <ArrowUpRight className="h-4 w-4" />
              ) : (
                <ArrowDownRight className="h-4 w-4" />
              )}
              <span className="tabular-nums">
                {formatChange(change)} ({formatPct(changePct)})
              </span>
            </div>
          </div>
        </div>

        {/* Price stats grid */}
        {price && (
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-6 gap-3 mt-5">
            <StatBox label="Open" value={formatBDT(price.open)} />
            <StatBox label="High" value={formatBDT(price.high)} />
            <StatBox label="Low" value={formatBDT(price.low)} />
            <StatBox label="Prev Close" value={formatBDT(price.close_prev)} />
            <StatBox label="Volume" value={formatCompact(price.volume)} />
            <StatBox label="Trades" value={formatNumber(price.trade_count)} />
          </div>
        )}
      </div>

      {/* Price chart */}
      <PriceChart symbol={symbol!} signal={signal} />

      {/* Technical summary gauges */}
      {signal && <TechnicalSummary signal={signal} />}

      {/* Prediction + T+2 Decision panels */}
      {signal && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <PredictionPanel signal={signal} currentPrice={ltp} />
          <T2DecisionPanel signal={signal} />
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Signal panel */}
        {signal && <SignalPanel signal={signal} />}

        {/* Technical indicators panel */}
        {signal && <IndicatorsPanel signal={signal} />}
      </div>

      {/* Peer comparison from same sector */}
      <PeerComparison symbol={symbol!} />
    </div>
  );
}

/* ================================================================== */
/*  Sub-components                                                      */
/* ================================================================== */

function BackLink() {
  return (
    <Link
      to="/"
      className="text-xs text-[var(--text-muted)] hover:text-[var(--text)] transition-colors"
    >
      {"<-"} Back to Dashboard
    </Link>
  );
}

function StatBox({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-[var(--bg)] rounded-md p-2.5">
      <div className="text-[10px] text-[var(--text-dim)] mb-0.5">{label}</div>
      <div className="text-xs font-semibold text-[var(--text)] tabular-nums">
        {value}
      </div>
    </div>
  );
}

/* ---------- Prediction panel ---------- */

const trendBadge: Record<string, { label: string; cls: string }> = {
  STRONG_UP: { label: "Strong Up", cls: "bg-green-500/20 text-green-300 border-green-500/40" },
  UP: { label: "Up", cls: "bg-emerald-500/20 text-emerald-300 border-emerald-500/40" },
  SIDEWAYS: { label: "Sideways", cls: "bg-yellow-500/20 text-yellow-300 border-yellow-500/40" },
  DOWN: { label: "Down", cls: "bg-red-500/20 text-red-300 border-red-500/40" },
  STRONG_DOWN: { label: "Strong Down", cls: "bg-red-600/20 text-red-200 border-red-600/40" },
};

const volatilityBadge: Record<string, { label: string; cls: string }> = {
  LOW: { label: "Low Vol", cls: "bg-blue-500/20 text-blue-300 border-blue-500/40" },
  MEDIUM: { label: "Med Vol", cls: "bg-yellow-500/20 text-yellow-300 border-yellow-500/40" },
  HIGH: { label: "High Vol", cls: "bg-red-500/20 text-red-300 border-red-500/40" },
};

/** Inline SVG sparkline for predicted prices. */
function PriceSparkline({ points, isPositive }: { points: number[]; isPositive: boolean }) {
  if (points.length < 2) return null;
  const w = 200;
  const h = 60;
  const pad = 8;
  const min = Math.min(...points);
  const max = Math.max(...points);
  const range = max - min || 1;

  const coords = points.map((p, i) => ({
    x: pad + (i / (points.length - 1)) * (w - 2 * pad),
    y: h - pad - ((p - min) / range) * (h - 2 * pad),
  }));

  const pathD = coords.map((c, i) => `${i === 0 ? "M" : "L"} ${c.x} ${c.y}`).join(" ");
  const stroke = isPositive ? "#4ade80" : "#f87171";
  const fill = isPositive ? "rgba(74,222,128,0.1)" : "rgba(248,113,113,0.1)";
  const areaD = `${pathD} L ${coords[coords.length - 1].x} ${h - pad} L ${coords[0].x} ${h - pad} Z`;

  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full h-[60px]">
      <path d={areaD} fill={fill} />
      <path d={pathD} fill="none" stroke={stroke} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
      {coords.map((c, i) => (
        <circle key={i} cx={c.x} cy={c.y} r="3" fill={stroke} />
      ))}
    </svg>
  );
}

function PredictionPanel({ signal, currentPrice }: { signal: StockSignal; currentPrice: number }) {
  const pp = signal.predicted_prices;
  const dr = signal.daily_ranges;
  const hasData = pp != null;

  const rows = hasData
    ? [
        { label: "Day 2 (T+2)", price: pp.day_2, key: "day_2", highlight: true },
        { label: "Day 3", price: pp.day_3, key: "day_3", highlight: false },
        { label: "Day 4", price: pp.day_4, key: "day_4", highlight: false },
        { label: "Day 5", price: pp.day_5, key: "day_5", highlight: false },
        { label: "Day 6", price: pp.day_6, key: "day_6", highlight: false },
        { label: "Day 7", price: pp.day_7, key: "day_7", highlight: false },
      ].filter((r) => r.price != null)
    : [];

  const sparklinePoints = hasData
    ? [currentPrice, pp.day_2, pp.day_3, pp.day_4, pp.day_5, pp.day_6, pp.day_7].filter((p) => p != null)
    : [];
  const isPositive = hasData && pp.day_7 > currentPrice;

  const trend = signal.trend_strength ? trendBadge[signal.trend_strength] : null;
  const vol = signal.volatility_level ? volatilityBadge[signal.volatility_level] : null;

  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg p-5">
      <h2 className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-4 flex items-center gap-2">
        <TrendingUp className="h-3.5 w-3.5 text-blue-500" />
        Price Prediction
      </h2>

      {/* Sparkline */}
      {sparklinePoints.length > 1 && (
        <div className="mb-4">
          <PriceSparkline points={sparklinePoints} isPositive={isPositive} />
        </div>
      )}

      {/* Prediction table */}
      {hasData ? (
        <div className="mb-4 overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider border-b border-[var(--border)]">
                <th className="text-left py-1.5 pr-2">Day</th>
                <th className="text-right py-1.5 px-2">Price</th>
                <th className="text-right py-1.5 px-2">Range</th>
                <th className="text-right py-1.5 pl-2">Change</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const chg = currentPrice > 0 ? ((r.price - currentPrice) / currentPrice) * 100 : 0;
                const range = dr?.[r.key];
                return (
                  <tr key={r.label} className={clsx("border-b border-[var(--border)]", r.highlight && "bg-[var(--hover)]")}>
                    <td className={clsx("py-1.5 pr-2", r.highlight ? "text-blue-400 font-medium" : "text-[var(--text-muted)]")}>{r.label}</td>
                    <td className="py-1.5 px-2 text-right text-[var(--text)] tabular-nums font-medium">
                      {formatBDT(r.price)}
                    </td>
                    <td className="py-1.5 px-2 text-right text-[var(--text-muted)] tabular-nums text-[10px]">
                      {range
                        ? `${formatNumber(range.min)} – ${formatNumber(range.max)}`
                        : "--"}
                    </td>
                    <td
                      className={clsx(
                        "py-1.5 pl-2 text-right tabular-nums font-medium",
                        colorBySign(chg),
                      )}
                    >
                      {formatPct(chg)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="text-xs text-[var(--text-dim)] mb-4">No prediction data available</div>
      )}

      {/* Support / Resistance */}
      {(signal.support_level != null || signal.resistance_level != null) && (
        <div className="grid grid-cols-2 gap-3 mb-4">
          <div className="bg-[var(--bg)] rounded-md p-2.5 text-center">
            <div className="text-[10px] text-[var(--text-dim)] mb-0.5">Support</div>
            <div className="text-xs font-bold text-green-400 tabular-nums">
              {signal.support_level != null ? formatBDT(signal.support_level) : "--"}
            </div>
          </div>
          <div className="bg-[var(--bg)] rounded-md p-2.5 text-center">
            <div className="text-[10px] text-[var(--text-dim)] mb-0.5">Resistance</div>
            <div className="text-xs font-bold text-red-400 tabular-nums">
              {signal.resistance_level != null ? formatBDT(signal.resistance_level) : "--"}
            </div>
          </div>
        </div>
      )}

      {/* Trend + Volatility badges */}
      <div className="flex items-center gap-2 flex-wrap">
        {trend && (
          <span className={clsx("inline-flex items-center px-2 py-0.5 rounded text-[10px] font-medium border", trend.cls)}>
            {trend.label}
          </span>
        )}
        {vol && (
          <span className={clsx("inline-flex items-center px-2 py-0.5 rounded text-[10px] font-medium border", vol.cls)}>
            {vol.label}
          </span>
        )}
      </div>
    </div>
  );
}

/* ---------- T+2 Decision panel ---------- */

function riskColor(score: number): string {
  if (score < 30) return "text-green-400";
  if (score <= 60) return "text-yellow-400";
  return "text-red-400";
}

function T2DecisionPanel({ signal }: { signal: StockSignal }) {
  const isSafe = signal.t2_safe;
  const hasT2Data = isSafe != null;

  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg p-5">
      <h2 className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-4 flex items-center gap-2">
        <Shield className="h-3.5 w-3.5 text-blue-500" />
        T+2 Decision
      </h2>

      {!hasT2Data ? (
        <div className="text-xs text-[var(--text-dim)]">No T+2 data available</div>
      ) : (
        <div className="space-y-4">
          {/* Safe / Risky badge */}
          <div className="flex items-center gap-3">
            <span
              className={clsx(
                "inline-flex items-center px-3 py-1.5 rounded-md text-sm font-bold border",
                isSafe
                  ? "bg-green-500/20 text-green-300 border-green-500/40"
                  : "bg-red-500/20 text-red-300 border-red-500/40",
              )}
            >
              {isSafe ? "SAFE" : "RISKY"}
            </span>
            <span className="text-[10px] text-[var(--text-dim)]">T+2 Assessment</span>
          </div>

          {/* Risk Score */}
          {signal.risk_score != null && (
            <div className="bg-[var(--bg)] rounded-md p-3 flex items-center justify-between">
              <span className="text-xs text-[var(--text-muted)]">Risk Score</span>
              <span className={clsx("text-lg font-bold tabular-nums", riskColor(signal.risk_score))}>
                {signal.risk_score.toFixed(0)}
                <span className="text-[10px] text-[var(--text-dim)] font-normal">/100</span>
              </span>
            </div>
          )}

          {/* Entry Strategy */}
          {signal.entry_strategy && (
            <div className="bg-emerald-950/30 border border-emerald-800/30 rounded-md p-3">
              <div className="text-[10px] text-emerald-500 uppercase tracking-wider mb-1">Entry Strategy</div>
              <p className="text-xs text-[var(--text)] leading-relaxed">{signal.entry_strategy}</p>
            </div>
          )}

          {/* Exit Strategy */}
          {signal.exit_strategy && (
            <div className="bg-red-950/30 border border-red-800/30 rounded-md p-3">
              <div className="text-[10px] text-red-500 uppercase tracking-wider mb-1">Exit Strategy</div>
              <p className="text-xs text-[var(--text)] leading-relaxed">{signal.exit_strategy}</p>
            </div>
          )}

          {/* Hold Days + Expected Return + Maturity */}
          <div className="grid grid-cols-3 gap-3">
            {signal.hold_days != null && (
              <div className="bg-[var(--bg)] rounded-md p-2.5 text-center">
                <div className="text-[10px] text-[var(--text-dim)] mb-0.5">Hold Days</div>
                <div className="text-base font-bold text-blue-400 tabular-nums">{signal.hold_days}d</div>
              </div>
            )}
            {signal.expected_return_pct != null && (
              <div className="bg-[var(--bg)] rounded-md p-2.5 text-center">
                <div className="text-[10px] text-[var(--text-dim)] mb-0.5">Exp. Return</div>
                <div className={clsx("text-base font-bold tabular-nums", colorBySign(signal.expected_return_pct))}>
                  {formatPct(signal.expected_return_pct)}
                </div>
              </div>
            )}
            {signal.t2_maturity_date && (
              <div className="bg-[var(--bg)] rounded-md p-2.5 text-center">
                <div className="text-[10px] text-[var(--text-dim)] mb-0.5">Maturity</div>
                <div className="text-xs font-bold text-[var(--text)] tabular-nums">{signal.t2_maturity_date}</div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

/* ---------- Signal panel ---------- */

const signalBadge: Record<
  StockSignal["signal_type"],
  { label: string; cls: string }
> = {
  STRONG_BUY: { label: "Strong Buy", cls: "bg-green-500/20 text-green-300 border-green-500/40" },
  BUY: { label: "Buy", cls: "bg-emerald-500/20 text-emerald-300 border-emerald-500/40" },
  HOLD: { label: "Hold", cls: "bg-yellow-500/20 text-yellow-300 border-yellow-500/40" },
  SELL: { label: "Sell", cls: "bg-red-500/20 text-red-300 border-red-500/40" },
  STRONG_SELL: { label: "Strong Sell", cls: "bg-red-600/20 text-red-200 border-red-600/40" },
};

const timingLabels: Record<StockSignal["timing"], string> = {
  BUY_NOW: "Buy Now",
  WAIT_FOR_DIP: "Wait for Dip",
  ACCUMULATE: "Accumulate",
  SELL_NOW: "Sell Now",
  HOLD_TIGHT: "Hold Tight",
};

function SignalPanel({ signal }: { signal: StockSignal }) {
  const badge = signalBadge[signal.signal_type];
  const isBuyish =
    signal.signal_type === "STRONG_BUY" || signal.signal_type === "BUY";

  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg p-5">
      <h2 className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-4 flex items-center gap-2">
        <Activity className="h-3.5 w-3.5 text-blue-500" />
        Trading Signal
      </h2>

      {/* Signal type + timing */}
      <div className="flex items-center gap-3 mb-4">
        <span
          className={clsx(
            "inline-flex items-center px-2.5 py-1 rounded-md text-xs font-bold border",
            badge.cls,
          )}
        >
          {badge.label}
        </span>
        <span className="text-xs text-[var(--text-muted)]">
          {timingLabels[signal.timing]}
        </span>
      </div>

      {/* Confidence as a number */}
      <div className="flex items-center justify-between mb-4 text-xs">
        <span className="text-[var(--text-muted)]">Confidence</span>
        <span
          className={clsx(
            "text-lg font-bold tabular-nums",
            isBuyish ? "text-green-400" : "text-red-400",
          )}
        >
          {(signal.confidence * 100).toFixed(0)}%
        </span>
      </div>

      {/* Scores */}
      <div className="grid grid-cols-2 gap-3 mb-4">
        <div className="bg-[var(--bg)] rounded-md p-3">
          <div className="text-[10px] text-[var(--text-dim)] mb-0.5">Short-term</div>
          <div className="text-base font-bold text-[var(--text)] tabular-nums">
            {signal.short_term_score.toFixed(1)}
          </div>
        </div>
        <div className="bg-[var(--bg)] rounded-md p-3">
          <div className="text-[10px] text-[var(--text-dim)] mb-0.5">Long-term</div>
          <div className="text-base font-bold text-[var(--text)] tabular-nums">
            {signal.long_term_score.toFixed(1)}
          </div>
        </div>
      </div>

      {/* Targets: Target | Stop Loss | R:R */}
      <div className="grid grid-cols-3 gap-3 mb-4">
        <div className="bg-[var(--bg)] rounded-md p-3 text-center">
          <div className="text-[10px] text-[var(--text-dim)] mb-0.5">Target</div>
          <div className="text-xs font-bold text-green-400 tabular-nums">
            {formatBDT(signal.target_price)}
          </div>
        </div>
        <div className="bg-[var(--bg)] rounded-md p-3 text-center">
          <div className="text-[10px] text-[var(--text-dim)] mb-0.5">Stop Loss</div>
          <div className="text-xs font-bold text-red-400 tabular-nums">
            {formatBDT(signal.stop_loss)}
          </div>
        </div>
        <div className="bg-[var(--bg)] rounded-md p-3 text-center">
          <div className="text-[10px] text-[var(--text-dim)] mb-0.5">R:R Ratio</div>
          <div className="text-xs font-bold text-blue-400 tabular-nums">
            {signal.risk_reward_ratio.toFixed(2)}
          </div>
        </div>
      </div>

      {/* Reasoning */}
      <div className="bg-[var(--bg)] rounded-md p-3">
        <div className="text-[10px] text-[var(--text-dim)] mb-1">Analysis</div>
        <p className="text-xs text-[var(--text-muted)] leading-relaxed">
          {signal.reasoning}
        </p>
      </div>

      {signal.created_at && (
        <div className="text-[10px] text-[var(--text-dim)] mt-3 text-right">
          Signal generated: {new Date(signal.created_at).toLocaleString()}
        </div>
      )}
    </div>
  );
}

/* ---------- Indicators panel ---------- */

function IndicatorsPanel({ signal }: { signal: StockSignal }) {
  const { indicators } = signal;

  const rows: { label: string; value: string | null; color?: string }[] = [
    {
      label: "RSI (14)",
      value: indicators.rsi != null ? indicators.rsi.toFixed(1) : null,
      color:
        indicators.rsi != null
          ? indicators.rsi < 30
            ? "text-green-400"
            : indicators.rsi > 70
              ? "text-red-400"
              : "text-[var(--text)]"
          : undefined,
    },
    {
      label: "MACD Signal",
      value: indicators.macd_signal ?? null,
      color: indicators.macd_signal?.toLowerCase().includes("bull")
        ? "text-green-400"
        : "text-red-400",
    },
    {
      label: "BB Position",
      value: indicators.bb_position ?? null,
    },
    {
      label: "EMA Crossover",
      value: indicators.ema_crossover ?? null,
      color: indicators.ema_crossover?.toLowerCase().includes("bull")
        ? "text-green-400"
        : "text-red-400",
    },
    {
      label: "Volume Signal",
      value: indicators.volume_signal ?? null,
      color: "text-blue-400",
    },
    {
      label: "3D Momentum",
      value:
        indicators.momentum_3d != null
          ? `${indicators.momentum_3d > 0 ? "+" : ""}${indicators.momentum_3d.toFixed(2)}%`
          : null,
      color:
        indicators.momentum_3d != null
          ? indicators.momentum_3d > 0
            ? "text-green-400"
            : "text-red-400"
          : undefined,
    },
    {
      label: "Stochastic %K",
      value:
        indicators.stoch_k != null ? indicators.stoch_k.toFixed(1) : null,
      color:
        indicators.stoch_k != null
          ? indicators.stoch_k < 20
            ? "text-green-400"
            : indicators.stoch_k > 80
              ? "text-red-400"
              : "text-[var(--text)]"
          : undefined,
    },
  ];

  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg p-5">
      <h2 className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-4 flex items-center gap-2">
        <BarChart3 className="h-3.5 w-3.5 text-blue-500" />
        Technical Indicators
      </h2>

      <div className="space-y-0">
        {rows.map((row) => (
          <div
            key={row.label}
            className="flex items-center justify-between py-2 border-b border-[var(--border)] last:border-0"
          >
            <span className="text-xs text-[var(--text-muted)]">{row.label}</span>
            <span
              className={clsx(
                "text-xs font-medium tabular-nums",
                row.color ?? "text-[var(--text)]",
              )}
            >
              {row.value ?? "--"}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
