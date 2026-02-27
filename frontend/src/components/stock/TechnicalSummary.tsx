import { BarChart3, TrendingUp, TrendingDown, Volume2, Gauge, Zap } from "lucide-react";
import { clsx } from "clsx";
import type { StockSignal } from "../../types/index.ts";

interface TechnicalSummaryProps {
  signal: StockSignal;
}

/* ------------------------------------------------------------------ */
/*  RSI Gauge: colored bar 0-100 with marker                          */
/* ------------------------------------------------------------------ */

function RSIGauge({ value }: { value: number | null | undefined }) {
  if (value == null) return <EmptyGauge label="RSI" />;
  const pct = Math.max(0, Math.min(100, value));
  const zone = pct < 30 ? "Oversold" : pct > 70 ? "Overbought" : "Neutral";
  const zoneColor = pct < 30 ? "text-green-400" : pct > 70 ? "text-red-400" : "text-yellow-400";

  return (
    <div className="bg-[var(--bg)] rounded-md p-2.5">
      <div className="flex items-center justify-between mb-1.5">
        <span className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider">RSI</span>
        <span className={clsx("text-xs font-bold tabular-nums", zoneColor)}>{pct.toFixed(1)}</span>
      </div>
      {/* Track with three zones */}
      <div className="relative h-2 rounded-full overflow-hidden flex">
        <div className="h-full" style={{ width: "30%", backgroundColor: "rgba(34,197,94,0.3)" }} />
        <div className="h-full" style={{ width: "40%", backgroundColor: "rgba(234,179,8,0.15)" }} />
        <div className="h-full" style={{ width: "30%", backgroundColor: "rgba(239,68,68,0.3)" }} />
        {/* Marker */}
        <div
          className="absolute top-0 h-full w-1 rounded-full bg-white shadow"
          style={{ left: `calc(${pct}% - 2px)` }}
        />
      </div>
      <div className={clsx("text-[10px] mt-1 text-center", zoneColor)}>{zone}</div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  MACD Direction: arrow icon with label                             */
/* ------------------------------------------------------------------ */

function MACDDirection({ value }: { value: string | undefined }) {
  if (!value) return <EmptyGauge label="MACD" />;
  const isBullish = value.toLowerCase().includes("bull");
  const Icon = isBullish ? TrendingUp : TrendingDown;
  const color = isBullish ? "text-green-400" : "text-red-400";

  return (
    <div className="bg-[var(--bg)] rounded-md p-2.5 flex flex-col items-center justify-center gap-1">
      <span className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider">MACD</span>
      <Icon className={clsx("h-5 w-5", color)} />
      <span className={clsx("text-[10px] font-medium", color)}>{value}</span>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Trend Meter: 5-segment bar STRONG_DOWN to STRONG_UP               */
/* ------------------------------------------------------------------ */

const trendSegments = ["STRONG_DOWN", "DOWN", "SIDEWAYS", "UP", "STRONG_UP"] as const;
const trendColors: Record<string, string> = {
  STRONG_DOWN: "bg-red-500",
  DOWN: "bg-red-400",
  SIDEWAYS: "bg-yellow-500",
  UP: "bg-emerald-400",
  STRONG_UP: "bg-green-500",
};
const trendLabels: Record<string, string> = {
  STRONG_DOWN: "Strong Down",
  DOWN: "Down",
  SIDEWAYS: "Sideways",
  UP: "Up",
  STRONG_UP: "Strong Up",
};

function TrendMeter({ value }: { value: string | undefined }) {
  if (!value) return <EmptyGauge label="Trend" />;

  return (
    <div className="bg-[var(--bg)] rounded-md p-2.5">
      <div className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider mb-1.5 text-center">
        Trend
      </div>
      <div className="flex gap-0.5 mb-1">
        {trendSegments.map((seg) => (
          <div
            key={seg}
            className={clsx(
              "h-2 flex-1 rounded-sm transition-opacity",
              seg === value ? trendColors[seg] : "bg-[var(--border)]",
              seg === value ? "opacity-100" : "opacity-40",
            )}
          />
        ))}
      </div>
      <div className="text-[10px] text-center text-[var(--text-muted)]">
        {trendLabels[value] ?? value}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Volume Meter: text with icon                                      */
/* ------------------------------------------------------------------ */

function VolumeMeter({ value }: { value: string | undefined }) {
  if (!value) return <EmptyGauge label="Volume" />;
  const isHigh = value.toLowerCase().includes("high") || value.toLowerCase().includes("spike");
  const color = isHigh ? "text-blue-400" : "text-[var(--text-muted)]";

  return (
    <div className="bg-[var(--bg)] rounded-md p-2.5 flex flex-col items-center justify-center gap-1">
      <span className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider">Volume</span>
      <Volume2 className={clsx("h-4 w-4", color)} />
      <span className={clsx("text-[10px] font-medium", color)}>{value}</span>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Momentum display                                                   */
/* ------------------------------------------------------------------ */

function MomentumDisplay({ value }: { value: number | null | undefined }) {
  if (value == null) return <EmptyGauge label="Momentum" />;
  const color = value > 0 ? "text-green-400" : value < 0 ? "text-red-400" : "text-yellow-400";

  return (
    <div className="bg-[var(--bg)] rounded-md p-2.5 flex flex-col items-center justify-center gap-1">
      <span className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider">3D Mom.</span>
      <Zap className={clsx("h-4 w-4", color)} />
      <span className={clsx("text-xs font-bold tabular-nums", color)}>
        {value > 0 ? "+" : ""}{value.toFixed(2)}%
      </span>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Overall Score bar: -100 to +100                                   */
/* ------------------------------------------------------------------ */

function OverallScore({ shortScore, longScore }: { shortScore: number; longScore: number }) {
  const combined = (shortScore + longScore) / 2;
  // Map from -100..+100 to 0..100 for positioning
  const pct = Math.max(0, Math.min(100, (combined + 100) / 2));
  const color = combined > 0 ? "text-green-400" : combined < 0 ? "text-red-400" : "text-yellow-400";

  return (
    <div className="bg-[var(--bg)] rounded-md p-2.5">
      <div className="flex items-center justify-between mb-1.5">
        <span className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider">Score</span>
        <span className={clsx("text-xs font-bold tabular-nums", color)}>
          {combined > 0 ? "+" : ""}{combined.toFixed(1)}
        </span>
      </div>
      {/* Bar from red (left) through neutral (center) to green (right) */}
      <div className="relative h-2 rounded-full overflow-hidden flex">
        <div className="h-full" style={{ width: "50%", backgroundColor: "rgba(239,68,68,0.25)" }} />
        <div className="h-full" style={{ width: "50%", backgroundColor: "rgba(34,197,94,0.25)" }} />
        {/* Center mark */}
        <div className="absolute top-0 h-full w-px bg-[var(--text-dim)]" style={{ left: "50%" }} />
        {/* Marker */}
        <div
          className="absolute top-0 h-full w-1.5 rounded-full bg-white shadow"
          style={{ left: `calc(${pct}% - 3px)` }}
        />
      </div>
      <div className="flex justify-between mt-0.5 text-[9px] text-[var(--text-dim)]">
        <span>Bearish</span>
        <span>Bullish</span>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Empty gauge placeholder                                           */
/* ------------------------------------------------------------------ */

function EmptyGauge({ label }: { label: string }) {
  return (
    <div className="bg-[var(--bg)] rounded-md p-2.5 flex flex-col items-center justify-center">
      <span className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider">{label}</span>
      <Gauge className="h-4 w-4 text-[var(--text-dim)] mt-1" />
      <span className="text-[10px] text-[var(--text-dim)] mt-0.5">--</span>
    </div>
  );
}

/* ================================================================== */
/*  Main component                                                     */
/* ================================================================== */

export default function TechnicalSummary({ signal }: TechnicalSummaryProps) {
  const { indicators } = signal;

  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg p-5">
      <h2 className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-4 flex items-center gap-2">
        <BarChart3 className="h-3.5 w-3.5 text-blue-500" />
        Technical Summary
      </h2>

      <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
        <RSIGauge value={indicators.rsi} />
        <MACDDirection value={indicators.macd_signal} />
        <TrendMeter value={signal.trend_strength} />
        <VolumeMeter value={indicators.volume_signal} />
        <MomentumDisplay value={indicators.momentum_3d} />
        <OverallScore
          shortScore={signal.short_term_score}
          longScore={signal.long_term_score}
        />
      </div>
    </div>
  );
}
