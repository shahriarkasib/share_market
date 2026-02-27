import { useNavigate } from "react-router-dom";
import {
  Target,
  Shield,
  Clock,
  TrendingUp,
  TrendingDown,
  Activity,
} from "lucide-react";
import { clsx } from "clsx";
import type { StockSignal } from "../../types/index.ts";
import { formatBDT, formatPct, colorBySign } from "../../lib/format.ts";

interface Props {
  signal: StockSignal;
}

/* ---------- Signal type badge config ---------- */

const signalBadge: Record<
  StockSignal["signal_type"],
  { label: string; cls: string }
> = {
  STRONG_BUY: {
    label: "Strong Buy",
    cls: "bg-green-500/20 text-green-300 border-green-500/40",
  },
  BUY: {
    label: "Buy",
    cls: "bg-emerald-500/20 text-emerald-300 border-emerald-500/40",
  },
  HOLD: {
    label: "Hold",
    cls: "bg-yellow-500/20 text-yellow-300 border-yellow-500/40",
  },
  SELL: {
    label: "Sell",
    cls: "bg-red-500/20 text-red-300 border-red-500/40",
  },
  STRONG_SELL: {
    label: "Strong Sell",
    cls: "bg-red-600/20 text-red-200 border-red-600/40",
  },
};

const timingBadge: Record<
  StockSignal["timing"],
  { label: string; cls: string }
> = {
  BUY_NOW: {
    label: "Buy Now",
    cls: "bg-green-500/15 text-green-400",
  },
  WAIT_FOR_DIP: {
    label: "Wait for Dip",
    cls: "bg-yellow-500/15 text-yellow-400",
  },
  ACCUMULATE: {
    label: "Accumulate",
    cls: "bg-blue-500/15 text-blue-400",
  },
  SELL_NOW: {
    label: "Sell Now",
    cls: "bg-red-500/15 text-red-400",
  },
  HOLD_TIGHT: {
    label: "Hold Tight",
    cls: "bg-slate-500/15 text-slate-400",
  },
};

/* ---------- Helpers ---------- */

function isBuyish(t: StockSignal["signal_type"]): boolean {
  return t === "STRONG_BUY" || t === "BUY";
}

/* ---------- Component ---------- */

export default function SignalCard({ signal }: Props) {
  const navigate = useNavigate();
  const badge = signalBadge[signal.signal_type];
  const timing = timingBadge[signal.timing];
  const buyish = isBuyish(signal.signal_type);

  return (
    <button
      type="button"
      onClick={() => navigate(`/stock/${signal.symbol}`)}
      className={clsx(
        "w-full text-left rounded-xl border p-4 transition-all hover:scale-[1.01] hover:shadow-lg cursor-pointer",
        buyish
          ? "bg-gradient-to-br from-green-950/40 to-slate-800 border-green-800/40 hover:border-green-700/60"
          : "bg-gradient-to-br from-red-950/40 to-slate-800 border-red-800/40 hover:border-red-700/60",
      )}
    >
      {/* Header row */}
      <div className="flex items-center justify-between mb-3">
        <div>
          <span className="text-base font-bold text-slate-100">
            {signal.symbol}
          </span>
          {signal.company_name && (
            <span className="ml-2 text-xs text-slate-500 truncate max-w-[140px] inline-block align-middle">
              {signal.company_name}
            </span>
          )}
        </div>
        <span
          className={clsx(
            "inline-flex items-center px-2 py-0.5 rounded-md text-xs font-semibold border",
            badge.cls,
          )}
        >
          {badge.label}
        </span>
      </div>

      {/* Price row */}
      <div className="flex items-baseline gap-3 mb-3">
        <span className="text-lg font-bold text-slate-100">
          {formatBDT(signal.ltp)}
        </span>
        <span
          className={clsx("text-sm font-medium", colorBySign(signal.change_pct))}
        >
          {signal.change_pct > 0 ? (
            <TrendingUp className="inline h-3.5 w-3.5 mr-0.5" />
          ) : (
            <TrendingDown className="inline h-3.5 w-3.5 mr-0.5" />
          )}
          {formatPct(signal.change_pct)}
        </span>
      </div>

      {/* Confidence bar */}
      <div className="mb-3">
        <div className="flex items-center justify-between text-xs mb-1">
          <span className="text-slate-500">Confidence</span>
          <span className="font-medium text-slate-300">
            {(signal.confidence * 100).toFixed(0)}%
          </span>
        </div>
        <div className="h-1.5 bg-slate-700 rounded-full overflow-hidden">
          <div
            className={clsx(
              "h-full rounded-full transition-all",
              buyish ? "bg-green-500" : "bg-red-500",
            )}
            style={{ width: `${Math.min(signal.confidence * 100, 100)}%` }}
          />
        </div>
      </div>

      {/* Target / Stop / R:R */}
      <div className="grid grid-cols-3 gap-2 mb-3 text-xs">
        <div className="bg-slate-900/50 rounded-md p-2">
          <div className="flex items-center gap-1 text-slate-500 mb-0.5">
            <Target className="h-3 w-3" />
            Target
          </div>
          <div className="font-semibold text-green-400">
            {formatBDT(signal.target_price)}
          </div>
        </div>
        <div className="bg-slate-900/50 rounded-md p-2">
          <div className="flex items-center gap-1 text-slate-500 mb-0.5">
            <Shield className="h-3 w-3" />
            Stop Loss
          </div>
          <div className="font-semibold text-red-400">
            {formatBDT(signal.stop_loss)}
          </div>
        </div>
        <div className="bg-slate-900/50 rounded-md p-2">
          <div className="text-slate-500 mb-0.5">R:R</div>
          <div className="font-semibold text-blue-400">
            {signal.risk_reward_ratio.toFixed(1)}
          </div>
        </div>
      </div>

      {/* Key indicators */}
      <div className="flex flex-wrap gap-1.5 mb-3">
        {signal.indicators.rsi != null && (
          <IndicatorChip
            label="RSI"
            value={signal.indicators.rsi.toFixed(0)}
            color={
              signal.indicators.rsi < 30
                ? "text-green-400"
                : signal.indicators.rsi > 70
                  ? "text-red-400"
                  : "text-slate-300"
            }
          />
        )}
        {signal.indicators.macd_signal && (
          <IndicatorChip
            label="MACD"
            value={signal.indicators.macd_signal}
            color={
              signal.indicators.macd_signal.toLowerCase().includes("bull")
                ? "text-green-400"
                : "text-red-400"
            }
          />
        )}
        {signal.indicators.volume_signal && (
          <IndicatorChip
            label="Vol"
            value={signal.indicators.volume_signal}
            color="text-blue-400"
          />
        )}
        {signal.indicators.ema_crossover && (
          <IndicatorChip
            label="EMA"
            value={signal.indicators.ema_crossover}
            color={
              signal.indicators.ema_crossover.toLowerCase().includes("bull")
                ? "text-green-400"
                : "text-red-400"
            }
          />
        )}
      </div>

      {/* Reasoning */}
      <p className="text-xs text-slate-400 leading-relaxed mb-3 line-clamp-2">
        {signal.reasoning}
      </p>

      {/* Timing badge + icon row */}
      <div className="flex items-center justify-between">
        <span
          className={clsx(
            "inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-xs font-medium",
            timing.cls,
          )}
        >
          <Clock className="h-3 w-3" />
          {timing.label}
        </span>
        <Activity className="h-3.5 w-3.5 text-slate-600" />
      </div>
    </button>
  );
}

/* ---------- Sub-components ---------- */

function IndicatorChip({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color: string;
}) {
  return (
    <span className="inline-flex items-center gap-1 bg-slate-900/60 border border-slate-700 rounded px-1.5 py-0.5 text-[10px]">
      <span className="text-slate-500">{label}</span>
      <span className={clsx("font-medium", color)}>{value}</span>
    </span>
  );
}
