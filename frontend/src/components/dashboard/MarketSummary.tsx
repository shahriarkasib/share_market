import {
  TrendingUp,
  TrendingDown,
  BarChart3,
  ArrowUpRight,
  ArrowDownRight,
  Activity,
} from "lucide-react";
import { clsx } from "clsx";
import type { MarketSummary as MarketSummaryType } from "../../types/index.ts";
import {
  formatNumber,
  formatChange,
  formatPct,
  formatCompact,
  colorBySign,
} from "../../lib/format.ts";

interface Props {
  data: MarketSummaryType | null;
}

export default function MarketSummary({ data }: Props) {
  if (!data) {
    return (
      <div className="bg-slate-800 rounded-xl border border-slate-700 p-4 animate-pulse">
        <div className="h-5 w-36 bg-slate-700 rounded mb-3" />
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="h-20 bg-slate-700/50 rounded-lg" />
          ))}
        </div>
      </div>
    );
  }

  const isPositive = data.dsex_change >= 0;

  return (
    <div className="bg-slate-800 rounded-xl border border-slate-700 p-4">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Activity className="h-4 w-4 text-blue-400" />
          <h2 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
            Market Overview
          </h2>
        </div>
        <StatusBadge status={data.market_status} />
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {/* DSEX Index */}
        <div className="bg-slate-900/60 rounded-lg p-3">
          <div className="text-[11px] text-slate-500 mb-1">DSEX Index</div>
          <div className="text-lg font-bold text-slate-100 tabular-nums">
            {formatNumber(data.dsex_index)}
          </div>
          <div className={clsx("flex items-center gap-1 text-xs font-medium mt-0.5", colorBySign(data.dsex_change))}>
            {isPositive ? (
              <ArrowUpRight className="h-3 w-3" />
            ) : (
              <ArrowDownRight className="h-3 w-3" />
            )}
            {formatChange(data.dsex_change)} ({formatPct(data.dsex_change_pct)})
          </div>
        </div>

        {/* Advances / Declines */}
        <div className="bg-slate-900/60 rounded-lg p-3">
          <div className="text-[11px] text-slate-500 mb-1">
            Adv / Dec
          </div>
          <div className="flex items-baseline gap-1.5">
            <span className="flex items-center gap-0.5 text-green-400 font-bold text-lg">
              <TrendingUp className="h-3 w-3" />
              {data.advances}
            </span>
            <span className="text-slate-600">/</span>
            <span className="flex items-center gap-0.5 text-red-400 font-bold text-lg">
              <TrendingDown className="h-3 w-3" />
              {data.declines}
            </span>
          </div>
          <div className="text-[11px] text-slate-500 mt-0.5">
            Unchanged: {data.unchanged}
          </div>
        </div>

        {/* Volume */}
        <div className="bg-slate-900/60 rounded-lg p-3">
          <div className="text-[11px] text-slate-500 mb-1">Total Volume</div>
          <div className="flex items-center gap-1 text-lg font-bold text-slate-100">
            <BarChart3 className="h-3.5 w-3.5 text-blue-400 shrink-0" />
            {formatCompact(data.total_volume)}
          </div>
          <div className="text-[11px] text-slate-500 mt-0.5">
            Trades: {formatCompact(data.total_trade)}
          </div>
        </div>

        {/* Total Value */}
        <div className="bg-slate-900/60 rounded-lg p-3">
          <div className="text-[11px] text-slate-500 mb-1">Total Value</div>
          <div className="text-lg font-bold text-slate-100">
            {data.total_value > 0 ? formatCompact(data.total_value * 1_000_000) : formatCompact(data.total_volume * 10)}
          </div>
          <div className="text-[11px] text-slate-500 mt-0.5">BDT (est.)</div>
        </div>
      </div>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const isOpen = status.toLowerCase().includes("open");
  return (
    <span
      className={clsx(
        "inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-medium",
        isOpen
          ? "bg-green-500/15 text-green-400 border border-green-500/30"
          : "bg-slate-700 text-slate-400 border border-slate-600",
      )}
    >
      <span
        className={clsx(
          "h-1.5 w-1.5 rounded-full",
          isOpen ? "bg-green-400 animate-pulse" : "bg-slate-500",
        )}
      />
      {status}
    </span>
  );
}
