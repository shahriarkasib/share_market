import { clsx } from "clsx";
import { Loader2 } from "lucide-react";
import type { SignalsSummary } from "../../types/index.ts";

interface Props {
  data: SignalsSummary | null;
}

const sentimentColor: Record<string, string> = {
  bullish: "text-green-400",
  bearish: "text-red-400",
  neutral: "text-yellow-400",
};

export default function SignalsSummaryBar({ data }: Props) {
  if (!data) {
    return (
      <div className="bg-slate-800 rounded-xl border border-slate-700 p-4 animate-pulse">
        <div className="h-4 w-32 bg-slate-700 rounded mb-3" />
        <div className="flex gap-3">
          {Array.from({ length: 5 }).map((_, i) => (
            <div key={i} className="flex-1 h-12 bg-slate-700/50 rounded" />
          ))}
        </div>
      </div>
    );
  }

  const isComputing = data.is_computing || data.total_stocks === 0;

  const items = [
    { label: "Strong Buy", count: data.strong_buy_count, cls: "text-green-400" },
    { label: "Buy", count: data.buy_count, cls: "text-emerald-400" },
    { label: "Hold", count: data.hold_count, cls: "text-yellow-400" },
    { label: "Sell", count: data.sell_count, cls: "text-orange-400" },
    { label: "Strong Sell", count: data.strong_sell_count, cls: "text-red-400" },
  ];

  return (
    <div className="bg-slate-800 rounded-xl border border-slate-700 p-4">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
          Signals Summary
        </h3>
        {isComputing ? (
          <span className="inline-flex items-center gap-1.5 text-xs font-medium px-2 py-0.5 rounded-md bg-blue-500/15 text-blue-400 border border-blue-500/30">
            <Loader2 className="h-3 w-3 animate-spin" />
            Computing...
          </span>
        ) : (
          <span
            className={clsx(
              "text-xs font-medium px-2 py-0.5 rounded-md bg-slate-700",
              sentimentColor[data.market_sentiment.toLowerCase()] ?? "text-slate-400",
            )}
          >
            {data.market_sentiment}
          </span>
        )}
      </div>

      {isComputing ? (
        <div className="flex items-center justify-center py-4 text-sm text-slate-500">
          <Loader2 className="h-4 w-4 animate-spin mr-2" />
          Analyzing ~100 stocks... signals will appear shortly
        </div>
      ) : (
        <>
          <div className="flex items-center gap-3">
            {items.map((item) => (
              <div key={item.label} className="text-center flex-1">
                <div className={clsx("text-lg font-bold", item.cls)}>
                  {item.count}
                </div>
                <div className="text-[10px] text-slate-500 mt-0.5">
                  {item.label}
                </div>
              </div>
            ))}
          </div>

          {/* Stacked bar */}
          <div className="flex h-2 rounded-full overflow-hidden mt-3 bg-slate-700">
            {items.map((item) => {
              const pct =
                data.total_stocks > 0
                  ? (item.count / data.total_stocks) * 100
                  : 0;
              if (pct === 0) return null;
              return (
                <div
                  key={item.label}
                  className={clsx(
                    "h-full",
                    item.label === "Strong Buy" && "bg-green-500",
                    item.label === "Buy" && "bg-emerald-500",
                    item.label === "Hold" && "bg-yellow-500",
                    item.label === "Sell" && "bg-orange-500",
                    item.label === "Strong Sell" && "bg-red-500",
                  )}
                  style={{ width: `${pct}%` }}
                />
              );
            })}
          </div>
          <div className="text-[11px] text-slate-500 mt-1 text-right">
            {data.total_stocks} stocks analyzed
          </div>
        </>
      )}
    </div>
  );
}
