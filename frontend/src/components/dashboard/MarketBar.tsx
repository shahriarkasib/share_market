import { clsx } from "clsx";
import { Loader2, RefreshCw } from "lucide-react";
import type { MarketSummary, SignalsSummary } from "../../types/index.ts";
import {
  formatNumber,
  formatChange,
  formatPct,
  formatCompact,
  colorBySign,
} from "../../lib/format.ts";

interface Props {
  market: MarketSummary | null;
  signals: SignalsSummary | null;
  secondsToRefresh: number;
  isRefreshing: boolean;
  onRefresh: () => void;
}

/**
 * One compact horizontal bar that replaces MarketSummary + SignalsSummaryBar
 * + RefreshTimer. Shows DSEX, advances/declines, volume, trades, market
 * status, signal counts, and the countdown to the next auto-refresh.
 */
export default function MarketBar({
  market,
  signals,
  secondsToRefresh,
  isRefreshing,
  onRefresh,
}: Props) {
  if (!market) {
    return (
      <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg px-4 py-2.5 flex items-center justify-center gap-2">
        <Loader2 className="h-3.5 w-3.5 animate-spin text-[var(--text-muted)]" />
        <span className="text-xs text-[var(--text-muted)]">Loading market data...</span>
      </div>
    );
  }

  const buyCount =
    (signals?.strong_buy_count ?? 0) + (signals?.buy_count ?? 0);
  const sellCount =
    (signals?.sell_count ?? 0) + (signals?.strong_sell_count ?? 0);
  const holdCount = signals?.hold_count ?? 0;

  const minutes = Math.floor(secondsToRefresh / 60);
  const seconds = secondsToRefresh % 60;
  const timerText = `${minutes}:${seconds.toString().padStart(2, "0")}`;

  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg px-4 py-2 flex items-center gap-4 overflow-x-auto text-xs whitespace-nowrap">
      {/* DSEX index */}
      <div className="flex items-center gap-2 font-medium shrink-0">
        <span className="text-[var(--text-muted)]">DSEX</span>
        <span className="text-[var(--text)] tabular-nums">
          {formatNumber(market.dsex_index)}
        </span>
        <span
          className={clsx(
            "tabular-nums font-medium",
            colorBySign(market.dsex_change),
          )}
        >
          {market.dsex_change > 0 ? "\u25B2" : market.dsex_change < 0 ? "\u25BC" : ""}
          {formatChange(market.dsex_change)} ({formatPct(market.dsex_change_pct)})
        </span>
      </div>

      <Separator />

      {/* Advances / Declines */}
      <div className="flex items-center gap-1.5 shrink-0">
        <span className="text-green-400 tabular-nums">
          {"\u2191"}{market.advances}
        </span>
        <span className="text-red-400 tabular-nums">
          {"\u2193"}{market.declines}
        </span>
      </div>

      <Separator />

      {/* Volume */}
      <div className="flex items-center gap-1 shrink-0">
        <span className="text-[var(--text-muted)]">Vol:</span>
        <span className="text-[var(--text)] tabular-nums">
          {formatCompact(market.total_volume)}
        </span>
      </div>

      {/* Trades */}
      <div className="flex items-center gap-1 shrink-0">
        <span className="text-[var(--text-muted)]">Trades:</span>
        <span className="text-[var(--text)] tabular-nums">
          {formatCompact(market.total_trade)}
        </span>
      </div>

      <Separator />

      {/* Market status */}
      <div className="flex items-center gap-1.5 shrink-0">
        <span
          className={clsx(
            "h-1.5 w-1.5 rounded-full",
            market.market_status.toLowerCase().includes("open")
              ? "bg-green-400"
              : "bg-[var(--text-dim)]",
          )}
        />
        <span className="text-[var(--text-muted)] uppercase tracking-wide">
          {market.market_status}
        </span>
      </div>

      <Separator />

      {/* Signal counts */}
      {signals && (
        <div className="flex items-center gap-2 shrink-0">
          <span className="text-green-400 tabular-nums">{buyCount} Buy</span>
          <span className="text-red-400 tabular-nums">{sellCount} Sell</span>
          <span className="text-[var(--text-muted)] tabular-nums">{holdCount} Hold</span>
        </div>
      )}

      {/* Spacer */}
      <div className="flex-1 min-w-4" />

      {/* Refresh timer */}
      <button
        type="button"
        onClick={onRefresh}
        disabled={isRefreshing}
        className="flex items-center gap-1.5 shrink-0 text-[var(--text-muted)] hover:text-[var(--text)] transition-colors disabled:opacity-50"
      >
        <span className="tabular-nums">Next: {timerText}</span>
        {isRefreshing ? (
          <Loader2 className="h-3 w-3 animate-spin" />
        ) : (
          <RefreshCw className="h-3 w-3" />
        )}
      </button>
    </div>
  );
}

function Separator() {
  return <div className="h-3 w-px bg-[var(--border)]" />;
}
