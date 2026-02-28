import { useNavigate } from "react-router-dom";
import { clsx } from "clsx";
import { Loader2 } from "lucide-react";
import type { StockSignal } from "../../types/index.ts";
import { formatNumber, formatPct, colorBySign } from "../../lib/format.ts";

interface Props {
  signals: StockSignal[];
  type: "buy" | "sell";
  title: string;
  isComputing?: boolean;
}

/**
 * Clean table component showing buy or sell signals.
 * Rows are clickable and navigate to /stock/:symbol.
 */
export default function SignalsTable({
  signals,
  type,
  title,
  isComputing = false,
}: Props) {
  const navigate = useNavigate();

  const isBuy = type === "buy";
  const accentBorder = isBuy ? "border-l-green-500" : "border-l-red-500";
  const accentText = isBuy ? "text-green-400" : "text-red-400";

  return (
    <section
      className={clsx(
        "bg-[var(--surface)] border border-[var(--border)] rounded-lg border-l-2 overflow-hidden",
        accentBorder,
      )}
    >
      {/* Header */}
      <div className="px-4 py-2.5 border-b border-[var(--border)] flex items-center justify-between">
        <h2 className={clsx("text-xs font-semibold uppercase tracking-wider", accentText)}>
          {title}
        </h2>
        <span className="text-[10px] text-[var(--text-dim)]">
          {signals.length} signal{signals.length !== 1 ? "s" : ""}
        </span>
      </div>

      {/* Computing state */}
      {isComputing ? (
        <div className="flex items-center justify-center gap-2 py-10">
          <Loader2 className="h-4 w-4 animate-spin text-blue-400" />
          <span className="text-xs text-[var(--text-muted)]">
            Loading analysis data...
          </span>
        </div>
      ) : signals.length === 0 ? (
        /* Empty state */
        <div className="py-10 text-center text-xs text-[var(--text-dim)]">
          No {type} signals at the moment
        </div>
      ) : (
        /* Table + expandable rows */
        <div className="divide-y divide-[var(--border)]">
          {signals.map((s, idx) => (
            <SignalRow
              key={s.symbol}
              signal={s}
              index={idx}
              isBuy={isBuy}
              onNavigate={() => navigate(`/stock/${s.symbol}`)}
            />
          ))}
        </div>
      )}
    </section>
  );
}

function SignalRow({
  signal: s,
  index,
  isBuy,
  onNavigate,
}: {
  signal: StockSignal;
  index: number;
  isBuy: boolean;
  onNavigate: () => void;
}) {
  return (
    <div
      onClick={onNavigate}
      className="hover:bg-[var(--hover)] cursor-pointer transition-colors px-4 py-2.5"
    >
      {/* Top line: symbol, price, change, T+2, expected return */}
      <div className="flex items-center gap-3 mb-1.5">
        <span className="text-[10px] text-[var(--text-dim)] tabular-nums w-4">
          {index + 1}
        </span>
        <span className="font-medium text-sm text-[var(--text)] w-24 shrink-0">
          {s.symbol}
        </span>
        <span className="text-xs text-[var(--text)] tabular-nums">
          {formatNumber(s.ltp)}
        </span>
        <span
          className={clsx(
            "text-xs font-medium tabular-nums",
            colorBySign(s.change_pct),
          )}
        >
          {formatPct(s.change_pct)}
        </span>

        <div className="ml-auto flex items-center gap-3">
          {/* T+2 badge */}
          {s.t2_safe != null && (
            <span
              className={clsx(
                "inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium",
                s.t2_safe
                  ? "bg-green-500/15 text-green-400 border border-green-500/30"
                  : "bg-red-500/15 text-red-400 border border-red-500/30",
              )}
            >
              T+2 {s.t2_safe ? "Safe" : "Risky"}
            </span>
          )}

          {/* Expected return */}
          {s.expected_return_pct != null && (
            <span
              className={clsx(
                "text-xs font-bold tabular-nums",
                colorBySign(s.expected_return_pct),
              )}
            >
              {s.expected_return_pct > 0 ? "+" : ""}
              {s.expected_return_pct.toFixed(1)}%
            </span>
          )}

          {/* Hold days */}
          {s.hold_days != null && (
            <span className="text-[10px] text-[var(--text-muted)] tabular-nums">
              {s.hold_days}d
            </span>
          )}

          {/* Risk score */}
          {s.risk_score != null && (
            <span
              className={clsx(
                "text-[10px] tabular-nums font-medium",
                s.risk_score < 30
                  ? "text-green-400"
                  : s.risk_score <= 60
                    ? "text-yellow-400"
                    : "text-red-400",
              )}
            >
              Risk {s.risk_score.toFixed(0)}
            </span>
          )}
        </div>
      </div>

      {/* Bottom line: entry + exit strategies */}
      <div className="flex items-start gap-3 ml-7">
        {s.entry_strategy && (
          <div className="flex items-start gap-1.5 flex-1 min-w-0">
            <span className={clsx(
              "text-[10px] font-semibold uppercase shrink-0 mt-px",
              isBuy ? "text-emerald-500" : "text-red-500",
            )}>
              Entry:
            </span>
            <span className="text-[11px] text-[var(--text-muted)] truncate">
              {s.entry_strategy}
            </span>
          </div>
        )}
        {s.exit_strategy && (
          <div className="flex items-start gap-1.5 flex-1 min-w-0">
            <span className="text-[10px] font-semibold uppercase shrink-0 mt-px text-amber-500">
              Exit:
            </span>
            <span className="text-[11px] text-[var(--text-muted)] truncate">
              {s.exit_strategy}
            </span>
          </div>
        )}
      </div>

      {/* Target / Stop Loss / R:R as tiny pills */}
      <div className="flex items-center gap-2 ml-7 mt-1">
        <span className="text-[10px] text-[var(--text-dim)]">
          Target <span className="text-green-500 tabular-nums">{formatNumber(s.target_price)}</span>
        </span>
        <span className="text-[10px] text-[var(--text-dim)]">
          Stop <span className="text-red-500 tabular-nums">{formatNumber(s.stop_loss)}</span>
        </span>
        <span className="text-[10px] text-[var(--text-dim)]">
          R:R <span className="text-blue-400 tabular-nums">{s.risk_reward_ratio.toFixed(1)}</span>
        </span>
        <span className="text-[10px] text-[var(--text-dim)]">
          Conf <span className="text-[var(--text-muted)] tabular-nums">{(s.confidence * 100).toFixed(0)}%</span>
        </span>
      </div>
    </div>
  );
}
