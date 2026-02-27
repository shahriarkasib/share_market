import { useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { clsx } from "clsx";
import type { StockPrice } from "../../types/index.ts";
import { formatNumber, formatPct, colorBySign } from "../../lib/format.ts";

interface Props {
  prices: StockPrice[];
  limit?: number;
}

/**
 * Two side-by-side sections showing Top Gainers and Top Losers.
 * Super compact -- three columns only: Symbol | LTP | Chg%.
 */
export default function TopMovers({ prices, limit = 10 }: Props) {
  const navigate = useNavigate();

  const gainers = useMemo(() => {
    return [...prices].sort((a, b) => b.change_pct - a.change_pct).slice(0, limit);
  }, [prices, limit]);

  const losers = useMemo(() => {
    return [...prices].sort((a, b) => a.change_pct - b.change_pct).slice(0, limit);
  }, [prices, limit]);

  return (
    <div className="grid grid-cols-1 gap-4">
      <MoverSection
        title="Top Gainers"
        accent="text-green-400"
        items={gainers}
        onRowClick={(sym) => navigate(`/stock/${sym}`)}
      />
      <MoverSection
        title="Top Losers"
        accent="text-red-400"
        items={losers}
        onRowClick={(sym) => navigate(`/stock/${sym}`)}
      />
    </div>
  );
}

/* ------------------------------------------------------------------ */

function MoverSection({
  title,
  accent,
  items,
  onRowClick,
}: {
  title: string;
  accent: string;
  items: StockPrice[];
  onRowClick: (symbol: string) => void;
}) {
  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg overflow-hidden">
      <div className="px-3 py-2 border-b border-[var(--border)]">
        <h3 className={clsx("text-[10px] font-semibold uppercase tracking-wider", accent)}>
          {title}
        </h3>
      </div>

      {items.length === 0 ? (
        <div className="py-6 text-center text-[10px] text-[var(--text-dim)]">
          No data available
        </div>
      ) : (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider border-b border-[var(--border)]">
              <th className="text-left px-3 py-1.5">Symbol</th>
              <th className="text-right px-3 py-1.5">LTP</th>
              <th className="text-right px-3 py-1.5">Chg%</th>
            </tr>
          </thead>
          <tbody>
            {items.map((s) => (
              <tr
                key={s.symbol}
                onClick={() => onRowClick(s.symbol)}
                className="border-b border-[var(--border)] hover:bg-[var(--hover)] cursor-pointer transition-colors"
              >
                <td className="px-3 py-1.5 font-medium text-[var(--text)] truncate max-w-[100px]">
                  {s.symbol}
                </td>
                <td className="px-3 py-1.5 text-right text-[var(--text-muted)] tabular-nums">
                  {formatNumber(s.ltp)}
                </td>
                <td
                  className={clsx(
                    "px-3 py-1.5 text-right font-medium tabular-nums",
                    colorBySign(s.change_pct),
                  )}
                >
                  {formatPct(s.change_pct)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
