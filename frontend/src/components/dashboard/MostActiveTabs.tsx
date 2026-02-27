import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { clsx } from "clsx";
import type { StockPrice } from "../../types/index.ts";
import { formatNumber, formatPct, formatCompact, colorBySign } from "../../lib/format.ts";

const TABS = [
  { key: "gainers", label: "Gainers", sort: (a: StockPrice, b: StockPrice) => b.change_pct - a.change_pct },
  { key: "losers", label: "Losers", sort: (a: StockPrice, b: StockPrice) => a.change_pct - b.change_pct },
  { key: "volume", label: "Volume", sort: (a: StockPrice, b: StockPrice) => b.volume - a.volume },
  { key: "turnover", label: "Turnover", sort: (a: StockPrice, b: StockPrice) => b.value - a.value },
] as const;

interface Props {
  prices: StockPrice[];
  limit?: number;
}

export default function MostActiveTabs({ prices, limit = 10 }: Props) {
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState("gainers");

  const tabDef = TABS.find((t) => t.key === activeTab) ?? TABS[0];

  const items = useMemo(() => {
    const filtered = prices.filter((p) => p.ltp > 0 && p.trade_count > 0);
    return [...filtered].sort(tabDef.sort).slice(0, limit);
  }, [prices, limit, tabDef]);

  const isVolTab = activeTab === "volume" || activeTab === "turnover";

  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg overflow-hidden">
      {/* Tab header */}
      <div className="flex items-center border-b border-[var(--border)]">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={clsx(
              "flex-1 px-2 py-2 text-[10px] font-semibold uppercase tracking-wider transition-colors",
              activeTab === tab.key
                ? "text-blue-500 border-b-2 border-blue-500 bg-[var(--surface-active)]"
                : "text-[var(--text-dim)] hover:text-[var(--text-muted)]",
            )}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Table */}
      {items.length === 0 ? (
        <div className="py-8 text-center text-[10px] text-[var(--text-dim)]">
          No data available
        </div>
      ) : (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider border-b border-[var(--border)]">
              <th className="text-left px-3 py-1.5">Symbol</th>
              <th className="text-right px-3 py-1.5">LTP</th>
              <th className="text-right px-3 py-1.5">Chg%</th>
              {isVolTab && (
                <th className="text-right px-3 py-1.5">
                  {activeTab === "volume" ? "Vol" : "Turn"}
                </th>
              )}
            </tr>
          </thead>
          <tbody>
            {items.map((s) => (
              <tr
                key={s.symbol}
                onClick={() => navigate(`/stock/${s.symbol}`)}
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
                {isVolTab && (
                  <td className="px-3 py-1.5 text-right text-[var(--text-muted)] tabular-nums">
                    {activeTab === "volume"
                      ? formatCompact(s.volume)
                      : formatCompact(s.value)}
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
