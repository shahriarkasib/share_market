import { Activity } from "lucide-react";

interface MarketBreadthProps {
  advances: number;
  declines: number;
  unchanged: number;
}

export default function MarketBreadth({ advances, declines, unchanged }: MarketBreadthProps) {
  const total = advances + declines + unchanged;
  if (total === 0) return null;

  const advPct = (advances / total) * 100;
  const decPct = (declines / total) * 100;
  const uncPct = (unchanged / total) * 100;

  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg p-3">
      {/* Header */}
      <div className="flex items-center gap-1.5 mb-2">
        <Activity className="h-3 w-3 text-blue-500" />
        <span className="text-[10px] font-semibold uppercase tracking-wider text-[var(--text-muted)]">
          Market Breadth
        </span>
        <span className="text-[10px] text-[var(--text-dim)] ml-auto tabular-nums">
          {total} stocks
        </span>
      </div>

      {/* Stacked horizontal bar */}
      <div className="flex h-4 rounded overflow-hidden">
        {advPct > 0 && (
          <div
            className="h-full"
            style={{ width: `${advPct}%`, backgroundColor: "#22c55e" }}
          />
        )}
        {uncPct > 0 && (
          <div
            className="h-full"
            style={{ width: `${uncPct}%`, backgroundColor: "#6b7280" }}
          />
        )}
        {decPct > 0 && (
          <div
            className="h-full"
            style={{ width: `${decPct}%`, backgroundColor: "#ef4444" }}
          />
        )}
      </div>

      {/* Labels below */}
      <div className="flex justify-between mt-1.5 text-[10px] tabular-nums">
        <span className="text-green-400">
          {advances} ({advPct.toFixed(1)}%)
        </span>
        <span className="text-gray-400">
          {unchanged} ({uncPct.toFixed(1)}%)
        </span>
        <span className="text-red-400">
          {declines} ({decPct.toFixed(1)}%)
        </span>
      </div>
    </div>
  );
}
