/**
 * Heatmap page -- wrapper around MarketHeatmap with size-by selector.
 *
 * Fetches heatmap data from the backend and lets users toggle between
 * Turnover, Volume, and Trades as the sizing metric.
 */

import { useEffect, useState } from "react";
import { Grid3X3, Loader2, AlertCircle } from "lucide-react";
import { clsx } from "clsx";
import type { HeatmapSector } from "../types/index.ts";
import { fetchHeatmapData } from "../api/client.ts";
import MarketHeatmap from "../components/heatmap/MarketHeatmap.tsx";

type SizeBy = "turnover" | "volume" | "trades";

const sizeOptions: { value: SizeBy; label: string }[] = [
  { value: "turnover", label: "Turnover" },
  { value: "volume", label: "Volume" },
  { value: "trades", label: "Trades" },
];

export default function Heatmap() {
  const [sizeBy, setSizeBy] = useState<SizeBy>("turnover");
  const [data, setData] = useState<HeatmapSector[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    fetchHeatmapData(sizeBy)
      .then((result) => {
        if (!cancelled) setData(result);
      })
      .catch((err) => {
        if (!cancelled)
          setError(
            err instanceof Error ? err.message : "Failed to load heatmap data",
          );
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [sizeBy]);

  return (
    <div className="space-y-4">
      {/* Header row: title + size-by toggle */}
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <h1 className="text-sm font-semibold text-[var(--text)] flex items-center gap-2">
          <Grid3X3 className="h-4 w-4 text-blue-500" />
          Market Heatmap
        </h1>

        <div className="flex items-center rounded-md border border-[var(--border)] overflow-hidden">
          {sizeOptions.map((opt) => (
            <button
              key={opt.value}
              type="button"
              onClick={() => setSizeBy(opt.value)}
              className={clsx(
                "px-3 py-1.5 text-xs font-medium transition-colors",
                sizeBy === opt.value
                  ? "bg-blue-600 text-white"
                  : "bg-[var(--surface)] text-[var(--text-muted)] hover:bg-[var(--hover)] hover:text-[var(--text)]",
              )}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {/* Error state */}
      {error && (
        <div className="bg-red-900/20 border border-red-800/40 rounded-lg px-4 py-2.5 text-xs text-red-400 flex items-center gap-2">
          <AlertCircle className="h-3.5 w-3.5 shrink-0" />
          {error}
        </div>
      )}

      {/* Loading state */}
      {loading ? (
        <div className="flex items-center justify-center gap-2 py-24">
          <Loader2 className="h-5 w-5 animate-spin text-blue-500" />
          <span className="text-xs text-[var(--text-muted)]">
            Loading heatmap...
          </span>
        </div>
      ) : (
        !error && <MarketHeatmap data={data} />
      )}
    </div>
  );
}
