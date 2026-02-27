import { RefreshCw, Clock } from "lucide-react";
import { clsx } from "clsx";

interface Props {
  secondsToRefresh: number;
  isRefreshing: boolean;
  onRefresh: () => void;
  lastUpdated: Date | null;
}

export default function RefreshTimer({
  secondsToRefresh,
  isRefreshing,
  onRefresh,
  lastUpdated,
}: Props) {
  const minutes = Math.floor(secondsToRefresh / 60);
  const seconds = secondsToRefresh % 60;
  const timeStr = `${minutes}:${seconds.toString().padStart(2, "0")}`;

  return (
    <div className="flex items-center gap-3">
      {lastUpdated && (
        <span className="text-xs text-slate-500 flex items-center gap-1">
          <Clock className="h-3 w-3" />
          {lastUpdated.toLocaleTimeString()}
        </span>
      )}

      <span className="text-xs text-slate-400 tabular-nums">
        Next refresh in {timeStr}
      </span>

      <button
        type="button"
        onClick={onRefresh}
        disabled={isRefreshing}
        className={clsx(
          "flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition-all",
          "bg-blue-600/20 text-blue-400 border border-blue-500/30",
          "hover:bg-blue-600/30 hover:border-blue-500/50",
          "disabled:opacity-50 disabled:cursor-not-allowed",
        )}
      >
        <RefreshCw
          className={clsx("h-3 w-3", isRefreshing && "animate-spin")}
        />
        {isRefreshing ? "Refreshing..." : "Refresh Now"}
      </button>
    </div>
  );
}
