import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { TrendingUp, TrendingDown, Pause, RefreshCw } from "lucide-react";
import { fetchSMCScreener, type SMCScreenerCandidate } from "../api/client";

type Filter = "all" | "buy" | "wait" | "avoid";

const ACTION_COLOR: Record<string, string> = {
  green: "text-emerald-500 border-emerald-500/40 bg-emerald-500/10",
  yellow: "text-amber-500 border-amber-500/40 bg-amber-500/10",
  orange: "text-orange-500 border-orange-500/40 bg-orange-500/10",
  red: "text-red-500 border-red-500/40 bg-red-500/10",
  gray: "text-gray-500 border-gray-500/40 bg-gray-500/10",
};

function actionColor(action: string): string {
  const a = action.toUpperCase();
  if (a.startsWith("BUY")) return ACTION_COLOR.green;
  if (a.includes("AVOID") || a.includes("EXIT")) return ACTION_COLOR.red;
  if (a.includes("TAKE PROFIT") || a.includes("PARABOLIC")) return ACTION_COLOR.orange;
  if (a.includes("WAIT") || a.includes("CLOSE ABOVE")) return ACTION_COLOR.yellow;
  return ACTION_COLOR.gray;
}

function actionIcon(action: string) {
  const a = action.toUpperCase();
  if (a.startsWith("BUY")) return <TrendingUp className="h-3.5 w-3.5" />;
  if (a.includes("AVOID") || a.includes("EXIT")) return <TrendingDown className="h-3.5 w-3.5" />;
  return <Pause className="h-3.5 w-3.5" />;
}

export default function Signals() {
  const [data, setData] = useState<SMCScreenerCandidate[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<Filter>("buy");
  const [minConfidence, setMinConfidence] = useState<"HIGH" | "MEDIUM" | "LOW">("MEDIUM");

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const rows = await fetchSMCScreener(minConfidence);
      setData(rows);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, [minConfidence]);

  const filtered = useMemo(() => {
    if (filter === "all") return data;
    return data.filter((c) => {
      const a = c.action.toUpperCase();
      if (filter === "buy") return a.startsWith("BUY");
      if (filter === "avoid") return a.includes("AVOID") || a.includes("EXIT");
      if (filter === "wait")
        return a.includes("WAIT") || a.includes("TAKE PROFIT") || a.includes("CLOSE ABOVE");
      return true;
    });
  }, [data, filter]);

  return (
    <div className="max-w-[1440px] mx-auto px-3 sm:px-4 lg:px-8 py-6">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <div>
          <h1 className="text-xl font-bold text-[var(--text)] flex items-center gap-2">
            SMC Buy / Sell Signals
          </h1>
          <p className="text-xs text-[var(--text-muted)] mt-0.5">
            Live FVG-based recommendations across all DSE stocks. Auto-derived from structure +
            FVG quality + premium/discount zone.
          </p>
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded text-xs border border-[var(--border)] hover:bg-[var(--hover)] disabled:opacity-50"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      <div className="flex flex-wrap items-center gap-2 mb-4">
        <span className="text-xs text-[var(--text-muted)]">Action:</span>
        {(["buy", "wait", "avoid", "all"] as const).map((f) => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`px-2.5 py-1 rounded text-xs border transition ${
              filter === f
                ? "bg-[var(--surface-active)] border-[var(--border)] text-[var(--text)]"
                : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]"
            }`}
          >
            {f.toUpperCase()}
          </button>
        ))}

        <span className="text-xs text-[var(--text-muted)] ml-4">Min confidence:</span>
        {(["HIGH", "MEDIUM", "LOW"] as const).map((c) => (
          <button
            key={c}
            onClick={() => setMinConfidence(c)}
            className={`px-2.5 py-1 rounded text-xs border transition ${
              minConfidence === c
                ? "bg-[var(--surface-active)] border-[var(--border)] text-[var(--text)]"
                : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]"
            }`}
          >
            {c}
          </button>
        ))}
      </div>

      {error && (
        <div className="rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-500 mb-3">
          {error}
        </div>
      )}

      <div className="rounded-lg border border-[var(--border)] overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-[var(--surface-active)] text-[var(--text-muted)] text-xs">
            <tr>
              <th className="text-left px-3 py-2 font-medium">Symbol</th>
              <th className="text-right px-3 py-2 font-medium">Price</th>
              <th className="text-left px-3 py-2 font-medium">Bias / Conf</th>
              <th className="text-left px-3 py-2 font-medium">Action</th>
              <th className="text-right px-3 py-2 font-medium">Entry</th>
              <th className="text-right px-3 py-2 font-medium">Stop</th>
              <th className="text-right px-3 py-2 font-medium">T1</th>
              <th className="text-right px-3 py-2 font-medium">T2</th>
              <th className="text-right px-3 py-2 font-medium">R:R</th>
              <th className="text-left px-3 py-2 font-medium">Why</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 && !loading && (
              <tr>
                <td colSpan={10} className="text-center py-8 text-[var(--text-muted)] text-xs">
                  No signals match this filter. Lower confidence threshold to see more.
                </td>
              </tr>
            )}
            {filtered.map((c) => (
              <tr
                key={c.symbol}
                className="border-t border-[var(--border)] hover:bg-[var(--hover)]"
              >
                <td className="px-3 py-2">
                  <Link
                    to={`/smc-chart/${c.symbol}`}
                    className="font-mono font-bold text-blue-500 hover:underline"
                  >
                    {c.symbol}
                  </Link>
                </td>
                <td className="px-3 py-2 text-right font-mono">৳{c.price.toFixed(1)}</td>
                <td className="px-3 py-2 text-xs">
                  <span
                    className={
                      c.bias === "BULLISH"
                        ? "text-emerald-500"
                        : c.bias === "BEARISH"
                        ? "text-red-500"
                        : "text-[var(--text-muted)]"
                    }
                  >
                    {c.bias}
                  </span>
                  <span className="text-[var(--text-muted)] ml-1">/ {c.confidence}</span>
                </td>
                <td className="px-3 py-2">
                  <span
                    className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs border ${actionColor(
                      c.action,
                    )}`}
                  >
                    {actionIcon(c.action)}
                    {c.action}
                  </span>
                </td>
                <td className="px-3 py-2 text-right font-mono text-xs">
                  {c.entry !== null ? `৳${c.entry.toFixed(1)}` : "—"}
                </td>
                <td className="px-3 py-2 text-right font-mono text-xs text-red-500/80">
                  {c.stop_loss !== null ? `৳${c.stop_loss.toFixed(1)}` : "—"}
                </td>
                <td className="px-3 py-2 text-right font-mono text-xs text-emerald-500/80">
                  {c.target1 !== null ? `৳${c.target1.toFixed(1)}` : "—"}
                </td>
                <td className="px-3 py-2 text-right font-mono text-xs text-emerald-500/80">
                  {c.target2 !== null ? `৳${c.target2.toFixed(1)}` : "—"}
                </td>
                <td className="px-3 py-2 text-right font-mono text-xs">
                  {c.risk_reward !== null ? `1:${c.risk_reward.toFixed(1)}` : "—"}
                </td>
                <td className="px-3 py-2 text-xs text-[var(--text-muted)] max-w-md truncate">
                  {c.summary}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
