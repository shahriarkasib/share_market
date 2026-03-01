import { useState, useEffect, useMemo } from "react";
import { Link } from "react-router-dom";
import { clsx } from "clsx";
import {
  Trophy,
  XCircle,
  Clock,
  Target,
  Filter,
  Search,
  ChevronDown,
  ChevronUp,
  BarChart3,
  Brain,
  Scale,
  Loader2,
} from "lucide-react";
import {
  fetchPredictionTracker,
  fetchAccuracyComparison,
} from "../api/client.ts";
import type { PredictionEntry, AccuracyData } from "../types/index.ts";

/* ── Outcome badge ── */

const OUTCOME_CFG: Record<
  string,
  { color: string; bg: string; icon: typeof Trophy }
> = {
  CORRECT: { color: "text-green-400", bg: "bg-green-500/15", icon: Trophy },
  WRONG: { color: "text-red-400", bg: "bg-red-500/15", icon: XCircle },
  PARTIAL: { color: "text-amber-400", bg: "bg-amber-500/15", icon: Target },
  PENDING: { color: "text-blue-400", bg: "bg-blue-500/15", icon: Clock },
};

function OutcomeBadge({ outcome }: { outcome: string }) {
  const cfg = OUTCOME_CFG[outcome] ?? OUTCOME_CFG.PENDING;
  const Icon = cfg.icon;
  return (
    <span
      className={clsx(
        "inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold",
        cfg.color,
        cfg.bg,
      )}
    >
      <Icon className="h-3 w-3" />
      {outcome}
    </span>
  );
}

/* ── Source badge ── */

const SOURCE_CFG: Record<string, { color: string; bg: string; icon: typeof BarChart3; label: string }> = {
  algo: { color: "text-blue-400", bg: "bg-blue-500/10", icon: BarChart3, label: "Algo" },
  llm: { color: "text-purple-400", bg: "bg-purple-500/10", icon: Brain, label: "LLM" },
  judge: { color: "text-amber-400", bg: "bg-amber-500/10", icon: Scale, label: "Judge" },
};

function SourceBadge({ source }: { source: string }) {
  const cfg = SOURCE_CFG[source] ?? SOURCE_CFG.algo;
  const Icon = cfg.icon;
  return (
    <span className={clsx("inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium", cfg.color, cfg.bg)}>
      <Icon className="h-3 w-3" />
      {cfg.label}
    </span>
  );
}

/* ── Action color helper ── */

function actionColor(action: string): string {
  if (action.includes("BUY")) return "text-green-400";
  if (action === "HOLD/WAIT") return "text-blue-400";
  if (action.includes("AVOID") || action.includes("SELL")) return "text-red-400";
  return "text-[var(--text-muted)]";
}

/* ── Accuracy Card ── */

function AccuracyCard({ data, source }: { data: AccuracyData | null; source: string }) {
  const cfg = SOURCE_CFG[source] ?? SOURCE_CFG.algo;
  const Icon = cfg.icon;
  const acc = data?.accuracy_pct;
  const accColor = acc == null ? "text-[var(--text-dim)]" : acc >= 60 ? "text-green-400" : acc >= 40 ? "text-amber-400" : "text-red-400";

  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg p-3">
      <div className="flex items-center gap-2 mb-2">
        <Icon className={clsx("h-4 w-4", cfg.color)} />
        <span className={clsx("text-xs font-semibold", cfg.color)}>{cfg.label}</span>
      </div>

      <div className={clsx("text-2xl font-bold", accColor)}>
        {acc != null ? `${acc.toFixed(1)}%` : "—"}
      </div>
      <div className="text-[10px] text-[var(--text-dim)]">Accuracy</div>

      {data && (
        <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1 text-[10px]">
          <div>
            <span className="text-[var(--text-dim)]">Total: </span>
            <span className="text-[var(--text-muted)]">{data.total_predictions}</span>
          </div>
          <div>
            <span className="text-[var(--text-dim)]">Avg Return: </span>
            <span className={data.avg_return_pct && data.avg_return_pct > 0 ? "text-green-400" : "text-red-400"}>
              {data.avg_return_pct != null ? `${data.avg_return_pct > 0 ? "+" : ""}${data.avg_return_pct.toFixed(2)}%` : "—"}
            </span>
          </div>
          <div>
            <span className="text-green-400">{data.correct}</span>
            <span className="text-[var(--text-dim)]"> correct</span>
          </div>
          <div>
            <span className="text-red-400">{data.wrong}</span>
            <span className="text-[var(--text-dim)]"> wrong</span>
          </div>
          <div>
            <span className="text-[var(--text-dim)]">BUY acc: </span>
            <span className="text-[var(--text-muted)]">{data.buy_accuracy_pct != null ? `${data.buy_accuracy_pct.toFixed(0)}%` : "—"}</span>
          </div>
          <div>
            <span className="text-[var(--text-dim)]">T1 hit: </span>
            <span className="text-[var(--text-muted)]">{data.t1_hit_rate != null ? `${data.t1_hit_rate.toFixed(0)}%` : "—"}</span>
          </div>
        </div>
      )}
    </div>
  );
}

/* ── Prediction Row ── */

function PredictionRow({ p, expanded, onToggle }: { p: PredictionEntry; expanded: boolean; onToggle: () => void }) {
  return (
    <>
      <tr
        className="border-b border-[var(--border)] hover:bg-[var(--hover)] cursor-pointer transition-colors"
        onClick={onToggle}
      >
        <td className="px-2 py-1.5 text-xs">
          <Link to={`/stock/${p.symbol}`} className="text-blue-400 hover:underline font-medium" onClick={(e) => e.stopPropagation()}>
            {p.symbol}
          </Link>
          {p.sector && <div className="text-[9px] text-[var(--text-dim)]">{p.sector}</div>}
        </td>
        <td className="px-2 py-1.5"><SourceBadge source={p.source} /></td>
        <td className={clsx("px-2 py-1.5 text-[11px] font-medium", actionColor(p.action))}>{p.action}</td>
        <td className="px-2 py-1.5 text-[11px] text-[var(--text-muted)]">{p.wait_days || "—"}</td>
        <td className="px-2 py-1.5 text-[11px] text-[var(--text-muted)]">{p.ltp_at_prediction?.toFixed(1) ?? "—"}</td>
        <td className="px-2 py-1.5"><OutcomeBadge outcome={p.outcome} /></td>
        <td className="px-2 py-1.5 text-[11px]">
          {p.outcome !== "PENDING" && (
            <>
              {p.transition_days != null && <span className="text-[var(--text-muted)]">{p.transition_days}d</span>}
              {p.t1_hit_days != null && <span className="text-green-400 ml-1">T1:{p.t1_hit_days}d</span>}
              {p.sl_hit_days != null && <span className="text-red-400 ml-1">SL:{p.sl_hit_days}d</span>}
            </>
          )}
        </td>
        <td className="px-2 py-1.5 text-[11px]">
          {p.final_return_pct != null ? (
            <span className={p.final_return_pct >= 0 ? "text-green-400" : "text-red-400"}>
              {p.final_return_pct > 0 ? "+" : ""}{p.final_return_pct.toFixed(1)}%
            </span>
          ) : p.max_gain_pct != null ? (
            <span className="text-[var(--text-dim)]">
              +{p.max_gain_pct.toFixed(1)}% / {p.max_loss_pct?.toFixed(1)}%
            </span>
          ) : "—"}
        </td>
        <td className="px-1 py-1.5 text-[var(--text-dim)]">
          {expanded ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
        </td>
      </tr>
      {expanded && (
        <tr className="border-b border-[var(--border)] bg-[var(--surface)]">
          <td colSpan={9} className="px-3 py-2">
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-[10px]">
              <div>
                <span className="text-[var(--text-dim)]">Entry: </span>
                <span className="text-[var(--text-muted)]">{p.entry_low?.toFixed(1)} - {p.entry_high?.toFixed(1)}</span>
              </div>
              <div>
                <span className="text-[var(--text-dim)]">SL: </span>
                <span className="text-red-400">{p.sl?.toFixed(1)}</span>
              </div>
              <div>
                <span className="text-[var(--text-dim)]">T1: </span>
                <span className="text-green-400">{p.t1?.toFixed(1)}</span>
              </div>
              <div>
                <span className="text-[var(--text-dim)]">T2: </span>
                <span className="text-green-400">{p.t2?.toFixed(1)}</span>
              </div>
              <div>
                <span className="text-[var(--text-dim)]">Score: </span>
                <span className="text-[var(--text-muted)]">{p.score?.toFixed(1)}</span>
              </div>
              <div>
                <span className="text-[var(--text-dim)]">Wait window: </span>
                <span className="text-[var(--text-muted)]">{p.wait_days_min}-{p.wait_days_max}d</span>
              </div>
              {p.transitioned_to && (
                <div className="col-span-2">
                  <span className="text-[var(--text-dim)]">Transitioned: </span>
                  <span className={actionColor(p.transitioned_to)}>{p.transitioned_to}</span>
                  {p.transition_date && <span className="text-[var(--text-dim)]"> on {p.transition_date}</span>}
                </div>
              )}
            </div>
            {p.outcome_reason && (
              <div className="mt-1.5 text-[10px] text-[var(--text-muted)] italic">{p.outcome_reason}</div>
            )}
          </td>
        </tr>
      )}
    </>
  );
}

/* ── Main Component ── */

export default function PredictionTracker() {
  const [predictions, setPredictions] = useState<PredictionEntry[]>([]);
  const [accuracy, setAccuracy] = useState<Record<string, AccuracyData | null>>({
    algo: null,
    llm: null,
    judge: null,
  });
  const [loading, setLoading] = useState(true);
  const [period, setPeriod] = useState<"7d" | "30d" | "90d">("30d");
  const [filterSource, setFilterSource] = useState<string>("");
  const [filterOutcome, setFilterOutcome] = useState<string>("");
  const [filterSymbol, setFilterSymbol] = useState<string>("");
  const [expandedId, setExpandedId] = useState<string | null>(null);

  // Load accuracy data
  useEffect(() => {
    fetchAccuracyComparison(period).then((res) => {
      const map: Record<string, AccuracyData | null> = { algo: null, llm: null, judge: null };
      for (const d of res.data) {
        map[d.source] = d;
      }
      setAccuracy(map);
    }).catch(() => {});
  }, [period]);

  // Load predictions
  useEffect(() => {
    setLoading(true);
    const params: Record<string, string | number> = { limit: 200 };
    if (filterSource) params.source = filterSource;
    if (filterOutcome) params.outcome = filterOutcome;
    if (filterSymbol) params.symbol = filterSymbol;

    fetchPredictionTracker(params)
      .then((res) => setPredictions(res.predictions))
      .catch(() => setPredictions([]))
      .finally(() => setLoading(false));
  }, [filterSource, filterOutcome, filterSymbol]);

  // Stats
  const stats = useMemo(() => {
    const total = predictions.length;
    const correct = predictions.filter((p) => p.outcome === "CORRECT").length;
    const wrong = predictions.filter((p) => p.outcome === "WRONG").length;
    const pending = predictions.filter((p) => p.outcome === "PENDING").length;
    return { total, correct, wrong, pending };
  }, [predictions]);

  return (
    <div className="space-y-3">
      {/* ── Accuracy Dashboard ── */}
      <div className="grid grid-cols-3 gap-3">
        <AccuracyCard data={accuracy.algo} source="algo" />
        <AccuracyCard data={accuracy.llm} source="llm" />
        <AccuracyCard data={accuracy.judge} source="judge" />
      </div>

      {/* Period selector */}
      <div className="flex items-center gap-2">
        <span className="text-[10px] text-[var(--text-dim)]">Period:</span>
        {(["7d", "30d", "90d"] as const).map((p) => (
          <button
            key={p}
            onClick={() => setPeriod(p)}
            className={clsx(
              "px-2 py-0.5 rounded text-[10px] font-medium transition-colors",
              period === p ? "bg-blue-500/15 text-blue-400" : "text-[var(--text-dim)] hover:text-[var(--text)]",
            )}
          >
            {p}
          </button>
        ))}
      </div>

      {/* ── Filters ── */}
      <div className="flex flex-wrap items-center gap-2">
        <Filter className="h-3.5 w-3.5 text-[var(--text-dim)]" />

        {/* Source filter */}
        <select
          value={filterSource}
          onChange={(e) => setFilterSource(e.target.value)}
          className="bg-[var(--surface)] border border-[var(--border)] rounded px-2 py-1 text-[11px] text-[var(--text)]"
        >
          <option value="">All Sources</option>
          <option value="algo">Algo</option>
          <option value="llm">LLM</option>
          <option value="judge">Judge</option>
        </select>

        {/* Outcome filter */}
        <select
          value={filterOutcome}
          onChange={(e) => setFilterOutcome(e.target.value)}
          className="bg-[var(--surface)] border border-[var(--border)] rounded px-2 py-1 text-[11px] text-[var(--text)]"
        >
          <option value="">All Outcomes</option>
          <option value="CORRECT">Correct</option>
          <option value="WRONG">Wrong</option>
          <option value="PARTIAL">Partial</option>
          <option value="PENDING">Pending</option>
        </select>

        {/* Symbol search */}
        <div className="relative">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3 w-3 text-[var(--text-dim)]" />
          <input
            type="text"
            placeholder="Symbol..."
            value={filterSymbol}
            onChange={(e) => setFilterSymbol(e.target.value.toUpperCase())}
            className="bg-[var(--surface)] border border-[var(--border)] rounded pl-6 pr-2 py-1 text-[11px] text-[var(--text)] w-24"
          />
        </div>

        {/* Quick stats */}
        <div className="ml-auto flex items-center gap-3 text-[10px]">
          <span className="text-[var(--text-dim)]">{stats.total} predictions</span>
          <span className="text-green-400">{stats.correct} correct</span>
          <span className="text-red-400">{stats.wrong} wrong</span>
          <span className="text-blue-400">{stats.pending} pending</span>
        </div>
      </div>

      {/* ── Prediction Table ── */}
      {loading ? (
        <div className="flex items-center justify-center py-8">
          <Loader2 className="h-5 w-5 animate-spin text-[var(--text-dim)]" />
        </div>
      ) : predictions.length === 0 ? (
        <div className="text-center py-8 text-[var(--text-dim)] text-xs">
          No predictions yet. Data will appear after the LLM analyzer runs on the GCP VM.
        </div>
      ) : (
        <div className="overflow-x-auto border border-[var(--border)] rounded-lg">
          <table className="w-full text-left">
            <thead>
              <tr className="bg-[var(--surface)] border-b border-[var(--border)]">
                <th className="px-2 py-1.5 text-[10px] font-semibold text-[var(--text-dim)]">Symbol</th>
                <th className="px-2 py-1.5 text-[10px] font-semibold text-[var(--text-dim)]">Source</th>
                <th className="px-2 py-1.5 text-[10px] font-semibold text-[var(--text-dim)]">Action</th>
                <th className="px-2 py-1.5 text-[10px] font-semibold text-[var(--text-dim)]">Wait</th>
                <th className="px-2 py-1.5 text-[10px] font-semibold text-[var(--text-dim)]">LTP</th>
                <th className="px-2 py-1.5 text-[10px] font-semibold text-[var(--text-dim)]">Outcome</th>
                <th className="px-2 py-1.5 text-[10px] font-semibold text-[var(--text-dim)]">Days</th>
                <th className="px-2 py-1.5 text-[10px] font-semibold text-[var(--text-dim)]">Return</th>
                <th className="px-1 py-1.5"></th>
              </tr>
            </thead>
            <tbody>
              {predictions.map((p) => {
                const id = `${p.date}-${p.symbol}-${p.source}`;
                return (
                  <PredictionRow
                    key={id}
                    p={p}
                    expanded={expandedId === id}
                    onToggle={() => setExpandedId(expandedId === id ? null : id)}
                  />
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
