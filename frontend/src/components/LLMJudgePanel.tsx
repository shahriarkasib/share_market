import { useState, useEffect, useRef } from "react";
import { clsx } from "clsx";
import {
  Brain,
  Scale,
  ChevronDown,
  ChevronUp,
  CheckCircle2,
  AlertTriangle,
  Clock,
  Loader2,
  Target,
  BarChart3,
  Calendar,
  DollarSign,
} from "lucide-react";
import { fetchLLMDailyAnalysis } from "../api/client.ts";
import type { LLMDailyAnalysis } from "../types/index.ts";

/* ── source badge ── */

const SOURCE_STYLES: Record<string, { color: string; bg: string; label: string }> = {
  algo:  { color: "text-blue-400",   bg: "bg-blue-500/15",   label: "Algo" },
  llm:   { color: "text-purple-400", bg: "bg-purple-500/15", label: "LLM" },
  judge: { color: "text-amber-400",  bg: "bg-amber-500/15",  label: "Judge" },
};

/* action → color */
function actionColor(a?: string): string {
  if (!a) return "text-[var(--text-dim)]";
  const u = a.toUpperCase();
  if (u.includes("STRONG") && u.includes("BUY")) return "text-green-300";
  if (u.includes("BUY")) return "text-green-400";
  if (u.includes("HOLD") || u.includes("WAIT")) return "text-blue-400";
  if (u.includes("SELL") || u.includes("AVOID")) return "text-red-400";
  return "text-[var(--text-muted)]";
}

interface Props {
  symbol: string;
  date: string;
}

export default function LLMJudgePanel({ symbol, date }: Props) {
  const [open, setOpen] = useState(false);
  const [entry, setEntry] = useState<LLMDailyAnalysis | null | undefined>(undefined);
  const [loading, setLoading] = useState(false);

  // Date-scoped cache: invalidates when date changes
  const cacheRef = useRef<{ date: string; data: Record<string, LLMDailyAnalysis | null> }>({ date: "", data: {} });

  useEffect(() => {
    if (!open) return;

    // Invalidate cache when date changes
    if (cacheRef.current.date !== date) {
      cacheRef.current = { date, data: {} };
    }

    const cache = cacheRef.current.data;
    if (symbol in cache) {
      setEntry(cache[symbol]);
      return;
    }

    setLoading(true);
    fetchLLMDailyAnalysis(date, undefined, symbol)
      .then((r) => {
        const found = r.analysis.find((a) => a.symbol === symbol) ?? null;
        cache[symbol] = found;
        setEntry(found);
      })
      .catch(() => {
        cache[symbol] = null;
        setEntry(null);
      })
      .finally(() => setLoading(false));
  }, [open, symbol, date]);

  return (
    <div className="border-t border-[var(--border)]">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-2 px-3 py-1.5 text-[10px] font-medium text-[var(--text-dim)] hover:text-[var(--text)] transition-colors"
      >
        <Brain className="h-3 w-3 text-purple-400" />
        <span>LLM + Judge</span>
        {open ? <ChevronUp className="h-3 w-3 ml-auto" /> : <ChevronDown className="h-3 w-3 ml-auto" />}
      </button>

      {open && (
        <div className="px-3 pb-2 space-y-2">
          {loading && (
            <div className="flex items-center gap-2 text-[10px] text-[var(--text-dim)]">
              <Loader2 className="h-3 w-3 animate-spin" />
              Loading LLM analysis...
            </div>
          )}

          {!loading && entry == null && (
            <p className="text-[10px] text-[var(--text-dim)]">No LLM analysis available for this date.</p>
          )}

          {!loading && entry && (
            <>
              {/* Action comparison row */}
              <div className="flex items-center gap-3 flex-wrap">
                {/* Algo */}
                {entry.algo_action && (
                  <div className="flex items-center gap-1">
                    <span className={clsx("text-[10px] font-semibold px-1.5 py-0.5 rounded", SOURCE_STYLES.algo.bg, SOURCE_STYLES.algo.color)}>
                      Algo
                    </span>
                    <span className={clsx("text-[10px] font-medium", actionColor(entry.algo_action))}>
                      {entry.algo_action}
                    </span>
                  </div>
                )}

                {/* LLM */}
                <div className="flex items-center gap-1">
                  <span className={clsx("text-[10px] font-semibold px-1.5 py-0.5 rounded", SOURCE_STYLES.llm.bg, SOURCE_STYLES.llm.color)}>
                    LLM
                  </span>
                  <span className={clsx("text-[10px] font-medium", actionColor(entry.action))}>
                    {entry.action}
                  </span>
                </div>

                {/* Judge */}
                {entry.final_action && (
                  <div className="flex items-center gap-1">
                    <span className={clsx("text-[10px] font-semibold px-1.5 py-0.5 rounded", SOURCE_STYLES.judge.bg, SOURCE_STYLES.judge.color)}>
                      <Scale className="h-2.5 w-2.5 inline mr-0.5" />Judge
                    </span>
                    <span className={clsx("text-[10px] font-bold", actionColor(entry.final_action))}>
                      {entry.final_action}
                    </span>
                  </div>
                )}

                {/* Agreement */}
                {entry.agreement !== undefined && (
                  <span className={clsx(
                    "text-[10px] flex items-center gap-0.5",
                    entry.agreement ? "text-green-400" : "text-amber-400",
                  )}>
                    {entry.agreement
                      ? <><CheckCircle2 className="h-3 w-3" /> Agree</>
                      : <><AlertTriangle className="h-3 w-3" /> Disagree</>}
                  </span>
                )}
              </div>

              {/* LLM reasoning */}
              <div className="bg-[var(--surface)] rounded-md p-2 border border-[var(--border)]">
                <p className="text-[10px] text-purple-300 font-semibold mb-0.5">LLM Reasoning</p>
                <p className="text-[10px] text-[var(--text-muted)] leading-relaxed">{entry.reasoning}</p>
              </div>

              {/* Wait for */}
              {entry.wait_for && (
                <div className="flex items-start gap-1.5">
                  <Clock className="h-3 w-3 text-amber-400 mt-0.5 shrink-0" />
                  <div>
                    <span className="text-[10px] font-medium text-amber-400">Wait for: </span>
                    <span className="text-[10px] text-[var(--text-muted)]">{entry.wait_for}</span>
                    {entry.wait_days && (
                      <span className="text-[10px] text-[var(--text-dim)] ml-1">({entry.wait_days})</span>
                    )}
                  </div>
                </div>
              )}

              {/* Judge reasoning */}
              {entry.judge_reasoning && (
                <div className="bg-amber-500/5 rounded-md p-2 border border-amber-500/20">
                  <p className="text-[10px] text-amber-300 font-semibold mb-0.5">Judge Verdict</p>
                  <p className="text-[10px] text-[var(--text-muted)] leading-relaxed">{entry.judge_reasoning}</p>
                  {(entry.algo_strengths || entry.llm_strengths) && (
                    <div className="mt-1 flex gap-3 flex-wrap">
                      {entry.algo_strengths && (
                        <span className="text-[10px] text-blue-300">
                          <strong>Algo+:</strong> {entry.algo_strengths}
                        </span>
                      )}
                      {entry.llm_strengths && (
                        <span className="text-[10px] text-purple-300">
                          <strong>LLM+:</strong> {entry.llm_strengths}
                        </span>
                      )}
                    </div>
                  )}
                  {entry.key_risk && (
                    <p className="text-[10px] text-red-300 mt-1">
                      <strong>Key Risk:</strong> {entry.key_risk}
                    </p>
                  )}
                </div>
              )}

              {/* Risk factors and catalysts */}
              {((entry.risk_factors?.length ?? 0) > 0 || (entry.catalysts?.length ?? 0) > 0) && (
                <div className="flex gap-3 flex-wrap">
                  {(entry.risk_factors?.length ?? 0) > 0 && (
                    <div>
                      <span className="text-[10px] text-red-400 font-medium">Risks: </span>
                      {entry.risk_factors?.map((r, i) => (
                        <span key={i} className="text-[10px] text-[var(--text-dim)] mr-1.5">{r}{i < (entry.risk_factors?.length ?? 0) - 1 ? "," : ""}</span>
                      ))}
                    </div>
                  )}
                  {(entry.catalysts?.length ?? 0) > 0 && (
                    <div>
                      <span className="text-[10px] text-green-400 font-medium">Catalysts: </span>
                      {entry.catalysts?.map((c, i) => (
                        <span key={i} className="text-[10px] text-[var(--text-dim)] mr-1.5">{c}{i < (entry.catalysts?.length ?? 0) - 1 ? "," : ""}</span>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {/* How to Buy */}
              {entry.how_to_buy && (
                <div className="bg-[var(--surface)] rounded-md p-2 border border-[var(--border)]">
                  <p className="text-[10px] text-green-300 font-semibold mb-0.5 flex items-center gap-1">
                    <Target className="h-3 w-3" /> How to Buy
                  </p>
                  <p className="text-[10px] text-[var(--text-muted)] leading-relaxed whitespace-pre-line">{entry.how_to_buy}</p>
                </div>
              )}

              {/* Volume Rule */}
              {entry.volume_rule && (
                <div className="bg-[var(--surface)] rounded-md p-2 border border-[var(--border)]">
                  <p className="text-[10px] text-blue-300 font-semibold mb-0.5 flex items-center gap-1">
                    <BarChart3 className="h-3 w-3" /> Volume Rule
                  </p>
                  <p className="text-[10px] text-[var(--text-muted)] leading-relaxed whitespace-pre-line">{entry.volume_rule}</p>
                </div>
              )}

              {/* Next Day Plan */}
              {entry.next_day_plan && (
                <div className="bg-[var(--surface)] rounded-md p-2 border border-[var(--border)]">
                  <p className="text-[10px] text-amber-300 font-semibold mb-0.5 flex items-center gap-1">
                    <Calendar className="h-3 w-3" /> Next Day Plan
                  </p>
                  <p className="text-[10px] text-[var(--text-muted)] leading-relaxed whitespace-pre-line">{entry.next_day_plan}</p>
                </div>
              )}

              {/* Sell Plan */}
              {entry.sell_plan && (
                <div className="bg-[var(--surface)] rounded-md p-2 border border-[var(--border)]">
                  <p className="text-[10px] text-red-300 font-semibold mb-0.5 flex items-center gap-1">
                    <DollarSign className="h-3 w-3" /> Sell Plan
                  </p>
                  <p className="text-[10px] text-[var(--text-muted)] leading-relaxed whitespace-pre-line">{entry.sell_plan}</p>
                </div>
              )}

              {/* Confidence + score */}
              <div className="flex items-center gap-3 text-[10px] text-[var(--text-dim)]">
                {entry.confidence && <span>Confidence: <strong className="text-[var(--text-muted)]">{entry.confidence}</strong></span>}
                {entry.final_confidence && <span>Judge confidence: <strong className="text-[var(--text-muted)]">{entry.final_confidence}</strong></span>}
                <span>Score: <strong className="text-[var(--text-muted)]">{entry.score}</strong></span>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
