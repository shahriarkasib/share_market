import { useEffect, useState } from "react";
import { RefreshCw, Trophy } from "lucide-react";
import {
  fetchCompositeSignalAccuracy,
  fetchNasdaqSignalAccuracy,
  type SignalAccuracyReport,
  type AccuracyBucket,
} from "../api/client";

interface Props { market?: "dse" | "nasdaq" }

function winRateColor(wr: number): string {
  if (wr >= 70) return "text-emerald-500";
  if (wr >= 55) return "text-emerald-400";
  if (wr >= 45) return "text-amber-500";
  return "text-red-500";
}

function Bucket({ label, bucket }: { label: string; bucket: AccuracyBucket }) {
  if (!bucket || bucket.trades === 0) {
    return (
      <div className="rounded border border-[var(--border)] px-3 py-2">
        <div className="text-xs text-[var(--text-muted)] mb-0.5">{label}</div>
        <div className="text-xs text-[var(--text-muted)]">no data yet</div>
      </div>
    );
  }
  return (
    <div className="rounded border border-[var(--border)] px-3 py-2">
      <div className="text-xs text-[var(--text-muted)] mb-0.5">{label}</div>
      <div className={`text-2xl font-bold ${winRateColor(bucket.win_rate)}`}>
        {bucket.win_rate}%
      </div>
      <div className="text-[10px] text-[var(--text-muted)]">
        {bucket.wins}/{bucket.trades} wins
        {bucket.avg_t1_days > 0 && ` · T1 in ~${bucket.avg_t1_days}d`}
      </div>
      {bucket.avg_max_fav_pct !== undefined && (
        <div className="text-[10px] text-[var(--text-muted)] mt-0.5">
          MFE +{bucket.avg_max_fav_pct}% · MAE {bucket.avg_max_adv_pct}%
        </div>
      )}
    </div>
  );
}

export default function SignalAccuracy({ market = "dse" }: Props = {}) {
  const isNasdaq = market === "nasdaq";
  const [data, setData] = useState<SignalAccuracyReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const r = isNasdaq
        ? await fetchNasdaqSignalAccuracy()
        : await fetchCompositeSignalAccuracy();
      setData(r);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  return (
    <div className="max-w-[1440px] mx-auto px-3 sm:px-4 lg:px-8 py-6">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <div>
          <h1 className="text-xl font-bold flex items-center gap-2">
            <Trophy className="h-5 w-5 text-amber-500" />
            {isNasdaq ? "NASDAQ" : "DSE"} Signal Accuracy / Empirical Edge
          </h1>
          <p className="text-xs text-[var(--text-muted)] mt-0.5">
            Win rate measured from real outcomes — not theory. Refreshed daily after market close.
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

      {error && (
        <div className="rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-500 mb-3">
          {error}
        </div>
      )}

      {data && (
        <>
          {/* Overall headline */}
          <div className="mb-6 rounded-lg border-2 border-[var(--border)] p-5">
            <div className="text-xs text-[var(--text-muted)] uppercase tracking-wider">
              Overall — {data.total_closed} closed signals
            </div>
            <div className={`text-5xl font-bold mt-1 ${winRateColor(data.overall.win_rate)}`}>
              {data.overall.win_rate}%
            </div>
            <div className="text-sm text-[var(--text-muted)] mt-1">
              {data.overall.wins}/{data.overall.trades} winners
              {data.overall.avg_t1_days > 0 && ` · avg T1 in ${data.overall.avg_t1_days} days`}
            </div>
            {data.overall.avg_max_fav_pct !== undefined && (
              <div className="text-xs text-[var(--text-muted)] mt-2">
                Avg max-favorable excursion: <span className="text-emerald-500">+{data.overall.avg_max_fav_pct}%</span>
                {" · "}avg max-adverse: <span className="text-red-500">{data.overall.avg_max_adv_pct}%</span>
              </div>
            )}
          </div>

          {/* By regime */}
          <div className="mb-6">
            <h2 className="text-sm font-bold mb-2">By Market Regime</h2>
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-2">
              {Object.entries(data.by_regime).map(([k, v]) => (
                <Bucket key={k} label={k.replace("_", " ")} bucket={v} />
              ))}
            </div>
          </div>

          {/* By score bucket */}
          <div className="mb-6">
            <h2 className="text-sm font-bold mb-2">By Composite Score</h2>
            <div className="grid grid-cols-3 gap-2">
              {Object.entries(data.by_score_bucket).map(([k, v]) => (
                <Bucket key={k} label={`Score ${k}`} bucket={v} />
              ))}
            </div>
          </div>

          {/* By action type */}
          <div className="mb-6">
            <h2 className="text-sm font-bold mb-2">By Action Type</h2>
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-2">
              {Object.entries(data.by_action_type).map(([k, v]) => (
                <Bucket key={k} label={k.replace("_", " ")} bucket={v} />
              ))}
            </div>
          </div>

          {/* Per strategy vote — which methodology actually predicts wins? */}
          <div className="mb-6">
            <h2 className="text-sm font-bold mb-2">
              Per-Strategy BUY Vote Win Rate
              <span className="text-xs font-normal text-[var(--text-muted)] ml-2">
                (when this strategy voted BUY, did the trade win?)
              </span>
            </h2>
            <div className="rounded-lg border border-[var(--border)] overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-[var(--surface-active)] text-[var(--text-muted)] text-xs">
                  <tr>
                    <th className="text-left px-3 py-2">Strategy</th>
                    <th className="text-right px-3 py-2">BUY signals</th>
                    <th className="text-right px-3 py-2">Wins</th>
                    <th className="text-right px-3 py-2">Win rate</th>
                    <th className="text-left px-3 py-2 w-72">Visual</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(data.by_strategy_vote)
                    .sort(([, a], [, b]) => b.win_rate - a.win_rate)
                    .map(([k, v]) => (
                      <tr key={k} className="border-t border-[var(--border)]">
                        <td className="px-3 py-2 font-mono">{k}</td>
                        <td className="px-3 py-2 text-right">{v.buy_signals}</td>
                        <td className="px-3 py-2 text-right">{v.wins}</td>
                        <td className={`px-3 py-2 text-right font-bold ${winRateColor(v.win_rate)}`}>
                          {v.win_rate}%
                        </td>
                        <td className="px-3 py-2">
                          <div className="h-2 rounded bg-[var(--hover)] overflow-hidden">
                            <div
                              className={
                                v.win_rate >= 60 ? "h-full bg-emerald-500" :
                                v.win_rate >= 45 ? "h-full bg-amber-500" :
                                "h-full bg-red-500"
                              }
                              style={{ width: `${v.win_rate}%` }}
                            />
                          </div>
                        </td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </div>
            <p className="text-xs text-[var(--text-muted)] mt-2">
              💡 Use this to <strong>rebalance regime weights</strong>. Strategies with high BUY-vote win rate deserve more weight.
            </p>
          </div>

          {/* Top stocks by historical win rate */}
          {data.by_stock?.top && data.by_stock.top.length > 0 && (
            <div className="mb-6">
              <h2 className="text-sm font-bold mb-2">Top Stocks by Win Rate (≥3 trades)</h2>
              <div className="rounded-lg border border-[var(--border)] overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-[var(--surface-active)] text-[var(--text-muted)] text-xs">
                    <tr>
                      <th className="text-left px-3 py-2">Symbol</th>
                      <th className="text-right px-3 py-2">Trades</th>
                      <th className="text-right px-3 py-2">Wins</th>
                      <th className="text-right px-3 py-2">Win rate</th>
                      <th className="text-right px-3 py-2">Avg MFE</th>
                      <th className="text-right px-3 py-2">Avg MAE</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.by_stock.top.map((s) => (
                      <tr key={s.symbol} className="border-t border-[var(--border)] hover:bg-[var(--hover)]">
                        <td className="px-3 py-2 font-mono font-bold">{s.symbol}</td>
                        <td className="px-3 py-2 text-right">{s.trades}</td>
                        <td className="px-3 py-2 text-right">{s.wins}</td>
                        <td className={`px-3 py-2 text-right font-bold ${winRateColor(s.win_rate)}`}>
                          {s.win_rate}%
                        </td>
                        <td className="px-3 py-2 text-right text-emerald-500">
                          +{s.avg_max_fav_pct}%
                        </td>
                        <td className="px-3 py-2 text-right text-red-500">
                          {s.avg_max_adv_pct}%
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {data.total_closed === 0 && (
            <div className="rounded border border-[var(--border)] px-4 py-8 text-center text-[var(--text-muted)] text-sm">
              No closed signals yet. Stats will appear after the first signals close
              (T1 hit, stop hit, or 7-day expiry).
            </div>
          )}
        </>
      )}
    </div>
  );
}
