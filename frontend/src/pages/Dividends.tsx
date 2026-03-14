/**
 * Dividends / Record Date Impact page.
 *
 * Three tabs:
 *  1. Upcoming Record Dates -- stocks with approaching record dates
 *  2. Post-Dividend Opportunities -- stocks that dropped more than expected (buying opps)
 *  3. Record Date Impact Analyzer -- per-symbol historical record-date pattern
 */

import { useEffect, useState, useMemo, useCallback } from "react";
import { Link } from "react-router-dom";
import { Percent, Loader2, AlertCircle, Search } from "lucide-react";
import { clsx } from "clsx";
import {
  fetchUpcomingRecordDates,
  fetchPostDividendOpportunities,
  fetchRecordDateImpact,
} from "../api/client.ts";
import type {
  UpcomingRecordDate,
  PostDividendOpportunity,
  RecordDateImpact,
} from "../api/client.ts";

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const TABS = ["Upcoming", "Opportunities", "Impact Analyzer"] as const;
type TabId = (typeof TABS)[number];

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function fmtPrice(v: number): string {
  return v.toFixed(1);
}

function fmtPct(v: number): string {
  return v.toFixed(1) + "%";
}

function daysUntilColor(days: number): string {
  if (days < 3) return "text-red-500";
  if (days <= 10) return "text-yellow-500";
  return "text-green-500";
}

function recoveryColor(v: number): string {
  return v >= 0 ? "text-green-500" : "text-red-500";
}

/* ------------------------------------------------------------------ */
/*  Shared UI                                                          */
/* ------------------------------------------------------------------ */

function LoadingState({ message }: { message?: string }) {
  return (
    <div className="flex items-center justify-center py-24 text-[var(--text-muted)]">
      <Loader2 className="h-5 w-5 animate-spin mr-2" />
      {message ?? "Loading..."}
    </div>
  );
}

function ErrorState({ message }: { message: string }) {
  return (
    <div className="flex items-center justify-center py-24 text-red-400 gap-2">
      <AlertCircle className="h-5 w-5" />
      {message}
    </div>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className="flex items-center justify-center py-24 text-[var(--text-muted)]">
      {message}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Tab 1: Upcoming Record Dates                                       */
/* ------------------------------------------------------------------ */

function UpcomingTab() {
  const [data, setData] = useState<UpcomingRecordDate[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    fetchUpcomingRecordDates(60)
      .then((r) => {
        setData(r.upcoming ?? []);
        setError("");
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const sorted = useMemo(
    () => [...data].sort((a, b) => a.days_until - b.days_until),
    [data],
  );

  if (loading) return <LoadingState message="Loading upcoming record dates..." />;
  if (error) return <ErrorState message={error} />;
  if (sorted.length === 0) return <EmptyState message="No upcoming record dates found." />;

  return (
    <div>
      <p className="text-xs text-[var(--text-dim)] mb-3">
        Ex-price = Current - (Dividend% x Face Value / 100). Sorted by nearest record date.
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-[var(--border)] text-[var(--text-muted)]">
              <th className="text-left py-2 px-2 font-medium">Symbol</th>
              <th className="text-left py-2 px-2 font-medium">Record Date</th>
              <th className="text-right py-2 px-2 font-medium">Days Until</th>
              <th className="text-right py-2 px-2 font-medium">Price</th>
              <th className="text-right py-2 px-2 font-medium">Exp. Ex-Price</th>
              <th className="text-right py-2 px-2 font-medium">Div %</th>
              <th className="text-right py-2 px-2 font-medium">Hist Avg Drop</th>
              <th className="text-right py-2 px-2 font-medium">Hist Events</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r) => (
              <tr
                key={r.symbol + r.record_date}
                className="border-b border-[var(--border)] hover:bg-[var(--hover)] transition-colors"
              >
                <td className="py-2 px-2 font-medium">
                  <Link to={`/stock/${r.symbol}`} className="text-blue-500 hover:underline">
                    {r.symbol}
                  </Link>
                </td>
                <td className="py-2 px-2 text-[var(--text-muted)]">{r.record_date}</td>
                <td className={clsx("py-2 px-2 text-right font-medium", daysUntilColor(r.days_until))}>
                  {r.days_until}d
                </td>
                <td className="py-2 px-2 text-right">{r.current_price != null ? fmtPrice(r.current_price) : "—"}</td>
                <td className="py-2 px-2 text-right">{r.expected_ex_price != null ? fmtPrice(r.expected_ex_price) : "—"}</td>
                <td className="py-2 px-2 text-right">{fmtPct(r.dividend_pct)}</td>
                <td className="py-2 px-2 text-right text-red-400">
                  {r.avg_historical_ex_drop_pct != null ? fmtPct(r.avg_historical_ex_drop_pct) : "—"}
                </td>
                <td className="py-2 px-2 text-right text-[var(--text-muted)]">
                  {r.historical_events || 0}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Tab 2: Post-Dividend Opportunities                                 */
/* ------------------------------------------------------------------ */

function OpportunitiesTab() {
  const [data, setData] = useState<PostDividendOpportunity[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    fetchPostDividendOpportunities(14)
      .then((r) => {
        setData(r.opportunities ?? []);
        setError("");
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const sorted = useMemo(
    () => [...data].sort((a, b) => b.excess_drop - a.excess_drop),
    [data],
  );

  if (loading) return <LoadingState message="Loading post-dividend opportunities..." />;
  if (error) return <ErrorState message={error} />;
  if (sorted.length === 0) return <EmptyState message="No post-dividend opportunities right now." />;

  return (
    <div>
      <p className="text-xs text-[var(--text-dim)] mb-3">
        Stocks that dropped more than expected after record date -- potential buying opportunities.
        Sorted by excess drop (biggest opportunity first).
      </p>
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {sorted.map((o) => {
          const strong = o.excess_drop > 3;
          return (
            <div
              key={o.symbol + o.record_date}
              className={clsx(
                "rounded-lg border p-3",
                strong
                  ? "border-green-500/40 bg-green-500/5"
                  : "border-[var(--border)] bg-[var(--surface)]",
              )}
            >
              <div className="flex items-center justify-between mb-2">
                <Link to={`/stock/${o.symbol}`} className="text-sm font-bold text-blue-500 hover:underline">
                  {o.symbol}
                </Link>
                {strong && (
                  <span className="text-[10px] font-semibold text-green-500 bg-green-500/10 px-1.5 py-0.5 rounded">
                    Strong opportunity
                  </span>
                )}
              </div>

              <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
                <span className="text-[var(--text-muted)]">Record Date</span>
                <span className="text-right">{o.record_date}</span>

                <span className="text-[var(--text-muted)]">Days Since</span>
                <span className="text-right">{o.days_since}d</span>

                <span className="text-[var(--text-muted)]">Drop %</span>
                <span className="text-right text-red-400">{fmtPct(o.drop_pct)}</span>

                <span className="text-[var(--text-muted)]">Expected Drop</span>
                <span className="text-right">{fmtPct(o.expected_drop)}</span>

                <span className="text-[var(--text-muted)]">Excess Drop</span>
                <span className="text-right font-medium text-green-500">{fmtPct(o.excess_drop)}</span>

                <span className="text-[var(--text-muted)]">Price</span>
                <span className="text-right">{fmtPrice(o.current_price)}</span>

                <span className="text-[var(--text-muted)]">Vol Ratio</span>
                <span className="text-right">{o.volume_ratio.toFixed(2)}x</span>

                <span className="text-[var(--text-muted)]">RSI</span>
                <span className={clsx("text-right", o.rsi < 30 ? "text-green-500" : o.rsi > 70 ? "text-red-400" : "")}>
                  {o.rsi.toFixed(1)}
                </span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Tab 3: Record Date Impact Analyzer                                 */
/* ------------------------------------------------------------------ */

function ImpactAnalyzerTab() {
  const [symbol, setSymbol] = useState("");
  const [query, setQuery] = useState("");
  const [data, setData] = useState<RecordDateImpact | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSearch = useCallback(() => {
    const s = symbol.trim().toUpperCase();
    if (!s) return;
    setQuery(s);
    setLoading(true);
    setError("");
    setData(null);
    fetchRecordDateImpact(s)
      .then((r) => setData(r))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [symbol]);

  return (
    <div>
      <p className="text-xs text-[var(--text-dim)] mb-3">
        Enter a symbol to see its full historical record-date pattern -- how the price behaves around ex-date.
      </p>

      {/* Search */}
      <div className="flex items-center gap-2 mb-4">
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-[var(--text-dim)]" />
          <input
            type="text"
            value={symbol}
            onChange={(e) => setSymbol(e.target.value.toUpperCase())}
            onKeyDown={(e) => e.key === "Enter" && handleSearch()}
            placeholder="e.g. GP, ROBI, BSRMSTEEL"
            className="w-full pl-8 pr-3 py-1.5 rounded-md border border-[var(--border)] bg-[var(--surface)] text-xs text-[var(--text)] placeholder:text-[var(--text-dim)] focus:outline-none focus:ring-1 focus:ring-blue-500"
          />
        </div>
        <button
          onClick={handleSearch}
          disabled={!symbol.trim()}
          className="px-3 py-1.5 rounded-md text-xs font-medium bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-40 transition-colors"
        >
          Analyze
        </button>
      </div>

      {loading && <LoadingState message={`Loading record-date history for ${query}...`} />}
      {error && <ErrorState message={error} />}

      {data && data.events.length > 0 && (
        <>
          {/* Summary card */}
          <div className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4 mb-4">
            <h3 className="text-sm font-bold text-[var(--text)] mb-2">{data.symbol} -- Record Date Summary ({data.averages.event_count} events)</h3>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
              <div>
                <span className="text-[var(--text-muted)] block">Avg Ex-Date Drop</span>
                <span className="text-red-400 font-medium text-sm">
                  {data.averages.avg_ex_drop_pct != null ? fmtPct(data.averages.avg_ex_drop_pct) : "—"}
                </span>
              </div>
              <div>
                <span className="text-[var(--text-muted)] block">Avg Bottom Day</span>
                <span className="font-medium text-sm text-[var(--text)]">
                  {data.averages.avg_bottom_day != null ? `Day ${data.averages.avg_bottom_day.toFixed(0)}` : "—"}
                </span>
              </div>
              <div>
                <span className="text-[var(--text-muted)] block">Avg 7d Recovery</span>
                <span className={clsx("font-medium text-sm", data.averages.avg_day_7_pct != null ? recoveryColor(data.averages.avg_day_7_pct) : "")}>
                  {data.averages.avg_day_7_pct != null ? fmtPct(data.averages.avg_day_7_pct) : "—"}
                </span>
              </div>
              <div>
                <span className="text-[var(--text-muted)] block">Avg 14d Recovery</span>
                <span className={clsx("font-medium text-sm", data.averages.avg_day_14_pct != null ? recoveryColor(data.averages.avg_day_14_pct) : "")}>
                  {data.averages.avg_day_14_pct != null ? fmtPct(data.averages.avg_day_14_pct) : "—"}
                </span>
              </div>
            </div>
          </div>

          {/* History table */}
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-[var(--border)] text-[var(--text-muted)]">
                  <th className="text-left py-2 px-2 font-medium">Record Date</th>
                  <th className="text-right py-2 px-2 font-medium">Div %</th>
                  <th className="text-right py-2 px-2 font-medium">Pre-Price</th>
                  <th className="text-right py-2 px-2 font-medium">Ex-Price</th>
                  <th className="text-right py-2 px-2 font-medium">Drop %</th>
                  <th className="text-right py-2 px-2 font-medium">Bottom Day</th>
                  <th className="text-right py-2 px-2 font-medium">Bottom Drop %</th>
                  <th className="text-right py-2 px-2 font-medium">7d</th>
                  <th className="text-right py-2 px-2 font-medium">14d</th>
                  <th className="text-right py-2 px-2 font-medium">20d</th>
                </tr>
              </thead>
              <tbody>
                {data.events.map((ev) => (
                  <tr
                    key={ev.record_date}
                    className="border-b border-[var(--border)] hover:bg-[var(--hover)] transition-colors"
                  >
                    <td className="py-2 px-2 text-[var(--text-muted)]">{ev.record_date}</td>
                    <td className="py-2 px-2 text-right">{fmtPct(ev.dividend_pct)}</td>
                    <td className="py-2 px-2 text-right">{fmtPrice(ev.pre_close)}</td>
                    <td className="py-2 px-2 text-right">{fmtPrice(ev.ex_close)}</td>
                    <td className="py-2 px-2 text-right text-red-400">{fmtPct(ev.ex_drop_pct)}</td>
                    <td className="py-2 px-2 text-right">{ev.bottom_day}</td>
                    <td className="py-2 px-2 text-right text-red-400">{fmtPct(ev.bottom_drop_pct)}</td>
                    <td className={clsx("py-2 px-2 text-right", ev.day_7_pct != null ? recoveryColor(ev.day_7_pct) : "")}>
                      {ev.day_7_pct != null ? fmtPct(ev.day_7_pct) : "—"}
                    </td>
                    <td className={clsx("py-2 px-2 text-right", ev.day_14_pct != null ? recoveryColor(ev.day_14_pct) : "")}>
                      {ev.day_14_pct != null ? fmtPct(ev.day_14_pct) : "—"}
                    </td>
                    <td className={clsx("py-2 px-2 text-right", ev.day_20_pct != null ? recoveryColor(ev.day_20_pct) : "")}>
                      {ev.day_20_pct != null ? fmtPct(ev.day_20_pct) : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {data && data.events.length === 0 && (
        <EmptyState message={`No record-date history found for ${query}.`} />
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Main page                                                          */
/* ------------------------------------------------------------------ */

export default function Dividends() {
  const [tab, setTab] = useState<TabId>("Upcoming");

  return (
    <div className="max-w-[1440px] mx-auto px-3 sm:px-4 lg:px-8 py-4">
      {/* Header */}
      <div className="flex items-center gap-2 mb-4">
        <Percent className="h-5 w-5 text-blue-500" />
        <h1 className="text-lg font-bold text-[var(--text)]">Dividends / Record Date Impact</h1>
      </div>

      {/* Tabs */}
      <div className="flex items-center gap-1 mb-4 border-b border-[var(--border)]">
        {TABS.map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={clsx(
              "px-3 py-2 text-xs font-medium border-b-2 transition-colors -mb-px",
              tab === t
                ? "border-blue-500 text-[var(--text)]"
                : "border-transparent text-[var(--text-muted)] hover:text-[var(--text)]",
            )}
          >
            {t}
          </button>
        ))}
      </div>

      {/* Content */}
      {tab === "Upcoming" && <UpcomingTab />}
      {tab === "Opportunities" && <OpportunitiesTab />}
      {tab === "Impact Analyzer" && <ImpactAnalyzerTab />}
    </div>
  );
}
