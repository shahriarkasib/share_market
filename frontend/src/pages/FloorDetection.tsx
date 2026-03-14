/**
 * Floor Detection — indicator-floor approach.
 *
 * Shows RSI, StochRSI, MACD histogram for each stock, their historical
 * floors, pace of decline, and days-to-floor estimates. Stocks approaching
 * floor on 2+ indicators are highlighted.
 */

import { useState, useEffect, useMemo } from "react";
import { clsx } from "clsx";
import { ShieldAlert, Loader2, AlertCircle, Search, ArrowDown, Minus } from "lucide-react";
import { fetchFloorTable, fetchFloorDates } from "../api/client.ts";
import type { FloorStock } from "../api/client.ts";

type SortKey =
  | "symbol" | "sector" | "ltp" | "rsi" | "stoch_rsi" | "macd_hist"
  | "rsi_proximity" | "stoch_proximity" | "approaching_count" | "score"
  | "rsi_days_to_floor" | "stoch_days_to_floor" | "macd_days_to_floor";

export default function FloorDetection() {
  const [stocks, setStocks] = useState<FloorStock[]>([]);
  const [dates, setDates] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [months, setMonths] = useState(6);
  const [asOf, setAsOf] = useState("");
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("score");
  const [sortAsc, setSortAsc] = useState(false);
  const [onlyApproaching, setOnlyApproaching] = useState(false);

  // Load available dates once
  useEffect(() => {
    fetchFloorDates().then((r) => setDates(r.dates)).catch(() => {});
  }, []);

  // Load floor data
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetchFloorTable(months, asOf || undefined)
      .then((res) => { if (!cancelled) setStocks(res.stocks); })
      .catch((err) => { if (!cancelled) setError(err instanceof Error ? err.message : "Failed"); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [months, asOf]);

  const handleSort = (key: SortKey) => {
    if (sortKey === key) setSortAsc((p) => !p);
    else { setSortKey(key); setSortAsc(key === "symbol" || key === "sector"); }
  };

  const filtered = useMemo(() => {
    let list = stocks;
    if (onlyApproaching) list = list.filter((s) => s.approaching_count >= 2);
    if (search) {
      const q = search.toUpperCase();
      list = list.filter((s) => s.symbol.includes(q) || (s.sector || "").toUpperCase().includes(q));
    }
    const sorted = [...list].sort((a, b) => {
      const av = a[sortKey] ?? 999;
      const bv = b[sortKey] ?? 999;
      if (typeof av === "string" && typeof bv === "string") return av.localeCompare(bv);
      return (av as number) - (bv as number);
    });
    return sortAsc ? sorted : sorted.reverse();
  }, [stocks, search, sortKey, sortAsc, onlyApproaching]);

  const approachingCount = stocks.filter((s) => s.approaching_count >= 2).length;

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center gap-2">
        <ShieldAlert className="h-4 w-4 text-amber-500" />
        <h1 className="text-sm font-semibold text-[var(--text)]">Floor Detection</h1>
      </div>

      {/* Controls */}
      <div className="flex items-center gap-3 flex-wrap text-xs">
        <label className="flex items-center gap-1.5 text-[var(--text-muted)]">
          Lookback:
          <select value={months} onChange={(e) => setMonths(Number(e.target.value))}
            className="px-1.5 py-1 rounded border border-[var(--border)] bg-[var(--bg)] text-[var(--text)]">
            <option value={3}>3 months</option>
            <option value={6}>6 months</option>
            <option value={12}>12 months</option>
          </select>
        </label>

        <label className="flex items-center gap-1.5 text-[var(--text-muted)]">
          As of:
          <select value={asOf} onChange={(e) => setAsOf(e.target.value)}
            className="px-1.5 py-1 rounded border border-[var(--border)] bg-[var(--bg)] text-[var(--text)]">
            <option value="">Latest</option>
            {dates.map((d) => <option key={d} value={d}>{d}</option>)}
          </select>
        </label>

        <div className="relative flex-1 max-w-[200px]">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-[var(--text-dim)]" />
          <input type="text" value={search} onChange={(e) => setSearch(e.target.value)}
            placeholder="Filter..."
            className="w-full pl-7 pr-2 py-1 rounded border border-[var(--border)] bg-[var(--bg)] text-[var(--text)] text-xs placeholder:text-[var(--text-dim)]" />
        </div>

        <label className="flex items-center gap-1.5 cursor-pointer">
          <input type="checkbox" checked={onlyApproaching}
            onChange={(e) => setOnlyApproaching(e.target.checked)}
            className="rounded border-[var(--border)]" />
          <span className="text-amber-400">Only approaching (2+)</span>
        </label>
      </div>

      {/* Stats bar */}
      <div className="flex items-center gap-4 text-[10px] text-[var(--text-dim)]">
        <span>{filtered.length} stocks</span>
        <span className="text-amber-400">{approachingCount} approaching floor</span>
        <span>Lookback: {months}mo</span>
        {asOf && <span>Viewing: {asOf}</span>}
      </div>

      {/* Table */}
      {loading ? (
        <div className="flex items-center justify-center py-20 text-[var(--text-muted)]">
          <Loader2 className="h-5 w-5 animate-spin mr-2" /> Loading floor data...
        </div>
      ) : error ? (
        <div className="flex items-center justify-center py-20 text-red-400 gap-2 text-sm">
          <AlertCircle className="h-4 w-4" /> {error}
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="text-[var(--text-muted)]">
                <TH k="symbol" label="Symbol" cur={sortKey} asc={sortAsc} onClick={handleSort} sticky />
                <TH k="sector" label="Sector" cur={sortKey} asc={sortAsc} onClick={handleSort} />
                <TH k="ltp" label="LTP" cur={sortKey} asc={sortAsc} onClick={handleSort} />
                {/* RSI group */}
                <th colSpan={4} className="px-1 py-1.5 text-center border-b-2 border-blue-500/40 text-blue-400">RSI</th>
                {/* StochRSI group */}
                <th colSpan={4} className="px-1 py-1.5 text-center border-b-2 border-purple-500/40 text-purple-400">StochRSI</th>
                {/* MACD group */}
                <th colSpan={3} className="px-1 py-1.5 text-center border-b-2 border-cyan-500/40 text-cyan-400">MACD Hist</th>
                <TH k="approaching_count" label="Appr" cur={sortKey} asc={sortAsc} onClick={handleSort} />
                <TH k="score" label="Score" cur={sortKey} asc={sortAsc} onClick={handleSort} />
              </tr>
              {/* Sub-headers */}
              <tr className="text-[10px] text-[var(--text-dim)]">
                <th className="sticky left-0 bg-[var(--surface)] z-10" />
                <th />
                <th />
                {/* RSI sub */}
                <TH k="rsi" label="Now" cur={sortKey} asc={sortAsc} onClick={handleSort} />
                <th className="px-1 py-0.5">Floor</th>
                <th className="px-1 py-0.5">Pace</th>
                <TH k="rsi_days_to_floor" label="DTF" cur={sortKey} asc={sortAsc} onClick={handleSort} />
                {/* StochRSI sub */}
                <TH k="stoch_rsi" label="Now" cur={sortKey} asc={sortAsc} onClick={handleSort} />
                <th className="px-1 py-0.5">Floor</th>
                <th className="px-1 py-0.5">Pace</th>
                <TH k="stoch_days_to_floor" label="DTF" cur={sortKey} asc={sortAsc} onClick={handleSort} />
                {/* MACD sub */}
                <TH k="macd_hist" label="Now" cur={sortKey} asc={sortAsc} onClick={handleSort} />
                <th className="px-1 py-0.5">Floor</th>
                <TH k="macd_days_to_floor" label="DTF" cur={sortKey} asc={sortAsc} onClick={handleSort} />
                <th />
                <th />
              </tr>
            </thead>
            <tbody>
              {filtered.map((s) => (
                <tr
                  key={s.symbol}
                  className={clsx(
                    "border-t border-[var(--border)] hover:bg-[var(--hover)]",
                    s.approaching_count >= 3 && "bg-amber-500/5",
                    s.approaching_count === 2 && "bg-yellow-500/3",
                  )}
                >
                  {/* Symbol */}
                  <td className="px-2 py-1.5 font-medium text-[var(--text)] sticky left-0 bg-[var(--surface)] z-10 whitespace-nowrap">
                    {s.approaching_count >= 2 && <span className="text-amber-400 mr-1">!</span>}
                    {s.symbol}
                  </td>
                  <td className="px-2 py-1.5 text-[var(--text-muted)] whitespace-nowrap">{s.sector || "—"}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums text-[var(--text)]">{s.ltp.toFixed(1)}</td>

                  {/* RSI */}
                  <td className={clsx("px-1 py-1.5 text-right tabular-nums", rsiBg(s.rsi))}>{s.rsi.toFixed(1)}</td>
                  <td className="px-1 py-1.5 text-right tabular-nums text-[var(--text-dim)]">{s.rsi_floor.toFixed(1)}</td>
                  <PaceCell value={s.rsi_pace} />
                  <DtfCell value={s.rsi_days_to_floor} approaching={s.rsi_approaching} />

                  {/* StochRSI */}
                  <td className={clsx("px-1 py-1.5 text-right tabular-nums", rsiBg(s.stoch_rsi))}>{s.stoch_rsi.toFixed(1)}</td>
                  <td className="px-1 py-1.5 text-right tabular-nums text-[var(--text-dim)]">{s.stoch_floor.toFixed(1)}</td>
                  <PaceCell value={s.stoch_pace} />
                  <DtfCell value={s.stoch_days_to_floor} approaching={s.stoch_approaching} />

                  {/* MACD */}
                  <td className={clsx("px-1 py-1.5 text-right tabular-nums", s.macd_hist < 0 ? "text-red-400" : "text-green-400")}>
                    {s.macd_hist.toFixed(2)}
                  </td>
                  <td className="px-1 py-1.5 text-right tabular-nums text-[var(--text-dim)]">{s.macd_floor.toFixed(2)}</td>
                  <DtfCell value={s.macd_days_to_floor} approaching={s.macd_approaching} />

                  {/* Approaching count */}
                  <td className="px-2 py-1.5 text-center">
                    {s.approaching_count >= 2 ? (
                      <span className={clsx(
                        "inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded-full text-[10px] font-bold",
                        s.approaching_count >= 3 ? "bg-amber-500/20 text-amber-400" : "bg-yellow-500/15 text-yellow-400",
                      )}>
                        {s.approaching_count}/3
                      </span>
                    ) : (
                      <span className="text-[var(--text-dim)]">{s.approaching_count}/3</span>
                    )}
                  </td>

                  {/* Score */}
                  <td className="px-2 py-1.5 text-right tabular-nums text-[var(--text-muted)]">
                    {s.score.toFixed(0)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Legend */}
      <div className="flex items-center gap-4 text-[10px] text-[var(--text-dim)] flex-wrap pt-2 border-t border-[var(--border)]">
        <span><b>DTF</b> = Days To Floor at current pace</span>
        <span><b>Pace</b> = Daily change rate (last 5 days)</span>
        <span><b>Floor</b> = Lowest value in {months}mo window</span>
        <span className="text-amber-400">! = Approaching floor on 2+ indicators</span>
      </div>
    </div>
  );
}

/* ── Helper components ── */

function TH({ k, label, cur, asc, onClick, sticky }: {
  k: SortKey; label: string; cur: SortKey; asc: boolean;
  onClick: (k: SortKey) => void; sticky?: boolean;
}) {
  const active = cur === k;
  return (
    <th
      className={clsx(
        "px-1 py-1.5 font-medium cursor-pointer select-none whitespace-nowrap",
        sticky && "text-left sticky left-0 bg-[var(--surface)] z-10",
        !sticky && "text-right",
      )}
      onClick={() => onClick(k)}
    >
      <span className={clsx("inline-flex items-center gap-0.5", active && "text-blue-400")}>
        {label}
        {active && (asc ? <ArrowDown className="h-2.5 w-2.5 rotate-180" /> : <ArrowDown className="h-2.5 w-2.5" />)}
      </span>
    </th>
  );
}

function PaceCell({ value }: { value: number }) {
  if (Math.abs(value) < 0.01) {
    return <td className="px-1 py-1.5 text-right text-[var(--text-dim)]"><Minus className="h-3 w-3 inline" /></td>;
  }
  return (
    <td className={clsx("px-1 py-1.5 text-right tabular-nums text-[10px]", value < 0 ? "text-red-400" : "text-green-400")}>
      {value > 0 ? "+" : ""}{value.toFixed(1)}/d
    </td>
  );
}

function DtfCell({ value, approaching }: { value: number | null; approaching: boolean }) {
  if (value === null) {
    return <td className="px-1 py-1.5 text-right text-[var(--text-dim)]">—</td>;
  }
  return (
    <td className={clsx(
      "px-1 py-1.5 text-right tabular-nums font-medium",
      approaching ? "text-amber-400" : "text-[var(--text-muted)]",
    )}>
      {value.toFixed(1)}d
    </td>
  );
}

function rsiBg(rsi: number): string {
  if (rsi <= 25) return "text-red-400 font-bold";
  if (rsi <= 35) return "text-red-400";
  if (rsi >= 70) return "text-green-400";
  return "text-[var(--text)]";
}
