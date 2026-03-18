import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Table2,
  Loader2,
  Search,
  ChevronLeft,
  ChevronRight,
  Filter,
  ArrowUpDown,
  X,
} from "lucide-react";
import { clsx } from "clsx";
import type { MatrixStock } from "../types/index.ts";
import { fetchMatrixData } from "../api/client.ts";
import {
  formatNumber,
  formatPct,
  formatChange,
  formatCompact,
  colorBySign,
} from "../lib/format.ts";

/* ── Action badge helpers ── */

const ACTION_COLORS: Record<string, string> = {
  "BUY (strong)": "bg-green-500/20 text-green-300 border-green-500/40",
  BUY: "bg-green-500/15 text-green-400 border-green-500/30",
  "BUY on pullback": "bg-teal-500/15 text-teal-400 border-teal-500/30",
  "BUY on dip": "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
  "BUY (wait for MACD cross)": "bg-amber-500/15 text-amber-400 border-amber-500/30",
  "HOLD/WAIT": "bg-blue-500/15 text-blue-400 border-blue-500/30",
  "SELL/AVOID": "bg-red-500/15 text-red-400 border-red-500/30",
  AVOID: "bg-red-500/15 text-red-400 border-red-500/30",
};

function actionBadgeClass(action: string): string {
  return ACTION_COLORS[action] || "bg-[var(--surface)] text-[var(--text-dim)] border-[var(--border)]";
}

function actionShort(action: string): string {
  if (action.includes("strong")) return "S.BUY";
  if (action.includes("pullback")) return "PULL";
  if (action.includes("dip")) return "DIP";
  if (action.includes("MACD")) return "MACD";
  if (action.includes("HOLD")) return "HOLD";
  if (action.includes("SELL") || action.includes("AVOID")) return "SELL";
  if (action === "BUY") return "BUY";
  return action.slice(0, 5);
}

/* ── Column definition ── */

interface Column {
  key: string;
  label: string;
  shortLabel?: string;
  align: "left" | "right" | "center";
  sortable: boolean;
  width?: string;
  render: (row: MatrixStock) => React.ReactNode;
  getValue: (row: MatrixStock) => number | string;
}

const columns: Column[] = [
  {
    key: "symbol",
    label: "Symbol",
    align: "left",
    sortable: true,
    width: "w-20",
    render: (r) => (
      <span className="font-semibold text-[var(--text)]">{r.symbol}</span>
    ),
    getValue: (r) => r.symbol,
  },
  {
    key: "category",
    label: "Cat",
    align: "center",
    sortable: true,
    width: "w-8",
    render: (r) => (
      <span
        className={clsx(
          "text-[9px] px-1.5 py-0.5 rounded font-bold",
          r.category === "A"
            ? "bg-blue-500/15 text-blue-400"
            : r.category === "B"
              ? "bg-amber-500/15 text-amber-400"
              : "bg-red-500/15 text-red-400",
        )}
      >
        {r.category || "–"}
      </span>
    ),
    getValue: (r) => r.category,
  },
  {
    key: "ltp",
    label: "LTP",
    align: "right",
    sortable: true,
    render: (r) => formatNumber(r.ltp),
    getValue: (r) => r.ltp,
  },
  {
    key: "change_pct",
    label: "Chg%",
    align: "right",
    sortable: true,
    render: (r) => (
      <span className={colorBySign(r.change_pct)}>{formatPct(r.change_pct)}</span>
    ),
    getValue: (r) => r.change_pct,
  },
  {
    key: "ai_action",
    label: "AI Signal",
    align: "center",
    sortable: true,
    render: (r) =>
      r.ai_action ? (
        <span
          className={clsx(
            "text-[9px] px-1.5 py-0.5 rounded font-bold border inline-block",
            actionBadgeClass(r.ai_action),
          )}
        >
          {actionShort(r.ai_action)}
        </span>
      ) : (
        <span className="text-[var(--text-dim)]">–</span>
      ),
    getValue: (r) => {
      const a = r.ai_action.toUpperCase();
      if (a.includes("STRONG") && a.includes("BUY")) return 1;
      if (a === "BUY") return 2;
      if (a.includes("BUY")) return 3;
      if (a.includes("HOLD")) return 5;
      if (a.includes("SELL") || a.includes("AVOID")) return 8;
      return 6;
    },
  },
  {
    key: "score",
    label: "Score",
    align: "right",
    sortable: true,
    render: (r) => (
      <span
        className={clsx(
          "font-semibold",
          r.score > 40
            ? "text-green-400"
            : r.score > 20
              ? "text-blue-400"
              : r.score > 0
                ? "text-amber-400"
                : "text-[var(--text-dim)]",
        )}
      >
        {r.score ? r.score.toFixed(0) : "–"}
      </span>
    ),
    getValue: (r) => r.score,
  },
  {
    key: "composite_score",
    label: "Rank",
    shortLabel: "Rank",
    align: "right",
    sortable: true,
    render: (r) => (
      <span
        className={clsx(
          "font-bold",
          r.composite_score >= 60
            ? "text-emerald-400"
            : r.composite_score >= 40
              ? "text-green-400"
              : r.composite_score >= 25
                ? "text-amber-400"
                : "text-[var(--text-dim)]",
        )}
      >
        {r.composite_score ? r.composite_score.toFixed(0) : "–"}
      </span>
    ),
    getValue: (r) => r.composite_score,
  },
  {
    key: "rsi",
    label: "RSI",
    align: "right",
    sortable: true,
    render: (r) => (
      <span
        className={clsx(
          r.rsi > 0 && r.rsi < 30
            ? "text-green-400 font-semibold"
            : r.rsi > 70
              ? "text-red-400 font-semibold"
              : "text-[var(--text-muted)]",
        )}
      >
        {r.rsi > 0 ? r.rsi.toFixed(0) : "–"}
      </span>
    ),
    getValue: (r) => r.rsi,
  },
  {
    key: "macd_status",
    label: "MACD",
    align: "center",
    sortable: true,
    render: (r) => {
      if (!r.macd_status) return <span className="text-[var(--text-dim)]">–</span>;
      const m = r.macd_status.toLowerCase();
      return (
        <span
          className={clsx(
            "text-[10px] font-medium",
            m.includes("bullish")
              ? "text-green-400"
              : m.includes("bearish")
                ? "text-red-400"
                : "text-amber-400",
          )}
        >
          {m.includes("bullish") ? "Bull" : m.includes("bearish") ? "Bear" : "Ntrl"}
        </span>
      );
    },
    getValue: (r) => {
      const m = (r.macd_status || "").toLowerCase();
      if (m.includes("bullish")) return 1;
      if (m.includes("bearish")) return 3;
      return 2;
    },
  },
  {
    key: "vol_ratio",
    label: "Vol×",
    align: "right",
    sortable: true,
    render: (r) => (
      <span
        className={clsx(
          r.vol_ratio >= 2
            ? "text-green-400 font-semibold"
            : r.vol_ratio < 0.5
              ? "text-red-400"
              : "text-[var(--text-muted)]",
        )}
      >
        {r.vol_ratio > 0 ? r.vol_ratio.toFixed(1) : "–"}
      </span>
    ),
    getValue: (r) => r.vol_ratio,
  },
  {
    key: "reward_pct",
    label: "R:R",
    align: "right",
    sortable: true,
    render: (r) => {
      const rr =
        r.risk_pct && Math.abs(r.risk_pct) > 0
          ? Math.abs(r.reward_pct / r.risk_pct)
          : 0;
      if (rr === 0)
        return <span className="text-[var(--text-dim)]">–</span>;
      return (
        <span
          className={clsx(
            "font-medium",
            rr >= 2.5
              ? "text-green-400"
              : rr >= 1.5
                ? "text-amber-400"
                : "text-red-400",
          )}
        >
          1:{rr.toFixed(1)}
        </span>
      );
    },
    getValue: (r) =>
      r.risk_pct && Math.abs(r.risk_pct) > 0
        ? Math.abs(r.reward_pct / r.risk_pct)
        : 0,
  },
  {
    key: "bid_ask_ratio",
    label: "B/A",
    align: "right",
    sortable: true,
    render: (r) => {
      if (!r.bid_ask_ratio || r.bid_ask_ratio === 0)
        return <span className="text-[var(--text-dim)]">–</span>;
      return (
        <span
          className={clsx(
            "font-medium",
            r.bid_ask_ratio >= 2
              ? "text-green-400"
              : r.bid_ask_ratio >= 1.2
                ? "text-emerald-400"
                : r.bid_ask_ratio >= 0.8
                  ? "text-[var(--text-muted)]"
                  : "text-red-400",
          )}
        >
          {r.bid_ask_ratio.toFixed(1)}
        </span>
      );
    },
    getValue: (r) => r.bid_ask_ratio || 0,
  },
  {
    key: "value",
    label: "Turnover",
    align: "right",
    sortable: true,
    render: (r) => <span className="text-[var(--text-muted)]">{formatCompact(r.value)}</span>,
    getValue: (r) => r.value,
  },
  {
    key: "volume",
    label: "Volume",
    align: "right",
    sortable: true,
    render: (r) => <span className="text-[var(--text-muted)]">{formatCompact(r.volume)}</span>,
    getValue: (r) => r.volume,
  },
];

/* ── Sort indicator ── */

function SortArrow({
  active,
  dir,
}: {
  active: boolean;
  dir: "asc" | "desc";
}) {
  if (!active) return null;
  return (
    <span className="ml-0.5 text-blue-400">{dir === "asc" ? "▲" : "▼"}</span>
  );
}

/* ── Preset sort modes ── */

type SortPreset = {
  key: string;
  label: string;
  sortKey: string;
  dir: "asc" | "desc";
  desc: string;
};

const SORT_PRESETS: SortPreset[] = [
  { key: "best", label: "Best Opportunities", sortKey: "composite_score", dir: "desc", desc: "AI score + RSI + R:R + momentum" },
  { key: "score", label: "Highest Score", sortKey: "score", dir: "desc", desc: "AI/Judge analysis score" },
  { key: "oversold", label: "Most Oversold", sortKey: "rsi", dir: "asc", desc: "Lowest RSI first" },
  { key: "movers", label: "Top Movers", sortKey: "change_pct", dir: "desc", desc: "Biggest gainers" },
  { key: "dippers", label: "Biggest Dips", sortKey: "change_pct", dir: "asc", desc: "Biggest drops (buy-the-dip)" },
  { key: "volume", label: "Most Active", sortKey: "value", dir: "desc", desc: "Highest turnover" },
];

/* ── Category filter ── */

const CATS = [
  { key: "", label: "All" },
  { key: "A", label: "A" },
  { key: "B", label: "B" },
  { key: "Z", label: "Z" },
];

/* ── Main component ── */

const PER_PAGE = 50;

export default function DataMatrix() {
  const navigate = useNavigate();

  const [data, setData] = useState<MatrixStock[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState("composite_score");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(1);
  const [catFilter, setCatFilter] = useState("");
  const [actionFilter, setActionFilter] = useState("");

  /* ── Fetch ── */
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetchMatrixData()
      .then((stocks) => {
        if (!cancelled) setData(stocks);
      })
      .catch((err) => {
        if (!cancelled)
          setError(err instanceof Error ? err.message : "Failed to load");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  /* ── Action options for filter ── */
  const actionOptions = useMemo(() => {
    const set = new Set<string>();
    for (const s of data) {
      if (s.ai_action) set.add(s.ai_action);
    }
    return [...set].sort();
  }, [data]);

  /* ── Filtered + sorted ── */
  const filtered = useMemo(() => {
    let items = data;
    if (search) {
      const q = search.toLowerCase();
      items = items.filter(
        (s) =>
          s.symbol.toLowerCase().includes(q) ||
          (s.sector && s.sector.toLowerCase().includes(q)),
      );
    }
    if (catFilter) {
      items = items.filter((s) => s.category === catFilter);
    }
    if (actionFilter) {
      items = items.filter((s) => s.ai_action === actionFilter);
    }

    const col = columns.find((c) => c.key === sortBy);
    if (col) {
      items = [...items].sort((a, b) => {
        const av = col.getValue(a);
        const bv = col.getValue(b);
        if (av < bv) return sortDir === "asc" ? -1 : 1;
        if (av > bv) return sortDir === "asc" ? 1 : -1;
        return 0;
      });
    }
    return items;
  }, [data, search, sortBy, sortDir, catFilter, actionFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PER_PAGE));
  const safePage = Math.min(page, totalPages);
  const pageItems = filtered.slice(
    (safePage - 1) * PER_PAGE,
    safePage * PER_PAGE,
  );

  useEffect(() => {
    setPage(1);
  }, [search, sortBy, sortDir, catFilter, actionFilter]);

  function handleSort(key: string) {
    if (sortBy === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(key);
      // Smart default: ascending for RSI (lower=better), descending for everything else
      setSortDir(key === "rsi" ? "asc" : "desc");
    }
  }

  function applyPreset(preset: SortPreset) {
    setSortBy(preset.sortKey);
    setSortDir(preset.dir);
  }

  const hasFilters = catFilter || actionFilter || search;

  return (
    <div className="space-y-3">
      {/* ── Header ── */}
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-center gap-2">
          <h1 className="text-sm font-semibold text-[var(--text)] flex items-center gap-2">
            <Table2 className="h-4 w-4 text-blue-500" />
            Data Matrix
          </h1>
          <span className="text-[10px] bg-[var(--surface-active)] text-[var(--text-muted)] px-2 py-0.5 rounded-full tabular-nums">
            {hasFilters
              ? `${filtered.length} / ${data.length}`
              : data.length}
          </span>
        </div>

        <div className="relative w-full sm:w-56">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-[var(--text-dim)]" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search symbol or sector..."
            className="w-full bg-[var(--surface)] border border-[var(--border)] rounded-md pl-8 pr-3 py-1.5 text-xs text-[var(--text)] placeholder-[var(--text-dim)] focus:outline-none focus:border-blue-500"
          />
        </div>
      </div>

      {/* ── Filters row ── */}
      <div className="flex items-center gap-2 flex-wrap">
        {/* Category pills */}
        {CATS.map((c) => (
          <button
            key={c.key}
            onClick={() => setCatFilter(catFilter === c.key ? "" : c.key)}
            className={clsx(
              "px-2.5 py-1 rounded-md text-[10px] font-semibold border transition-colors",
              catFilter === c.key
                ? "bg-blue-500/15 text-blue-400 border-blue-500/30"
                : "text-[var(--text-dim)] border-[var(--border)] hover:text-[var(--text)] hover:bg-[var(--hover)]",
            )}
          >
            {c.label}
          </button>
        ))}

        <span className="text-[var(--border)]">|</span>

        {/* Action filter */}
        <select
          value={actionFilter}
          onChange={(e) => setActionFilter(e.target.value)}
          className="bg-[var(--surface)] border border-[var(--border)] rounded-md px-2 py-1 text-[10px] text-[var(--text)] focus:outline-none focus:ring-1 focus:ring-blue-500"
        >
          <option value="">All Signals</option>
          {actionOptions.map((a) => (
            <option key={a} value={a}>
              {a}
            </option>
          ))}
        </select>

        {hasFilters && (
          <button
            onClick={() => {
              setSearch("");
              setCatFilter("");
              setActionFilter("");
            }}
            className="flex items-center gap-1 px-2 py-1 rounded-md text-[10px] font-medium text-[var(--text-dim)] hover:text-[var(--text)] hover:bg-[var(--hover)] transition-colors"
          >
            <X className="h-3 w-3" /> Clear
          </button>
        )}
      </div>

      {/* ── Sort presets ── */}
      <div className="flex items-center gap-2 flex-wrap">
        <ArrowUpDown className="h-3.5 w-3.5 text-[var(--text-dim)]" />
        <span className="text-[10px] text-[var(--text-dim)] font-medium">Sort:</span>
        {SORT_PRESETS.map((p) => (
          <button
            key={p.key}
            onClick={() => applyPreset(p)}
            title={p.desc}
            className={clsx(
              "px-2 py-1 rounded text-[10px] font-medium border transition-colors",
              sortBy === p.sortKey && sortDir === p.dir
                ? "bg-blue-500/15 text-blue-400 border-blue-500/30"
                : "text-[var(--text-dim)] border-transparent hover:text-[var(--text)] hover:bg-[var(--hover)]",
            )}
          >
            {p.label}
          </button>
        ))}
      </div>

      {/* ── Error ── */}
      {error && (
        <div className="bg-red-900/20 border border-red-800/40 rounded-lg px-4 py-2.5 text-xs text-red-400">
          {error}
        </div>
      )}

      {/* ── Table ── */}
      <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center gap-2 py-16">
            <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
            <span className="text-xs text-[var(--text-muted)]">
              Loading matrix data...
            </span>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="sticky top-0 z-10 bg-[var(--surface-active)] border-b border-[var(--border)]">
                  {columns.map((col) => (
                    <th
                      key={col.key}
                      onClick={col.sortable ? () => handleSort(col.key) : undefined}
                      className={clsx(
                        "px-2 py-2 font-medium text-[var(--text-muted)] whitespace-nowrap select-none transition-colors",
                        col.sortable &&
                          "cursor-pointer hover:text-[var(--text)]",
                        col.align === "right"
                          ? "text-right"
                          : col.align === "center"
                            ? "text-center"
                            : "text-left",
                        col.width,
                      )}
                    >
                      {col.shortLabel || col.label}
                      {col.sortable && (
                        <SortArrow
                          active={sortBy === col.key}
                          dir={sortDir}
                        />
                      )}
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody className="divide-y divide-[var(--border)]">
                {pageItems.length === 0 ? (
                  <tr>
                    <td
                      colSpan={columns.length}
                      className="text-center text-[var(--text-dim)] py-12"
                    >
                      No stocks match your filters
                    </td>
                  </tr>
                ) : (
                  pageItems.map((row, idx) => (
                    <tr
                      key={row.symbol}
                      onClick={() => navigate(`/stock/${row.symbol}`)}
                      className={clsx(
                        "cursor-pointer transition-colors hover:bg-[var(--hover)]",
                        idx % 2 === 1 && "bg-[var(--bg)]/30",
                      )}
                    >
                      {columns.map((col) => (
                        <td
                          key={col.key}
                          className={clsx(
                            "px-2 py-1.5 whitespace-nowrap tabular-nums",
                            col.align === "right"
                              ? "text-right"
                              : col.align === "center"
                                ? "text-center"
                                : "text-left",
                          )}
                        >
                          {col.render(row)}
                        </td>
                      ))}
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        )}

        {/* ── Pagination ── */}
        {!loading && filtered.length > PER_PAGE && (
          <div className="flex items-center justify-between px-4 py-2 border-t border-[var(--border)]">
            <span className="text-[10px] text-[var(--text-dim)] tabular-nums">
              {(safePage - 1) * PER_PAGE + 1}–
              {Math.min(safePage * PER_PAGE, filtered.length)} of{" "}
              {filtered.length}
            </span>
            <div className="flex items-center gap-2">
              <button
                type="button"
                disabled={safePage <= 1}
                onClick={() => setPage((p) => Math.max(1, p - 1))}
                className="flex items-center gap-1 px-2 py-1 rounded text-[10px] font-medium text-[var(--text-muted)] hover:bg-[var(--hover)] disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
              >
                <ChevronLeft className="h-3 w-3" />
                Prev
              </button>
              <span className="text-[10px] text-[var(--text-muted)] tabular-nums">
                Page {safePage} of {totalPages}
              </span>
              <button
                type="button"
                disabled={safePage >= totalPages}
                onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                className="flex items-center gap-1 px-2 py-1 rounded text-[10px] font-medium text-[var(--text-muted)] hover:bg-[var(--hover)] disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
              >
                Next
                <ChevronRight className="h-3 w-3" />
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
