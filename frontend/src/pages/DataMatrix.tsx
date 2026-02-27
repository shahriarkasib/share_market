import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Table2, Loader2, Search, ChevronLeft, ChevronRight } from "lucide-react";
import { clsx } from "clsx";
import type { StockPrice } from "../types/index.ts";
import { fetchAllPrices } from "../api/client.ts";
import {
  formatNumber,
  formatPct,
  formatChange,
  formatCompact,
  colorBySign,
} from "../lib/format.ts";

/* ---------- Column definition ---------- */

interface Column {
  key: keyof StockPrice;
  label: string;
  align: "left" | "right";
  /** Render cell value. Falls back to raw toString. */
  render: (row: StockPrice) => string;
  /** Extra CSS class for the cell value. */
  cellClass?: (row: StockPrice) => string;
}

const columns: Column[] = [
  { key: "symbol", label: "Symbol", align: "left", render: (r) => r.symbol },
  { key: "ltp", label: "LTP", align: "right", render: (r) => formatNumber(r.ltp) },
  {
    key: "change",
    label: "Change",
    align: "right",
    render: (r) => formatChange(r.change),
    cellClass: (r) => colorBySign(r.change),
  },
  {
    key: "change_pct",
    label: "Change%",
    align: "right",
    render: (r) => formatPct(r.change_pct),
    cellClass: (r) => colorBySign(r.change_pct),
  },
  { key: "open", label: "Open", align: "right", render: (r) => formatNumber(r.open) },
  { key: "high", label: "High", align: "right", render: (r) => formatNumber(r.high) },
  { key: "low", label: "Low", align: "right", render: (r) => formatNumber(r.low) },
  {
    key: "close_prev",
    label: "Prev Close",
    align: "right",
    render: (r) => formatNumber(r.close_prev),
  },
  {
    key: "volume",
    label: "Volume",
    align: "right",
    render: (r) => formatCompact(r.volume),
  },
  {
    key: "value",
    label: "Turnover",
    align: "right",
    render: (r) => formatCompact(r.value),
  },
  {
    key: "trade_count",
    label: "Trades",
    align: "right",
    render: (r) => formatNumber(r.trade_count),
  },
];

/* ---------- Sort indicator ---------- */

function SortArrow({ active, dir }: { active: boolean; dir: "asc" | "desc" }) {
  if (!active) return <span className="text-transparent select-none ml-1">▲</span>;
  return (
    <span className="ml-1 text-blue-400">
      {dir === "asc" ? "▲" : "▼"}
    </span>
  );
}

/* ---------- Component ---------- */

const PER_PAGE = 50;

export default function DataMatrix() {
  const navigate = useNavigate();

  /* ---- state ---- */
  const [data, setData] = useState<StockPrice[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState<keyof StockPrice>("value");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(1);

  /* ---- fetch ---- */
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetchAllPrices()
      .then((prices) => {
        if (!cancelled) setData(prices);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "Failed to load prices");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  /* ---- derived ---- */
  const filtered = useMemo(() => {
    let items = data;
    if (search) {
      const q = search.toLowerCase();
      items = items.filter((s) => s.symbol.toLowerCase().includes(q));
    }
    items = [...items].sort((a, b) => {
      const av = a[sortBy] ?? 0;
      const bv = b[sortBy] ?? 0;
      if (av < bv) return sortDir === "asc" ? -1 : 1;
      if (av > bv) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
    return items;
  }, [data, search, sortBy, sortDir]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PER_PAGE));
  const safePage = Math.min(page, totalPages);
  const pageItems = filtered.slice((safePage - 1) * PER_PAGE, safePage * PER_PAGE);

  /* Reset page when search or sort changes */
  useEffect(() => {
    setPage(1);
  }, [search, sortBy, sortDir]);

  /* ---- handlers ---- */
  function handleSort(key: keyof StockPrice) {
    if (sortBy === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(key);
      setSortDir("desc");
    }
  }

  /* ---- render ---- */
  return (
    <div className="space-y-3">
      {/* Header area */}
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-center gap-2">
          <h1 className="text-sm font-semibold text-[var(--text)] flex items-center gap-2">
            <Table2 className="h-4 w-4 text-blue-500" />
            Data Matrix
          </h1>
          <span className="text-[10px] bg-[var(--surface-active)] text-[var(--text-muted)] px-2 py-0.5 rounded-full tabular-nums">
            {search ? `${filtered.length} / ${data.length}` : data.length}
          </span>
        </div>

        {/* Search */}
        <div className="relative w-56">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-[var(--text-dim)]" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Filter by symbol..."
            className="w-full bg-[var(--surface)] border border-[var(--border)] rounded-md pl-8 pr-3 py-1.5 text-xs text-[var(--text)] placeholder-[var(--text-dim)] focus:outline-none focus:border-blue-500"
          />
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="bg-red-900/20 border border-red-800/40 rounded-lg px-4 py-2.5 text-xs text-red-400">
          {error}
        </div>
      )}

      {/* Table container */}
      <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center gap-2 py-16">
            <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
            <span className="text-xs text-[var(--text-muted)]">Loading all stocks...</span>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              {/* Sticky header */}
              <thead>
                <tr className="sticky top-0 z-10 bg-[var(--surface-active)] border-b border-[var(--border)]">
                  {columns.map((col) => (
                    <th
                      key={col.key}
                      onClick={() => handleSort(col.key)}
                      className={clsx(
                        "px-3 py-2 font-medium text-[var(--text-muted)] whitespace-nowrap select-none cursor-pointer hover:text-[var(--text)] transition-colors",
                        col.align === "right" ? "text-right" : "text-left",
                      )}
                    >
                      {col.label}
                      <SortArrow active={sortBy === col.key} dir={sortDir} />
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
                      No stocks match your search
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
                            "px-3 py-1.5 whitespace-nowrap tabular-nums",
                            col.align === "right" ? "text-right" : "text-left",
                            col.key === "symbol"
                              ? "font-medium text-[var(--text)]"
                              : "text-[var(--text-muted)]",
                            col.cellClass?.(row),
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

        {/* Pagination */}
        {!loading && filtered.length > PER_PAGE && (
          <div className="flex items-center justify-between px-4 py-2 border-t border-[var(--border)]">
            <span className="text-[10px] text-[var(--text-dim)] tabular-nums">
              {(safePage - 1) * PER_PAGE + 1}--{Math.min(safePage * PER_PAGE, filtered.length)} of{" "}
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
