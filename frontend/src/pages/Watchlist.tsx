import { useCallback, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Eye, X, Loader2 } from "lucide-react";
import { clsx } from "clsx";
import type { WatchlistItem, StockPrice } from "../types/index.ts";
import {
  fetchWatchlist,
  addToWatchlist,
  removeFromWatchlist,
  fetchAllPrices,
} from "../api/client.ts";
import { formatNumber, formatPct, formatCompact, colorBySign } from "../lib/format.ts";
import SymbolSearch from "../components/search/SymbolSearch.tsx";

export default function Watchlist() {
  const navigate = useNavigate();
  const [items, setItems] = useState<WatchlistItem[]>([]);
  const [prices, setPrices] = useState<Map<string, StockPrice>>(new Map());
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [removingId, setRemovingId] = useState<number | null>(null);

  /* ---- Fetch watchlist + prices ---- */
  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [watchlistData, priceData] = await Promise.all([
        fetchWatchlist(),
        fetchAllPrices(),
      ]);
      setItems(watchlistData);
      const priceMap = new Map<string, StockPrice>();
      for (const p of priceData) {
        priceMap.set(p.symbol, p);
      }
      setPrices(priceMap);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load watchlist");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadData();
  }, [loadData]);

  /* ---- Add symbol ---- */
  const handleAdd = async (sym: string) => {
    if (!sym) return;

    if (items.some((i) => i.symbol === sym)) {
      setError(`${sym} is already in your watchlist`);
      return;
    }

    setAdding(true);
    setError(null);
    try {
      await addToWatchlist(sym);
      await loadData();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to add symbol");
    } finally {
      setAdding(false);
    }
  };

  /* ---- Remove symbol ---- */
  const handleRemove = async (id: number, symbol: string) => {
    setRemovingId(id);
    setError(null);
    try {
      await removeFromWatchlist(symbol);
      setItems((prev) => prev.filter((i) => i.id !== id));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to remove symbol");
    } finally {
      setRemovingId(null);
    }
  };

  return (
    <div className="space-y-4">
      {/* Header with inline add form */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-sm font-semibold text-[var(--text)] flex items-center gap-2">
          <Eye className="h-4 w-4 text-blue-500" />
          Watchlist
          <span className="text-[10px] text-[var(--text-dim)] font-normal">
            {items.length} stock{items.length !== 1 ? "s" : ""}
          </span>
        </h1>

        {/* Add with autocomplete */}
        <div className="w-56">
          <SymbolSearch
            onSelect={(sym) => void handleAdd(sym)}
            navigateOnSelect={false}
            placeholder={adding ? "Adding..." : "Search & add symbol..."}
            compact
          />
        </div>
      </div>

      {/* Error banner */}
      {error && (
        <div className="bg-red-900/20 border border-red-800/40 rounded-lg px-4 py-2.5 text-xs text-red-400 flex items-center justify-between">
          {error}
          <button
            type="button"
            onClick={() => setError(null)}
            className="text-red-500 hover:text-red-400"
          >
            <X className="h-3 w-3" />
          </button>
        </div>
      )}

      {/* Table */}
      <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center py-12 gap-2">
            <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
            <span className="text-xs text-[var(--text-muted)]">Loading watchlist...</span>
          </div>
        ) : items.length === 0 ? (
          <div className="text-center py-12 text-xs text-[var(--text-dim)]">
            Your watchlist is empty. Add a symbol above to start tracking.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider border-b border-[var(--border)]">
                  <th className="text-left px-4 py-2">Symbol</th>
                  <th className="text-right px-3 py-2">LTP</th>
                  <th className="text-right px-3 py-2">Chg%</th>
                  <th className="text-right px-3 py-2">High</th>
                  <th className="text-right px-3 py-2">Low</th>
                  <th className="text-right px-3 py-2">Volume</th>
                  <th className="text-left px-3 py-2">Added</th>
                  <th className="px-3 py-2 w-8" />
                </tr>
              </thead>
              <tbody>
                {items.map((item) => {
                  const p = prices.get(item.symbol);
                  const changePct = p?.change_pct ?? 0;
                  return (
                    <tr
                      key={item.id}
                      className="border-b border-[var(--border)] hover:bg-[var(--hover)] transition-colors"
                    >
                      <td
                        className="px-4 py-2 font-medium text-[var(--text)] cursor-pointer hover:text-blue-400 transition-colors"
                        onClick={() => navigate(`/stock/${item.symbol}`)}
                      >
                        {item.symbol}
                      </td>
                      <td className="px-3 py-2 text-right text-[var(--text)] tabular-nums">
                        {p ? formatNumber(p.ltp) : "--"}
                      </td>
                      <td
                        className={clsx(
                          "px-3 py-2 text-right font-medium tabular-nums",
                          p ? colorBySign(changePct) : "text-[var(--text-dim)]",
                        )}
                      >
                        {p ? formatPct(changePct) : "--"}
                      </td>
                      <td className="px-3 py-2 text-right text-[var(--text-muted)] tabular-nums">
                        {p ? formatNumber(p.high) : "--"}
                      </td>
                      <td className="px-3 py-2 text-right text-[var(--text-muted)] tabular-nums">
                        {p ? formatNumber(p.low) : "--"}
                      </td>
                      <td className="px-3 py-2 text-right text-[var(--text-muted)] tabular-nums">
                        {p ? formatCompact(p.volume) : "--"}
                      </td>
                      <td className="px-3 py-2 text-[var(--text-dim)] text-[10px]">
                        {new Date(item.added_at).toLocaleDateString()}
                      </td>
                      <td className="px-3 py-2 text-right">
                        <button
                          type="button"
                          onClick={() => void handleRemove(item.id, item.symbol)}
                          disabled={removingId === item.id}
                          className="text-[var(--text-dim)] hover:text-red-400 transition-colors disabled:opacity-50"
                          title="Remove"
                        >
                          {removingId === item.id ? (
                            <Loader2 className="h-3 w-3 animate-spin" />
                          ) : (
                            <X className="h-3 w-3" />
                          )}
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
