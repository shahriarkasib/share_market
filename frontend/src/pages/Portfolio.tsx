import { useCallback, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { BarChart3, Plus, X, Loader2, Trash2, AlertTriangle } from "lucide-react";
import { clsx } from "clsx";
import type { Holding, PortfolioSummary, PortfolioAlert } from "../types/index.ts";
import {
  fetchHoldings,
  addHolding,
  fetchPortfolioSummary,
  fetchPortfolioAlerts,
  deleteHolding,
} from "../api/client.ts";
import { formatNumber, formatBDT, formatPct, colorBySign } from "../lib/format.ts";
import SymbolSearch from "../components/search/SymbolSearch.tsx";

/* ---- Helpers ---- */

/** Return a human-readable maturity status string. */
function maturityLabel(h: Holding): string {
  if (h.is_mature) return "Mature";
  const buy = new Date(h.buy_date);
  const maturity = new Date(h.maturity_date);
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  // Total calendar days from buy to maturity
  const totalDays = Math.max(1, Math.round((maturity.getTime() - buy.getTime()) / 86_400_000));
  const elapsed = Math.round((today.getTime() - buy.getTime()) / 86_400_000);
  const dayNum = Math.max(1, Math.min(elapsed + 1, totalDays));
  return `Day ${dayNum}`;
}

/** Urgency color mapping. */
function urgencyColor(urgency: PortfolioAlert["urgency"]): string {
  switch (urgency) {
    case "HIGH":
      return "text-red-400 bg-red-900/20 border-red-800/40";
    case "MEDIUM":
      return "text-yellow-400 bg-yellow-900/20 border-yellow-800/40";
    case "LOW":
      return "text-blue-400 bg-blue-900/20 border-blue-800/40";
  }
}

function urgencyDot(urgency: PortfolioAlert["urgency"]): string {
  switch (urgency) {
    case "HIGH":
      return "bg-red-500";
    case "MEDIUM":
      return "bg-yellow-500";
    case "LOW":
      return "bg-blue-500";
  }
}

/** Today as YYYY-MM-DD for the date input default. */
function todayISO(): string {
  const d = new Date();
  return d.toISOString().slice(0, 10);
}

/* ---- Component ---- */

export default function Portfolio() {
  const navigate = useNavigate();

  /* Data state */
  const [holdings, setHoldings] = useState<Holding[]>([]);
  const [summary, setSummary] = useState<PortfolioSummary | null>(null);
  const [alerts, setAlerts] = useState<PortfolioAlert[]>([]);

  /* UI state */
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [removingId, setRemovingId] = useState<number | null>(null);

  /* Form state */
  const [formSymbol, setFormSymbol] = useState("");
  const [formQty, setFormQty] = useState("");
  const [formPrice, setFormPrice] = useState("");
  const [formDate, setFormDate] = useState(todayISO());
  const [formNotes, setFormNotes] = useState("");

  /* ---- Fetch all portfolio data ---- */
  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [holdingsData, summaryData, alertsData] = await Promise.all([
        fetchHoldings(),
        fetchPortfolioSummary(),
        fetchPortfolioAlerts(),
      ]);
      setHoldings(holdingsData);
      setSummary(summaryData);
      setAlerts(alertsData);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load portfolio");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadData();
  }, [loadData]);

  /* ---- Add holding ---- */
  const handleAdd = async () => {
    const sym = formSymbol.trim().toUpperCase();
    const qty = Number(formQty);
    const price = Number(formPrice);

    if (!sym || !qty || !price || !formDate) {
      setError("Symbol, quantity, buy price, and date are required");
      return;
    }
    if (qty <= 0 || price <= 0) {
      setError("Quantity and price must be positive numbers");
      return;
    }

    setAdding(true);
    setError(null);
    try {
      await addHolding({
        symbol: sym,
        quantity: qty,
        buy_price: price,
        buy_date: formDate,
        notes: formNotes.trim() || undefined,
      });
      setFormSymbol("");
      setFormQty("");
      setFormPrice("");
      setFormDate(todayISO());
      setFormNotes("");
      await loadData();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to add holding");
    } finally {
      setAdding(false);
    }
  };

  /* ---- Delete holding ---- */
  const handleDelete = async (id: number) => {
    if (!window.confirm("Delete this holding? This action cannot be undone.")) return;

    setRemovingId(id);
    setError(null);
    try {
      await deleteHolding(id);
      setHoldings((prev) => prev.filter((h) => h.id !== id));
      // Refresh summary after deletion
      const updatedSummary = await fetchPortfolioSummary();
      setSummary(updatedSummary);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete holding");
    } finally {
      setRemovingId(null);
    }
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center gap-2">
        <h1 className="text-sm font-semibold text-[var(--text)] flex items-center gap-2">
          <BarChart3 className="h-4 w-4 text-blue-500" />
          Portfolio
          <span className="text-[10px] text-[var(--text-dim)] font-normal">
            {holdings.length} holding{holdings.length !== 1 ? "s" : ""}
          </span>
        </h1>
      </div>

      {/* Summary bar */}
      {summary && !loading && (
        <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg px-4 py-2.5 flex flex-wrap items-center gap-x-5 gap-y-1 text-xs">
          <div className="text-[var(--text-muted)]">
            Invested:{" "}
            <span className="text-[var(--text)] font-medium">{formatBDT(summary.total_invested)}</span>
          </div>
          <div className="text-[var(--text-muted)]">
            Current:{" "}
            <span className="text-[var(--text)] font-medium">{formatBDT(summary.current_value)}</span>
          </div>
          <div className="text-[var(--text-muted)]">
            P&L:{" "}
            <span className={clsx("font-medium", colorBySign(summary.total_pnl))}>
              {summary.total_pnl >= 0 ? "+" : ""}
              {formatBDT(summary.total_pnl)} ({formatPct(summary.total_pnl_pct)})
            </span>
          </div>
          <div className="text-[var(--text-muted)]">
            <span className="text-[var(--text)] font-medium">{summary.active_holdings}</span> Active
          </div>
          <div className="text-[var(--text-muted)]">
            <span className="text-green-400 font-medium">{summary.mature_holdings}</span> Mature
          </div>
          {summary.at_risk_holdings > 0 && (
            <div className="text-[var(--text-muted)]">
              <span className="text-red-400 font-medium">{summary.at_risk_holdings}</span> At Risk
            </div>
          )}
        </div>
      )}

      {/* Alerts */}
      {alerts.length > 0 && !loading && (
        <div className="space-y-1.5">
          {alerts.map((alert, idx) => (
            <div
              key={`${alert.holding_id}-${alert.alert_type}-${idx}`}
              className={clsx(
                "border rounded-lg px-4 py-2 text-xs flex items-center gap-2",
                urgencyColor(alert.urgency),
              )}
            >
              <span className={clsx("h-2 w-2 rounded-full flex-shrink-0", urgencyDot(alert.urgency))} />
              <AlertTriangle className="h-3 w-3 flex-shrink-0" />
              <span className="font-medium">{alert.symbol}</span>
              <span className="text-inherit opacity-80">{alert.message}</span>
            </div>
          ))}
        </div>
      )}

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

      {/* Add holding form */}
      <form
        onSubmit={(e) => {
          e.preventDefault();
          void handleAdd();
        }}
        className="bg-[var(--surface)] border border-[var(--border)] rounded-lg px-4 py-3 flex flex-wrap items-end gap-2"
      >
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider">Symbol</label>
          <div className="w-40">
            <SymbolSearch
              onSelect={(sym) => setFormSymbol(sym)}
              navigateOnSelect={false}
              placeholder={formSymbol || "Search ticker..."}
              compact
            />
          </div>
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider">Qty</label>
          <input
            type="number"
            value={formQty}
            onChange={(e) => setFormQty(e.target.value)}
            placeholder="100"
            min="1"
            className="bg-[var(--bg)] border border-[var(--border)] rounded-md px-3 py-1.5 text-xs text-[var(--text)] placeholder-[var(--text-dim)] focus:outline-none focus:border-blue-500 w-20"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider">Buy Price</label>
          <input
            type="number"
            value={formPrice}
            onChange={(e) => setFormPrice(e.target.value)}
            placeholder="125.50"
            min="0.01"
            step="0.01"
            className="bg-[var(--bg)] border border-[var(--border)] rounded-md px-3 py-1.5 text-xs text-[var(--text)] placeholder-[var(--text-dim)] focus:outline-none focus:border-blue-500 w-24"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider">Buy Date</label>
          <input
            type="date"
            value={formDate}
            onChange={(e) => setFormDate(e.target.value)}
            className="bg-[var(--bg)] border border-[var(--border)] rounded-md px-3 py-1.5 text-xs text-[var(--text)] focus:outline-none focus:border-blue-500 w-32"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider">Notes</label>
          <input
            type="text"
            value={formNotes}
            onChange={(e) => setFormNotes(e.target.value)}
            placeholder="Optional"
            className="bg-[var(--bg)] border border-[var(--border)] rounded-md px-3 py-1.5 text-xs text-[var(--text)] placeholder-[var(--text-dim)] focus:outline-none focus:border-blue-500 w-32"
          />
        </div>
        <button
          type="submit"
          disabled={adding || !formSymbol.trim() || !formQty || !formPrice}
          className={clsx(
            "flex items-center gap-1 px-3 py-1.5 rounded-md text-xs font-medium transition-colors",
            "bg-blue-600 hover:bg-blue-500 text-white",
            "disabled:opacity-50 disabled:cursor-not-allowed",
          )}
        >
          {adding ? (
            <Loader2 className="h-3 w-3 animate-spin" />
          ) : (
            <Plus className="h-3 w-3" />
          )}
          Add
        </button>
      </form>

      {/* Holdings table */}
      <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center py-12 gap-2">
            <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
            <span className="text-xs text-[var(--text-muted)]">Loading portfolio...</span>
          </div>
        ) : holdings.length === 0 ? (
          <div className="text-center py-12 text-xs text-[var(--text-dim)]">
            No holdings yet. Add a stock purchase above to start tracking.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider border-b border-[var(--border)]">
                  <th className="text-left px-4 py-2">Symbol</th>
                  <th className="text-right px-3 py-2">Qty</th>
                  <th className="text-right px-3 py-2">Buy</th>
                  <th className="text-right px-3 py-2">Current</th>
                  <th className="text-right px-3 py-2">P&L</th>
                  <th className="text-right px-3 py-2">P&L%</th>
                  <th className="text-center px-3 py-2">Status</th>
                  <th className="text-left px-3 py-2">Rec</th>
                  <th className="px-3 py-2 w-8" />
                </tr>
              </thead>
              <tbody>
                {holdings.map((h) => (
                  <tr
                    key={h.id}
                    className="border-b border-[var(--border)] hover:bg-[var(--hover)] transition-colors"
                  >
                    {/* Symbol (clickable) */}
                    <td
                      className="px-4 py-2 font-medium text-[var(--text)] cursor-pointer hover:text-blue-400 transition-colors"
                      onClick={() => navigate(`/stock/${h.symbol}`)}
                    >
                      {h.symbol}
                    </td>

                    {/* Quantity */}
                    <td className="px-3 py-2 text-right text-[var(--text)] tabular-nums">
                      {h.remaining_quantity}
                    </td>

                    {/* Buy price */}
                    <td className="px-3 py-2 text-right text-[var(--text-muted)] tabular-nums">
                      {formatNumber(h.buy_price)}
                    </td>

                    {/* Current price */}
                    <td className="px-3 py-2 text-right text-[var(--text)] tabular-nums">
                      {formatNumber(h.current_price)}
                    </td>

                    {/* P&L */}
                    <td
                      className={clsx(
                        "px-3 py-2 text-right font-medium tabular-nums",
                        colorBySign(h.unrealized_pnl),
                      )}
                    >
                      {h.unrealized_pnl >= 0 ? "+" : ""}
                      {formatNumber(h.unrealized_pnl)}
                    </td>

                    {/* P&L% */}
                    <td
                      className={clsx(
                        "px-3 py-2 text-right font-medium tabular-nums",
                        colorBySign(h.unrealized_pnl_pct),
                      )}
                    >
                      {formatPct(h.unrealized_pnl_pct)}
                    </td>

                    {/* Maturity status */}
                    <td className="px-3 py-2 text-center">
                      <span
                        className={clsx(
                          "inline-block px-2 py-0.5 rounded-full text-[10px] font-medium",
                          h.is_mature
                            ? "bg-green-900/30 text-green-400 border border-green-800/40"
                            : "bg-yellow-900/30 text-yellow-400 border border-yellow-800/40",
                        )}
                      >
                        {maturityLabel(h)}
                      </span>
                    </td>

                    {/* Sell recommendation (truncated) */}
                    <td
                      className="px-3 py-2 text-[var(--text-muted)] max-w-[120px] truncate"
                      title={h.sell_recommendation}
                    >
                      {h.sell_recommendation || "--"}
                    </td>

                    {/* Delete button */}
                    <td className="px-3 py-2 text-right">
                      <button
                        type="button"
                        onClick={() => void handleDelete(h.id)}
                        disabled={removingId === h.id}
                        className="text-[var(--text-dim)] hover:text-red-400 transition-colors disabled:opacity-50"
                        title="Delete holding"
                      >
                        {removingId === h.id ? (
                          <Loader2 className="h-3 w-3 animate-spin" />
                        ) : (
                          <Trash2 className="h-3 w-3" />
                        )}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
