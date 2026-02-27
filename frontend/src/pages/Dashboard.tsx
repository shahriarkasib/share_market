import { useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { clsx } from "clsx";
import { Loader2, TrendingUp, TrendingDown, ShieldCheck, AlertTriangle } from "lucide-react";
import { useMarketStore } from "../store/marketStore.ts";
import { useAutoRefresh } from "../hooks/useAutoRefresh.ts";
import MarketBar from "../components/dashboard/MarketBar.tsx";
import MarketBreadth from "../components/dashboard/MarketBreadth.tsx";
import DSEXChart from "../components/dashboard/DSEXChart.tsx";
import SignalsTable from "../components/dashboard/SignalsTable.tsx";
import MostActiveTabs from "../components/dashboard/MostActiveTabs.tsx";
import type { StockSignal, ExitAlert } from "../types/index.ts";
import { formatNumber, formatPct, colorBySign } from "../lib/format.ts";

export default function Dashboard() {
  const {
    marketSummary,
    topBuySignals,
    topSellSignals,
    allPrices,
    signalsSummary,
    suggestions,
    dsexHistory,
    isLoading,
    error,
    fetchDashboard,
  } = useMarketStore();

  const fetchFn = useCallback(() => fetchDashboard(), [fetchDashboard]);

  const { secondsToRefresh, refresh, isRefreshing } = useAutoRefresh({
    fetchFn,
    intervalMs: 300_000,
    immediate: true,
  });

  const signalsComputing =
    signalsSummary?.is_computing || signalsSummary?.total_stocks === 0;

  return (
    <div className="space-y-4">
      {/* Error banner */}
      {error && (
        <div className="bg-red-900/20 border border-red-800/40 rounded-lg px-4 py-2.5 text-xs text-red-400">
          {error}
        </div>
      )}

      {/* Market bar - single compact row */}
      <MarketBar
        market={marketSummary}
        signals={signalsSummary}
        secondsToRefresh={secondsToRefresh}
        isRefreshing={isRefreshing || isLoading}
        onRefresh={refresh}
      />

      {/* Market breadth bar */}
      {marketSummary && (
        <MarketBreadth
          advances={marketSummary.advances}
          declines={marketSummary.declines}
          unchanged={marketSummary.unchanged}
        />
      )}

      {/* DSEX index chart */}
      {dsexHistory.length > 0 && <DSEXChart data={dsexHistory} />}

      {/* Entry & Exit suggestion panels */}
      {suggestions && (suggestions.entry.length > 0 || suggestions.exit.length > 0) && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {suggestions.entry.length > 0 && (
            <EntryPicksPanel picks={suggestions.entry} />
          )}
          {suggestions.exit.length > 0 && (
            <ExitAlertsPanel alerts={suggestions.exit} />
          )}
        </div>
      )}

      {/* Main grid: signals (2/3) + movers (1/3) */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Left: Buy and Sell signal tables */}
        <div className="lg:col-span-2 space-y-4">
          <SignalsTable
            signals={topBuySignals}
            type="buy"
            title="Buy Signals"
            isComputing={signalsComputing && topBuySignals.length === 0}
          />
          <SignalsTable
            signals={topSellSignals}
            type="sell"
            title="Sell Signals"
            isComputing={signalsComputing && topSellSignals.length === 0}
          />
        </div>

        {/* Right: Most active tabs */}
        <div>
          <MostActiveTabs prices={allPrices} limit={10} />
        </div>
      </div>

      {/* Full-screen loading overlay for initial load */}
      {isLoading && !marketSummary && <LoadingOverlay />}
    </div>
  );
}

/* ================================================================== */
/*  Top Entry Picks Panel                                              */
/* ================================================================== */

function EntryPicksPanel({ picks }: { picks: StockSignal[] }) {
  const navigate = useNavigate();

  return (
    <section className="bg-[var(--surface)] border border-[var(--border)] rounded-lg border-l-2 border-l-emerald-500 overflow-hidden">
      <div className="px-4 py-2.5 border-b border-[var(--border)] flex items-center gap-2">
        <TrendingUp className="h-3.5 w-3.5 text-emerald-400" />
        <h2 className="text-xs font-semibold uppercase tracking-wider text-emerald-400">
          Top Entry Picks
        </h2>
        <span className="text-[10px] text-[var(--text-dim)] ml-auto">
          {picks.length} stock{picks.length !== 1 ? "s" : ""}
        </span>
      </div>
      <div className="divide-y divide-[var(--border)]">
        {picks.map((s, idx) => (
          <div
            key={s.symbol}
            onClick={() => navigate(`/stock/${s.symbol}`)}
            className="hover:bg-[var(--hover)] cursor-pointer transition-colors px-4 py-2.5"
          >
            {/* Row 1: rank, symbol, price, T+2 badge */}
            <div className="flex items-center gap-2 mb-1">
              <span className="text-[10px] text-[var(--text-dim)] tabular-nums w-4">
                {idx + 1}
              </span>
              <span className="font-medium text-sm text-[var(--text)] w-24 shrink-0">
                {s.symbol}
              </span>
              <span className="text-xs text-[var(--text)] tabular-nums">
                {formatNumber(s.ltp)}
              </span>
              <span className={clsx("text-xs font-medium tabular-nums", colorBySign(s.change_pct))}>
                {formatPct(s.change_pct)}
              </span>
              <div className="ml-auto flex items-center gap-2">
                {s.t2_safe != null && (
                  <span
                    className={clsx(
                      "inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium",
                      s.t2_safe
                        ? "bg-green-500/15 text-green-400 border border-green-500/30"
                        : "bg-amber-500/15 text-amber-400 border border-amber-500/30",
                    )}
                  >
                    {s.t2_safe ? <ShieldCheck className="h-2.5 w-2.5" /> : null}
                    T+2 {s.t2_safe ? "Safe" : "Caution"}
                  </span>
                )}
                {s.expected_return_pct != null && (
                  <span className={clsx("text-xs font-bold tabular-nums", colorBySign(s.expected_return_pct))}>
                    {s.expected_return_pct > 0 ? "+" : ""}{s.expected_return_pct.toFixed(1)}%
                  </span>
                )}
              </div>
            </div>

            {/* Row 2: entry strategy */}
            {s.entry_strategy && (
              <div className="flex items-start gap-1.5 ml-6 mb-1">
                <span className="text-[10px] font-semibold uppercase shrink-0 mt-px text-emerald-500">
                  Entry:
                </span>
                <span className="text-[11px] text-[var(--text-muted)] truncate">
                  {s.entry_strategy}
                </span>
              </div>
            )}

            {/* Row 3: meta pills */}
            <div className="flex items-center gap-2 ml-6">
              <span className="text-[10px] text-[var(--text-dim)]">
                Target <span className="text-green-500 tabular-nums">{formatNumber(s.target_price)}</span>
              </span>
              <span className="text-[10px] text-[var(--text-dim)]">
                Stop <span className="text-red-500 tabular-nums">{formatNumber(s.stop_loss)}</span>
              </span>
              {s.hold_days != null && (
                <span className="text-[10px] text-[var(--text-dim)]">
                  Hold <span className="text-blue-400 tabular-nums">{s.hold_days}d</span>
                </span>
              )}
              <span className="text-[10px] text-[var(--text-dim)]">
                Conf <span className="text-[var(--text-muted)] tabular-nums">{(s.confidence * 100).toFixed(0)}%</span>
              </span>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

/* ================================================================== */
/*  Exit Alerts Panel                                                   */
/* ================================================================== */

function ExitAlertsPanel({ alerts }: { alerts: ExitAlert[] }) {
  const navigate = useNavigate();

  return (
    <section className="bg-[var(--surface)] border border-[var(--border)] rounded-lg border-l-2 border-l-red-500 overflow-hidden">
      <div className="px-4 py-2.5 border-b border-[var(--border)] flex items-center gap-2">
        <TrendingDown className="h-3.5 w-3.5 text-red-400" />
        <h2 className="text-xs font-semibold uppercase tracking-wider text-red-400">
          Exit Alerts
        </h2>
        <span className="text-[10px] text-[var(--text-dim)] ml-auto">
          {alerts.length} holding{alerts.length !== 1 ? "s" : ""}
        </span>
      </div>
      <div className="divide-y divide-[var(--border)]">
        {alerts.map((alert) => {
          const h = alert.holding;
          const sig = alert.signal;
          return (
            <div
              key={h.id}
              onClick={() => navigate(`/stock/${h.symbol}`)}
              className="hover:bg-[var(--hover)] cursor-pointer transition-colors px-4 py-2.5"
            >
              {/* Row 1: symbol, current price, P&L */}
              <div className="flex items-center gap-2 mb-1">
                <AlertTriangle className="h-3.5 w-3.5 text-amber-400 shrink-0" />
                <span className="font-medium text-sm text-[var(--text)] w-24 shrink-0">
                  {h.symbol}
                </span>
                {sig && (
                  <span className="text-xs text-[var(--text)] tabular-nums">
                    {formatNumber(sig.ltp)}
                  </span>
                )}
                <span className="text-xs text-[var(--text-muted)]">
                  bought at {formatNumber(h.buy_price)}
                </span>
                <div className="ml-auto flex items-center gap-2">
                  <span className={clsx(
                    "text-xs font-bold tabular-nums",
                    colorBySign(alert.pnl_pct),
                  )}>
                    {alert.pnl_pct > 0 ? "+" : ""}{alert.pnl_pct.toFixed(1)}%
                  </span>
                  {sig && (
                    <span className={clsx(
                      "px-1.5 py-0.5 rounded text-[10px] font-medium",
                      sig.signal_type === "STRONG_SELL"
                        ? "bg-red-500/20 text-red-400"
                        : sig.signal_type === "SELL"
                          ? "bg-red-500/15 text-red-400"
                          : "bg-[var(--surface-elevated)] text-[var(--text-muted)]",
                    )}>
                      {sig.signal_type}
                    </span>
                  )}
                </div>
              </div>

              {/* Row 2: reasons */}
              <div className="ml-6">
                {alert.reasons.map((r, i) => (
                  <span key={i} className="text-[11px] text-amber-400/80 mr-3">
                    {r}
                  </span>
                ))}
              </div>

              {/* Row 3: exit strategy if available */}
              {sig?.exit_strategy && (
                <div className="flex items-start gap-1.5 ml-6 mt-1">
                  <span className="text-[10px] font-semibold uppercase shrink-0 mt-px text-amber-500">
                    Exit:
                  </span>
                  <span className="text-[11px] text-[var(--text-muted)] truncate">
                    {sig.exit_strategy}
                  </span>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </section>
  );
}

/* ------------------------------------------------------------------ */

function LoadingOverlay() {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80">
      <div className="flex flex-col items-center gap-3">
        <Loader2 className="h-6 w-6 animate-spin text-blue-500" />
        <span className="text-xs text-[var(--text-muted)]">Loading market data...</span>
      </div>
    </div>
  );
}
