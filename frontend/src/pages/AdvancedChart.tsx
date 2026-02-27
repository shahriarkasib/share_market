import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { Loader2, TrendingUp, TrendingDown, Target, ShieldAlert } from "lucide-react";
import { clsx } from "clsx";
import SymbolSearch from "../components/search/SymbolSearch.tsx";
import PriceChart from "../components/chart/PriceChart.tsx";
import { fetchStockSignal, fetchStockPrice } from "../api/client.ts";
import type { StockSignal, StockPrice } from "../types/index.ts";

const DEFAULT_SYMBOL = "ROBI";

const SIGNAL_COLORS: Record<string, string> = {
  STRONG_BUY: "text-emerald-400",
  BUY: "text-green-400",
  HOLD: "text-yellow-400",
  SELL: "text-red-400",
  STRONG_SELL: "text-red-500",
};

const SIGNAL_BG: Record<string, string> = {
  STRONG_BUY: "bg-emerald-500/15 border-emerald-500/30",
  BUY: "bg-green-500/15 border-green-500/30",
  HOLD: "bg-yellow-500/15 border-yellow-500/30",
  SELL: "bg-red-500/15 border-red-500/30",
  STRONG_SELL: "bg-red-600/15 border-red-600/30",
};

export default function AdvancedChart() {
  const [searchParams, setSearchParams] = useSearchParams();
  const symbolParam = searchParams.get("symbol");
  const [currentSymbol, setCurrentSymbol] = useState(
    symbolParam || DEFAULT_SYMBOL,
  );

  const [signal, setSignal] = useState<StockSignal | null>(null);
  const [price, setPrice] = useState<StockPrice | null>(null);
  const [headerLoading, setHeaderLoading] = useState(true);

  const handleSymbolSelect = (symbol: string) => {
    setCurrentSymbol(symbol);
    setSearchParams({ symbol });
  };

  // Fetch signal + price data for header
  useEffect(() => {
    let cancelled = false;
    setHeaderLoading(true);

    Promise.allSettled([
      fetchStockSignal(currentSymbol),
      fetchStockPrice(currentSymbol),
    ]).then(([sigResult, priceResult]) => {
      if (cancelled) return;
      setSignal(sigResult.status === "fulfilled" ? sigResult.value : null);
      setPrice(priceResult.status === "fulfilled" ? priceResult.value : null);
      setHeaderLoading(false);
    });

    return () => { cancelled = true; };
  }, [currentSymbol]);

  const ltp = signal?.ltp ?? price?.ltp ?? 0;
  const changePct = signal?.change_pct ?? price?.change_pct ?? 0;
  const isUp = changePct >= 0;

  return (
    <div className="space-y-2">
      {/* Header */}
      <div className="flex items-center justify-between gap-3 flex-wrap">
        {/* Left: Symbol info */}
        <div className="flex items-center gap-3 flex-wrap">
          <div>
            <h1 className="text-lg font-bold text-[var(--text)] leading-tight">
              {currentSymbol}
            </h1>
            {(signal?.company_name || price?.company_name) && (
              <p className="text-[11px] text-[var(--text-muted)] leading-tight">
                {signal?.company_name || price?.company_name}
              </p>
            )}
          </div>

          {headerLoading ? (
            <Loader2 className="h-4 w-4 animate-spin text-[var(--text-muted)]" />
          ) : (
            <>
              {/* LTP + change */}
              <div className="flex items-center gap-2">
                <span className="text-lg font-bold tabular-nums text-[var(--text)]">
                  {ltp > 0 ? `৳${ltp.toFixed(2)}` : "—"}
                </span>
                <span
                  className={clsx(
                    "flex items-center gap-0.5 text-xs font-semibold tabular-nums",
                    isUp ? "text-green-400" : "text-red-400",
                  )}
                >
                  {isUp ? <TrendingUp className="h-3.5 w-3.5" /> : <TrendingDown className="h-3.5 w-3.5" />}
                  {isUp ? "+" : ""}{changePct.toFixed(2)}%
                </span>
              </div>

              {/* Signal badge */}
              {signal && (
                <span
                  className={clsx(
                    "px-2 py-0.5 rounded text-[10px] font-bold border",
                    SIGNAL_BG[signal.signal_type] ?? "bg-[var(--surface-active)] border-[var(--border)]",
                    SIGNAL_COLORS[signal.signal_type] ?? "text-[var(--text)]",
                  )}
                >
                  {signal.signal_type.replace("_", " ")}
                </span>
              )}

              {/* Key stats */}
              {signal && (
                <div className="hidden sm:flex items-center gap-3 text-[10px] text-[var(--text-muted)]">
                  {signal.target_price > 0 && (
                    <span className="flex items-center gap-0.5">
                      <Target className="h-3 w-3 text-blue-400" />
                      Target: ৳{signal.target_price.toFixed(2)}
                    </span>
                  )}
                  {signal.stop_loss > 0 && (
                    <span className="flex items-center gap-0.5">
                      <ShieldAlert className="h-3 w-3 text-red-400" />
                      SL: ৳{signal.stop_loss.toFixed(2)}
                    </span>
                  )}
                  {signal.indicators?.rsi != null && (
                    <span>RSI: {signal.indicators.rsi.toFixed(1)}</span>
                  )}
                  {signal.confidence > 0 && (
                    <span>Conf: {(signal.confidence * 100).toFixed(0)}%</span>
                  )}
                </div>
              )}
            </>
          )}
        </div>

        {/* Right: Symbol search */}
        <div className="w-full sm:w-64">
          <SymbolSearch
            placeholder="Change symbol..."
            compact
            navigateOnSelect={false}
            onSelect={handleSymbolSelect}
          />
        </div>
      </div>

      {/* Full-page chart */}
      <PriceChart
        symbol={currentSymbol}
        signal={signal}
        height={Math.max(
          400,
          typeof window !== "undefined" ? window.innerHeight - 200 : 600,
        )}
      />
    </div>
  );
}
