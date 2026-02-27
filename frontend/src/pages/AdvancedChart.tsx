import { useEffect, useState, useMemo, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import {
  Loader2,
  TrendingUp,
  TrendingDown,
  Target,
  ShieldAlert,
  ArrowLeft,
  Search,
  BarChart3,
  Activity,
} from "lucide-react";
import { clsx } from "clsx";
import PriceChart from "../components/chart/PriceChart.tsx";
import {
  fetchStockSignal,
  fetchAllPrices,
  fetchDSEXChart,
  fetchTopBuySignals,
  fetchTopSellSignals,
  type DSEXChartBar,
} from "../api/client.ts";
import type { StockSignal, StockPrice } from "../types/index.ts";

/* ─── DSEX lightweight-charts inline component ─── */
import { useRef } from "react";
import {
  createChart,
  LineSeries,
  HistogramSeries,
  type IChartApi,
  type Time,
} from "lightweight-charts";

function DSEXChart({
  data,
  height,
}: {
  data: DSEXChartBar[];
  height: number;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!containerRef.current || data.length === 0) return;

    const s = getComputedStyle(document.documentElement);
    const bg = s.getPropertyValue("--chart-bg").trim() || "#0f172a";
    const text = s.getPropertyValue("--chart-text").trim() || "#94a3b8";
    const grid = s.getPropertyValue("--chart-grid").trim() || "#1e293b40";

    // Clean up old chart
    if (chartRef.current) {
      chartRef.current.remove();
      chartRef.current = null;
    }

    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height,
      layout: { background: { color: bg }, textColor: text, fontSize: 11 },
      grid: {
        vertLines: { color: grid },
        horzLines: { color: grid },
      },
      rightPriceScale: { borderColor: grid },
      timeScale: { borderColor: grid, timeVisible: false },
      crosshair: {
        horzLine: { labelBackgroundColor: "#334155" },
        vertLine: { labelBackgroundColor: "#334155" },
      },
    });
    chartRef.current = chart;

    // DSEX line
    const lineSeries = chart.addSeries(LineSeries, {
      color: "#3b82f6",
      lineWidth: 2,
      priceScaleId: "right",
    });
    lineSeries.setData(
      data.map((d) => ({ time: d.date as Time, value: d.value })),
    );

    // Volume histogram on separate scale
    const volSeries = chart.addSeries(HistogramSeries, {
      priceScaleId: "vol",
      priceFormat: { type: "volume" },
    });
    volSeries.setData(
      data.map((d) => ({
        time: d.date as Time,
        value: d.volume,
        color: "#3b82f640",
      })),
    );
    chart.priceScale("vol").applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    chart.timeScale().fitContent();

    const ro = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect?.width;
      if (w && w > 0) chart.applyOptions({ width: w });
    });
    ro.observe(containerRef.current);

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
    };
  }, [data, height]);

  return <div ref={containerRef} className="w-full" />;
}

/* ─── Styling maps ─── */

const SIGNAL_COLORS: Record<string, string> = {
  STRONG_BUY: "text-emerald-400",
  BUY: "text-green-400",
  HOLD: "text-yellow-400",
  SELL: "text-red-400",
  STRONG_SELL: "text-red-500",
};

const SIGNAL_BG: Record<string, string> = {
  STRONG_BUY: "bg-emerald-500/20 border-emerald-500/40",
  BUY: "bg-green-500/20 border-green-500/40",
  HOLD: "bg-yellow-500/20 border-yellow-500/40",
  SELL: "bg-red-500/20 border-red-500/40",
  STRONG_SELL: "bg-red-600/20 border-red-600/40",
};

const SIGNAL_DOT: Record<string, string> = {
  STRONG_BUY: "bg-emerald-400",
  BUY: "bg-green-400",
  HOLD: "bg-yellow-400",
  SELL: "bg-red-400",
  STRONG_SELL: "bg-red-500",
};

function rsiColor(rsi: number | null | undefined): string {
  if (rsi == null) return "text-[var(--text-muted)]";
  if (rsi <= 30) return "text-emerald-400";
  if (rsi <= 40) return "text-green-400";
  if (rsi >= 70) return "text-red-400";
  if (rsi >= 60) return "text-orange-400";
  return "text-yellow-400";
}

type SortKey = "change_pct" | "rsi" | "value" | "symbol";

/* ─── Main Component ─── */

export default function AdvancedChart() {
  const [searchParams, setSearchParams] = useSearchParams();
  const symbolParam = searchParams.get("symbol");

  // null means DSEX view, string means stock view
  const [selectedStock, setSelectedStock] = useState<string | null>(
    symbolParam || null,
  );

  // Sidebar data
  const [aCatStocks, setACatStocks] = useState<StockPrice[]>([]);
  const [signals, setSignals] = useState<Map<string, StockSignal>>(new Map());
  const [sidebarLoading, setSidebarLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState("");
  const [sortBy, setSortBy] = useState<SortKey>("value");

  // DSEX chart data
  const [dsexData, setDsexData] = useState<DSEXChartBar[]>([]);
  const [dsexLoading, setDsexLoading] = useState(true);

  // Selected stock signal
  const [stockSignal, setStockSignal] = useState<StockSignal | null>(null);
  const [signalLoading, setSignalLoading] = useState(false);

  // Sidebar collapse on mobile
  const [sidebarOpen, setSidebarOpen] = useState(false);

  // Load sidebar data: A category stocks + signals
  useEffect(() => {
    let cancelled = false;
    setSidebarLoading(true);

    Promise.allSettled([
      fetchAllPrices("A"),
      fetchTopBuySignals(100),
      fetchTopSellSignals(100),
    ]).then(([pricesResult, buyResult, sellResult]) => {
      if (cancelled) return;

      if (pricesResult.status === "fulfilled") {
        setACatStocks(pricesResult.value);
      }

      // Build signal map
      const sigMap = new Map<string, StockSignal>();
      if (buyResult.status === "fulfilled") {
        for (const s of buyResult.value) sigMap.set(s.symbol, s);
      }
      if (sellResult.status === "fulfilled") {
        for (const s of sellResult.value) sigMap.set(s.symbol, s);
      }
      setSignals(sigMap);
      setSidebarLoading(false);
    });

    return () => {
      cancelled = true;
    };
  }, []);

  // Load DSEX chart data
  useEffect(() => {
    setDsexLoading(true);
    fetchDSEXChart()
      .then(setDsexData)
      .catch(() => {})
      .finally(() => setDsexLoading(false));
  }, []);

  // Load signal when stock is selected
  useEffect(() => {
    if (!selectedStock) {
      setStockSignal(null);
      return;
    }
    // Check if we already have it from sidebar signals
    const cached = signals.get(selectedStock);
    if (cached) {
      setStockSignal(cached);
    }
    // Always fetch full signal for accuracy
    setSignalLoading(true);
    fetchStockSignal(selectedStock)
      .then((s) => {
        setStockSignal(s);
      })
      .catch(() => {})
      .finally(() => setSignalLoading(false));
  }, [selectedStock, signals]);

  const handleSelectStock = useCallback(
    (symbol: string) => {
      setSelectedStock(symbol);
      setSearchParams({ symbol });
      setSidebarOpen(false); // close on mobile
    },
    [setSearchParams],
  );

  const handleBackToDSEX = useCallback(() => {
    setSelectedStock(null);
    setSearchParams({});
  }, [setSearchParams]);

  // Filter and sort sidebar stocks
  const filteredStocks = useMemo(() => {
    let list = aCatStocks;
    if (searchQuery) {
      const q = searchQuery.toUpperCase();
      list = list.filter(
        (s) =>
          s.symbol.includes(q) ||
          (s.company_name?.toUpperCase().includes(q) ?? false),
      );
    }
    return [...list].sort((a, b) => {
      switch (sortBy) {
        case "change_pct":
          return (b.change_pct ?? 0) - (a.change_pct ?? 0);
        case "rsi": {
          const rsiA = signals.get(a.symbol)?.indicators?.rsi ?? 50;
          const rsiB = signals.get(b.symbol)?.indicators?.rsi ?? 50;
          return rsiA - rsiB; // lowest RSI first (best buy)
        }
        case "symbol":
          return a.symbol.localeCompare(b.symbol);
        case "value":
        default:
          return (b.value ?? 0) - (a.value ?? 0);
      }
    });
  }, [aCatStocks, searchQuery, sortBy, signals]);

  // DSEX summary stats
  const dsexLatest = dsexData.length > 0 ? dsexData[dsexData.length - 1] : null;
  const dsexPrev =
    dsexData.length > 1 ? dsexData[dsexData.length - 2] : null;
  const dsexChange = dsexLatest && dsexPrev ? dsexLatest.value - dsexPrev.value : 0;
  const dsexChangePct =
    dsexPrev && dsexPrev.value > 0 ? (dsexChange / dsexPrev.value) * 100 : 0;

  const chartHeight = Math.max(
    400,
    typeof window !== "undefined" ? window.innerHeight - 220 : 600,
  );

  return (
    <div className="flex h-[calc(100vh-4rem)] overflow-hidden">
      {/* Mobile sidebar toggle */}
      <button
        onClick={() => setSidebarOpen(!sidebarOpen)}
        className="fixed bottom-4 right-4 z-50 md:hidden bg-blue-600 text-white p-3 rounded-full shadow-lg"
      >
        <BarChart3 className="h-5 w-5" />
      </button>

      {/* ─── Left Sidebar: A Category Stocks ─── */}
      <div
        className={clsx(
          "flex-shrink-0 bg-[var(--surface)] border-r border-[var(--border)] flex flex-col",
          "w-64 transition-transform duration-200",
          // Mobile: overlay
          "fixed md:relative inset-y-0 left-0 z-40",
          sidebarOpen ? "translate-x-0" : "-translate-x-full md:translate-x-0",
        )}
      >
        {/* Sidebar header */}
        <div className="p-2 border-b border-[var(--border)] space-y-1.5">
          <div className="flex items-center justify-between">
            <span className="text-xs font-bold text-[var(--text)] uppercase tracking-wide">
              A Category
            </span>
            <span className="text-[10px] text-[var(--text-muted)]">
              {filteredStocks.length} stocks
            </span>
          </div>

          {/* Search */}
          <div className="relative">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3 w-3 text-[var(--text-muted)]" />
            <input
              type="text"
              placeholder="Search..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-7 pr-2 py-1 text-xs bg-[var(--surface-active)] border border-[var(--border)] rounded text-[var(--text)] placeholder-[var(--text-muted)] focus:outline-none focus:border-blue-500"
            />
          </div>

          {/* Sort */}
          <div className="flex gap-1">
            {(
              [
                ["value", "Turnover"],
                ["change_pct", "Change"],
                ["rsi", "RSI"],
                ["symbol", "A-Z"],
              ] as [SortKey, string][]
            ).map(([key, label]) => (
              <button
                key={key}
                onClick={() => setSortBy(key)}
                className={clsx(
                  "flex-1 text-[9px] py-0.5 rounded font-medium transition-colors",
                  sortBy === key
                    ? "bg-blue-600 text-white"
                    : "bg-[var(--surface-active)] text-[var(--text-muted)] hover:text-[var(--text)]",
                )}
              >
                {label}
              </button>
            ))}
          </div>
        </div>

        {/* Stock list */}
        <div className="flex-1 overflow-y-auto overscroll-contain">
          {sidebarLoading ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="h-5 w-5 animate-spin text-[var(--text-muted)]" />
            </div>
          ) : filteredStocks.length === 0 ? (
            <p className="text-xs text-[var(--text-muted)] text-center py-6">
              No stocks found
            </p>
          ) : (
            filteredStocks.map((stock) => {
              const sig = signals.get(stock.symbol);
              const rsi = sig?.indicators?.rsi;
              const isActive = selectedStock === stock.symbol;
              const chg = stock.change_pct ?? 0;
              const isUp = chg > 0;
              const isDown = chg < 0;

              return (
                <button
                  key={stock.symbol}
                  onClick={() => handleSelectStock(stock.symbol)}
                  className={clsx(
                    "w-full text-left px-2 py-1.5 border-b border-[var(--border)] transition-colors",
                    "hover:bg-[var(--surface-active)]",
                    isActive && "bg-blue-500/10 border-l-2 border-l-blue-500",
                  )}
                >
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-bold text-[var(--text)] truncate max-w-[80px]">
                      {stock.symbol}
                    </span>
                    <span
                      className={clsx(
                        "text-[10px] font-semibold tabular-nums",
                        isUp && "text-green-400",
                        isDown && "text-red-400",
                        !isUp && !isDown && "text-[var(--text-muted)]",
                      )}
                    >
                      {isUp ? "+" : ""}
                      {chg.toFixed(2)}%
                    </span>
                  </div>
                  <div className="flex items-center justify-between mt-0.5">
                    <span className="text-[10px] text-[var(--text-muted)] tabular-nums">
                      ৳{stock.ltp?.toFixed(1) ?? "—"}
                    </span>
                    <div className="flex items-center gap-1.5">
                      {rsi != null && (
                        <span
                          className={clsx(
                            "text-[9px] font-medium tabular-nums",
                            rsiColor(rsi),
                          )}
                        >
                          RSI:{rsi.toFixed(0)}
                        </span>
                      )}
                      {sig && (
                        <span
                          className={clsx(
                            "w-1.5 h-1.5 rounded-full inline-block",
                            SIGNAL_DOT[sig.signal_type] ?? "bg-gray-500",
                          )}
                          title={sig.signal_type}
                        />
                      )}
                    </div>
                  </div>
                </button>
              );
            })
          )}
        </div>
      </div>

      {/* Mobile overlay backdrop */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 z-30 bg-black/50 md:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* ─── Main Chart Area ─── */}
      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        {/* ── Header / Decision Metrics Bar ── */}
        <div className="flex-shrink-0 px-3 py-2 border-b border-[var(--border)] bg-[var(--surface)]">
          {selectedStock ? (
            /* Stock view header */
            <div className="flex items-center gap-3 flex-wrap">
              <button
                onClick={handleBackToDSEX}
                className="text-[var(--text-muted)] hover:text-[var(--text)] transition-colors"
                title="Back to DSEX"
              >
                <ArrowLeft className="h-4 w-4" />
              </button>

              <div className="flex items-center gap-2">
                <h2 className="text-base font-bold text-[var(--text)]">
                  {selectedStock}
                </h2>
                {stockSignal?.company_name && (
                  <span className="text-[10px] text-[var(--text-muted)] hidden sm:inline">
                    {stockSignal.company_name}
                  </span>
                )}
              </div>

              {signalLoading ? (
                <Loader2 className="h-4 w-4 animate-spin text-[var(--text-muted)]" />
              ) : (
                stockSignal && (
                  <>
                    {/* LTP + Change */}
                    <div className="flex items-center gap-1.5">
                      <span className="text-base font-bold tabular-nums text-[var(--text)]">
                        ৳{stockSignal.ltp?.toFixed(2) ?? "—"}
                      </span>
                      <span
                        className={clsx(
                          "flex items-center gap-0.5 text-xs font-semibold tabular-nums",
                          (stockSignal.change_pct ?? 0) >= 0
                            ? "text-green-400"
                            : "text-red-400",
                        )}
                      >
                        {(stockSignal.change_pct ?? 0) >= 0 ? (
                          <TrendingUp className="h-3 w-3" />
                        ) : (
                          <TrendingDown className="h-3 w-3" />
                        )}
                        {(stockSignal.change_pct ?? 0) >= 0 ? "+" : ""}
                        {(stockSignal.change_pct ?? 0).toFixed(2)}%
                      </span>
                    </div>

                    {/* Signal badge */}
                    <span
                      className={clsx(
                        "px-2 py-0.5 rounded text-[10px] font-bold border",
                        SIGNAL_BG[stockSignal.signal_type] ??
                          "bg-[var(--surface-active)] border-[var(--border)]",
                        SIGNAL_COLORS[stockSignal.signal_type] ??
                          "text-[var(--text)]",
                      )}
                    >
                      {stockSignal.signal_type.replace("_", " ")}
                    </span>

                    {/* Decision metrics */}
                    <div className="flex items-center gap-2.5 flex-wrap text-[10px]">
                      {/* RSI */}
                      {stockSignal.indicators?.rsi != null && (
                        <div className="flex items-center gap-1">
                          <Activity className="h-3 w-3 text-blue-400" />
                          <span className="text-[var(--text-muted)]">RSI:</span>
                          <span
                            className={clsx(
                              "font-bold tabular-nums",
                              rsiColor(stockSignal.indicators.rsi),
                            )}
                          >
                            {stockSignal.indicators.rsi.toFixed(1)}
                          </span>
                        </div>
                      )}

                      {/* MACD */}
                      {stockSignal.indicators?.macd_signal && (
                        <div className="flex items-center gap-1">
                          <span className="text-[var(--text-muted)]">MACD:</span>
                          <span
                            className={clsx(
                              "font-bold",
                              stockSignal.indicators.macd_signal === "BULLISH"
                                ? "text-green-400"
                                : stockSignal.indicators.macd_signal ===
                                    "BEARISH"
                                  ? "text-red-400"
                                  : "text-yellow-400",
                            )}
                          >
                            {stockSignal.indicators.macd_signal}
                          </span>
                        </div>
                      )}

                      {/* Volume signal */}
                      {stockSignal.indicators?.volume_signal && (
                        <div className="flex items-center gap-1">
                          <span className="text-[var(--text-muted)]">Vol:</span>
                          <span
                            className={clsx(
                              "font-bold",
                              stockSignal.indicators.volume_signal.includes(
                                "HIGH",
                              )
                                ? "text-green-400"
                                : stockSignal.indicators.volume_signal.includes(
                                      "LOW",
                                    )
                                  ? "text-red-400"
                                  : "text-yellow-400",
                            )}
                          >
                            {stockSignal.indicators.volume_signal}
                          </span>
                        </div>
                      )}

                      {/* Target & SL */}
                      {stockSignal.target_price > 0 && (
                        <span className="flex items-center gap-0.5 text-[var(--text-muted)]">
                          <Target className="h-3 w-3 text-blue-400" />
                          ৳{stockSignal.target_price.toFixed(1)}
                        </span>
                      )}
                      {stockSignal.stop_loss > 0 && (
                        <span className="flex items-center gap-0.5 text-[var(--text-muted)]">
                          <ShieldAlert className="h-3 w-3 text-red-400" />
                          ৳{stockSignal.stop_loss.toFixed(1)}
                        </span>
                      )}

                      {/* Confidence */}
                      {stockSignal.confidence > 0 && (
                        <span className="text-[var(--text-muted)]">
                          Conf:{" "}
                          <span className="font-bold text-[var(--text)]">
                            {(stockSignal.confidence * 100).toFixed(0)}%
                          </span>
                        </span>
                      )}
                    </div>
                  </>
                )
              )}
            </div>
          ) : (
            /* DSEX view header */
            <div className="flex items-center gap-3 flex-wrap">
              <h2 className="text-base font-bold text-[var(--text)]">
                DSEX Index
              </h2>
              {dsexLatest && (
                <>
                  <span className="text-base font-bold tabular-nums text-[var(--text)]">
                    {dsexLatest.value.toFixed(2)}
                  </span>
                  <span
                    className={clsx(
                      "flex items-center gap-0.5 text-xs font-semibold tabular-nums",
                      dsexChange >= 0 ? "text-green-400" : "text-red-400",
                    )}
                  >
                    {dsexChange >= 0 ? (
                      <TrendingUp className="h-3.5 w-3.5" />
                    ) : (
                      <TrendingDown className="h-3.5 w-3.5" />
                    )}
                    {dsexChange >= 0 ? "+" : ""}
                    {dsexChange.toFixed(2)} ({dsexChangePct.toFixed(2)}%)
                  </span>
                  <span className="text-[10px] text-[var(--text-muted)]">
                    {dsexLatest.date}
                  </span>
                </>
              )}
              <span className="text-[10px] text-[var(--text-muted)] ml-auto">
                Select a stock from the sidebar to view its chart
              </span>
            </div>
          )}
        </div>

        {/* ── Chart Area ── */}
        <div className="flex-1 overflow-hidden p-1">
          {selectedStock ? (
            /* Stock candlestick chart */
            <PriceChart
              symbol={selectedStock}
              signal={stockSignal}
              height={chartHeight}
            />
          ) : dsexLoading ? (
            <div className="flex items-center justify-center h-full">
              <Loader2 className="h-8 w-8 animate-spin text-blue-500" />
            </div>
          ) : dsexData.length === 0 ? (
            <div className="flex items-center justify-center h-full text-[var(--text-muted)]">
              No DSEX history data available
            </div>
          ) : (
            /* DSEX line chart */
            <DSEXChart data={dsexData} height={chartHeight} />
          )}
        </div>
      </div>
    </div>
  );
}
