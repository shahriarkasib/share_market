import { useEffect, useState, useMemo, useCallback, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import {
  Loader2,
  TrendingUp,
  TrendingDown,
  Search,
  ChevronDown,
  Activity,
  Target,
  ShieldAlert,
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

/* ─── DSEX inline chart ─── */
import {
  createChart,
  LineSeries,
  HistogramSeries,
  type IChartApi,
  type Time,
} from "lightweight-charts";

function DSEXChart({ data, height }: { data: DSEXChartBar[]; height: number }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!containerRef.current || data.length === 0) return;
    if (chartRef.current) { chartRef.current.remove(); chartRef.current = null; }

    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth, height,
      layout: { background: { color: "#ffffff" }, textColor: "#475569", fontSize: 11 },
      grid: { vertLines: { color: "#e2e8f018" }, horzLines: { color: "#e2e8f018" } },
      rightPriceScale: { borderColor: "#e2e8f0" },
      timeScale: { borderColor: "#e2e8f0", timeVisible: false },
      crosshair: { horzLine: { labelBackgroundColor: "#334155" }, vertLine: { labelBackgroundColor: "#334155" } },
    });
    chartRef.current = chart;

    const line = chart.addSeries(LineSeries, { color: "#3b82f6", lineWidth: 2, priceScaleId: "right" });
    line.setData(data.map((d) => ({ time: d.date as Time, value: d.value })));

    const vol = chart.addSeries(HistogramSeries, { priceScaleId: "vol", priceFormat: { type: "volume" } });
    vol.setData(data.map((d) => ({ time: d.date as Time, value: d.volume, color: "#3b82f640" })));
    chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    chart.timeScale().fitContent();

    const ro = new ResizeObserver((e) => { const w = e[0]?.contentRect?.width; if (w && w > 0) chart.applyOptions({ width: w }); });
    ro.observe(containerRef.current);
    return () => { ro.disconnect(); chart.remove(); chartRef.current = null; };
  }, [data, height]);

  return <div ref={containerRef} className="w-full" />;
}

/* ─── Stock Search Dropdown ─── */
function StockSelector({
  stocks,
  signals,
  selected,
  onSelect,
  loading,
}: {
  stocks: StockPrice[];
  signals: Map<string, StockSignal>;
  selected: string | null;
  onSelect: (sym: string) => void;
  loading: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);
  const dropRef = useRef<HTMLDivElement>(null);

  // Close on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (dropRef.current && !dropRef.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const filtered = useMemo(() => {
    if (!query) return stocks.slice(0, 30);
    const q = query.toUpperCase();
    return stocks.filter((s) => s.symbol.includes(q) || (s.company_name?.toUpperCase().includes(q) ?? false)).slice(0, 30);
  }, [stocks, query]);

  return (
    <div ref={dropRef} className="relative">
      <button
        onClick={() => { setOpen(!open); setTimeout(() => inputRef.current?.focus(), 50); }}
        className="flex items-center gap-1.5 px-2.5 py-1 rounded bg-[var(--surface-active)] border border-[var(--border)] hover:border-blue-500 transition-colors"
      >
        <Search className="h-3.5 w-3.5 text-[var(--text-muted)]" />
        <span className="text-sm font-bold text-[var(--text)]">{selected || "DSEX"}</span>
        <ChevronDown className="h-3 w-3 text-[var(--text-muted)]" />
      </button>

      {open && (
        <div className="absolute top-full left-0 mt-1 z-50 w-72 bg-[var(--surface)] border border-[var(--border)] rounded-lg shadow-xl overflow-hidden">
          {/* Search input */}
          <div className="p-2 border-b border-[var(--border)]">
            <input
              ref={inputRef}
              type="text"
              placeholder="Search symbol or company..."
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="w-full px-2.5 py-1.5 text-xs bg-[var(--surface-active)] border border-[var(--border)] rounded text-[var(--text)] placeholder-[var(--text-muted)] focus:outline-none focus:border-blue-500"
            />
          </div>

          {/* DSEX option */}
          <button
            onClick={() => { onSelect(""); setOpen(false); setQuery(""); }}
            className={clsx(
              "w-full text-left px-3 py-2 text-xs font-bold border-b border-[var(--border)] transition-colors hover:bg-[var(--surface-active)]",
              !selected ? "bg-blue-500/10 text-blue-400" : "text-[var(--text)]",
            )}
          >
            DSEX Index
          </button>

          {/* Stock list */}
          <div className="max-h-64 overflow-y-auto">
            {loading ? (
              <div className="flex items-center justify-center py-4">
                <Loader2 className="h-4 w-4 animate-spin text-[var(--text-muted)]" />
              </div>
            ) : filtered.length === 0 ? (
              <p className="text-xs text-[var(--text-muted)] text-center py-4">No stocks found</p>
            ) : (
              filtered.map((stock) => {
                const sig = signals.get(stock.symbol);
                const chg = stock.change_pct ?? 0;
                return (
                  <button
                    key={stock.symbol}
                    onClick={() => { onSelect(stock.symbol); setOpen(false); setQuery(""); }}
                    className={clsx(
                      "w-full text-left px-3 py-1.5 border-b border-[var(--border)] transition-colors hover:bg-[var(--surface-active)] flex items-center justify-between",
                      selected === stock.symbol && "bg-blue-500/10",
                    )}
                  >
                    <div>
                      <span className="text-xs font-bold text-[var(--text)]">{stock.symbol}</span>
                      {stock.company_name && (
                        <span className="text-[9px] text-[var(--text-muted)] ml-1.5 truncate max-w-[120px] inline-block align-middle">
                          {stock.company_name.slice(0, 25)}
                        </span>
                      )}
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-[10px] text-[var(--text-muted)] tabular-nums">
                        ৳{stock.ltp?.toFixed(1) ?? "—"}
                      </span>
                      <span className={clsx("text-[10px] font-semibold tabular-nums",
                        chg > 0 ? "text-green-500" : chg < 0 ? "text-red-500" : "text-[var(--text-muted)]")}>
                        {chg > 0 ? "+" : ""}{chg.toFixed(1)}%
                      </span>
                      {sig && (
                        <span className={clsx("w-1.5 h-1.5 rounded-full",
                          sig.signal_type === "STRONG_BUY" ? "bg-emerald-400" :
                          sig.signal_type === "BUY" ? "bg-green-400" :
                          sig.signal_type === "SELL" ? "bg-red-400" :
                          sig.signal_type === "STRONG_SELL" ? "bg-red-500" : "bg-yellow-400"
                        )} />
                      )}
                    </div>
                  </button>
                );
              })
            )}
          </div>
        </div>
      )}
    </div>
  );
}

/* ─── Main Component ─── */

export default function AdvancedChart() {
  const [searchParams, setSearchParams] = useSearchParams();
  const symbolParam = searchParams.get("symbol");

  const [selectedStock, setSelectedStock] = useState<string | null>(symbolParam || null);
  const [aCatStocks, setACatStocks] = useState<StockPrice[]>([]);
  const [signals, setSignals] = useState<Map<string, StockSignal>>(new Map());
  const [stocksLoading, setStocksLoading] = useState(true);

  const [dsexData, setDsexData] = useState<DSEXChartBar[]>([]);
  const [dsexLoading, setDsexLoading] = useState(true);

  const [stockSignal, setStockSignal] = useState<StockSignal | null>(null);
  const [signalLoading, setSignalLoading] = useState(false);

  // Load stocks + signals
  useEffect(() => {
    let cancelled = false;
    setStocksLoading(true);
    Promise.allSettled([fetchAllPrices("A"), fetchTopBuySignals(100), fetchTopSellSignals(100)])
      .then(([pr, br, sr]) => {
        if (cancelled) return;
        if (pr.status === "fulfilled") setACatStocks(pr.value);
        const m = new Map<string, StockSignal>();
        if (br.status === "fulfilled") for (const s of br.value) m.set(s.symbol, s);
        if (sr.status === "fulfilled") for (const s of sr.value) m.set(s.symbol, s);
        setSignals(m);
        setStocksLoading(false);
      });
    return () => { cancelled = true; };
  }, []);

  // Auto-select first stock
  useEffect(() => {
    if (!selectedStock && aCatStocks.length > 0) {
      const top = aCatStocks[0].symbol;
      setSelectedStock(top);
      setSearchParams({ symbol: top }, { replace: true });
    }
  }, [aCatStocks, selectedStock, setSearchParams]);

  // Load DSEX
  useEffect(() => {
    setDsexLoading(true);
    fetchDSEXChart().then(setDsexData).catch(() => {}).finally(() => setDsexLoading(false));
  }, []);

  // Load signal for selected stock
  useEffect(() => {
    if (!selectedStock) { setStockSignal(null); return; }
    const cached = signals.get(selectedStock);
    if (cached) setStockSignal(cached);
    setSignalLoading(true);
    fetchStockSignal(selectedStock).then(setStockSignal).catch(() => {}).finally(() => setSignalLoading(false));
  }, [selectedStock, signals]);

  const handleSelect = useCallback((sym: string) => {
    if (sym) {
      setSelectedStock(sym);
      setSearchParams({ symbol: sym });
    } else {
      setSelectedStock(null);
      setSearchParams({});
    }
  }, [setSearchParams]);

  // DSEX stats
  const dsexLatest = dsexData.length > 0 ? dsexData[dsexData.length - 1] : null;
  const dsexPrev = dsexData.length > 1 ? dsexData[dsexData.length - 2] : null;
  const dsexChange = dsexLatest && dsexPrev ? dsexLatest.value - dsexPrev.value : 0;
  const dsexChangePct = dsexPrev && dsexPrev.value > 0 ? (dsexChange / dsexPrev.value) * 100 : 0;

  return (
    <div className="flex flex-col overflow-hidden -mx-4 lg:-mx-8 -mt-4 -mb-4" style={{ height: 'calc(100vh - 3rem)' }}>
      {/* ── Top Header Bar: Stock Selector + Signal Metrics ── */}
      <div className="flex-shrink-0 px-2 py-0.5 border-b border-[var(--border)] bg-[var(--surface)] flex items-center gap-2 flex-wrap">
        {/* Stock selector dropdown */}
        <StockSelector
          stocks={aCatStocks}
          signals={signals}
          selected={selectedStock}
          onSelect={handleSelect}
          loading={stocksLoading}
        />

        {selectedStock && stockSignal && !signalLoading ? (
          <>
            {/* LTP + Change */}
            <div className="flex items-center gap-1.5">
              <span className="text-sm font-bold tabular-nums text-[var(--text)]">
                ৳{stockSignal.ltp?.toFixed(1) ?? "—"}
              </span>
              <span className={clsx(
                "flex items-center gap-0.5 text-xs font-semibold tabular-nums",
                (stockSignal.change_pct ?? 0) >= 0 ? "text-green-500" : "text-red-500",
              )}>
                {(stockSignal.change_pct ?? 0) >= 0 ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
                {(stockSignal.change_pct ?? 0) >= 0 ? "+" : ""}{(stockSignal.change_pct ?? 0).toFixed(2)}%
              </span>
            </div>

            {/* Signal badge */}
            <span className={clsx("px-2 py-0.5 rounded text-[10px] font-bold border",
              stockSignal.signal_type === "STRONG_BUY" ? "bg-emerald-500/20 border-emerald-500/40 text-emerald-400" :
              stockSignal.signal_type === "BUY" ? "bg-green-500/20 border-green-500/40 text-green-400" :
              stockSignal.signal_type === "SELL" ? "bg-red-500/20 border-red-500/40 text-red-400" :
              stockSignal.signal_type === "STRONG_SELL" ? "bg-red-600/20 border-red-600/40 text-red-500" :
              "bg-yellow-500/20 border-yellow-500/40 text-yellow-400"
            )}>
              {stockSignal.signal_type.replace("_", " ")}
            </span>

            {/* Compact metrics */}
            <div className="flex items-center gap-2 text-[10px]">
              {stockSignal.indicators?.rsi != null && (
                <span className="text-[var(--text-muted)]">
                  <Activity className="h-3 w-3 inline text-blue-400 mr-0.5" />
                  RSI: <span className={clsx("font-bold tabular-nums",
                    stockSignal.indicators.rsi <= 30 ? "text-emerald-400" :
                    stockSignal.indicators.rsi >= 70 ? "text-red-400" : "text-yellow-400"
                  )}>{stockSignal.indicators.rsi.toFixed(0)}</span>
                </span>
              )}
              {stockSignal.indicators?.macd_signal && (
                <span className="text-[var(--text-muted)]">MACD: <span className={clsx("font-bold",
                  stockSignal.indicators.macd_signal === "BULLISH" ? "text-green-400" :
                  stockSignal.indicators.macd_signal === "BEARISH" ? "text-red-400" : "text-yellow-400"
                )}>{stockSignal.indicators.macd_signal}</span></span>
              )}
              {stockSignal.target_price > 0 && (
                <span className="text-[var(--text-muted)]">
                  <Target className="h-3 w-3 inline text-blue-400 mr-0.5" />৳{stockSignal.target_price.toFixed(1)}
                </span>
              )}
              {stockSignal.stop_loss > 0 && (
                <span className="text-[var(--text-muted)]">
                  <ShieldAlert className="h-3 w-3 inline text-red-400 mr-0.5" />৳{stockSignal.stop_loss.toFixed(1)}
                </span>
              )}
            </div>

            {stockSignal.company_name && (
              <span className="text-[10px] text-[var(--text-muted)] ml-auto hidden lg:inline">
                {stockSignal.company_name}
              </span>
            )}
          </>
        ) : selectedStock && signalLoading ? (
          <Loader2 className="h-4 w-4 animate-spin text-[var(--text-muted)]" />
        ) : !selectedStock && dsexLatest ? (
          /* DSEX header */
          <>
            <span className="text-sm font-bold tabular-nums text-[var(--text)]">
              {dsexLatest.value.toFixed(2)}
            </span>
            <span className={clsx(
              "flex items-center gap-0.5 text-xs font-semibold tabular-nums",
              dsexChange >= 0 ? "text-green-500" : "text-red-500",
            )}>
              {dsexChange >= 0 ? <TrendingUp className="h-3.5 w-3.5" /> : <TrendingDown className="h-3.5 w-3.5" />}
              {dsexChange >= 0 ? "+" : ""}{dsexChange.toFixed(2)} ({dsexChangePct.toFixed(2)}%)
            </span>
          </>
        ) : null}
      </div>

      {/* ── Chart Area — full width, fills remaining height ── */}
      <div className="flex-1 min-h-0 flex flex-col">
        {selectedStock ? (
          <PriceChart symbol={selectedStock} signal={stockSignal} />
        ) : dsexLoading ? (
          <div className="flex items-center justify-center h-full">
            <Loader2 className="h-8 w-8 animate-spin text-blue-500" />
          </div>
        ) : dsexData.length === 0 ? (
          <div className="flex items-center justify-center h-full text-[var(--text-muted)]">
            No DSEX history data available
          </div>
        ) : (
          <div className="flex-1 min-h-0 p-1">
            <DSEXChart data={dsexData} height={500} />
          </div>
        )}
      </div>
    </div>
  );
}
