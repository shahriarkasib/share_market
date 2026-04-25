import { useEffect, useMemo, useRef, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  createChart,
  CandlestickSeries,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  type Time,
} from "lightweight-charts";
import clsx from "clsx";
import { ArrowLeft, RefreshCw, Search } from "lucide-react";
import {
  fetchSMCChart,
  fetchAllPrices,
  type SMCChartData,
} from "../api/client";
import type { StockPrice } from "../types/index";

type Period = "1m" | "3m" | "6m" | "1y" | "2y";

export default function SMCChart() {
  const { symbol = "GP" } = useParams();
  const nav = useNavigate();
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);

  const [data, setData] = useState<SMCChartData | null>(null);
  const [stocks, setStocks] = useState<StockPrice[]>([]);
  const [period, setPeriod] = useState<Period>("6m");
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [showDropdown, setShowDropdown] = useState(false);

  // Fetch stock list once
  useEffect(() => {
    fetchAllPrices()
      .then((s) => setStocks(s))
      .catch(() => setStocks([]));
  }, []);

  // Fetch chart data when symbol/period changes
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    fetchSMCChart(symbol, period)
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch(() => {
        if (!cancelled) setData(null);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [symbol, period]);

  // Render chart
  useEffect(() => {
    if (!data || !containerRef.current) return;

    if (chartRef.current) {
      chartRef.current.remove();
      chartRef.current = null;
    }

    const isLight = document.documentElement.getAttribute("data-theme") === "light";
    const bg = isLight ? "#ffffff" : "#0a0e17";
    const text = isLight ? "#374151" : "#d1d5db";
    const grid = isLight ? "#e5e7eb" : "#1f2937";

    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height: 600,
      layout: { background: { color: bg }, textColor: text },
      grid: { vertLines: { color: grid }, horzLines: { color: grid } },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: grid },
      timeScale: { borderColor: grid, timeVisible: true },
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderVisible: false,
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350",
    });
    candleSeries.setData(
      data.candles.map((c) => ({ ...c, time: c.time as Time })),
    );

    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
    });
    volumeSeries
      .priceScale()
      .applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } });
    volumeSeries.setData(
      data.volumes.map((v) => ({ ...v, time: v.time as Time })),
    );

    // FVG bands as price lines
    data.fvgs.forEach((fvg) => {
      const color =
        fvg.type === "bullish"
          ? "rgba(38, 166, 154, 0.4)"
          : "rgba(239, 83, 80, 0.4)";
      candleSeries.createPriceLine({
        price: fvg.top,
        color,
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: false,
        title: fvg.type === "bullish" ? "FVG↑" : "FVG↓",
      });
      candleSeries.createPriceLine({
        price: fvg.bottom,
        color,
        lineWidth: 1,
        lineStyle: 2,
        axisLabelVisible: false,
        title: "",
      });
    });

    // BOS / ChoCh structure markers
    data.structure.forEach((ev) => {
      const isBullish = ev.type.startsWith("bullish");
      const isBOS = ev.type.includes("BOS");
      candleSeries.createPriceLine({
        price: ev.from_price,
        color: isBullish ? "#26a69a" : "#ef5350",
        lineWidth: 2,
        lineStyle: 0,
        axisLabelVisible: true,
        title: `${isBOS ? "BOS" : "ChoCh"} ${isBullish ? "↑" : "↓"}`,
      });
    });

    seriesRef.current = candleSeries;
    chartRef.current = chart;
    chart.timeScale().fitContent();

    const ro = new ResizeObserver(() => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: containerRef.current.clientWidth,
        });
      }
    });
    ro.observe(containerRef.current);

    return () => {
      ro.disconnect();
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
    };
  }, [data]);

  // Filtered stock list for selector
  const filteredStocks = useMemo(() => {
    if (!search.trim()) return stocks.slice(0, 50);
    const s = search.toLowerCase();
    return stocks
      .filter(
        (st) =>
          st.symbol.toLowerCase().includes(s) ||
          (st.company_name || "").toLowerCase().includes(s),
      )
      .slice(0, 50);
  }, [stocks, search]);

  function selectStock(sym: string) {
    setSearch("");
    setShowDropdown(false);
    nav(`/smc-chart/${sym}`);
  }

  return (
    <div className="min-h-screen p-4">
      {/* Header */}
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <div className="flex items-center gap-3">
          <button
            onClick={() => nav("/")}
            className="p-2 rounded bg-gray-800 hover:bg-gray-700 dark:bg-gray-800 dark:hover:bg-gray-700"
          >
            <ArrowLeft className="w-4 h-4" />
          </button>
          <div>
            <h1 className="text-2xl font-bold">{data?.symbol || symbol}</h1>
            {data && (
              <span className="text-emerald-500 text-lg font-mono">
                {data.current_price.toFixed(1)} ৳
              </span>
            )}
          </div>
        </div>

        {/* Stock selector */}
        <div className="relative flex-1 max-w-xs">
          <div className="flex items-center gap-2 bg-gray-100 dark:bg-gray-800 rounded px-3 py-1.5 border border-gray-300 dark:border-gray-700">
            <Search className="w-4 h-4 text-gray-500" />
            <input
              type="text"
              value={search}
              onChange={(e) => {
                setSearch(e.target.value.toUpperCase());
                setShowDropdown(true);
              }}
              onFocus={() => setShowDropdown(true)}
              onBlur={() => setTimeout(() => setShowDropdown(false), 150)}
              placeholder="Search stock..."
              className="bg-transparent flex-1 outline-none text-sm"
            />
          </div>
          {showDropdown && filteredStocks.length > 0 && (
            <div className="absolute top-full mt-1 w-full max-h-80 overflow-y-auto bg-white dark:bg-gray-900 border border-gray-300 dark:border-gray-700 rounded shadow-lg z-50">
              {filteredStocks.map((s) => (
                <button
                  key={s.symbol}
                  onMouseDown={() => selectStock(s.symbol)}
                  className="w-full text-left px-3 py-1.5 text-sm hover:bg-gray-100 dark:hover:bg-gray-800 flex justify-between items-center"
                >
                  <div>
                    <span className="font-mono font-bold">{s.symbol}</span>
                    {s.company_name && (
                      <span className="text-gray-500 text-xs ml-2">
                        {s.company_name.slice(0, 25)}
                      </span>
                    )}
                  </div>
                  <span className="font-mono text-xs">
                    {s.ltp.toFixed(1)}
                  </span>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Period selector */}
        <div className="flex items-center gap-1 bg-gray-100 dark:bg-gray-800/50 rounded p-1">
          {(["1m", "3m", "6m", "1y", "2y"] as const).map((p) => (
            <button
              key={p}
              onClick={() => setPeriod(p)}
              className={clsx(
                "px-3 py-1 rounded text-xs",
                period === p
                  ? "bg-emerald-500/20 text-emerald-600 dark:text-emerald-400"
                  : "text-gray-500 dark:text-gray-400",
              )}
            >
              {p}
            </button>
          ))}
          <button
            onClick={() => fetchSMCChart(symbol, period).then(setData)}
            className="p-1 rounded hover:bg-gray-200 dark:hover:bg-gray-700"
          >
            <RefreshCw
              className={clsx("w-4 h-4", loading && "animate-spin")}
            />
          </button>
        </div>
      </div>

      {/* Chart */}
      <div className="bg-white dark:bg-gray-900/30 rounded-lg border border-gray-300 dark:border-gray-700/50 p-2">
        <div ref={containerRef} className="w-full" />
        {loading && (
          <div className="text-center py-20 text-gray-500">Loading chart...</div>
        )}
        {!loading && !data && (
          <div className="text-center py-20 text-red-400">
            No data found for {symbol}
          </div>
        )}
      </div>

      {/* FVG + Structure lists */}
      {data && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
          <div className="bg-white dark:bg-gray-800/30 rounded-lg p-4 border border-gray-300 dark:border-gray-700/50">
            <h3 className="text-sm font-bold text-emerald-600 dark:text-emerald-400 mb-2">
              FVG Zones ({data.fvgs.length})
            </h3>
            <div className="space-y-1 max-h-60 overflow-y-auto">
              {data.fvgs.length === 0 && (
                <p className="text-xs text-gray-500">No FVG detected.</p>
              )}
              {data.fvgs.slice(0, 12).map((f, i) => (
                <div
                  key={i}
                  className="text-xs flex items-center justify-between border-b border-gray-200 dark:border-gray-700/30 py-1"
                >
                  <span
                    className={
                      f.type === "bullish"
                        ? "text-emerald-600 dark:text-emerald-400"
                        : "text-red-500 dark:text-red-400"
                    }
                  >
                    {f.type === "bullish" ? "↑" : "↓"} {f.type.toUpperCase()}
                  </span>
                  <span className="font-mono text-gray-500 dark:text-gray-400">
                    {f.bottom.toFixed(1)} – {f.top.toFixed(1)} ৳
                  </span>
                  <span className="text-gray-500">{f.start_time}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800/30 rounded-lg p-4 border border-gray-300 dark:border-gray-700/50">
            <h3 className="text-sm font-bold text-yellow-600 dark:text-yellow-400 mb-2">
              Structure Events ({data.structure.length})
            </h3>
            <div className="space-y-1 max-h-60 overflow-y-auto">
              {data.structure.length === 0 && (
                <p className="text-xs text-gray-500">No BOS/ChoCh detected.</p>
              )}
              {data.structure.slice(0, 12).map((s, i) => {
                const isBullish = s.type.startsWith("bullish");
                const isBOS = s.type.includes("BOS");
                return (
                  <div
                    key={i}
                    className="text-xs flex items-center justify-between border-b border-gray-200 dark:border-gray-700/30 py-1"
                  >
                    <span
                      className={
                        isBullish
                          ? "text-emerald-600 dark:text-emerald-400"
                          : "text-red-500 dark:text-red-400"
                      }
                    >
                      {isBullish ? "↑" : "↓"} {isBOS ? "BOS" : "ChoCh"}
                    </span>
                    <span className="font-mono text-gray-500 dark:text-gray-400">
                      {s.price.toFixed(1)} ৳
                    </span>
                    <span className="text-gray-500">{s.time}</span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}

      <div className="mt-4 text-xs text-gray-500 text-center">
        FVG = Fair Value Gap (3-candle imbalance) • BOS = Break of Structure •
        ChoCh = Change of Character
      </div>
    </div>
  );
}
