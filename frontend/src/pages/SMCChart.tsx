import { useEffect, useMemo, useRef, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  createChart,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  AreaSeries,
  createSeriesMarkers,
  type IChartApi,
  type ISeriesApi,
  type IPriceLine,
  type ISeriesMarkersPluginApi,
  type Time,
} from "lightweight-charts";
import clsx from "clsx";
import {
  ArrowLeft,
  RefreshCw,
  Search,
  Eye,
  EyeOff,
} from "lucide-react";
import {
  fetchSMCChart,
  fetchAllPrices,
  type SMCChartData,
} from "../api/client";
import type { StockPrice } from "../types/index";

type Period = "1m" | "3m" | "6m" | "1y" | "2y";

interface Toggles {
  fvg: boolean;
  bos: boolean;
  fib: boolean;
  pivots: boolean;
  ma20: boolean;
  ma50: boolean;
  ma200: boolean;
}

const DEFAULT_TOGGLES: Toggles = {
  fvg: true,
  bos: true,
  fib: false,
  pivots: false,
  ma20: false,
  ma50: false,
  ma200: false,
};

// Limit visible events so the chart stays readable
const MAX_BOS = 8;
const MAX_FVG = 10;

export default function SMCChart() {
  const { symbol = "GP" } = useParams();
  const nav = useNavigate();
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);

  // Overlay objects to clean up on toggle off / data change
  const fvgSeriesRef = useRef<ISeriesApi<"Area" | "Line">[]>([]);
  const bosSeriesRef = useRef<ISeriesApi<"Line">[]>([]);
  const bosMarkersRef = useRef<ISeriesMarkersPluginApi<Time> | null>(null);
  const fibLinesRef = useRef<IPriceLine[]>([]);
  const pivotLinesRef = useRef<IPriceLine[]>([]);
  const maSeriesRef = useRef<Record<string, ISeriesApi<"Line">>>({});

  const [data, setData] = useState<SMCChartData | null>(null);
  const [stocks, setStocks] = useState<StockPrice[]>([]);
  const [period, setPeriod] = useState<Period>("6m");
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [showDropdown, setShowDropdown] = useState(false);
  const [toggles, setToggles] = useState<Toggles>(DEFAULT_TOGGLES);

  useEffect(() => {
    fetchAllPrices()
      .then((s) => setStocks(s))
      .catch(() => setStocks([]));
  }, []);

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

  // Build the base chart when data arrives
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
    volumeSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.85, bottom: 0 },
    });
    volumeSeries.setData(
      data.volumes.map((v) => ({ ...v, time: v.time as Time })),
    );

    candleSeriesRef.current = candleSeries;
    chartRef.current = chart;

    // Reset overlay refs (chart was rebuilt)
    fvgSeriesRef.current = [];
    bosSeriesRef.current = [];
    bosMarkersRef.current = null;
    fibLinesRef.current = [];
    pivotLinesRef.current = [];
    maSeriesRef.current = {};

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

  // FVG zones — localized rectangles via two line series + area fill between them
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !data) return;

    fvgSeriesRef.current.forEach((s) => {
      try { chart.removeSeries(s); } catch { /* already removed */ }
    });
    fvgSeriesRef.current = [];

    if (!toggles.fvg) return;

    // Show only the most recent FVGs to keep the chart readable
    const fvgs = data.fvgs.slice(-MAX_FVG);

    fvgs.forEach((f) => {
      const isBull = f.type === "bullish";
      const fillColor = isBull ? "rgba(38, 166, 154, 0.15)" : "rgba(239, 83, 80, 0.15)";
      const lineColor = isBull ? "rgba(38, 166, 154, 0.8)" : "rgba(239, 83, 80, 0.8)";

      // Top line series — shows for the FVG's lifetime only
      const topLine = chart.addSeries(LineSeries, {
        color: lineColor,
        lineWidth: 1,
        lineStyle: 0,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
      topLine.setData([
        { time: f.start_time as Time, value: f.top },
        { time: f.end_time as Time, value: f.top },
      ]);
      fvgSeriesRef.current.push(topLine);

      // Bottom line series — at the bottom of the FVG zone
      const botLine = chart.addSeries(LineSeries, {
        color: lineColor,
        lineWidth: 1,
        lineStyle: 0,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
      botLine.setData([
        { time: f.start_time as Time, value: f.bottom },
        { time: f.end_time as Time, value: f.bottom },
      ]);
      fvgSeriesRef.current.push(botLine);

      // Area fill — gives the rectangle look between top and bottom
      // Trick: AreaSeries fills between baseline and the value. We approximate
      // by using the bottom line's price as base and a thin AreaSeries at top.
      const areaTop = chart.addSeries(AreaSeries, {
        topColor: fillColor,
        bottomColor: fillColor,
        lineColor: "rgba(0,0,0,0)",
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
      // Set base to bottom of FVG so the fill goes from bottom → top
      areaTop.applyOptions({
        // @ts-expect-error baseValue exists on AreaSeries options
        baseValue: { type: "price", price: f.bottom },
      });
      areaTop.setData([
        { time: f.start_time as Time, value: f.top },
        { time: f.end_time as Time, value: f.top },
      ]);
      fvgSeriesRef.current.push(areaTop);
    });
  }, [data, toggles.fvg]);

  // BOS / ChoCh — localized horizontal line from broken swing to breaking candle + marker
  useEffect(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    if (!chart || !candleSeries || !data) return;

    bosSeriesRef.current.forEach((s) => {
      try { chart.removeSeries(s); } catch { /* already removed */ }
    });
    bosSeriesRef.current = [];

    if (bosMarkersRef.current) {
      try { bosMarkersRef.current.detach(); } catch { /* already removed */ }
      bosMarkersRef.current = null;
    }

    if (!toggles.bos) return;

    // Show only the most recent N events to avoid clutter
    const events = data.structure.slice(-MAX_BOS);

    events.forEach((ev) => {
      const isBull = ev.type.startsWith("bullish");
      const color = isBull ? "#26a69a" : "#ef5350";

      // Horizontal line from broken swing time to the candle that broke it
      const line = chart.addSeries(LineSeries, {
        color,
        lineWidth: 1,
        lineStyle: 2, // dashed
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
      line.setData([
        { time: ev.from_time as Time, value: ev.from_price },
        { time: ev.time as Time, value: ev.from_price },
      ]);
      bosSeriesRef.current.push(line);
    });

    // Markers on the actual breaking candles with BOS / ChoCh label
    const markers = events.map((ev) => {
      const isBull = ev.type.startsWith("bullish");
      const isBOS = ev.type.includes("BOS");
      return {
        time: ev.time as Time,
        position: isBull ? ("aboveBar" as const) : ("belowBar" as const),
        color: isBull ? "#26a69a" : "#ef5350",
        shape: isBull ? ("arrowDown" as const) : ("arrowUp" as const),
        text: isBOS ? "BOS" : "ChoCh",
      };
    });
    bosMarkersRef.current = createSeriesMarkers(candleSeries, markers);
  }, [data, toggles.bos]);

  // Fibonacci toggle
  useEffect(() => {
    const series = candleSeriesRef.current;
    if (!series || !data) return;
    fibLinesRef.current.forEach((ln) => {
      try { series.removePriceLine(ln); } catch { /* already removed */ }
    });
    fibLinesRef.current = [];
    if (!toggles.fib || !data.fibonacci) return;
    const colors: Record<string, string> = {
      "0%": "#9ca3af",
      "23.6%": "#fbbf24",
      "38.2%": "#fb923c",
      "50%": "#a855f7",
      "61.8%": "#f472b6",
      "78.6%": "#60a5fa",
      "100%": "#9ca3af",
    };
    data.fibonacci.levels.forEach((lvl) => {
      fibLinesRef.current.push(
        series.createPriceLine({
          price: lvl.price,
          color: colors[lvl.label] ?? "#a78bfa",
          lineWidth: 1,
          lineStyle: 1,
          axisLabelVisible: true,
          title: `Fib ${lvl.label}`,
        }),
      );
    });
  }, [data, toggles.fib]);

  // Pivot points toggle
  useEffect(() => {
    const series = candleSeriesRef.current;
    if (!series || !data) return;
    pivotLinesRef.current.forEach((ln) => {
      try { series.removePriceLine(ln); } catch { /* already removed */ }
    });
    pivotLinesRef.current = [];
    if (!toggles.pivots || !data.pivots) return;
    const p = data.pivots;
    const lines: Array<[string, number, string]> = [
      ["R3", p.r3, "#ef4444"],
      ["R2", p.r2, "#f87171"],
      ["R1", p.r1, "#fca5a5"],
      ["P", p.pivot, "#facc15"],
      ["S1", p.s1, "#86efac"],
      ["S2", p.s2, "#4ade80"],
      ["S3", p.s3, "#22c55e"],
    ];
    lines.forEach(([label, price, color]) => {
      pivotLinesRef.current.push(
        series.createPriceLine({
          price,
          color,
          lineWidth: 1,
          lineStyle: 3,
          axisLabelVisible: true,
          title: label,
        }),
      );
    });
  }, [data, toggles.pivots]);

  // Moving averages
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !data?.moving_averages) return;

    const maConfigs: Array<[string, keyof Toggles, string]> = [
      ["ma_20", "ma20", "#facc15"],
      ["ma_50", "ma50", "#60a5fa"],
      ["ma_200", "ma200", "#f472b6"],
    ];

    maConfigs.forEach(([key, toggleKey, color]) => {
      const enabled = toggles[toggleKey];
      const existing = maSeriesRef.current[key];
      if (enabled && !existing) {
        const series = chart.addSeries(LineSeries, {
          color,
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
        });
        const points = (data.moving_averages?.[key] ?? []).map((pt) => ({
          time: pt.time as Time,
          value: pt.value,
        }));
        series.setData(points);
        maSeriesRef.current[key] = series;
      } else if (!enabled && existing) {
        try { chart.removeSeries(existing); } catch { /* already removed */ }
        delete maSeriesRef.current[key];
      }
    });
  }, [data, toggles.ma20, toggles.ma50, toggles.ma200]);

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

  function toggle(key: keyof Toggles) {
    setToggles((t) => ({ ...t, [key]: !t[key] }));
  }

  const toggleButtons: Array<{ key: keyof Toggles; label: string; color: string }> = [
    { key: "fvg", label: "FVG", color: "text-emerald-500" },
    { key: "bos", label: "BOS/ChoCh", color: "text-yellow-500" },
    { key: "fib", label: "Fibonacci", color: "text-purple-500" },
    { key: "pivots", label: "Pivots", color: "text-orange-500" },
    { key: "ma20", label: "MA20", color: "text-yellow-400" },
    { key: "ma50", label: "MA50", color: "text-blue-400" },
    { key: "ma200", label: "MA200", color: "text-pink-400" },
  ];

  return (
    <div className="min-h-screen p-4">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <div className="flex items-center gap-3">
          <button
            onClick={() => nav("/")}
            className="p-2 rounded bg-gray-200 hover:bg-gray-300 dark:bg-gray-800 dark:hover:bg-gray-700"
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
                  <span className="font-mono text-xs">{s.ltp.toFixed(1)}</span>
                </button>
              ))}
            </div>
          )}
        </div>

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
            <RefreshCw className={clsx("w-4 h-4", loading && "animate-spin")} />
          </button>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-2 mb-3">
        <span className="text-xs text-gray-500">Indicators:</span>
        {toggleButtons.map((b) => {
          const on = toggles[b.key];
          return (
            <button
              key={b.key}
              onClick={() => toggle(b.key)}
              className={clsx(
                "flex items-center gap-1 px-2.5 py-1 rounded text-xs border transition",
                on
                  ? "bg-gray-100 dark:bg-gray-800 border-gray-300 dark:border-gray-600"
                  : "bg-transparent border-gray-200 dark:border-gray-800 opacity-60",
              )}
            >
              {on ? (
                <Eye className="w-3 h-3" />
              ) : (
                <EyeOff className="w-3 h-3" />
              )}
              <span className={on ? b.color : "text-gray-500"}>{b.label}</span>
            </button>
          );
        })}
      </div>

      <div className="bg-white dark:bg-gray-900/30 rounded-lg border border-gray-300 dark:border-gray-700/50 p-2 relative">
        <div ref={containerRef} className="w-full" />
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-white/70 dark:bg-gray-900/70 rounded-lg">
            <div className="text-center">
              <RefreshCw className="w-8 h-8 animate-spin mx-auto text-emerald-500 mb-2" />
              <p className="text-sm text-gray-500">Loading {symbol}...</p>
            </div>
          </div>
        )}
        {!loading && !data && (
          <div className="text-center py-20 text-red-400">
            No data found for {symbol}
          </div>
        )}
      </div>

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
              {data.fvgs.slice(-12).reverse().map((f, i) => (
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
              {data.structure.slice(-12).reverse().map((s, i) => {
                const isBull = s.type.startsWith("bullish");
                const isBOS = s.type.includes("BOS");
                return (
                  <div
                    key={i}
                    className="text-xs flex items-center justify-between border-b border-gray-200 dark:border-gray-700/30 py-1"
                  >
                    <span
                      className={
                        isBull
                          ? "text-emerald-600 dark:text-emerald-400"
                          : "text-red-500 dark:text-red-400"
                      }
                    >
                      {isBull ? "↑" : "↓"} {isBOS ? "BOS" : "ChoCh"}
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
        Showing last {MAX_BOS} BOS/ChoCh events and last {MAX_FVG} FVG zones •
        FVG = Fair Value Gap • BOS = Break of Structure • ChoCh = Change of
        Character
      </div>
    </div>
  );
}
