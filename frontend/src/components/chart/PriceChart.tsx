import { useEffect, useRef, useState, useCallback } from "react";
import {
  createChart,
  CandlestickSeries,
  LineSeries,
  AreaSeries,
  HistogramSeries,
  LineStyle,
  type IChartApi,
  type ISeriesApi,
  type SeriesType,
  type Time,
} from "lightweight-charts";
import { clsx } from "clsx";
import { Loader2 } from "lucide-react";
import type { StockSignal } from "../../types/index.ts";
import { fetchOHLCV } from "../../api/client.ts";
import {
  computeEMA,
  computeSMA,
  computeBollingerBands,
  computeRSI,
  computeMACD,
  computeStochastic,
  computeVWAP,
  computeStochRSI,
  computeATR,
  computeOBV,
  computeADX,
} from "./indicators.ts";
type ChartType = "candlestick" | "line" | "area";

const PERIODS = ["1w", "2w", "1m", "3m", "6m", "1y", "2y", "3y"] as const;
const PERIOD_LABELS: Record<string, string> = {
  "1w": "1W", "2w": "2W", "1m": "1M", "3m": "3M",
  "6m": "6M", "1y": "1Y", "2y": "2Y", "3y": "3Y",
};

const OVERLAY_DEFS = [
  { key: "ema9", label: "EMA 9", color: "#3b82f6" },
  { key: "ema21", label: "EMA 21", color: "#f97316" },
  { key: "sma50", label: "SMA 50", color: "#a855f7" },
  { key: "sma200", label: "SMA 200", color: "#dc2626" },
  { key: "bb", label: "BB", color: "#64748b" },
  { key: "vwap", label: "VWAP", color: "#eab308" },
] as const;

const SUB_PANE_DEFS = [
  { key: "rsi", label: "RSI" },
  { key: "macd", label: "MACD" },
  { key: "stoch", label: "Stoch" },
  { key: "stochrsi", label: "StochRSI" },
  { key: "atr", label: "ATR" },
  { key: "obv", label: "OBV" },
  { key: "adx", label: "ADX" },
] as const;

/** Read current CSS variable values for chart theming. */
function getChartColors() {
  const s = getComputedStyle(document.documentElement);
  return {
    bg: s.getPropertyValue("--chart-bg").trim() || "#ffffff",
    text: s.getPropertyValue("--chart-text").trim() || "#475569",
    grid: s.getPropertyValue("--chart-grid").trim() || "#e2e8f020",
    crosshair: s.getPropertyValue("--chart-crosshair").trim() || "#94a3b8",
    crosshairLabel: s.getPropertyValue("--chart-crosshair-label").trim() || "#e2e8f0",
    border: s.getPropertyValue("--chart-border").trim() || "#e2e8f0",
  };
}

interface Props {
  symbol: string;
  signal?: StockSignal | null;
  /** Base chart height in px. Defaults to 420. Sub-panes add 120px each. */
  height?: number;
}

export default function PriceChart({ symbol, signal, height: baseHeight = 420 }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRefs = useRef<Map<string, ISeriesApi<SeriesType>>>(new Map());
  const baseHeightRef = useRef(baseHeight);
  baseHeightRef.current = baseHeight;
  const signalRef = useRef(signal);
  signalRef.current = signal;
  const theme = "dark" as const;

  const [chartType, setChartType] = useState<ChartType>("candlestick");
  const [period, setPeriod] = useState("3m");
  const [overlays, setOverlays] = useState<Set<string>>(() => new Set(["ema9", "ema21"]));
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const toggleOverlay = useCallback((key: string) => {
    setOverlays((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const [subPanes, setSubPanes] = useState<Set<string>>(() => new Set());
  const subPanesRef = useRef(subPanes);
  subPanesRef.current = subPanes;
  const subPaneCount = subPanes.size;
  const totalHeightRef = useRef(baseHeight);
  totalHeightRef.current = baseHeight + subPaneCount * 150;
  const toggleSubPane = useCallback((key: string) => {
    setSubPanes((prev) => {
      const n = new Set(prev);
      if (n.has(key)) n.delete(key);
      else n.add(key);
      return n;
    });
  }, []);

  // Create chart — recreate when sub-pane count changes so height allocates correctly
  useEffect(() => {
    if (!containerRef.current) return;

    const colors = getChartColors();
    const h = totalHeightRef.current;
    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height: h,
      layout: {
        background: { color: colors.bg },
        textColor: colors.text,
        fontSize: 11,
      },
      grid: {
        vertLines: { color: colors.grid },
        horzLines: { color: colors.grid },
      },
      crosshair: {
        vertLine: { color: colors.crosshair, width: 1, style: LineStyle.Dashed, labelBackgroundColor: colors.crosshairLabel },
        horzLine: { color: colors.crosshair, width: 1, style: LineStyle.Dashed, labelBackgroundColor: colors.crosshairLabel },
      },
      rightPriceScale: {
        borderColor: colors.border,
        scaleMargins: { top: 0.1, bottom: 0.25 },
      },
      timeScale: {
        borderColor: colors.border,
        timeVisible: false,
      },
    });

    chartRef.current = chart;

    const handleResize = () => {
      if (containerRef.current) {
        const w = containerRef.current.clientWidth;
        if (w === 0) return;
        chart.resize(w, totalHeightRef.current);
      }
    };
    const ro = new ResizeObserver(handleResize);
    ro.observe(containerRef.current);

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRefs.current.clear();
    };
  }, [subPaneCount]);

  // Resize chart when baseHeight changes
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !containerRef.current) return;
    chart.resize(containerRef.current.clientWidth, totalHeightRef.current);
  }, [baseHeight]);

  // Update chart colors when theme changes
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;

    // Small delay to let CSS vars update
    requestAnimationFrame(() => {
      const colors = getChartColors();
      chart.applyOptions({
        layout: {
          background: { color: colors.bg },
          textColor: colors.text,
        },
        grid: {
          vertLines: { color: colors.grid },
          horzLines: { color: colors.grid },
        },
        crosshair: {
          vertLine: { color: colors.crosshair, labelBackgroundColor: colors.crosshairLabel },
          horzLine: { color: colors.crosshair, labelBackgroundColor: colors.crosshairLabel },
        },
        rightPriceScale: { borderColor: colors.border },
        timeScale: { borderColor: colors.border },
      });
    });
  }, [theme]);

  // Fetch data + render series when symbol/period/chartType/overlays change
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;

    let cancelled = false;

    const render = async () => {
      setLoading(true);
      setError(null);

      try {
        const bars = await fetchOHLCV(symbol, period);
        if (cancelled || !bars.length) {
          if (!cancelled && !bars.length) setError("No historical data");
          return;
        }

        // Remove all existing series
        for (const s of seriesRefs.current.values()) {
          try { chart.removeSeries(s); } catch { /* already removed */ }
        }
        seriesRefs.current.clear();

        const closes = bars.map((b) => b.close);
        const dates = bars.map((b) => b.date as Time);

        // ---- Main series ----
        if (chartType === "candlestick") {
          const series = chart.addSeries(CandlestickSeries, {
            upColor: "#22c55e",
            downColor: "#ef4444",
            borderUpColor: "#22c55e",
            borderDownColor: "#ef4444",
            wickUpColor: "#22c55e80",
            wickDownColor: "#ef444480",
          });
          series.setData(
            bars.map((b) => ({
              time: b.date as Time,
              open: b.open,
              high: b.high,
              low: b.low,
              close: b.close,
            })),
          );
          seriesRefs.current.set("main", series);
        } else if (chartType === "line") {
          const series = chart.addSeries(LineSeries, {
            color: "#3b82f6",
            lineWidth: 2,
          });
          series.setData(
            bars.map((b) => ({ time: b.date as Time, value: b.close })),
          );
          seriesRefs.current.set("main", series);
        } else {
          const series = chart.addSeries(AreaSeries, {
            topColor: "rgba(59,130,246,0.4)",
            bottomColor: "rgba(59,130,246,0.05)",
            lineColor: "#3b82f6",
            lineWidth: 2,
          });
          series.setData(
            bars.map((b) => ({ time: b.date as Time, value: b.close })),
          );
          seriesRefs.current.set("main", series);
        }

        // ---- Volume histogram ----
        const volSeries = chart.addSeries(HistogramSeries, {
          priceFormat: { type: "volume" },
          priceScaleId: "vol",
        });
        chart.priceScale("vol").applyOptions({
          scaleMargins: { top: 0.8, bottom: 0 },
        });
        volSeries.setData(
          bars.map((b) => ({
            time: b.date as Time,
            value: b.volume,
            color: b.close >= b.open ? "#22c55e40" : "#ef444440",
          })),
        );
        seriesRefs.current.set("volume", volSeries);

        // ---- Overlays ----
        const refs = seriesRefs.current;
        if (overlays.has("ema9")) {
          addLineSeries(chart, "ema9", dates, computeEMA(closes, 9), "#3b82f6", refs);
        }
        if (overlays.has("ema21")) {
          addLineSeries(chart, "ema21", dates, computeEMA(closes, 21), "#f97316", refs);
        }
        if (overlays.has("sma50")) {
          addLineSeries(chart, "sma50", dates, computeSMA(closes, 50), "#a855f7", refs);
        }
        if (overlays.has("sma200")) {
          addLineSeries(chart, "sma200", dates, computeSMA(closes, 200), "#dc2626", refs, 1, LineStyle.Solid);
        }
        if (overlays.has("bb")) {
          const bb = computeBollingerBands(closes);
          addLineSeries(chart, "bb_upper", dates, bb.upper, "#64748b", refs, 1, LineStyle.Dashed);
          addLineSeries(chart, "bb_middle", dates, bb.middle, "#64748b", refs, 1, LineStyle.Dotted);
          addLineSeries(chart, "bb_lower", dates, bb.lower, "#64748b", refs, 1, LineStyle.Dashed);
        }
        if (overlays.has("vwap")) {
          const highs = bars.map((b) => b.high);
          const lows = bars.map((b) => b.low);
          const vols = bars.map((b) => b.volume);
          addLineSeries(chart, "vwap", dates, computeVWAP(highs, lows, closes, vols), "#eab308", refs, 2);
        }

        // ---- Sub-pane indicators ----
        let nextPane = 1;

        if (subPanes.has("rsi")) {
          const paneIdx = nextPane++;
          const rsiVals = computeRSI(closes);
          addLineSeriesToPane(chart, "rsi", dates, rsiVals, "#8b5cf6", refs, paneIdx, 2);
          // Overbought (70) and oversold (30) reference lines
          const constDates = dates.filter((_, i) => rsiVals[i] != null);
          addConstantLine(chart, "rsi_70", constDates, 70, "#ef4444", refs, paneIdx);
          addConstantLine(chart, "rsi_30", constDates, 30, "#22c55e", refs, paneIdx);
        }

        if (subPanes.has("macd")) {
          const paneIdx = nextPane++;
          const macdData = computeMACD(closes);
          addLineSeriesToPane(chart, "macd_line", dates, macdData.macd, "#3b82f6", refs, paneIdx, 2);
          addLineSeriesToPane(chart, "macd_signal", dates, macdData.signal, "#ef4444", refs, paneIdx, 1);
          // Histogram with per-bar coloring
          const histSeries = chart.addSeries(HistogramSeries, {
            priceFormat: { type: "price", precision: 4, minMove: 0.0001 },
            priceScaleId: "",
            lastValueVisible: false,
            priceLineVisible: false,
          }, { pane: paneIdx } as any);
          const histData: { time: Time; value: number; color: string }[] = [];
          for (let i = 0; i < dates.length; i++) {
            if (macdData.histogram[i] != null) {
              histData.push({
                time: dates[i],
                value: macdData.histogram[i]!,
                color: macdData.histogram[i]! >= 0 ? "#22c55e80" : "#ef444480",
              });
            }
          }
          histSeries.setData(histData);
          refs.set("macd_hist", histSeries);
        }

        if (subPanes.has("stoch")) {
          const paneIdx = nextPane++;
          const highs = bars.map((b) => b.high);
          const lows = bars.map((b) => b.low);
          const stochData = computeStochastic(highs, lows, closes);
          addLineSeriesToPane(chart, "stoch_k", dates, stochData.k, "#3b82f6", refs, paneIdx, 2);
          addLineSeriesToPane(chart, "stoch_d", dates, stochData.d, "#ef4444", refs, paneIdx, 1);
          // Overbought (80) and oversold (20) reference lines
          const constDates = dates.filter((_, i) => stochData.k[i] != null);
          addConstantLine(chart, "stoch_80", constDates, 80, "#ef4444", refs, paneIdx);
          addConstantLine(chart, "stoch_20", constDates, 20, "#22c55e", refs, paneIdx);
        }

        if (subPanes.has("stochrsi")) {
          const paneIdx = nextPane++;
          const stochRsiData = computeStochRSI(closes);
          addLineSeriesToPane(chart, "stochrsi_k", dates, stochRsiData.k, "#3b82f6", refs, paneIdx, 2);
          addLineSeriesToPane(chart, "stochrsi_d", dates, stochRsiData.d, "#ef4444", refs, paneIdx, 1);
          const constDates = dates.filter((_, i) => stochRsiData.k[i] != null);
          addConstantLine(chart, "stochrsi_80", constDates, 80, "#ef4444", refs, paneIdx);
          addConstantLine(chart, "stochrsi_20", constDates, 20, "#22c55e", refs, paneIdx);
        }

        if (subPanes.has("atr")) {
          const paneIdx = nextPane++;
          const highs = bars.map((b) => b.high);
          const lows = bars.map((b) => b.low);
          const atrData = computeATR(highs, lows, closes);
          addLineSeriesToPane(chart, "atr_line", dates, atrData, "#f97316", refs, paneIdx, 2);
        }

        if (subPanes.has("obv")) {
          const paneIdx = nextPane++;
          const vols = bars.map((b) => b.volume);
          const obvData = computeOBV(closes, vols);
          addLineSeriesToPane(chart, "obv_line", dates, obvData, "#06b6d4", refs, paneIdx, 2);
        }

        if (subPanes.has("adx")) {
          const paneIdx = nextPane++;
          const highs = bars.map((b) => b.high);
          const lows = bars.map((b) => b.low);
          const adxData = computeADX(highs, lows, closes);
          addLineSeriesToPane(chart, "adx_line", dates, adxData.adx, "#eab308", refs, paneIdx, 2);
          addLineSeriesToPane(chart, "adx_plusdi", dates, adxData.plusDI, "#22c55e", refs, paneIdx, 1);
          addLineSeriesToPane(chart, "adx_minusdi", dates, adxData.minusDI, "#ef4444", refs, paneIdx, 1);
          const constDates = dates.filter((_, i) => adxData.adx[i] != null);
          addConstantLine(chart, "adx_25", constDates, 25, "#ffffff40", refs, paneIdx);
        }

        // ---- Prediction overlay ----
        const sig = signalRef.current;
        if (sig?.predicted_prices) {
          const pp = sig.predicted_prices;
          const lastDate = bars[bars.length - 1].date;
          const predData: { time: Time; value: number }[] = [
            { time: lastDate as Time, value: closes[closes.length - 1] },
          ];

          const dayPrices = [
            { day: 2, price: pp.day_2 },
            { day: 3, price: pp.day_3 },
            { day: 4, price: pp.day_4 },
            { day: 5, price: pp.day_5 },
            { day: 6, price: pp.day_6 },
            { day: 7, price: pp.day_7 },
          ];

          for (const dp of dayPrices) {
            if (dp.price == null) continue;
            const futureDate = addBusinessDays(lastDate, dp.day);
            predData.push({ time: futureDate as Time, value: dp.price });
          }

          if (predData.length > 1) {
            const predSeries = chart.addSeries(LineSeries, {
              color: "#facc15",
              lineWidth: 2,
              lineStyle: LineStyle.Dashed,
              crosshairMarkerVisible: false,
              lastValueVisible: true,
              priceLineVisible: false,
            });
            predSeries.setData(predData);
            seriesRefs.current.set("prediction", predSeries);
          }
        }

        // ---- Support / Resistance price lines ----
        const mainSeries = seriesRefs.current.get("main");
        if (mainSeries && sig?.support_level) {
          mainSeries.createPriceLine({
            price: sig.support_level,
            color: "#22c55e80",
            lineWidth: 1,
            lineStyle: LineStyle.Dashed,
            axisLabelVisible: true,
            title: "S",
          });
        }
        if (mainSeries && sig?.resistance_level) {
          mainSeries.createPriceLine({
            price: sig.resistance_level,
            color: "#ef444480",
            lineWidth: 1,
            lineStyle: LineStyle.Dashed,
            axisLabelVisible: true,
            title: "R",
          });
        }

        chart.timeScale().fitContent();
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load chart");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    void render();
    return () => { cancelled = true; };
  }, [symbol, period, chartType, overlays, subPanes]);

  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg p-4">
      {/* Controls */}
      <div className="flex items-center justify-between gap-3 mb-3 flex-wrap">
        {/* Chart type toggles */}
        <div className="flex items-center gap-1">
          {(["candlestick", "line", "area"] as ChartType[]).map((t) => (
            <button
              key={t}
              onClick={() => setChartType(t)}
              className={clsx(
                "px-2.5 py-1 rounded text-[11px] font-medium transition-colors capitalize",
                chartType === t
                  ? "bg-blue-600 text-white"
                  : "bg-[var(--surface-active)] text-[var(--text-muted)] hover:text-[var(--text)]",
              )}
            >
              {t}
            </button>
          ))}
        </div>

        {/* Overlay toggles */}
        <div className="flex items-center gap-1">
          {OVERLAY_DEFS.map((o) => (
            <button
              key={o.key}
              onClick={() => toggleOverlay(o.key)}
              className={clsx(
                "px-2 py-1 rounded text-[10px] font-medium transition-colors border",
                overlays.has(o.key)
                  ? "border-current text-opacity-100"
                  : "border-[var(--border)] text-[var(--text-dim)] hover:text-[var(--text-muted)]",
              )}
              style={overlays.has(o.key) ? { color: o.color, borderColor: o.color + "60" } : undefined}
            >
              {o.label}
            </button>
          ))}
        </div>

        {/* Sub-pane indicators */}
        <div className="flex items-center gap-1">
          {SUB_PANE_DEFS.map((sp) => (
            <button
              key={sp.key}
              onClick={() => toggleSubPane(sp.key)}
              className={clsx(
                "px-2 py-1 rounded text-[10px] font-medium transition-colors border",
                subPanes.has(sp.key)
                  ? "bg-blue-600/20 text-blue-400 border-blue-500/40"
                  : "border-[var(--border)] text-[var(--text-dim)] hover:text-[var(--text-muted)]",
              )}
            >
              {sp.label}
            </button>
          ))}
        </div>
      </div>

      {/* Period selector */}
      <div className="flex items-center gap-1 mb-3">
        {PERIODS.map((p) => (
          <button
            key={p}
            onClick={() => setPeriod(p)}
            className={clsx(
              "px-2 py-0.5 rounded text-[10px] font-medium transition-colors tabular-nums",
              period === p
                ? "bg-[var(--surface-elevated)] text-[var(--text)]"
                : "text-[var(--text-muted)] hover:text-[var(--text)]",
            )}
          >
            {PERIOD_LABELS[p]}
          </button>
        ))}
      </div>

      {/* Chart container */}
      <div className="relative">
        <div ref={containerRef} className="rounded-md overflow-hidden" />
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-black/60 rounded-md">
            <Loader2 className="h-5 w-5 animate-spin text-blue-500" />
          </div>
        )}
        {error && !loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-black/60 rounded-md">
            <span className="text-xs text-red-400">{error}</span>
          </div>
        )}
      </div>
    </div>
  );
}

/* ---- Helpers ---- */

function addLineSeries(
  chart: IChartApi,
  key: string,
  dates: Time[],
  values: (number | null)[],
  color: string,
  refs: Map<string, ISeriesApi<SeriesType>>,
  lineWidth: number = 1,
  lineStyle = LineStyle.Solid,
) {
  const series = chart.addSeries(LineSeries, {
    color,
    lineWidth: lineWidth as any,
    lineStyle,
    crosshairMarkerVisible: false,
    lastValueVisible: false,
    priceLineVisible: false,
  });
  const data: { time: Time; value: number }[] = [];
  for (let i = 0; i < dates.length; i++) {
    if (values[i] != null) {
      data.push({ time: dates[i], value: values[i]! });
    }
  }
  series.setData(data);
  refs.set(key, series);
}

/** Add a line series in a specific sub-pane. */
function addLineSeriesToPane(
  chart: IChartApi,
  key: string,
  dates: Time[],
  values: (number | null)[],
  color: string,
  refs: Map<string, ISeriesApi<SeriesType>>,
  pane: number,
  lineWidth: number = 1,
  lineStyle = LineStyle.Solid,
) {
  const series = chart.addSeries(LineSeries, {
    color,
    lineWidth: lineWidth as any,
    lineStyle,
    crosshairMarkerVisible: false,
    lastValueVisible: false,
    priceLineVisible: false,
  }, { pane } as any);
  const data: { time: Time; value: number }[] = [];
  for (let i = 0; i < dates.length; i++) {
    if (values[i] != null) {
      data.push({ time: dates[i], value: values[i]! });
    }
  }
  series.setData(data);
  refs.set(key, series);
}

/** Add a constant horizontal dashed reference line in a sub-pane. */
function addConstantLine(
  chart: IChartApi,
  key: string,
  dates: Time[],
  value: number,
  color: string,
  refs: Map<string, ISeriesApi<SeriesType>>,
  pane: number,
) {
  if (dates.length === 0) return;
  const series = chart.addSeries(LineSeries, {
    color,
    lineWidth: 1,
    lineStyle: LineStyle.Dashed,
    crosshairMarkerVisible: false,
    lastValueVisible: false,
    priceLineVisible: false,
  }, { pane } as any);
  series.setData(dates.map((t) => ({ time: t, value })));
  refs.set(key, series);
}

/** Add N business days (skip Fri=5/Sat=6 in Bangladesh). */
function addBusinessDays(dateStr: string, days: number): string {
  const d = new Date(dateStr);
  let added = 0;
  while (added < days) {
    d.setDate(d.getDate() + 1);
    const dow = d.getDay(); // JS: Sun=0, Mon=1, ..., Sat=6
    // Bangladesh weekends: Friday (5) and Saturday (6)
    if (dow !== 5 && dow !== 6) {
      added++;
    }
  }
  return d.toISOString().slice(0, 10);
}
