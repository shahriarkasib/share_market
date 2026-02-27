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
import type { StockSignal, OHLCVBar } from "../../types/index.ts";
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

const PANE_LABELS: Record<string, string> = {
  rsi: "RSI (14)",
  macd: "MACD (12, 26, 9)",
  stoch: "Stoch (14, 3)",
  stochrsi: "StochRSI (14, 14, 3, 3)",
  atr: "ATR (14)",
  obv: "OBV",
  adx: "ADX (14)",
};

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
  height?: number;
}

export default function PriceChart({ symbol, signal, height: baseHeight = 420 }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRefs = useRef<Map<string, ISeriesApi<SeriesType>>>(new Map());

  const [chartType, setChartType] = useState<ChartType>("candlestick");
  const [period, setPeriod] = useState("3m");
  const [overlays, setOverlays] = useState<Set<string>>(() => new Set(["ema9", "ema21"]));
  const [subPanes, setSubPanes] = useState<Set<string>>(() => new Set());
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [bars, setBars] = useState<OHLCVBar[]>([]);

  const toggleOverlay = useCallback((key: string) => {
    setOverlays((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const toggleSubPane = useCallback((key: string) => {
    setSubPanes((prev) => {
      const n = new Set(prev);
      if (n.has(key)) n.delete(key);
      else n.add(key);
      return n;
    });
  }, []);

  // Fetch OHLCV data
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    setBars([]);

    fetchOHLCV(symbol, period)
      .then((data) => {
        if (cancelled) return;
        setBars(data);
        if (data.length === 0) setError("No historical data");
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "Failed to load");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [symbol, period]);

  // Render main chart (candlestick/line/area + overlays + volume + S/R)
  useEffect(() => {
    if (!containerRef.current || bars.length === 0) return;

    const container = containerRef.current;
    const colors = getChartColors();
    const chart = createChart(container, {
      width: container.clientWidth,
      height: baseHeight,
      layout: { background: { color: colors.bg }, textColor: colors.text, fontSize: 11 },
      grid: { vertLines: { color: colors.grid }, horzLines: { color: colors.grid } },
      crosshair: {
        vertLine: { color: colors.crosshair, width: 1, style: LineStyle.Dashed, labelBackgroundColor: colors.crosshairLabel },
        horzLine: { color: colors.crosshair, width: 1, style: LineStyle.Dashed, labelBackgroundColor: colors.crosshairLabel },
      },
      rightPriceScale: { borderColor: colors.border, scaleMargins: { top: 0.1, bottom: 0.25 } },
      timeScale: { borderColor: colors.border, timeVisible: false },
    });
    chartRef.current = chart;

    const closes = bars.map((b) => b.close);
    const dates = bars.map((b) => b.date as Time);
    const refs = seriesRefs.current;

    // ---- Main series ----
    if (chartType === "candlestick") {
      const series = chart.addSeries(CandlestickSeries, {
        upColor: "#22c55e", downColor: "#ef4444",
        borderUpColor: "#22c55e", borderDownColor: "#ef4444",
        wickUpColor: "#22c55e80", wickDownColor: "#ef444480",
      });
      series.setData(bars.map((b) => ({ time: b.date as Time, open: b.open, high: b.high, low: b.low, close: b.close })));
      refs.set("main", series);
    } else if (chartType === "line") {
      const series = chart.addSeries(LineSeries, { color: "#3b82f6", lineWidth: 2 });
      series.setData(bars.map((b) => ({ time: b.date as Time, value: b.close })));
      refs.set("main", series);
    } else {
      const series = chart.addSeries(AreaSeries, {
        topColor: "rgba(59,130,246,0.4)", bottomColor: "rgba(59,130,246,0.05)",
        lineColor: "#3b82f6", lineWidth: 2,
      });
      series.setData(bars.map((b) => ({ time: b.date as Time, value: b.close })));
      refs.set("main", series);
    }

    // ---- Volume histogram ----
    const volSeries = chart.addSeries(HistogramSeries, { priceFormat: { type: "volume" }, priceScaleId: "vol" });
    chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    volSeries.setData(bars.map((b) => ({
      time: b.date as Time, value: b.volume,
      color: b.close >= b.open ? "#22c55e40" : "#ef444440",
    })));

    // ---- Overlays ----
    if (overlays.has("ema9")) addOverlay(chart, dates, computeEMA(closes, 9), "#3b82f6");
    if (overlays.has("ema21")) addOverlay(chart, dates, computeEMA(closes, 21), "#f97316");
    if (overlays.has("sma50")) addOverlay(chart, dates, computeSMA(closes, 50), "#a855f7");
    if (overlays.has("sma200")) addOverlay(chart, dates, computeSMA(closes, 200), "#dc2626");
    if (overlays.has("bb")) {
      const bb = computeBollingerBands(closes);
      addOverlay(chart, dates, bb.upper, "#64748b", 1, LineStyle.Dashed);
      addOverlay(chart, dates, bb.middle, "#64748b", 1, LineStyle.Dotted);
      addOverlay(chart, dates, bb.lower, "#64748b", 1, LineStyle.Dashed);
    }
    if (overlays.has("vwap")) {
      addOverlay(chart, dates, computeVWAP(bars.map((b) => b.high), bars.map((b) => b.low), closes, bars.map((b) => b.volume)), "#eab308", 2);
    }

    // ---- Support / Resistance price lines ----
    const mainSeries = refs.get("main");
    if (mainSeries && signal?.support_level) {
      mainSeries.createPriceLine({ price: signal.support_level, color: "#22c55e80", lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: "S" });
    }
    if (mainSeries && signal?.resistance_level) {
      mainSeries.createPriceLine({ price: signal.resistance_level, color: "#ef444480", lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: "R" });
    }

    chart.timeScale().fitContent();

    const ro = new ResizeObserver(() => {
      const w = container.clientWidth;
      if (w > 0) chart.resize(w, baseHeight);
    });
    ro.observe(container);

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRefs.current.clear();
    };
  }, [bars, chartType, overlays, baseHeight, signal]);

  // Ordered list of active sub-panes (preserve definition order)
  const activeSubPanes = SUB_PANE_DEFS.filter((d) => subPanes.has(d.key)).map((d) => d.key);

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

      {/* Main chart container */}
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

      {/* Indicator sub-panes — each is a separate chart instance */}
      {bars.length > 0 && activeSubPanes.map((key) => (
        <IndicatorPane key={`${key}-${symbol}-${period}`} paneKey={key} bars={bars} />
      ))}
    </div>
  );
}

/* ─── Indicator Sub-Pane (self-contained chart) ─── */

const PANE_HEIGHT = 130;

function IndicatorPane({ paneKey, bars }: { paneKey: string; bars: OHLCVBar[] }) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!ref.current || bars.length === 0) return;
    const container = ref.current;
    const colors = getChartColors();

    const chart = createChart(container, {
      width: container.clientWidth,
      height: PANE_HEIGHT,
      layout: { background: { color: colors.bg }, textColor: colors.text, fontSize: 10 },
      grid: { vertLines: { color: colors.grid }, horzLines: { color: colors.grid } },
      rightPriceScale: { borderColor: colors.border },
      timeScale: { borderColor: colors.border, timeVisible: false, visible: false },
      crosshair: {
        vertLine: { color: colors.crosshair, width: 1, style: LineStyle.Dashed, labelBackgroundColor: colors.crosshairLabel },
        horzLine: { color: colors.crosshair, width: 1, style: LineStyle.Dashed, labelBackgroundColor: colors.crosshairLabel },
      },
    });

    const closes = bars.map((b) => b.close);
    const dates = bars.map((b) => b.date as Time);
    const highs = bars.map((b) => b.high);
    const lows = bars.map((b) => b.low);
    const vols = bars.map((b) => b.volume);

    const addLine = (values: (number | null)[], color: string, width = 2, style = LineStyle.Solid) => {
      const series = chart.addSeries(LineSeries, {
        color, lineWidth: width as any, lineStyle: style,
        crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false,
      });
      const data: { time: Time; value: number }[] = [];
      for (let i = 0; i < dates.length; i++) {
        if (values[i] != null) data.push({ time: dates[i], value: values[i]! });
      }
      series.setData(data);
      return data;
    };

    const addRef = (refDates: { time: Time }[], value: number, color: string) => {
      if (refDates.length === 0) return;
      const series = chart.addSeries(LineSeries, {
        color, lineWidth: 1, lineStyle: LineStyle.Dashed,
        crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false,
      });
      series.setData(refDates.map((d) => ({ time: d.time, value })));
    };

    switch (paneKey) {
      case "rsi": {
        const vals = computeRSI(closes);
        const data = addLine(vals, "#8b5cf6");
        addRef(data, 70, "#ef4444");
        addRef(data, 30, "#22c55e");
        break;
      }
      case "macd": {
        const m = computeMACD(closes);
        addLine(m.macd, "#3b82f6");
        addLine(m.signal, "#ef4444", 1);
        const histSeries = chart.addSeries(HistogramSeries, {
          priceFormat: { type: "price", precision: 4, minMove: 0.0001 },
          lastValueVisible: false, priceLineVisible: false,
        });
        const hd: { time: Time; value: number; color: string }[] = [];
        for (let i = 0; i < dates.length; i++) {
          if (m.histogram[i] != null) {
            hd.push({ time: dates[i], value: m.histogram[i]!, color: m.histogram[i]! >= 0 ? "#22c55e80" : "#ef444480" });
          }
        }
        histSeries.setData(hd);
        break;
      }
      case "stoch": {
        const st = computeStochastic(highs, lows, closes);
        const data = addLine(st.k, "#3b82f6");
        addLine(st.d, "#ef4444", 1);
        addRef(data, 80, "#ef4444");
        addRef(data, 20, "#22c55e");
        break;
      }
      case "stochrsi": {
        const sr = computeStochRSI(closes);
        const data = addLine(sr.k, "#3b82f6");
        addLine(sr.d, "#ef4444", 1);
        addRef(data, 80, "#ef4444");
        addRef(data, 20, "#22c55e");
        break;
      }
      case "atr": {
        addLine(computeATR(highs, lows, closes), "#f97316");
        break;
      }
      case "obv": {
        addLine(computeOBV(closes, vols), "#06b6d4");
        break;
      }
      case "adx": {
        const a = computeADX(highs, lows, closes);
        const data = addLine(a.adx, "#eab308");
        addLine(a.plusDI, "#22c55e", 1);
        addLine(a.minusDI, "#ef4444", 1);
        addRef(data, 25, "#ffffff40");
        break;
      }
    }

    chart.timeScale().fitContent();

    const ro = new ResizeObserver(() => {
      const w = container.clientWidth;
      if (w > 0) chart.resize(w, PANE_HEIGHT);
    });
    ro.observe(container);

    return () => {
      ro.disconnect();
      chart.remove();
    };
  }, [paneKey, bars]);

  return (
    <div className="mt-2">
      <span className="text-[10px] text-[var(--text-muted)] font-medium px-1">
        {PANE_LABELS[paneKey] ?? paneKey}
      </span>
      <div ref={ref} className="rounded-sm overflow-hidden" />
    </div>
  );
}

/* ─── Helpers ─── */

function addOverlay(
  chart: IChartApi,
  dates: Time[],
  values: (number | null)[],
  color: string,
  lineWidth = 1,
  lineStyle = LineStyle.Solid,
) {
  const series = chart.addSeries(LineSeries, {
    color, lineWidth: lineWidth as any, lineStyle,
    crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false,
  });
  const data: { time: Time; value: number }[] = [];
  for (let i = 0; i < dates.length; i++) {
    if (values[i] != null) data.push({ time: dates[i], value: values[i]! });
  }
  series.setData(data);
}
