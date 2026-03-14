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
  type LogicalRange,
  type MouseEventParams,
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
  computeMFI,
  computeCMF,
  computeCCI,
  computeWilliamsR,
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
  { key: "mfi", label: "MFI" },
  { key: "cmf", label: "CMF" },
  { key: "cci", label: "CCI" },
  { key: "willr", label: "%R" },
  { key: "atr", label: "ATR" },
  { key: "obv", label: "OBV" },
  { key: "adx", label: "ADX" },
] as const;

const PANE_LABELS: Record<string, string> = {
  rsi: "RSI (14)",
  macd: "MACD (12, 26, close, 9)",
  stoch: "Stoch (14, 3)",
  stochrsi: "Stoch RSI (14, 14, 3, 3)",
  mfi: "MFI (14)",
  cmf: "CMF (20)",
  cci: "CCI (20)",
  willr: "Williams %R (14)",
  atr: "ATR (14)",
  obv: "OBV",
  adx: "ADX (14)",
};

/* Pane heights by indicator complexity (like LankaBangla — compact, fit in viewport) */
const COMPLEX_PANE_HEIGHT = 80;  // RSI, MACD, StochRSI, Stoch (bands/histograms)
const SIMPLE_PANE_HEIGHT = 50;   // ATR, OBV, ADX (single line)
const COMPLEX_PANES = new Set(["rsi", "macd", "stoch", "stochrsi", "mfi", "cci", "willr"]);
const PANE_HEADER_HEIGHT = 14;

/** Always use light/white chart colors — matches LankaBangla style */
function getChartColors() {
  return {
    bg: "#ffffff",
    text: "#475569",
    grid: "#e2e8f030",
    crosshair: "#94a3b8",
    crosshairLabel: "#334155",
    border: "#e2e8f0",
  };
}

interface Props {
  symbol: string;
  signal?: StockSignal | null;
  height?: number; // optional override; if omitted, fills parent
}

/* ─── OHLCV Legend ─── */
function OHLCVLegend({ bar, overlayValues }: {
  bar: OHLCVBar | null;
  overlayValues: { label: string; value: number | null; color: string }[];
}) {
  if (!bar) return null;
  const isUp = bar.close >= bar.open;
  const clr = isUp ? "text-green-600" : "text-red-600";
  return (
    <div className="flex items-center gap-3 flex-wrap text-[10px] font-mono leading-none">
      <span className="text-slate-500">O <span className={clr}>{bar.open.toFixed(1)}</span></span>
      <span className="text-slate-500">H <span className={clr}>{bar.high.toFixed(1)}</span></span>
      <span className="text-slate-500">L <span className={clr}>{bar.low.toFixed(1)}</span></span>
      <span className="text-slate-500">C <span className={clr}>{bar.close.toFixed(1)}</span></span>
      <span className="text-slate-500">V <span className="text-slate-700">{(bar.volume / 1000).toFixed(0)}K</span></span>
      {overlayValues.map((ov) =>
        ov.value != null ? (
          <span key={ov.label} style={{ color: ov.color }} className="font-medium">
            {ov.label} {ov.value.toFixed(2)}
          </span>
        ) : null,
      )}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════
 * Main PriceChart — fills parent height, no scrolling
 * ═══════════════════════════════════════════════════════════ */
export default function PriceChart({ symbol, signal, height: fixedHeight }: Props) {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const mainContainerRef = useRef<HTMLDivElement>(null);
  const paneContainerRefs = useRef<Map<string, HTMLDivElement>>(new Map());
  const allChartsRef = useRef<IChartApi[]>([]);
  const chartSeriesRef = useRef<Map<IChartApi, ISeriesApi<SeriesType>>>(new Map());
  const syncingRef = useRef(false);

  const [chartType, setChartType] = useState<ChartType>("candlestick");
  const [overlays, setOverlays] = useState<Set<string>>(() => new Set(["ema9", "ema21"]));
  const [subPanes, setSubPanes] = useState<Set<string>>(() => new Set(["macd"]));
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [bars, setBars] = useState<OHLCVBar[]>([]);

  const [legendBar, setLegendBar] = useState<OHLCVBar | null>(null);
  const [legendOverlays, setLegendOverlays] = useState<{ label: string; value: number | null; color: string }[]>([]);
  const [paneValues, setPaneValues] = useState<Record<string, string>>({});

  const indicatorDataRef = useRef<Record<string, (number | null)[]>>({});
  const overlayDataRef = useRef<Record<string, { values: (number | null)[]; color: string }>>({});

  const toggleOverlay = useCallback((key: string) => {
    setOverlays((prev) => { const n = new Set(prev); n.has(key) ? n.delete(key) : n.add(key); return n; });
  }, []);
  const toggleSubPane = useCallback((key: string) => {
    setSubPanes((prev) => { const n = new Set(prev); n.has(key) ? n.delete(key) : n.add(key); return n; });
  }, []);

  // Period buttons control visible range (not data fetch)
  const PERIOD_BARS: Record<string, number> = {
    "1w": 5, "2w": 10, "1m": 22, "3m": 65, "6m": 130, "1y": 252, "2y": 504, "all": 9999,
  };
  const setVisiblePeriod = useCallback((p: string) => {
    const total = bars.length;
    if (total === 0) return;
    const count = Math.min(PERIOD_BARS[p] || 65, total);
    const range = { from: total - count - 2, to: total + 5 };
    for (const c of allChartsRef.current) {
      c.timeScale().setVisibleLogicalRange(range);
    }
  }, [bars]);

  // ── Fetch all OHLCV data once (3y), view range controlled by period buttons ──
  useEffect(() => {
    let cancelled = false;
    setLoading(true); setError(null); setBars([]);
    fetchOHLCV(symbol, "3y")
      .then((data) => { if (!cancelled) { setBars(data); if (data.length === 0) setError("No historical data"); } })
      .catch((err) => { if (!cancelled) setError(err instanceof Error ? err.message : "Failed to load"); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [symbol]);

  // ── Pre-compute indicators ──
  useEffect(() => {
    if (bars.length === 0) return;
    const closes = bars.map((b) => b.close);
    const highs = bars.map((b) => b.high);
    const lows = bars.map((b) => b.low);
    const vols = bars.map((b) => b.volume);

    const ov: Record<string, { values: (number | null)[]; color: string }> = {};
    ov.ema9 = { values: computeEMA(closes, 9), color: "#3b82f6" };
    ov.ema21 = { values: computeEMA(closes, 21), color: "#f97316" };
    ov.sma50 = { values: computeSMA(closes, 50), color: "#a855f7" };
    ov.sma200 = { values: computeSMA(closes, 200), color: "#dc2626" };
    const bb = computeBollingerBands(closes);
    ov.bb_upper = { values: bb.upper, color: "#64748b" };
    ov.bb_middle = { values: bb.middle, color: "#64748b" };
    ov.bb_lower = { values: bb.lower, color: "#64748b" };
    ov.vwap = { values: computeVWAP(highs, lows, closes, vols), color: "#eab308" };
    overlayDataRef.current = ov;

    const ind: Record<string, (number | null)[]> = {};
    const rsi = computeRSI(closes); ind.rsi = rsi;
    const macd = computeMACD(closes); ind.macd_line = macd.macd; ind.macd_signal = macd.signal; ind.macd_hist = macd.histogram;
    const stoch = computeStochastic(highs, lows, closes); ind.stoch_k = stoch.k; ind.stoch_d = stoch.d;
    const stochrsi = computeStochRSI(closes); ind.stochrsi_k = stochrsi.k; ind.stochrsi_d = stochrsi.d;
    ind.mfi = computeMFI(highs, lows, closes, vols);
    ind.cmf = computeCMF(highs, lows, closes, vols);
    ind.cci = computeCCI(highs, lows, closes);
    ind.willr = computeWilliamsR(highs, lows, closes);
    ind.atr = computeATR(highs, lows, closes);
    ind.obv = computeOBV(closes, vols);
    const adx = computeADX(highs, lows, closes); ind.adx = adx.adx; ind.plusDI = adx.plusDI; ind.minusDI = adx.minusDI;
    indicatorDataRef.current = ind;

    setLegendBar(bars[bars.length - 1]);
    updateLegendForIndex(bars.length - 1, ov);
    updatePaneValuesForIndex(bars.length - 1, ind);
  }, [bars]);

  function updateLegendForIndex(idx: number, ov: Record<string, { values: (number | null)[]; color: string }>) {
    const out: { label: string; value: number | null; color: string }[] = [];
    for (const def of OVERLAY_DEFS) {
      if (!overlays.has(def.key)) continue;
      if (def.key === "bb") {
        out.push({ label: "BB\u2191", value: ov.bb_upper?.values[idx] ?? null, color: "#64748b" });
        out.push({ label: "BB\u2193", value: ov.bb_lower?.values[idx] ?? null, color: "#64748b" });
      } else {
        out.push({ label: def.label, value: ov[def.key]?.values[idx] ?? null, color: def.color });
      }
    }
    setLegendOverlays(out);
  }

  function updatePaneValuesForIndex(idx: number, ind: Record<string, (number | null)[]>) {
    const f = (v: number | null | undefined, d = 2) => v != null ? v.toFixed(d) : "\u2014";
    setPaneValues({
      rsi: f(ind.rsi?.[idx]),
      macd: `${f(ind.macd_line?.[idx], 4)} ${f(ind.macd_signal?.[idx], 4)} ${f(ind.macd_hist?.[idx], 4)}`,
      stoch: `%K ${f(ind.stoch_k?.[idx])} %D ${f(ind.stoch_d?.[idx])}`,
      stochrsi: `%K ${f(ind.stochrsi_k?.[idx])} %D ${f(ind.stochrsi_d?.[idx])}`,
      mfi: f(ind.mfi?.[idx]),
      cmf: f(ind.cmf?.[idx], 4),
      cci: f(ind.cci?.[idx]),
      willr: f(ind.willr?.[idx]),
      atr: f(ind.atr?.[idx]),
      obv: ind.obv?.[idx] != null ? (ind.obv[idx]! / 1000).toFixed(0) + "K" : "\u2014",
      adx: `ADX ${f(ind.adx?.[idx])} +DI ${f(ind.plusDI?.[idx])} -DI ${f(ind.minusDI?.[idx])}`,
    });
  }

  // ── Crosshair sync ──
  function syncCrosshair(src: IChartApi, time: Time | undefined) {
    if (syncingRef.current) return;
    syncingRef.current = true;
    for (const c of allChartsRef.current) {
      if (c !== src) {
        const series = chartSeriesRef.current.get(c);
        if (time && series) c.setCrosshairPosition(NaN, time, series);
        else c.clearCrosshairPosition();
      }
    }
    syncingRef.current = false;
  }
  function syncTimeScale(src: IChartApi, range: LogicalRange | null) {
    if (syncingRef.current || !range) return;
    syncingRef.current = true;
    for (const c of allChartsRef.current) { if (c !== src) c.timeScale().setVisibleLogicalRange(range); }
    syncingRef.current = false;
  }

  // ── Compute heights — panes get fixed compact sizes, main chart fills the rest ──
  const activeSubPanes = SUB_PANE_DEFS.filter((d) => subPanes.has(d.key)).map((d) => d.key);

  function getPaneHeight(key: string) {
    return COMPLEX_PANES.has(key) ? COMPLEX_PANE_HEIGHT : SIMPLE_PANE_HEIGHT;
  }
  function getMainHeight(containerH: number) {
    // Toolbar ~28px
    const toolbarH = 28;
    const totalPaneH = activeSubPanes.reduce((sum, k) => sum + getPaneHeight(k) + PANE_HEADER_HEIGHT, 0);
    return Math.max(200, containerH - toolbarH - totalPaneH);
  }

  // ── Render all charts ──
  useEffect(() => {
    if (!mainContainerRef.current || !wrapperRef.current || bars.length === 0) return;

    for (const c of allChartsRef.current) { try { c.remove(); } catch { /* ok */ } }
    allChartsRef.current = [];
    chartSeriesRef.current.clear();

    const colors = getChartColors();
    const dates = bars.map((b) => b.date as Time);

    const containerH = fixedHeight || wrapperRef.current.clientHeight || 600;
    const mainH = getMainHeight(containerH);
    const hasSubPanes = activeSubPanes.length > 0;

    // Fixed price scale width across ALL charts so date columns align
    // Must be wide enough for the longest label (main chart prices like "1234.5")
    const PRICE_SCALE_WIDTH = 85;

    const mkOpts = (el: HTMLElement, h: number, showTime: boolean) => ({
      width: el.clientWidth,
      height: h,
      layout: { background: { color: colors.bg }, textColor: colors.text, fontSize: 11 },
      grid: { vertLines: { color: colors.grid }, horzLines: { color: colors.grid } },
      crosshair: {
        vertLine: { color: colors.crosshair, width: 1 as const, style: LineStyle.Dashed, labelBackgroundColor: colors.crosshairLabel },
        horzLine: { color: colors.crosshair, width: 1 as const, style: LineStyle.Dashed, labelBackgroundColor: colors.crosshairLabel },
      },
      rightPriceScale: { borderColor: colors.border, minimumWidth: PRICE_SCALE_WIDTH },
      timeScale: { borderColor: colors.border, timeVisible: false, visible: showTime },
    });

    // Compact price formatter — right-padded to consistent width so all price scales match
    const compactFormat = (v: number) => {
      const a = Math.abs(v);
      let s: string;
      if (a >= 1e9) s = (v / 1e9).toFixed(1) + "B";
      else if (a >= 1e6) s = (v / 1e6).toFixed(1) + "M";
      else if (a >= 1e3) s = (v / 1e3).toFixed(1) + "K";
      else if (a >= 10) s = v.toFixed(1);
      else if (a >= 1) s = v.toFixed(2);
      else if (a >= 0.001) s = v.toFixed(3);
      else s = v.toFixed(4);
      // Pad to 8 chars so all price scales have identical width
      return s.padStart(8);
    };

    // ──── MAIN CHART ────
    const mainEl = mainContainerRef.current;
    const mainChart = createChart(mainEl, {
      ...mkOpts(mainEl, mainH, !hasSubPanes),
      rightPriceScale: { borderColor: colors.border, minimumWidth: PRICE_SCALE_WIDTH, scaleMargins: { top: 0.05, bottom: 0.18 } },
    });
    allChartsRef.current.push(mainChart);

    // Main series — use same compactFormat so price scale width matches sub-panes
    const mainPriceFmt = { type: "custom" as const, formatter: compactFormat };
    if (chartType === "candlestick") {
      const s = mainChart.addSeries(CandlestickSeries, {
        upColor: "#22c55e", downColor: "#ef4444",
        borderUpColor: "#16a34a", borderDownColor: "#dc2626",
        wickUpColor: "#22c55e", wickDownColor: "#ef4444",
        priceFormat: mainPriceFmt,
      });
      s.setData(bars.map((b) => ({ time: b.date as Time, open: b.open, high: b.high, low: b.low, close: b.close })));
      chartSeriesRef.current.set(mainChart, s as ISeriesApi<SeriesType>);
      if (signal?.support_level) s.createPriceLine({ price: signal.support_level, color: "#22c55e80", lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: "S" });
      if (signal?.resistance_level) s.createPriceLine({ price: signal.resistance_level, color: "#ef444480", lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: "R" });
    } else if (chartType === "line") {
      const s = mainChart.addSeries(LineSeries, { color: "#3b82f6", lineWidth: 2, priceFormat: mainPriceFmt });
      s.setData(bars.map((b) => ({ time: b.date as Time, value: b.close })));
      chartSeriesRef.current.set(mainChart, s as ISeriesApi<SeriesType>);
    } else {
      const s = mainChart.addSeries(AreaSeries, { topColor: "rgba(59,130,246,0.4)", bottomColor: "rgba(59,130,246,0.05)", lineColor: "#3b82f6", lineWidth: 2, priceFormat: mainPriceFmt });
      s.setData(bars.map((b) => ({ time: b.date as Time, value: b.close })));
      chartSeriesRef.current.set(mainChart, s as ISeriesApi<SeriesType>);
    }

    // Volume (colored histogram + SMA 20 line, like LankaBangla)
    const volS = mainChart.addSeries(HistogramSeries, { priceFormat: { type: "volume" }, priceScaleId: "vol" });
    mainChart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.65, bottom: 0 } });
    volS.setData(bars.map((b) => ({ time: b.date as Time, value: b.volume, color: b.close >= b.open ? "rgba(38,166,154,0.6)" : "rgba(239,83,80,0.6)" })));
    // Volume SMA(20) line
    const volSma = computeSMA(bars.map((b) => b.volume), 20);
    const volSmaS = mainChart.addSeries(LineSeries, { color: "#ff6d00", lineWidth: 1, priceScaleId: "vol", crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false });
    const volSmaD: { time: Time; value: number }[] = [];
    for (let i = 0; i < dates.length; i++) if (volSma[i] != null) volSmaD.push({ time: dates[i], value: volSma[i]! });
    volSmaS.setData(volSmaD);

    // Overlays
    const addOv = (vals: (number | null)[], color: string, w = 1, ls = LineStyle.Solid) => {
      const s = mainChart.addSeries(LineSeries, { color, lineWidth: w as any, lineStyle: ls, crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false });
      const d: { time: Time; value: number }[] = [];
      for (let i = 0; i < dates.length; i++) if (vals[i] != null) d.push({ time: dates[i], value: vals[i]! });
      s.setData(d);
    };
    const ov = overlayDataRef.current;
    if (overlays.has("ema9") && ov.ema9) addOv(ov.ema9.values, "#3b82f6");
    if (overlays.has("ema21") && ov.ema21) addOv(ov.ema21.values, "#f97316");
    if (overlays.has("sma50") && ov.sma50) addOv(ov.sma50.values, "#a855f7");
    if (overlays.has("sma200") && ov.sma200) addOv(ov.sma200.values, "#dc2626");
    if (overlays.has("bb")) {
      if (ov.bb_upper) addOv(ov.bb_upper.values, "#64748b", 1, LineStyle.Dashed);
      if (ov.bb_middle) addOv(ov.bb_middle.values, "#64748b", 1, LineStyle.Dotted);
      if (ov.bb_lower) addOv(ov.bb_lower.values, "#64748b", 1, LineStyle.Dashed);
    }
    if (overlays.has("vwap") && ov.vwap) addOv(ov.vwap.values, "#eab308", 2);

    // ──── SUB-PANES ────
    const ind = indicatorDataRef.current;
    for (let pi = 0; pi < activeSubPanes.length; pi++) {
      const key = activeSubPanes[pi];
      const el = paneContainerRefs.current.get(key);
      if (!el) continue;
      const isLast = pi === activeSubPanes.length - 1;
      const thisPaneH = getPaneHeight(key);
      const paneOpts = mkOpts(el, thisPaneH, isLast);
      paneOpts.rightPriceScale = {
        ...paneOpts.rightPriceScale,
        minimumWidth: PRICE_SCALE_WIDTH,
      };
      const pc = createChart(el, paneOpts);
      allChartsRef.current.push(pc);

      let firstSeriesStored = false;
      const priceFmt = { type: "custom" as const, formatter: compactFormat };
      const addL = (vals: (number | null)[], color: string, w = 2, ls = LineStyle.Solid) => {
        const s = pc.addSeries(LineSeries, { color, lineWidth: w as any, lineStyle: ls, priceFormat: priceFmt, crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false });
        const d: { time: Time; value: number }[] = [];
        for (let i = 0; i < dates.length; i++) if (vals[i] != null) d.push({ time: dates[i], value: vals[i]! });
        s.setData(d);
        if (!firstSeriesStored) { chartSeriesRef.current.set(pc, s as ISeriesApi<SeriesType>); firstSeriesStored = true; }
        return d;
      };
      const addRef = (rd: { time: Time }[], v: number, c: string) => {
        if (!rd.length) return;
        const s = pc.addSeries(LineSeries, { color: c, lineWidth: 1, lineStyle: LineStyle.Dashed, priceFormat: priceFmt, crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false });
        s.setData(rd.map((x) => ({ time: x.time, value: v })));
      };
      // Purple shaded band (like LankaBangla) — add BEFORE indicator lines so lines render on top
      const addBand = (lo: number, hi: number) => {
        // Need dates array for the band — use the full date range
        const bandDates = dates.map((t) => ({ time: t }));
        if (!bandDates.length) return;
        const bandS = pc.addSeries(AreaSeries, {
          topColor: "rgba(206, 147, 216, 0.25)",
          bottomColor: "rgba(206, 147, 216, 0.25)",
          lineColor: "rgba(156, 39, 176, 0.12)",
          lineWidth: 1, priceFormat: priceFmt,
          crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false,
        });
        bandS.setData(bandDates.map((x) => ({ time: x.time, value: hi })));
        const bandLo = pc.addSeries(AreaSeries, {
          topColor: "#ffffff",
          bottomColor: "#ffffff",
          lineColor: "rgba(156, 39, 176, 0.12)",
          lineWidth: 1, priceFormat: priceFmt,
          crosshairMarkerVisible: false, lastValueVisible: false, priceLineVisible: false,
        });
        bandLo.setData(bandDates.map((x) => ({ time: x.time, value: lo })));
      };

      switch (key) {
        case "rsi": {
          addBand(30, 70);
          addL(ind.rsi || [], "#8b5cf6");
          addRef(dates.map((t) => ({ time: t })), 70, "#9c27b050");
          addRef(dates.map((t) => ({ time: t })), 30, "#9c27b050");
          break;
        }
        case "macd": {
          // Histogram first (behind lines)
          const hs = pc.addSeries(HistogramSeries, { priceFormat: priceFmt, lastValueVisible: false, priceLineVisible: false });
          if (!firstSeriesStored) { chartSeriesRef.current.set(pc, hs as ISeriesApi<SeriesType>); firstSeriesStored = true; }
          const hd: { time: Time; value: number; color: string }[] = [];
          const hist = ind.macd_hist || [];
          for (let i = 0; i < dates.length; i++) if (hist[i] != null) hd.push({ time: dates[i], value: hist[i]!, color: hist[i]! >= 0 ? "#26a69acc" : "#ef5350cc" });
          hs.setData(hd);
          addL(ind.macd_line || [], "#3b82f6"); addL(ind.macd_signal || [], "#ef4444", 1);
          break;
        }
        case "stoch": {
          addBand(20, 80);
          addL(ind.stoch_k || [], "#3b82f6"); addL(ind.stoch_d || [], "#ef4444", 1);
          addRef(dates.map((t) => ({ time: t })), 80, "#9c27b050");
          addRef(dates.map((t) => ({ time: t })), 20, "#9c27b050");
          break;
        }
        case "stochrsi": {
          addBand(20, 80);
          addL(ind.stochrsi_k || [], "#3b82f6"); addL(ind.stochrsi_d || [], "#ef4444", 1);
          addRef(dates.map((t) => ({ time: t })), 80, "#9c27b050");
          addRef(dates.map((t) => ({ time: t })), 20, "#9c27b050");
          break;
        }
        case "mfi": {
          addBand(20, 80);
          addL(ind.mfi || [], "#e91e63");
          addRef(dates.map((t) => ({ time: t })), 80, "#9c27b050");
          addRef(dates.map((t) => ({ time: t })), 20, "#9c27b050");
          break;
        }
        case "cmf": {
          addL(ind.cmf || [], "#00897b");
          addRef(dates.map((t) => ({ time: t })), 0, "#94a3b850");
          break;
        }
        case "cci": {
          addBand(-100, 100);
          addL(ind.cci || [], "#00bcd4");
          addRef(dates.map((t) => ({ time: t })), 100, "#9c27b050");
          addRef(dates.map((t) => ({ time: t })), -100, "#9c27b050");
          break;
        }
        case "willr": {
          addBand(-80, -20);
          addL(ind.willr || [], "#9c27b0");
          addRef(dates.map((t) => ({ time: t })), -20, "#9c27b050");
          addRef(dates.map((t) => ({ time: t })), -80, "#9c27b050");
          break;
        }
        case "atr": { addL(ind.atr || [], "#f97316"); break; }
        case "obv": { addL(ind.obv || [], "#06b6d4"); break; }
        case "adx": { const d = addL(ind.adx || [], "#eab308"); addL(ind.plusDI || [], "#22c55e", 1); addL(ind.minusDI || [], "#ef4444", 1); addRef(d, 25, "#94a3b840"); break; }
      }
    }

    // ──── SET DEFAULT VIEW on ALL charts: last 3 months ────
    const totalBars = bars.length;
    const barsFor3m = Math.min(65, totalBars);
    const defaultRange = { from: totalBars - barsFor3m - 2, to: totalBars + 5 };
    for (const c of allChartsRef.current) {
      c.timeScale().setVisibleLogicalRange(defaultRange);
    }

    // ──── CROSSHAIR SYNC ────
    const dateToIdx = new Map<string, number>();
    bars.forEach((b, i) => dateToIdx.set(String(b.date), i));

    function onCrosshair(params: MouseEventParams, src: IChartApi) {
      const t = params.time as Time | undefined;
      syncCrosshair(src, t);
      if (t) {
        const idx = dateToIdx.get(String(t));
        if (idx != null) {
          setLegendBar(bars[idx]);
          updateLegendForIndex(idx, overlayDataRef.current);
          updatePaneValuesForIndex(idx, indicatorDataRef.current);
        }
      }
    }
    for (const c of allChartsRef.current) {
      c.subscribeCrosshairMove((p) => onCrosshair(p, c));
      c.timeScale().subscribeVisibleLogicalRangeChange((r: LogicalRange | null) => syncTimeScale(c, r));
    }

    // ──── RESIZE ────
    const ro = new ResizeObserver(() => {
      if (!wrapperRef.current) return;
      const newContainerH = fixedHeight || wrapperRef.current.clientHeight || 600;
      const mh = getMainHeight(newContainerH);
      const w = mainEl.clientWidth;
      if (w <= 0) return;
      allChartsRef.current[0]?.resize(w, mh);
      for (let i = 1; i < allChartsRef.current.length; i++) {
        const key = activeSubPanes[i - 1];
        if (key) allChartsRef.current[i]?.resize(w, getPaneHeight(key));
      }
    });
    ro.observe(wrapperRef.current);

    return () => {
      ro.disconnect();
      for (const c of allChartsRef.current) { try { c.remove(); } catch { /* ok */ } }
      allChartsRef.current = [];
      chartSeriesRef.current.clear();
    };
  }, [bars, chartType, overlays, subPanes, fixedHeight, signal]);

  return (
    <div ref={wrapperRef} className="flex flex-col flex-1 min-h-0 h-full bg-white border border-slate-200 rounded overflow-hidden">
      {/* ── Toolbar ── */}
      <div className="flex-shrink-0 flex items-center gap-1.5 px-2 py-0.5 border-b border-slate-200 bg-slate-50 flex-wrap">
        <div className="flex items-center gap-0.5">
          {(["candlestick", "line", "area"] as ChartType[]).map((t) => (
            <button key={t} onClick={() => setChartType(t)}
              className={clsx("px-2 py-0.5 rounded text-[10px] font-medium capitalize",
                chartType === t ? "bg-blue-600 text-white" : "text-slate-500 hover:text-slate-800")}>
              {t}
            </button>
          ))}
        </div>
        <div className="w-px h-4 bg-slate-300" />
        <div className="flex items-center gap-0.5">
          {([...PERIODS, "all"] as const).map((p) => (
            <button key={p} onClick={() => setVisiblePeriod(p)}
              className="px-1.5 py-0.5 rounded text-[10px] font-medium tabular-nums text-slate-500 hover:text-slate-800 hover:bg-slate-200">
              {p === "all" ? "All" : PERIOD_LABELS[p as keyof typeof PERIOD_LABELS]}
            </button>
          ))}
        </div>
        <div className="w-px h-4 bg-slate-300" />
        <div className="flex items-center gap-0.5">
          {OVERLAY_DEFS.map((o) => (
            <button key={o.key} onClick={() => toggleOverlay(o.key)}
              className={clsx("px-1.5 py-0.5 rounded text-[10px] font-medium",
                overlays.has(o.key) ? "font-bold" : "text-slate-400 hover:text-slate-600")}
              style={overlays.has(o.key) ? { color: o.color } : undefined}>
              {o.label}
            </button>
          ))}
        </div>
        <div className="w-px h-4 bg-slate-300" />
        <div className="flex items-center gap-0.5">
          {SUB_PANE_DEFS.map((sp) => (
            <button key={sp.key} onClick={() => toggleSubPane(sp.key)}
              className={clsx("px-1.5 py-0.5 rounded text-[10px] font-medium",
                subPanes.has(sp.key) ? "bg-blue-100 text-blue-600 font-bold" : "text-slate-400 hover:text-slate-600")}>
              {sp.label}
            </button>
          ))}
        </div>
      </div>

      {/* ── Main Chart (flexes to fill remaining space) ── */}
      <div className="flex-1 min-h-0 relative">
        {/* OHLCV legend overlaid on chart like LankaBangla */}
        <div className="absolute top-1 left-2 z-10 pointer-events-none">
          <OHLCVLegend bar={legendBar} overlayValues={legendOverlays} />
        </div>
        <div ref={mainContainerRef} className="w-full h-full" />
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-black/60">
            <Loader2 className="h-5 w-5 animate-spin text-blue-500" />
          </div>
        )}
        {error && !loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-black/60">
            <span className="text-xs text-red-400">{error}</span>
          </div>
        )}
      </div>

      {/* ── Sub-Panes (fixed size, below main chart) ── */}
      {bars.length > 0 && activeSubPanes.map((key) => (
        <div key={key} className="flex-shrink-0">
          <div className="flex items-center gap-2 px-2 py-0 border-t border-slate-200 bg-white">
            <span className="text-[10px] text-slate-500 font-medium">{PANE_LABELS[key] ?? key}</span>
            <span className="text-[10px] font-mono text-blue-600">{paneValues[key] ?? ""}</span>
          </div>
          <div ref={(el) => { if (el) paneContainerRefs.current.set(key, el); else paneContainerRefs.current.delete(key); }} className="w-full" />
        </div>
      ))}
    </div>
  );
}
