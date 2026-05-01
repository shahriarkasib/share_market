import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  createChart,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  createSeriesMarkers,
  type IChartApi,
  type ISeriesApi,
  type IPriceLine,
  type ISeriesMarkersPluginApi,
  type Time,
} from "lightweight-charts";
import { FVGPrimitive } from "./fvgPrimitive";
import { GannPrimitive, FibCirclesPrimitive } from "./gannFibPrimitives";
import { OrderBlockPrimitive } from "./orderBlockPrimitive";
import { PatternPrimitive, type ChartPattern } from "./patternPrimitive";
import { HarmonicPrimitive, type HarmonicPattern } from "./harmonicPrimitive";
import clsx from "clsx";
import {
  ArrowLeft,
  RefreshCw,
  Search,
  Eye,
  EyeOff,
  AlertTriangle,
} from "lucide-react";
import {
  fetchSMCChart,
  fetchAllPrices,
  fetchNasdaqChart,
  fetchNasdaqTickers,
  type SMCChartData,
} from "../api/client";
import type { StockPrice } from "../types/index";

interface SMCChartProps {
  market?: "dse" | "nasdaq";
}

type Period = "1m" | "3m" | "6m" | "1y" | "2y";
type Timeframe = "daily" | "weekly";

interface Toggles {
  fvg: boolean;
  bos: boolean;
  ob: boolean;
  levels: boolean;
  fib: boolean;
  fibCircles: boolean;
  gann: boolean;
  pivots: boolean;
  ma20: boolean;
  ma50: boolean;
  ma200: boolean;
  bb: boolean;
  rsi: boolean;
  macd: boolean;
  stoch: boolean;
  patterns: boolean;
  harmonics: boolean;
  candles: boolean;
  sr: boolean;
  premiumDiscount: boolean;
  bosZones: boolean;
  vwap: boolean;
  volumeProfile: boolean;
}

const DEFAULT_TOGGLES: Toggles = {
  fvg: true,
  bos: true,
  ob: false,
  levels: false,
  fib: false,
  fibCircles: false,
  gann: false,
  pivots: false,
  ma20: false,
  ma50: false,
  ma200: false,
  bb: false,
  rsi: false,
  macd: false,
  stoch: false,
  patterns: false,
  harmonics: false,
  candles: false,
  sr: false,
  premiumDiscount: true,
  bosZones: true,
  vwap: true,
  volumeProfile: false,
};

const TOGGLES_STORAGE_KEY = "smc-chart-toggles-v5";
const MAX_BOS = 2;
const MAX_FVG = 6;
const CHART_HEIGHT = 600;

function loadToggles(): Toggles {
  if (typeof window === "undefined") return DEFAULT_TOGGLES;
  try {
    const raw = window.localStorage.getItem(TOGGLES_STORAGE_KEY);
    if (!raw) return DEFAULT_TOGGLES;
    const parsed = JSON.parse(raw);
    return { ...DEFAULT_TOGGLES, ...parsed };
  } catch {
    return DEFAULT_TOGGLES;
  }
}

function saveToggles(t: Toggles) {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(TOGGLES_STORAGE_KEY, JSON.stringify(t));
  } catch {
    /* ignore */
  }
}

export default function SMCChart({ market = "dse" }: SMCChartProps = {}) {
  const isNasdaq = market === "nasdaq";
  const cur = isNasdaq ? "$" : "৳";
  const defaultSymbol = isNasdaq ? "NVDA" : "GP";
  const { symbol = defaultSymbol } = useParams();
  const nav = useNavigate();

  // DSE uses "1m","3m","6m","1y","2y"; yfinance uses "1mo","3mo","6mo","1y","2y"
  const periodForApi = (p: string): string => {
    if (!isNasdaq) return p;
    const map: Record<string, string> = {
      "1m": "1mo", "3m": "3mo", "6m": "6mo", "1y": "1y", "2y": "2y",
    };
    return map[p] ?? "1y";
  };
  const fetchChart = (
    sym: string,
    period: string,
    tf: "daily" | "weekly",
    opts: { force?: boolean; signal?: AbortSignal } = {},
  ) =>
    isNasdaq
      ? fetchNasdaqChart(sym, periodForApi(period)) as unknown as Promise<SMCChartData>
      : fetchSMCChart(sym, period as "1m" | "3m" | "6m" | "1y" | "2y", tf, opts);

  // Refs — chart skeleton
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  // Refs — overlays (cleared when chart rebuilds)
  const fvgPrimitiveRef = useRef<FVGPrimitive | null>(null);
  const obPrimitiveRef = useRef<OrderBlockPrimitive | null>(null);
  const gannPrimitiveRef = useRef<GannPrimitive | null>(null);
  const fibCirclesPrimitiveRef = useRef<FibCirclesPrimitive | null>(null);
  const patternPrimitiveRef = useRef<PatternPrimitive | null>(null);
  const harmonicPrimitiveRef = useRef<HarmonicPrimitive | null>(null);
  const candleMarkersRef = useRef<ISeriesMarkersPluginApi<Time> | null>(null);
  const srLinesRef = useRef<IPriceLine[]>([]);
  const bosSeriesRef = useRef<ISeriesApi<"Line">[]>([]);
  const bosMarkersRef = useRef<ISeriesMarkersPluginApi<Time> | null>(null);
  const levelsLinesRef = useRef<IPriceLine[]>([]);
  const pdLinesRef = useRef<IPriceLine[]>([]);
  const bosZoneLinesRef = useRef<IPriceLine[]>([]);
  const vwapSeriesRef = useRef<ISeriesApi<"Line">[]>([]);
  const volumeProfileLinesRef = useRef<IPriceLine[]>([]);
  const fibLinesRef = useRef<IPriceLine[]>([]);
  const pivotLinesRef = useRef<IPriceLine[]>([]);
  const maSeriesRef = useRef<Record<string, ISeriesApi<"Line">>>({});
  const bbSeriesRef = useRef<{ upper?: ISeriesApi<"Line">; middle?: ISeriesApi<"Line">; lower?: ISeriesApi<"Line"> }>({});
  // Sub-pane charts: each in its own DOM container, time-synced with main
  const rsiContainerRef = useRef<HTMLDivElement>(null);
  const macdContainerRef = useRef<HTMLDivElement>(null);
  const stochContainerRef = useRef<HTMLDivElement>(null);
  const rsiChartRef = useRef<IChartApi | null>(null);
  const macdChartRef = useRef<IChartApi | null>(null);
  const stochChartRef = useRef<IChartApi | null>(null);

  // State
  const [data, setData] = useState<SMCChartData | null>(null);
  const [stocks, setStocks] = useState<StockPrice[]>([]);
  const [period, setPeriod] = useState<Period>("2y");
  const [timeframe, setTimeframe] = useState<Timeframe>("daily");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [showDropdown, setShowDropdown] = useState(false);
  const [toggles, setTogglesState] = useState<Toggles>(loadToggles);
  const [chartReady, setChartReady] = useState(false);

  // Persist toggle changes to localStorage
  const setToggles = useCallback((updater: (t: Toggles) => Toggles) => {
    setTogglesState((prev) => {
      const next = updater(prev);
      saveToggles(next);
      return next;
    });
  }, []);

  // Load stocks list once. DSE → live prices endpoint. NASDAQ → full halal
  // universe (~1.8k tickers) from the dedicated tickers endpoint.
  useEffect(() => {
    const ac = new AbortController();
    if (isNasdaq) {
      fetchNasdaqTickers()
        .then((tickers) => {
          if (ac.signal.aborted) return;
          setStocks(
            tickers.map((t) => ({
              symbol: t.symbol,
              company_name: t.name ?? undefined,
              sector: t.sector ?? undefined,
              halal_status: t.halal_status,
              ltp: 0,
              high: 0,
              low: 0,
              open: 0,
              close: 0,
              close_prev: 0,
              change: 0,
              change_pct: 0,
              volume: 0,
              value: 0,
              trade_count: 0,
            })) as unknown as StockPrice[],
          );
        })
        .catch(() => {
          if (!ac.signal.aborted) setStocks([]);
        });
    } else {
      fetchAllPrices()
        .then((s) => {
          if (!ac.signal.aborted) setStocks(s);
        })
        .catch(() => {
          if (!ac.signal.aborted) setStocks([]);
        });
    }
    return () => ac.abort();
  }, [isNasdaq]);

  // Fetch chart data — abortable, with proper error display
  const loadData = useCallback(
    async (force: boolean) => {
      const ac = new AbortController();
      setLoading(true);
      setError(null);
      try {
        const d = await fetchChart(symbol, period, timeframe, {
          force,
          signal: ac.signal,
        });
        if (!ac.signal.aborted) setData(d);
      } catch (err: unknown) {
        if (ac.signal.aborted) return;
        const msg =
          err && typeof err === "object" && "message" in err
            ? String((err as { message?: string }).message)
            : "Failed to load chart";
        setError(msg);
        setData(null);
      } finally {
        if (!ac.signal.aborted) setLoading(false);
      }
      return () => ac.abort();
    },
    [symbol, period, timeframe],
  );

  // Auto-fetch when symbol / period / timeframe changes
  useEffect(() => {
    let cancelled = false;
    const ac = new AbortController();
    setLoading(true);
    setError(null);
    fetchChart(symbol, period, timeframe, { signal: ac.signal })
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch((err: unknown) => {
        if (cancelled || ac.signal.aborted) return;
        const msg =
          err && typeof err === "object" && "message" in err
            ? String((err as { message?: string }).message)
            : "Failed to load chart";
        setError(msg);
        setData(null);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
      ac.abort();
    };
  }, [symbol, period, timeframe]);

  // === LIVE AUTO-REFRESH during DSE market hours (Sun-Thu, 10:00-14:30 BST) ===
  // Polls every 30s so the live bar updates while the market is open.
  useEffect(() => {
    function isMarketOpen(): boolean {
      const now = new Date();
      // Convert to BST (UTC+6)
      const bstOffset = 6 * 60;
      const utcMinutes = now.getUTCHours() * 60 + now.getUTCMinutes();
      const bstMinutes = (utcMinutes + bstOffset) % (24 * 60);
      const bstDay = (now.getUTCDay() + (utcMinutes + bstOffset >= 24 * 60 ? 1 : 0)) % 7;
      // 0=Sun, 1=Mon, ..., 4=Thu, 5=Fri, 6=Sat
      // DSE: Sunday(0) - Thursday(4), 10:00 - 14:30
      const isWeekday = bstDay >= 0 && bstDay <= 4;
      const inHours = bstMinutes >= 10 * 60 && bstMinutes <= 14 * 60 + 30;
      return isWeekday && inHours;
    }

    const tick = window.setInterval(() => {
      if (!isMarketOpen()) return;
      const ac = new AbortController();
      fetchChart(symbol, period, timeframe, { force: true, signal: ac.signal })
        .then((d) => setData(d))
        .catch(() => { /* silent during background refresh */ });
    }, 30_000); // 30s

    return () => window.clearInterval(tick);
  }, [symbol, period, timeframe]);

  // === Chart skeleton: build once per symbol/timeframe (NOT per data refresh) ===
  useEffect(() => {
    if (!containerRef.current) return;

    const isLight =
      document.documentElement.getAttribute("data-theme") === "light";
    const bg = isLight ? "#ffffff" : "#0a0e17";
    const text = isLight ? "#374151" : "#d1d5db";
    const grid = isLight ? "#e5e7eb" : "#1f2937";

    // Force a non-zero width — fall back to parent or window if container is 0
    const width =
      containerRef.current.clientWidth ||
      containerRef.current.parentElement?.clientWidth ||
      Math.max(window.innerWidth - 64, 320);

    const chart = createChart(containerRef.current, {
      width,
      height: CHART_HEIGHT,
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

    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
    });
    volumeSeries
      .priceScale()
      .applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } });

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;

    // Reset overlay refs (chart was just rebuilt)
    fvgPrimitiveRef.current = null;
    obPrimitiveRef.current = null;
    gannPrimitiveRef.current = null;
    fibCirclesPrimitiveRef.current = null;
    patternPrimitiveRef.current = null;
    harmonicPrimitiveRef.current = null;
    candleMarkersRef.current = null;
    srLinesRef.current = [];
    bosSeriesRef.current = [];
    bosMarkersRef.current = null;
    levelsLinesRef.current = [];
    fibLinesRef.current = [];
    pivotLinesRef.current = [];
    pdLinesRef.current = [];
    bosZoneLinesRef.current = [];
    vwapSeriesRef.current = [];
    volumeProfileLinesRef.current = [];
    maSeriesRef.current = {};

    setChartReady(true);

    const ro = new ResizeObserver(() => {
      if (containerRef.current && chartRef.current) {
        const newWidth =
          containerRef.current.clientWidth ||
          containerRef.current.parentElement?.clientWidth ||
          width;
        chartRef.current.applyOptions({ width: newWidth });
      }
    });
    ro.observe(containerRef.current);

    return () => {
      ro.disconnect();
      setChartReady(false);
      if (chartRef.current) {
        try {
          chartRef.current.remove();
        } catch {
          /* already disposed */
        }
        chartRef.current = null;
      }
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
    };
  }, [symbol, timeframe]);

  // === Series data: update when data changes (no chart rebuild) ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const candle = candleSeriesRef.current;
    const vol = volumeSeriesRef.current;
    const chart = chartRef.current;
    if (!candle || !vol || !chart) return;

    candle.setData(data.candles.map((c) => ({ ...c, time: c.time as Time })));
    vol.setData(data.volumes.map((v) => ({ ...v, time: v.time as Time })));
    chart.timeScale().fitContent();
  }, [chartReady, data]);

  // === FVG zones ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const candleSeries = candleSeriesRef.current;
    if (!candleSeries) return;

    if (fvgPrimitiveRef.current) {
      try {
        candleSeries.detachPrimitive(fvgPrimitiveRef.current);
      } catch {
        /* */
      }
      fvgPrimitiveRef.current = null;
    }
    if (!toggles.fvg || data.fvgs.length === 0) return;

    try {
      // Show only unmitigated FVGs near current price (≤15% away), most recent first
      const cp = data.current_price;
      const filtered = data.fvgs
        .filter((f) => !f.mitigated)
        .filter((f) => {
          const mid = (f.top + f.bottom) / 2;
          return Math.abs(mid - cp) / cp < 0.15;
        })
        .slice(-MAX_FVG);
      const primitive = new FVGPrimitive(filtered);
      candleSeries.attachPrimitive(primitive);
      fvgPrimitiveRef.current = primitive;
    } catch {
      /* primitive failed — chart still works */
    }
  }, [chartReady, data, toggles.fvg]);

  // === Order Blocks ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const candleSeries = candleSeriesRef.current;
    if (!candleSeries) return;

    if (obPrimitiveRef.current) {
      try {
        candleSeries.detachPrimitive(obPrimitiveRef.current);
      } catch {
        /* */
      }
      obPrimitiveRef.current = null;
    }
    if (!toggles.ob || !data.order_blocks || data.order_blocks.length === 0) return;
    try {
      const primitive = new OrderBlockPrimitive(data.order_blocks);
      candleSeries.attachPrimitive(primitive);
      obPrimitiveRef.current = primitive;
    } catch {
      /* */
    }
  }, [chartReady, data, toggles.ob]);

  // === Gann Fan ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const candleSeries = candleSeriesRef.current;
    if (!candleSeries) return;

    if (gannPrimitiveRef.current) {
      try {
        candleSeries.detachPrimitive(gannPrimitiveRef.current);
      } catch {
        /* */
      }
      gannPrimitiveRef.current = null;
    }
    if (!toggles.gann || !data.gann_fan) return;
    try {
      const primitive = new GannPrimitive(data.gann_fan);
      candleSeries.attachPrimitive(primitive);
      gannPrimitiveRef.current = primitive;
    } catch {
      /* */
    }
  }, [chartReady, data, toggles.gann]);

  // === Fibonacci Circles ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const candleSeries = candleSeriesRef.current;
    if (!candleSeries) return;

    if (fibCirclesPrimitiveRef.current) {
      try {
        candleSeries.detachPrimitive(fibCirclesPrimitiveRef.current);
      } catch {
        /* */
      }
      fibCirclesPrimitiveRef.current = null;
    }
    if (!toggles.fibCircles || !data.fib_circles) return;
    try {
      const primitive = new FibCirclesPrimitive(data.fib_circles);
      candleSeries.attachPrimitive(primitive);
      fibCirclesPrimitiveRef.current = primitive;
    } catch {
      /* */
    }
  }, [chartReady, data, toggles.fibCircles]);

  // === Chart Patterns (Cup & Handle / Flag / Triangle / Double Top/Bottom) ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const candleSeries = candleSeriesRef.current;
    if (!candleSeries) return;

    if (patternPrimitiveRef.current) {
      try { candleSeries.detachPrimitive(patternPrimitiveRef.current); } catch { /* */ }
      patternPrimitiveRef.current = null;
    }
    if (!toggles.patterns || !data.chart_patterns?.length) return;
    try {
      const primitive = new PatternPrimitive(data.chart_patterns as ChartPattern[]);
      candleSeries.attachPrimitive(primitive);
      patternPrimitiveRef.current = primitive;
    } catch { /* */ }
  }, [chartReady, data, toggles.patterns]);

  // === Harmonic Patterns (XABCD) ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const candleSeries = candleSeriesRef.current;
    if (!candleSeries) return;

    if (harmonicPrimitiveRef.current) {
      try { candleSeries.detachPrimitive(harmonicPrimitiveRef.current); } catch { /* */ }
      harmonicPrimitiveRef.current = null;
    }
    if (!toggles.harmonics || !data.harmonic_patterns?.length) return;
    try {
      const primitive = new HarmonicPrimitive(data.harmonic_patterns as HarmonicPattern[]);
      candleSeries.attachPrimitive(primitive);
      harmonicPrimitiveRef.current = primitive;
    } catch { /* */ }
  }, [chartReady, data, toggles.harmonics]);

  // === Markers (BOS/ChoCh + Candle Patterns combined) ===
  // Note: createSeriesMarkers REPLACES all markers each call, so BOS and
  // candles must be merged into one marker set.
  useEffect(() => {
    if (!chartReady || !data) return;
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    if (!chart || !candleSeries) return;

    // Always clean up old line segments + markers
    bosSeriesRef.current.forEach((s) => {
      try { chart.removeSeries(s); } catch {}
    });
    bosSeriesRef.current = [];
    if (bosMarkersRef.current) {
      try { bosMarkersRef.current.detach(); } catch {}
      bosMarkersRef.current = null;
    }

    type Marker = {
      time: Time;
      position: "aboveBar" | "belowBar" | "inBar";
      color: string;
      shape: "arrowUp" | "arrowDown" | "circle" | "square";
      text?: string;
    };
    const allMarkers: Marker[] = [];

    // BOS / ChoCh — show only the LATEST BOS and LATEST ChoCh (matches TradingView SMC indicator)
    if (toggles.bos && data.structure.length > 0) {
      const latestBOS = [...data.structure].reverse().find((e) => e.type.includes("BOS"));
      const latestChoCh = [...data.structure].reverse().find((e) => e.type.includes("ChoCh"));
      const events = [latestBOS, latestChoCh].filter(Boolean) as typeof data.structure;

      events.forEach((ev) => {
        try {
          const isBull = ev.type.startsWith("bullish");
          const isBOS = ev.type.includes("BOS");
          // BOS = solid green, ChoCh = solid pink (matches reference)
          const color = isBOS
            ? (isBull ? "rgba(38, 166, 154, 0.9)" : "rgba(239, 83, 80, 0.9)")
            : (isBull ? "rgba(38, 166, 154, 0.9)" : "rgba(236, 72, 153, 0.9)");
          const line = chart.addSeries(LineSeries, {
            color, lineWidth: 1, lineStyle: 0,  // solid line
            priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false,
          });
          line.setData([
            { time: ev.from_time as Time, value: ev.from_price },
            { time: ev.time as Time, value: ev.from_price },
          ]);
          bosSeriesRef.current.push(line);
        } catch {}
      });

      events.forEach((ev) => {
        const isBull = ev.type.startsWith("bullish");
        const isBOS = ev.type.includes("BOS");
        allMarkers.push({
          time: ev.from_time as Time,
          position: isBull ? ("aboveBar" as const) : ("belowBar" as const),
          color: isBOS ? (isBull ? "#26a69a" : "#ef5350") : (isBull ? "#26a69a" : "#ec4899"),
          shape: "circle" as const,
          text: isBOS ? "bos" : "ChoCh",
        });
      });
    }

    // Candle patterns
    if (toggles.candles && data.candle_patterns) {
      const seenAt: Record<string, number> = {};  // stack count for same-day patterns
      data.candle_patterns.forEach((p) => {
        const isBull = p.bias === "bullish";
        const isBear = p.bias === "bearish";
        const color = isBull ? "#22d3ee" : isBear ? "#f97316" : "#a78bfa";
        // Stack labels above/below to avoid overlap with BOS markers
        const stackIdx = (seenAt[p.time] = (seenAt[p.time] ?? 0) + 1);
        const position = (isBull
          ? "belowBar"
          : isBear
          ? "aboveBar"
          : (stackIdx % 2 ? "aboveBar" : "belowBar")) as "aboveBar" | "belowBar";
        // Compact label: take first letters of type, e.g. "Bullish Engulfing" → "BE"
        const short = p.type
          .replace(/[()]/g, "")
          .split(" ")
          .map((w) => w[0])
          .join("")
          .toUpperCase()
          .slice(0, 4);
        allMarkers.push({
          time: p.time as Time,
          position,
          color,
          shape: isBull
            ? ("circle" as const)
            : isBear
            ? ("circle" as const)
            : ("square" as const),
          text: short,
        });
      });
    }

    if (allMarkers.length > 0) {
      // Sort by time so lightweight-charts renders them correctly
      allMarkers.sort((a, b) => String(a.time).localeCompare(String(b.time)));
      try {
        bosMarkersRef.current = createSeriesMarkers(candleSeries, allMarkers);
      } catch {}
    }
  }, [chartReady, data, toggles.bos, toggles.candles]);

  // === Key Levels ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const series = candleSeriesRef.current;
    if (!series) return;
    levelsLinesRef.current.forEach((ln) => {
      try {
        series.removePriceLine(ln);
      } catch {
        /* */
      }
    });
    levelsLinesRef.current = [];
    if (!toggles.levels || !data.key_levels) return;
    data.key_levels.forEach((lvl) => {
      try {
        const isBreakout =
          lvl.purpose === "breakout_long" || lvl.purpose === "breakout_short";
        levelsLinesRef.current.push(
          series.createPriceLine({
            price: lvl.price,
            color: lvl.color,
            lineWidth: isBreakout ? 2 : 1,
            lineStyle: isBreakout ? 0 : 2,
            axisLabelVisible: true,
            title: lvl.label,
          }),
        );
      } catch {
        /* */
      }
    });
  }, [chartReady, data, toggles.levels]);

  // === Multi-touch Support / Resistance ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const series = candleSeriesRef.current;
    if (!series) return;
    srLinesRef.current.forEach((ln) => {
      try { series.removePriceLine(ln); } catch {}
    });
    srLinesRef.current = [];
    if (!toggles.sr || !data.support_resistance) return;

    data.support_resistance.forEach((lvl) => {
      try {
        const isSupport = lvl.role === "support";
        // Stronger levels = thicker, more opaque
        const lineWidth = lvl.strength >= 4 ? 2 : 1;
        const baseAlpha = 0.4 + Math.min(0.5, lvl.strength * 0.12);
        const color = isSupport
          ? `rgba(38, 166, 154, ${baseAlpha.toFixed(2)})`
          : `rgba(239, 83, 80, ${baseAlpha.toFixed(2)})`;
        srLinesRef.current.push(
          series.createPriceLine({
            price: lvl.price,
            color,
            lineWidth,
            lineStyle: 2, // dashed so they don't compete with key levels (solid)
            axisLabelVisible: true,
            title: `${isSupport ? "S" : "R"}·${lvl.touches}t·${"⭐".repeat(lvl.strength)}`,
          }),
        );
      } catch {}
    });
  }, [chartReady, data, toggles.sr]);

  // === Premium / Discount zones ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const series = candleSeriesRef.current;
    if (!series) return;
    pdLinesRef.current.forEach((ln) => {
      try { series.removePriceLine(ln); } catch {}
    });
    pdLinesRef.current = [];
    if (!toggles.premiumDiscount || !data.premium_discount) return;
    const pd = data.premium_discount;
    const lines: Array<{ price: number; color: string; title: string; w?: number }> = [
      { price: pd.range_high, color: "rgba(239,83,80,0.9)", title: "Range High (100%)", w: 2 },
      { price: pd.extreme_premium, color: "rgba(239,83,80,0.6)", title: "Premium 79%" },
      { price: pd.equilibrium, color: "rgba(168,85,247,0.85)", title: "Equilibrium (50%)", w: 2 },
      { price: pd.extreme_discount, color: "rgba(38,166,154,0.6)", title: "Discount 21%" },
      { price: pd.range_low, color: "rgba(38,166,154,0.9)", title: "Range Low (0%)", w: 2 },
    ];
    lines.forEach((ln) => {
      try {
        pdLinesRef.current.push(
          series.createPriceLine({
            price: ln.price,
            color: ln.color,
            lineWidth: (ln.w ?? 1) as 1 | 2,
            lineStyle: 2,
            axisLabelVisible: true,
            title: ln.title,
          }),
        );
      } catch {}
    });
  }, [chartReady, data, toggles.premiumDiscount]);

  // === BOS Trigger Zones (the levels that produce next BOS on close above/below) ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const series = candleSeriesRef.current;
    if (!series) return;
    bosZoneLinesRef.current.forEach((ln) => {
      try { series.removePriceLine(ln); } catch {}
    });
    bosZoneLinesRef.current = [];
    if (!toggles.bosZones || !data.bos_zones) return;
    const z = data.bos_zones;
    if (z.bullish_trigger) {
      try {
        bosZoneLinesRef.current.push(
          series.createPriceLine({
            price: z.bullish_trigger.price,
            color: "rgba(34,197,94,0.95)",
            lineWidth: 2,
            lineStyle: 0,
            axisLabelVisible: true,
            title: z.bullish_trigger.label,
          }),
        );
      } catch {}
    }
    if (z.bearish_trigger) {
      try {
        bosZoneLinesRef.current.push(
          series.createPriceLine({
            price: z.bearish_trigger.price,
            color: "rgba(239,68,68,0.95)",
            lineWidth: 2,
            lineStyle: 0,
            axisLabelVisible: true,
            title: z.bearish_trigger.label,
          }),
        );
      } catch {}
    }
  }, [chartReady, data, toggles.bosZones]);

  // === VWAP + bands ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const chart = chartRef.current;
    if (!chart) return;
    vwapSeriesRef.current.forEach((s) => {
      try { chart.removeSeries(s); } catch {}
    });
    vwapSeriesRef.current = [];
    if (!toggles.vwap || !data.order_flow?.vwap?.series?.length) return;
    const vwap = data.order_flow.vwap;
    const lines = [
      { key: "vwap", color: "rgba(99, 102, 241, 0.95)", width: 2 as const },
      { key: "upper_1sd", color: "rgba(99, 102, 241, 0.4)", width: 1 as const },
      { key: "lower_1sd", color: "rgba(99, 102, 241, 0.4)", width: 1 as const },
      { key: "upper_2sd", color: "rgba(99, 102, 241, 0.25)", width: 1 as const },
      { key: "lower_2sd", color: "rgba(99, 102, 241, 0.25)", width: 1 as const },
    ];
    lines.forEach(({ key, color, width }) => {
      try {
        const s = chart.addSeries(LineSeries, {
          color, lineWidth: width, lineStyle: key === "vwap" ? 0 : 2,
          priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false,
        });
        s.setData(
          vwap.series.map((p) => ({
            time: p.time as Time,
            value: p[key as "vwap" | "upper_1sd" | "lower_1sd" | "upper_2sd" | "lower_2sd"],
          })),
        );
        vwapSeriesRef.current.push(s);
      } catch {}
    });
  }, [chartReady, data, toggles.vwap]);

  // === Volume Profile (POC, VAH, VAL, HVN, LVN) ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const series = candleSeriesRef.current;
    if (!series) return;
    volumeProfileLinesRef.current.forEach((ln) => {
      try { series.removePriceLine(ln); } catch {}
    });
    volumeProfileLinesRef.current = [];
    if (!toggles.volumeProfile || !data.order_flow?.volume_profile) return;
    const vp = data.order_flow.volume_profile;
    const lines = [
      { price: vp.poc, color: "rgba(245, 158, 11, 0.95)", title: `POC ${vp.poc}`, w: 2 as const },
      { price: vp.vah, color: "rgba(245, 158, 11, 0.5)", title: "VAH", w: 1 as const },
      { price: vp.val, color: "rgba(245, 158, 11, 0.5)", title: "VAL", w: 1 as const },
    ];
    lines.forEach((ln) => {
      try {
        volumeProfileLinesRef.current.push(
          series.createPriceLine({
            price: ln.price,
            color: ln.color,
            lineWidth: ln.w,
            lineStyle: 2,
            axisLabelVisible: true,
            title: ln.title,
          }),
        );
      } catch {}
    });
    // HVN nodes (top volume bins) — thinner lines
    vp.hvn.forEach((px, i) => {
      try {
        volumeProfileLinesRef.current.push(
          series.createPriceLine({
            price: px,
            color: "rgba(168, 85, 247, 0.35)",
            lineWidth: 1,
            lineStyle: 3, // dotted
            axisLabelVisible: false,
            title: i === 0 ? "HVN" : "",
          }),
        );
      } catch {}
    });
  }, [chartReady, data, toggles.volumeProfile]);

  // === Fibonacci retracement ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const series = candleSeriesRef.current;
    if (!series) return;
    fibLinesRef.current.forEach((ln) => {
      try {
        series.removePriceLine(ln);
      } catch {
        /* */
      }
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
      try {
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
      } catch {
        /* */
      }
    });
  }, [chartReady, data, toggles.fib]);

  // === Pivot points ===
  useEffect(() => {
    if (!chartReady || !data) return;
    const series = candleSeriesRef.current;
    if (!series) return;
    pivotLinesRef.current.forEach((ln) => {
      try {
        series.removePriceLine(ln);
      } catch {
        /* */
      }
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
      try {
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
      } catch {
        /* */
      }
    });
  }, [chartReady, data, toggles.pivots]);

  // === Moving averages ===
  useEffect(() => {
    if (!chartReady || !data?.moving_averages) return;
    const chart = chartRef.current;
    if (!chart) return;
    const maConfigs: Array<[string, keyof Toggles, string]> = [
      ["ma_20", "ma20", "#facc15"],
      ["ma_50", "ma50", "#60a5fa"],
      ["ma_200", "ma200", "#f472b6"],
    ];
    maConfigs.forEach(([key, toggleKey, color]) => {
      const enabled = toggles[toggleKey];
      const existing = maSeriesRef.current[key];
      try {
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
          chart.removeSeries(existing);
          delete maSeriesRef.current[key];
        }
      } catch {
        /* */
      }
    });
  }, [chartReady, data, toggles.ma20, toggles.ma50, toggles.ma200]);

  // === Bollinger Bands overlay ===
  useEffect(() => {
    if (!chartReady || !data?.bollinger_bands) return;
    const chart = chartRef.current;
    if (!chart) return;
    const enabled = toggles.bb;
    const ref = bbSeriesRef.current;

    if (enabled && !ref.upper) {
      try {
        const up = chart.addSeries(LineSeries, {
          color: "rgba(167, 139, 250, 0.7)",
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
        });
        up.setData(data.bollinger_bands.upper.map((p) => ({ ...p, time: p.time as Time })));
        const mid = chart.addSeries(LineSeries, {
          color: "rgba(167, 139, 250, 0.4)",
          lineWidth: 1,
          lineStyle: 2,
          priceLineVisible: false,
          lastValueVisible: false,
        });
        mid.setData(data.bollinger_bands.middle.map((p) => ({ ...p, time: p.time as Time })));
        const lo = chart.addSeries(LineSeries, {
          color: "rgba(167, 139, 250, 0.7)",
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
        });
        lo.setData(data.bollinger_bands.lower.map((p) => ({ ...p, time: p.time as Time })));
        bbSeriesRef.current = { upper: up, middle: mid, lower: lo };
      } catch { /* */ }
    } else if (!enabled && ref.upper) {
      try { chart.removeSeries(ref.upper); } catch {}
      try { ref.middle && chart.removeSeries(ref.middle); } catch {}
      try { ref.lower && chart.removeSeries(ref.lower); } catch {}
      bbSeriesRef.current = {};
    }
  }, [chartReady, data, toggles.bb]);

  // === Sub-pane: RSI ===
  useEffect(() => {
    if (!toggles.rsi || !data?.rsi) {
      if (rsiChartRef.current) {
        try { rsiChartRef.current.remove(); } catch {}
        rsiChartRef.current = null;
      }
      return;
    }
    if (!rsiContainerRef.current) return;
    if (rsiChartRef.current) {
      try { rsiChartRef.current.remove(); } catch {}
      rsiChartRef.current = null;
    }

    const chart = createChart(rsiContainerRef.current, {
      width: rsiContainerRef.current.clientWidth || 800,
      height: 140,
      layout: { background: { color: "transparent" }, textColor: "#9ca3af" },
      grid: { vertLines: { color: "#1f2937" }, horzLines: { color: "#1f2937" } },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: "#374151" },
      timeScale: { borderColor: "#374151", timeVisible: true },
    });

    const rsiSeries = chart.addSeries(LineSeries, {
      color: "#a78bfa",
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: true,
    });
    rsiSeries.setData(data.rsi.map((p) => ({ ...p, time: p.time as Time })));

    // 70 / 30 reference lines
    rsiSeries.createPriceLine({
      price: 70, color: "rgba(239, 83, 80, 0.5)", lineWidth: 1, lineStyle: 2,
      axisLabelVisible: true, title: "70",
    });
    rsiSeries.createPriceLine({
      price: 30, color: "rgba(38, 166, 154, 0.5)", lineWidth: 1, lineStyle: 2,
      axisLabelVisible: true, title: "30",
    });

    rsiChartRef.current = chart;
    chart.timeScale().fitContent();

    const ro = new ResizeObserver(() => {
      if (rsiContainerRef.current && rsiChartRef.current) {
        rsiChartRef.current.applyOptions({ width: rsiContainerRef.current.clientWidth });
      }
    });
    ro.observe(rsiContainerRef.current);
    return () => {
      ro.disconnect();
      if (rsiChartRef.current) {
        try { rsiChartRef.current.remove(); } catch {}
        rsiChartRef.current = null;
      }
    };
  }, [data, toggles.rsi]);

  // === Sub-pane: MACD ===
  useEffect(() => {
    if (!toggles.macd || !data?.macd) {
      if (macdChartRef.current) {
        try { macdChartRef.current.remove(); } catch {}
        macdChartRef.current = null;
      }
      return;
    }
    if (!macdContainerRef.current) return;
    if (macdChartRef.current) {
      try { macdChartRef.current.remove(); } catch {}
      macdChartRef.current = null;
    }

    const chart = createChart(macdContainerRef.current, {
      width: macdContainerRef.current.clientWidth || 800,
      height: 140,
      layout: { background: { color: "transparent" }, textColor: "#9ca3af" },
      grid: { vertLines: { color: "#1f2937" }, horzLines: { color: "#1f2937" } },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: "#374151" },
      timeScale: { borderColor: "#374151", timeVisible: true },
    });

    const histSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "price", precision: 3, minMove: 0.001 },
    });
    histSeries.setData(data.macd.histogram.map((p) => ({ ...p, time: p.time as Time })));

    const macdLine = chart.addSeries(LineSeries, {
      color: "#60a5fa", lineWidth: 1,
      priceLineVisible: false, lastValueVisible: true,
    });
    macdLine.setData(data.macd.macd.map((p) => ({ ...p, time: p.time as Time })));

    const sigLine = chart.addSeries(LineSeries, {
      color: "#f97316", lineWidth: 1,
      priceLineVisible: false, lastValueVisible: true,
    });
    sigLine.setData(data.macd.signal.map((p) => ({ ...p, time: p.time as Time })));

    macdChartRef.current = chart;
    chart.timeScale().fitContent();

    const ro = new ResizeObserver(() => {
      if (macdContainerRef.current && macdChartRef.current) {
        macdChartRef.current.applyOptions({ width: macdContainerRef.current.clientWidth });
      }
    });
    ro.observe(macdContainerRef.current);
    return () => {
      ro.disconnect();
      if (macdChartRef.current) {
        try { macdChartRef.current.remove(); } catch {}
        macdChartRef.current = null;
      }
    };
  }, [data, toggles.macd]);

  // === Sub-pane: Stochastic ===
  useEffect(() => {
    if (!toggles.stoch || !data?.stochastic) {
      if (stochChartRef.current) {
        try { stochChartRef.current.remove(); } catch {}
        stochChartRef.current = null;
      }
      return;
    }
    if (!stochContainerRef.current) return;
    if (stochChartRef.current) {
      try { stochChartRef.current.remove(); } catch {}
      stochChartRef.current = null;
    }

    const chart = createChart(stochContainerRef.current, {
      width: stochContainerRef.current.clientWidth || 800,
      height: 140,
      layout: { background: { color: "transparent" }, textColor: "#9ca3af" },
      grid: { vertLines: { color: "#1f2937" }, horzLines: { color: "#1f2937" } },
      crosshair: { mode: 1 },
      rightPriceScale: { borderColor: "#374151" },
      timeScale: { borderColor: "#374151", timeVisible: true },
    });

    const k = chart.addSeries(LineSeries, {
      color: "#60a5fa", lineWidth: 1,
      priceLineVisible: false, lastValueVisible: true,
    });
    k.setData(data.stochastic.k.map((p) => ({ ...p, time: p.time as Time })));

    const d = chart.addSeries(LineSeries, {
      color: "#f97316", lineWidth: 1,
      priceLineVisible: false, lastValueVisible: true,
    });
    d.setData(data.stochastic.d.map((p) => ({ ...p, time: p.time as Time })));

    k.createPriceLine({ price: 80, color: "rgba(239, 83, 80, 0.5)", lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: "80" });
    k.createPriceLine({ price: 20, color: "rgba(38, 166, 154, 0.5)", lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: "20" });

    stochChartRef.current = chart;
    chart.timeScale().fitContent();

    const ro = new ResizeObserver(() => {
      if (stochContainerRef.current && stochChartRef.current) {
        stochChartRef.current.applyOptions({ width: stochContainerRef.current.clientWidth });
      }
    });
    ro.observe(stochContainerRef.current);
    return () => {
      ro.disconnect();
      if (stochChartRef.current) {
        try { stochChartRef.current.remove(); } catch {}
        stochChartRef.current = null;
      }
    };
  }, [data, toggles.stoch]);

  // Filtered stock list
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

  const selectStock = useCallback(
    (sym: string) => {
      const upper = sym.toUpperCase().trim();
      if (!upper) return;
      setSearch("");
      setShowDropdown(false);
      // Cross-market routing: if typed ticker not in current market's universe
      // but looks like the OTHER market, navigate to that market's chart.
      const inLocal = stocks.some((s) => s.symbol.toUpperCase() === upper);
      if (!inLocal) {
        // NASDAQ tickers are typically 1-5 alpha chars all uppercase
        const looksNasdaq = /^[A-Z]{1,5}$/.test(upper);
        if (isNasdaq) {
          // On NASDAQ chart, just go to NASDAQ chart (Enter for any ticker)
          nav(`/nasdaq/smc-chart/${upper}`);
        } else if (looksNasdaq) {
          // On DSE chart but ticker looks NASDAQ → route to NASDAQ
          nav(`/nasdaq/smc-chart/${upper}`);
        } else {
          nav(`/smc-chart/${upper}`);
        }
        return;
      }
      nav(isNasdaq ? `/nasdaq/smc-chart/${upper}` : `/smc-chart/${upper}`);
    },
    [nav, stocks, isNasdaq],
  );

  const toggle = useCallback(
    (key: keyof Toggles) => {
      setToggles((t) => ({ ...t, [key]: !t[key] }));
    },
    [setToggles],
  );

  const handleRefresh = useCallback(() => {
    void loadData(true);
  }, [loadData]);

  const toggleButtons: Array<{
    key: keyof Toggles;
    label: string;
    color: string;
  }> = [
    { key: "fvg", label: "FVG", color: "text-emerald-500" },
    { key: "ob", label: "Order Blocks", color: "text-violet-500" },
    { key: "bos", label: "BOS/ChoCh", color: "text-yellow-500" },
    { key: "premiumDiscount", label: "Premium/Discount", color: "text-fuchsia-400" },
    { key: "bosZones", label: "BOS Triggers", color: "text-lime-400" },
    { key: "vwap", label: "VWAP", color: "text-indigo-400" },
    { key: "volumeProfile", label: "Volume Profile", color: "text-amber-400" },
    { key: "levels", label: "Key Levels", color: "text-amber-400" },
    { key: "sr", label: "S/R Zones", color: "text-teal-400" },
    { key: "fib", label: "Fibonacci", color: "text-purple-500" },
    { key: "fibCircles", label: "Fib Circles", color: "text-pink-500" },
    { key: "gann", label: "Gann Fan", color: "text-amber-500" },
    { key: "pivots", label: "Pivots", color: "text-orange-500" },
    { key: "ma20", label: "MA20", color: "text-yellow-400" },
    { key: "ma50", label: "MA50", color: "text-blue-400" },
    { key: "ma200", label: "MA200", color: "text-pink-400" },
    { key: "bb", label: "Boll Bands", color: "text-violet-300" },
    { key: "patterns", label: "Patterns", color: "text-cyan-400" },
    { key: "harmonics", label: "Harmonics", color: "text-rose-400" },
    { key: "candles", label: "Candles", color: "text-cyan-300" },
    { key: "rsi", label: "RSI", color: "text-purple-300" },
    { key: "macd", label: "MACD", color: "text-blue-300" },
    { key: "stoch", label: "Stoch", color: "text-cyan-300" },
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
            <h1 className="text-2xl font-bold flex items-center gap-2">
              {data?.symbol || symbol}
              <span className="inline-flex items-center gap-1 text-[10px] font-normal px-1.5 py-0.5 rounded bg-emerald-500/15 text-emerald-500 border border-emerald-500/30">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                LIVE
              </span>
            </h1>
            {data && (
              <span className="text-emerald-500 text-lg font-mono">
                {data.current_price.toFixed(1)} {cur}
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
              onBlur={() => {
                window.setTimeout(() => setShowDropdown(false), 150);
              }}
              onKeyDown={(e) => {
                // Enter → navigate to typed ticker even if not in our list.
                // Lets users open any NASDAQ stock not in the halal universe.
                if (e.key === "Enter" && search.trim()) {
                  selectStock(search.trim().toUpperCase());
                }
              }}
              placeholder={isNasdaq ? "Search ticker (Enter for any)" : "Search stock..."}
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
            onClick={handleRefresh}
            className="p-1 rounded hover:bg-gray-200 dark:hover:bg-gray-700"
            title="Force refresh (bypass cache)"
          >
            <RefreshCw
              className={clsx("w-4 h-4", loading && "animate-spin")}
            />
          </button>
        </div>

        <div className="flex items-center gap-1 bg-gray-100 dark:bg-gray-800/50 rounded p-1">
          {(["daily", "weekly"] as const).map((tf) => (
            <button
              key={tf}
              onClick={() => setTimeframe(tf)}
              className={clsx(
                "px-3 py-1 rounded text-xs capitalize",
                timeframe === tf
                  ? "bg-blue-500/20 text-blue-600 dark:text-blue-400"
                  : "text-gray-500 dark:text-gray-400",
              )}
            >
              {tf === "daily" ? "1D" : "1W"}
            </button>
          ))}
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-2 mb-3">
        <span className="text-xs text-gray-500">Indicators:</span>
        {toggleButtons.map((b) => {
          const on = toggles[b.key];
          let count: number | null = null;
          if (b.key === "patterns") count = data?.chart_patterns?.length ?? null;
          else if (b.key === "harmonics") count = data?.harmonic_patterns?.length ?? null;
          else if (b.key === "candles") count = data?.candle_patterns?.length ?? null;
          else if (b.key === "fvg") count = data?.fvgs?.length ?? null;
          else if (b.key === "ob") count = data?.order_blocks?.length ?? null;
          else if (b.key === "sr") count = data?.support_resistance?.length ?? null;
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
              {count !== null && (
                <span className={clsx(
                  "px-1 rounded text-[10px] font-mono",
                  count > 0 ? "bg-gray-200 dark:bg-gray-700" : "text-gray-400",
                )}>
                  {count}
                </span>
              )}
            </button>
          );
        })}
      </div>

      {data?.analysis && (
        <div className="mb-4 rounded-lg border-2 overflow-hidden"
          style={{
            borderColor:
              data.analysis.action_color === "green" ? "#10b981" :
              data.analysis.action_color === "yellow" ? "#f59e0b" :
              data.analysis.action_color === "orange" ? "#f97316" :
              data.analysis.action_color === "red" ? "#ef4444" :
              "#6b7280",
          }}
        >
          <div
            className="px-4 py-3 flex items-center justify-between flex-wrap gap-3"
            style={{
              background:
                data.analysis.action_color === "green" ? "rgba(16,185,129,0.12)" :
                data.analysis.action_color === "yellow" ? "rgba(245,158,11,0.12)" :
                data.analysis.action_color === "orange" ? "rgba(249,115,22,0.12)" :
                data.analysis.action_color === "red" ? "rgba(239,68,68,0.12)" :
                "rgba(107,114,128,0.12)",
            }}
          >
            <div className="flex items-center gap-3 flex-wrap">
              <span className={clsx(
                "inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-bold tracking-wide",
                data.analysis.bias === "BULLISH" && "bg-emerald-500/25 text-emerald-700 dark:text-emerald-300",
                data.analysis.bias === "BEARISH" && "bg-red-500/25 text-red-700 dark:text-red-300",
                data.analysis.bias === "WHIPSAW" && "bg-amber-500/25 text-amber-700 dark:text-amber-300",
                data.analysis.bias === "NEUTRAL" && "bg-gray-500/25 text-gray-700 dark:text-gray-300",
              )}>
                BIAS: {data.analysis.bias}
              </span>
              <span className="text-xs text-gray-500">
                Confidence: <strong className={clsx(
                  data.analysis.confidence === "HIGH" && "text-emerald-500",
                  data.analysis.confidence === "MEDIUM" && "text-yellow-500",
                  data.analysis.confidence === "LOW" && "text-gray-500",
                )}>{data.analysis.confidence}</strong>
              </span>
              <span className="text-base font-bold"
                style={{
                  color:
                    data.analysis.action_color === "green" ? "#10b981" :
                    data.analysis.action_color === "yellow" ? "#f59e0b" :
                    data.analysis.action_color === "orange" ? "#f97316" :
                    data.analysis.action_color === "red" ? "#ef4444" :
                    "#9ca3af",
                }}
              >
                → {data.analysis.action}
              </span>
            </div>
          </div>
          <div className="px-4 py-3 bg-white/80 dark:bg-gray-900/40">
            <p className="text-sm mb-3">{data.analysis.summary}</p>

            {/* Plain-language Trade Thesis — per-stock specific */}
            {(data.analysis as { thesis?: string[] }).thesis && (data.analysis as { thesis?: string[] }).thesis!.length > 0 && (
              <div className="mb-3 rounded border border-blue-500/30 bg-blue-500/5 px-3 py-2">
                <div className="text-[10px] uppercase tracking-wider text-blue-500 font-semibold mb-1">
                  📋 Trade Thesis
                </div>
                <div className="text-xs space-y-1.5 text-[var(--text)]">
                  {(data.analysis as { thesis?: string[] }).thesis!.map((para, i) => (
                    <p key={i} dangerouslySetInnerHTML={{
                      __html: para
                        .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
                        .replace(/⚠/g, '<span style="color:#f97316">⚠</span>')
                    }} />
                  ))}
                </div>
              </div>
            )}

            {/* Cross-Signal Alignment — how every metric agrees/disagrees */}
            {(data.analysis as { alignment?: string[] }).alignment && (data.analysis as { alignment?: string[] }).alignment!.length > 0 && (
              <div className="mb-3 rounded border border-purple-500/30 bg-purple-500/5 px-3 py-2">
                <div className="text-[10px] uppercase tracking-wider text-purple-400 font-semibold mb-1">
                  🎯 Cross-Signal Alignment
                </div>
                <ul className="text-xs space-y-1 text-[var(--text)]">
                  {(data.analysis as { alignment?: string[] }).alignment!.map((line, i) => (
                    <li key={i} dangerouslySetInnerHTML={{
                      __html: line
                        .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
                    }} />
                  ))}
                </ul>
              </div>
            )}

            {/* Fibonacci Dealing Range — the strategy in the Bengali post */}
            {data.fib_dealing_range && data.fib_dealing_range.valid && (
              <div className="mb-3 rounded border border-amber-500/30 bg-amber-500/5 px-3 py-2">
                <div className="flex items-center justify-between mb-1">
                  <div className="text-[10px] uppercase tracking-wider text-amber-500 font-semibold">
                    📐 Fibonacci Dealing Range
                  </div>
                  <span className={`text-[11px] font-bold px-2 py-0.5 rounded ${
                    data.fib_dealing_range.action_text.includes("BUY") ? "bg-emerald-500/20 text-emerald-500" :
                    data.fib_dealing_range.action_text.includes("SELL") ? "bg-red-500/20 text-red-500" :
                    "bg-gray-500/20 text-gray-500"
                  }`}>
                    {data.fib_dealing_range.action_text}
                  </span>
                </div>
                <p className="text-xs text-[var(--text)] mb-2">{data.fib_dealing_range.narrative}</p>
                <div className="text-[10px] grid grid-cols-2 sm:grid-cols-4 gap-1">
                  {data.fib_dealing_range.levels.map((lvl) => {
                    const isCurrent = Math.abs(data.current_price - lvl.price) / lvl.price < 0.01;
                    return (
                      <div key={lvl.ratio} className={`rounded px-2 py-1 ${
                        lvl.zone.includes("discount") ? "bg-emerald-500/10 border border-emerald-500/30" :
                        lvl.zone.includes("premium") ? "bg-red-500/10 border border-red-500/30" :
                        "bg-gray-500/10 border border-gray-500/30"
                      } ${isCurrent ? "ring-2 ring-amber-500" : ""}`}>
                        <div className="font-mono">{(lvl.ratio * 100).toFixed(1)}%</div>
                        <div className="font-mono font-bold">{cur}{lvl.price}</div>
                        <div className="opacity-70">{lvl.action.split(" — ")[0]}</div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {/* Demand & Supply Zones (Sam Seiden) + Volatility Imbalance */}
            {(((data.demand_zones?.length ?? 0) > 0) ||
              ((data.supply_zones?.length ?? 0) > 0) ||
              ((data.volatility_imbalances?.filter((v) => !v.mitigated).length ?? 0) > 0)) && (
              <div className="mb-3 rounded border border-cyan-500/30 bg-cyan-500/5 px-3 py-2">
                <div className="text-[10px] uppercase tracking-wider text-cyan-400 font-semibold mb-1">
                  🎯 Demand / Supply / Volatility Zones
                </div>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-2 text-xs">
                  {data.demand_zones && data.demand_zones.length > 0 && (
                    <div>
                      <div className="text-[10px] text-emerald-500 font-bold mb-1">DEMAND (BUY zones)</div>
                      {data.demand_zones.slice(0, 3).map((z, i) => (
                        <div key={i} className="font-mono text-[11px] mb-0.5">
                          <span className="text-emerald-500">{z.subtype}</span> {cur}{z.bottom}-{z.top}
                          <span className="text-[var(--text-muted)] ml-1">→ +{z.impulse_pct}%</span>
                        </div>
                      ))}
                    </div>
                  )}
                  {data.supply_zones && data.supply_zones.length > 0 && (
                    <div>
                      <div className="text-[10px] text-red-500 font-bold mb-1">SUPPLY (SELL zones)</div>
                      {data.supply_zones.slice(0, 3).map((z, i) => (
                        <div key={i} className="font-mono text-[11px] mb-0.5">
                          <span className="text-red-500">{z.subtype}</span> {cur}{z.bottom}-{z.top}
                          <span className="text-[var(--text-muted)] ml-1">→ -{z.impulse_pct}%</span>
                        </div>
                      ))}
                    </div>
                  )}
                  {data.volatility_imbalances && data.volatility_imbalances.length > 0 && (
                    <div>
                      <div className="text-[10px] text-amber-400 font-bold mb-1">VOLATILITY IMBALANCE</div>
                      {data.volatility_imbalances.filter((v) => !v.mitigated).slice(0, 3).map((v, i) => (
                        <div key={i} className="font-mono text-[11px] mb-0.5">
                          <span className={v.type === "VI_BULLISH" ? "text-emerald-500" : "text-red-500"}>
                            {v.type === "VI_BULLISH" ? "↑VI" : "↓VI"}
                          </span> {cur}{v.bottom}-{v.top}
                          <span className="text-[var(--text-muted)] ml-1">{v.time}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
                <div className="text-[10px] text-[var(--text-muted)] mt-2">
                  RBR=Rally-Base-Rally · DBR=Drop-Base-Rally · DBD=Drop-Base-Drop · RBD=Rally-Base-Drop · VI=single-bar gap (news/spike)
                </div>
              </div>
            )}

            {/* Hedge-fund 3-pillar verdict cards */}
            {data.analysis && (
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-2 mb-3">
                {[
                  ["🏗️ STRUCTURE (where)", "structure_narrative", "structure_verdict"],
                  ["🌊 ORDER FLOW (who)", "order_flow_narrative", "order_flow_verdict"],
                  ["💪 VOLUME (strength)", "volume_narrative", "volume_verdict"],
                ].map(([title, narrKey, verdictKey]) => {
                  const a = data.analysis as unknown as Record<string, unknown>;
                  const narr = (a[narrKey] as string) || "";
                  const verdict = (a[verdictKey] as string) || "";
                  if (!narr) return null;
                  const color =
                    verdict === "BUY" ? "border-emerald-500/40 bg-emerald-500/5 text-emerald-500" :
                    verdict === "AVOID" ? "border-red-500/40 bg-red-500/5 text-red-500" :
                    verdict === "MIXED" ? "border-amber-500/30 bg-amber-500/5 text-amber-500" :
                    "border-[var(--border)]";
                  return (
                    <div key={title as string} className={`rounded border px-3 py-2 ${color}`}>
                      <div className="text-[10px] uppercase tracking-wider font-semibold mb-1 flex items-center justify-between">
                        <span>{title as string}</span>
                        <span className="font-bold">{verdict}</span>
                      </div>
                      <p className="text-xs text-[var(--text)]">{narr}</p>
                    </div>
                  );
                })}
              </div>
            )}
            {data.analysis && (data.analysis as unknown as Record<string, unknown>).hedge_fund_verdict ? (
              <div className="mb-3 rounded border border-purple-500/40 bg-purple-500/10 px-3 py-2 text-sm font-semibold text-center">
                {(data.analysis as unknown as Record<string, unknown>).hedge_fund_verdict as string}
              </div>
            ) : null}

            {/* Elliott Wave Triangle (A-B-C-D-E) */}
            {data.elliott_triangle && (
              <div className="mb-3 rounded border border-fuchsia-500/30 bg-fuchsia-500/5 px-3 py-2">
                <div className="text-[10px] uppercase tracking-wider text-fuchsia-400 font-semibold mb-1">
                  🔺 Elliott Wave Triangle ({data.elliott_triangle.kind} contracting)
                </div>
                <p className="text-xs mb-2">{data.elliott_triangle.narrative}</p>
                <div className="flex flex-wrap gap-2 text-[11px] font-mono mb-1">
                  {data.elliott_triangle.points.map((p) => (
                    <span key={p.label} className="px-2 py-0.5 rounded bg-fuchsia-500/10 border border-fuchsia-500/20">
                      <strong>{p.label}</strong>: {cur}{p.price}
                    </span>
                  ))}
                </div>
                <div className="text-[11px]">
                  Targets — ↑ <strong className="text-emerald-500">{cur}{data.elliott_triangle.breakout_up_target}</strong>
                  · ↓ <strong className="text-red-500">{cur}{data.elliott_triangle.breakdown_target}</strong>
                </div>
              </div>
            )}

            {data.analysis.reasons.length > 0 && (
              <ul className="text-xs text-gray-600 dark:text-gray-400 space-y-1 mb-3">
                {data.analysis.reasons.map((r, i) => (
                  <li key={i} className="flex gap-2">
                    <span>•</span>
                    <span>{r}</span>
                  </li>
                ))}
              </ul>
            )}
            {data.analysis.entry !== null && (
              <div className="grid grid-cols-2 sm:grid-cols-5 gap-2 text-xs mb-3">
                <div className="bg-emerald-500/10 border border-emerald-500/30 rounded px-2 py-1.5">
                  <div className="text-gray-500 text-[10px]">Entry</div>
                  <div className="font-mono font-bold text-emerald-600 dark:text-emerald-400">
                    {cur}{data.analysis.entry}
                  </div>
                </div>
                <div className="bg-red-500/10 border border-red-500/30 rounded px-2 py-1.5">
                  <div className="text-gray-500 text-[10px]">Stop Loss</div>
                  <div className="font-mono font-bold text-red-600 dark:text-red-400">
                    {cur}{data.analysis.stop_loss}
                  </div>
                </div>
                <div className="bg-blue-500/10 border border-blue-500/30 rounded px-2 py-1.5">
                  <div className="text-gray-500 text-[10px]">Target 1</div>
                  <div className="font-mono font-bold text-blue-600 dark:text-blue-400">
                    {cur}{data.analysis.target1}
                  </div>
                </div>
                <div className="bg-purple-500/10 border border-purple-500/30 rounded px-2 py-1.5">
                  <div className="text-gray-500 text-[10px]">Target 2</div>
                  <div className="font-mono font-bold text-purple-600 dark:text-purple-400">
                    {cur}{data.analysis.target2}
                  </div>
                </div>
                <div className="bg-gray-500/10 border border-gray-500/30 rounded px-2 py-1.5">
                  <div className="text-gray-500 text-[10px]">R/R</div>
                  <div className="font-mono font-bold">
                    1 : {data.analysis.risk_reward}
                  </div>
                </div>
              </div>
            )}
            {data.analysis.entry_label && (
              <p className="text-xs text-gray-500 italic mb-2">
                {data.analysis.entry_label}
              </p>
            )}
            {data.analysis.triggers.length > 0 && (
              <div className="border-t border-gray-200 dark:border-gray-700/50 pt-2">
                <p className="text-[11px] uppercase tracking-wide text-gray-500 mb-1">
                  Tomorrow — what to watch
                </p>
                {data.analysis.triggers.map((t, i) => (
                  <div key={i} className="text-xs flex gap-2 py-0.5">
                    <span>{t.icon}</span>
                    <span>{t.text}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Advanced Signals — VSA + Wyckoff events + OBV + MFI + Ichimoku */}
      {data && (data.vsa_events?.length || data.wyckoff_events?.length || data.obv || data.mfi || data.ichimoku) && (
        <div className="mb-3 rounded border border-[var(--border)] px-3 py-2 text-xs">
          <div className="font-semibold uppercase tracking-wide text-purple-400 mb-1.5">
            🔬 Advanced Signals
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
            {/* VSA latest events */}
            {data.vsa_events && data.vsa_events.length > 0 && (
              <div className="rounded border border-[var(--border)] px-2 py-1.5"
                title="Volume Spread Analysis — Tom Williams. Detects institutional footprints by combining volume + spread + close position. Higher strength = more institutional involvement.">
                <div className="text-[10px] uppercase opacity-70 mb-1">VSA — last {data.vsa_events.length} events</div>
                <div className="space-y-1">
                  {data.vsa_events.slice(-3).reverse().map((e, i) => (
                    <div key={i} className="text-[11px]">
                      <span className={
                        e.bias === "bullish" ? "text-emerald-500 font-medium" :
                        e.bias === "bearish" ? "text-red-500 font-medium" :
                        "text-[var(--text-muted)]"
                      }>
                        {e.type.replace(/_/g, " ")}
                      </span>
                      <span className="text-[var(--text-muted)] ml-1">· {e.time}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
            {/* Wyckoff events */}
            {data.wyckoff_events && data.wyckoff_events.length > 0 && (
              <div className="rounded border border-fuchsia-500/30 bg-fuchsia-500/5 px-2 py-1.5"
                title="Wyckoff Spring/SOS/UTAD events within accumulation/distribution phases. Strongest entry triggers in the Wyckoff method.">
                <div className="text-[10px] uppercase text-fuchsia-400 mb-1">Wyckoff Events</div>
                {data.wyckoff_events.slice(-2).map((e, i) => (
                  <div key={i} className="text-[11px]">
                    <span className={e.bias === "bullish" ? "text-emerald-500 font-bold" : "text-red-500 font-bold"}>
                      {e.type.replace(/_/g, " ")}
                    </span>
                    <span className="text-[var(--text-muted)] ml-1">· {e.time}</span>
                  </div>
                ))}
              </div>
            )}
            {/* OBV */}
            {data.obv && (
              <div className="rounded border border-[var(--border)] px-2 py-1.5"
                title="On-Balance Volume = cumulative volume flow. Adds volume on up days, subtracts on down days. DIVERGENCE is the key signal: price LL but OBV HL = bullish reversal coming. Price HH but OBV LH = bearish exhaustion.">
                <div className="text-[10px] uppercase opacity-70 mb-1">OBV</div>
                <div className="text-[11px]">
                  <span className={data.obv.trend === "rising" ? "text-emerald-500" : "text-red-500"}>
                    {data.obv.trend === "rising" ? "↑ rising" : "↓ falling"}
                  </span>
                  {data.obv.divergence && (
                    <span className={`ml-2 font-bold ${
                      data.obv.divergence === "bullish" ? "text-emerald-500" : "text-red-500"
                    }`}>
                      ⚡ {data.obv.divergence} divergence
                    </span>
                  )}
                </div>
              </div>
            )}
            {/* MFI */}
            {data.mfi && (
              <div className="rounded border border-[var(--border)] px-2 py-1.5"
                title="Money Flow Index = volume-weighted RSI. Overbought >80 (sell signal), oversold <20 (buy signal). More reliable than vanilla RSI in volume-driven markets.">
                <div className="text-[10px] uppercase opacity-70 mb-1">MFI (vol-weighted RSI)</div>
                <div className="text-[11px]">
                  <span className={
                    data.mfi.signal === "overbought" ? "text-red-500 font-bold" :
                    data.mfi.signal === "oversold" ? "text-emerald-500 font-bold" :
                    "text-[var(--text)]"
                  }>
                    {data.mfi.current}
                    {data.mfi.signal !== "neutral" && ` (${data.mfi.signal})`}
                  </span>
                </div>
              </div>
            )}
            {/* Ichimoku */}
            {data.ichimoku && (
              <div className="rounded border border-[var(--border)] px-2 py-1.5"
                title="Ichimoku Cloud — Japanese 5-component trend system. Above cloud = bullish bias. Below cloud = bearish. Inside cloud = consolidation. TK cross = momentum signal.">
                <div className="text-[10px] uppercase opacity-70 mb-1">Ichimoku</div>
                <div className="text-[11px]">
                  <span className={
                    data.ichimoku.signal === "above_cloud_bullish" ? "text-emerald-500 font-medium" :
                    data.ichimoku.signal === "below_cloud_bearish" ? "text-red-500 font-medium" :
                    "text-[var(--text-muted)]"
                  }>
                    {data.ichimoku.signal.replace(/_/g, " ")}
                  </span>
                  {data.ichimoku.tk_cross && (
                    <span className={`ml-2 font-bold ${
                      data.ichimoku.tk_cross === "bullish" ? "text-emerald-500" : "text-red-500"
                    }`}>
                      TK {data.ichimoku.tk_cross}
                    </span>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Order Flow — each metric has hover tooltip explaining MEANING + HOW TO TRADE */}
      {data?.order_flow && (
        <div className="mb-3 rounded border border-[var(--border)] px-3 py-2 text-xs">
          <div className="font-semibold uppercase tracking-wide text-indigo-400 mb-1.5 flex items-center gap-2">
            Order Flow
            <span className="text-[10px] font-normal text-[var(--text-muted)] normal-case">
              hover any metric for explanation + how to trade
            </span>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2">
            {data.order_flow.absorption && (() => {
              const a = data.order_flow.absorption;
              const tip = a.absorbed
                ? `BUYER ABSORPTION — high volume (${a.vol_ratio}x avg) + long lower wick (${Math.round((a.lower_wick_ratio || 0) * 100)}%) + closed near high (${Math.round(a.close_strength * 100)}%). Institutions stepped in. NEXT 1-3 DAYS USUALLY BULLISH. Trade: buy at close, stop below today's low.`
                : `No absorption today. Strength score ${Math.round(a.strength * 100)}/100. Wait for >65% strength to confirm institutional buying.`;
              return (
                <div
                  className={`rounded border px-2 py-1 cursor-help ${a.absorbed
                    ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-500"
                    : "border-[var(--border)]"}`}
                  title={tip}
                >
                  <div className="text-[10px] uppercase opacity-70">Absorption</div>
                  <div className="font-bold">
                    {a.absorbed ? "🟢 YES" : "—"}
                    <span className="text-[10px] font-normal ml-1 opacity-80">
                      {Math.round(a.strength * 100)}%
                    </span>
                  </div>
                </div>
              );
            })()}
            {data.order_flow.vwap && (() => {
              const v = data.order_flow.vwap;
              const cp = data.current_price;
              let zone = "fair value", color = "text-[var(--text-muted)]";
              if (cp > v.upper_2sd) { zone = "+2σ extreme — fade short"; color = "text-orange-500"; }
              else if (cp > v.upper_1sd) { zone = "above +1σ — strong"; color = "text-emerald-500"; }
              else if (cp < v.lower_2sd) { zone = "−2σ extreme — fade long"; color = "text-red-500"; }
              else if (cp < v.lower_1sd) { zone = "below −1σ — weak"; color = "text-amber-500"; }
              const tip = `VWAP = Volume-Weighted Average Price. Institutions try to fill near VWAP. ABOVE VWAP = bullish bias day. ±1σ/±2σ bands act as mean-reversion levels. Trade: buy at lower band reject, sell at upper band reject when in chop. In trends, stay long while above VWAP.`;
              return (
                <div className="rounded border border-[var(--border)] px-2 py-1 cursor-help" title={tip}>
                  <div className="text-[10px] uppercase opacity-70">VWAP</div>
                  <div className="font-mono">{cur}{v.value}</div>
                  <div className={`text-[10px] ${color}`}>{zone}</div>
                </div>
              );
            })()}
            {data.order_flow.volume_profile && (() => {
              const vp = data.order_flow.volume_profile;
              const cp = data.current_price;
              const above = cp > vp.poc;
              const tip = `POC = Point of Control = price level where MOST volume traded. This is institutional "fair value" — price magnetizes here. VA (Value Area) = where 70% of volume happened. Trade: above POC = bulls in control; below POC = bears. Price often returns to POC after extension. Use POC as profit target if you're long below it, or stop level if short above it.`;
              return (
                <div className="rounded border border-[var(--border)] px-2 py-1 cursor-help" title={tip}>
                  <div className="text-[10px] uppercase opacity-70">POC (fair value)</div>
                  <div className="font-mono">{cur}{vp.poc}</div>
                  <div className="text-[10px] text-[var(--text-muted)]">
                    VA {cur}{vp.val}–{vp.vah} · price {above ? "above" : "below"}
                  </div>
                </div>
              );
            })()}
            {data.order_flow.volume_delta && (() => {
              const vd = data.order_flow.volume_delta;
              const positive = vd.delta_5d > 0;
              const tip = `Cumulative Volume Delta over 5 days = (buy volume) − (sell volume). Approximated from candle-position weighting. POSITIVE = net buying pressure → bullish. NEGATIVE = net selling pressure → bearish. Watch for DIVERGENCE: if price up but Δ negative = exhaustion (likely top). If price flat but Δ rising = absorption (likely break up).`;
              return (
                <div className="rounded border border-[var(--border)] px-2 py-1 cursor-help" title={tip}>
                  <div className="text-[10px] uppercase opacity-70">Volume Delta 5d</div>
                  <div className={`font-mono ${positive ? "text-emerald-500" : "text-red-500"}`}>
                    {positive ? "+" : ""}{vd.delta_5d.toLocaleString()}
                  </div>
                  <div className="text-[10px] text-[var(--text-muted)]">
                    {positive ? "net buyers" : "net sellers"}
                  </div>
                </div>
              );
            })()}
            {data.order_flow.orderbook_imbalance && (() => {
              const obi = data.order_flow.orderbook_imbalance;
              const tip = `Order Book Imbalance = (bid size − ask size) / total. Live snapshot from order book depth. POSITIVE = more buyers than sellers waiting → bullish lean. NEGATIVE = more sellers waiting → bearish lean. >+15% = strong, >+30% = institutional accumulation. Combine with absorption: positive imbalance + absorption = high-conviction long.`;
              const color = obi.imbalance > 0.05 ? "text-emerald-500"
                : obi.imbalance < -0.05 ? "text-red-500" : "text-[var(--text-muted)]";
              return (
                <div className="rounded border border-[var(--border)] px-2 py-1 cursor-help" title={tip}>
                  <div className="text-[10px] uppercase opacity-70">Bid/Ask</div>
                  <div className={`font-mono ${color}`}>
                    {obi.imbalance_pct > 0 ? "+" : ""}{obi.imbalance_pct}%
                  </div>
                  <div className="text-[10px] opacity-70">{obi.verdict}</div>
                </div>
              );
            })()}
          </div>
        </div>
      )}

      {/* Premium / Discount badge — quick read of where price sits in the recent range */}
      {data?.premium_discount && (
        <div
          className="mb-3 rounded border px-3 py-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-sm"
          style={{
            borderColor:
              data.premium_discount.current_zone.includes("premium")
                ? "rgba(239,68,68,0.5)"
                : data.premium_discount.current_zone.includes("discount")
                ? "rgba(38,166,154,0.5)"
                : "rgba(168,85,247,0.5)",
            background:
              data.premium_discount.current_zone.includes("premium")
                ? "rgba(239,68,68,0.06)"
                : data.premium_discount.current_zone.includes("discount")
                ? "rgba(38,166,154,0.06)"
                : "rgba(168,85,247,0.06)",
          }}
        >
          <span className="font-semibold uppercase text-xs tracking-wide">
            {data.premium_discount.current_zone.replace("_", " ")}
            <span className="ml-1 font-mono opacity-70">
              ({data.premium_discount.current_pct}%)
            </span>
          </span>
          <span className="text-xs opacity-80">
            Range {cur}{data.premium_discount.range_low}–{data.premium_discount.range_high}
            {" · "}EQ {cur}{data.premium_discount.equilibrium}
          </span>
          <span className="text-xs italic opacity-90">{data.premium_discount.bias_action}</span>
          {data.bos_zones?.bullish_trigger && (
            <span className="text-xs">
              <span className="text-green-500">↑BOS @ {cur}{data.bos_zones.bullish_trigger.price}</span>
            </span>
          )}
          {data.bos_zones?.bearish_trigger && (
            <span className="text-xs">
              <span className="text-red-500">↓BOS @ {cur}{data.bos_zones.bearish_trigger.price}</span>
            </span>
          )}
        </div>
      )}

      {/* Accumulation / Distribution card */}
      {data?.accumulation && (
        <div
          className="mb-4 rounded-lg border-2 overflow-hidden"
          style={{
            borderColor:
              data.accumulation.bias === "bullish" ? "#14b8a6" :
              data.accumulation.bias === "bearish" ? "#ef4444" :
              "#a78bfa",
          }}
        >
          <div
            className="px-4 py-2 flex items-center gap-3 flex-wrap"
            style={{
              background:
                data.accumulation.bias === "bullish" ? "rgba(20,184,166,0.1)" :
                data.accumulation.bias === "bearish" ? "rgba(239,68,68,0.1)" :
                "rgba(167,139,250,0.1)",
            }}
          >
            <span
              className={clsx(
                "px-2 py-0.5 rounded-full text-xs font-bold tracking-wide",
                data.accumulation.phase === "ACCUMULATION" && "bg-teal-500/25 text-teal-300",
                data.accumulation.phase === "DISTRIBUTION" && "bg-red-500/25 text-red-300",
                data.accumulation.phase === "CONSOLIDATION" && "bg-violet-500/25 text-violet-300",
              )}
            >
              📊 {data.accumulation.phase}
            </span>
            <span className="text-xs text-gray-500">
              Confidence: <strong>{data.accumulation.confidence}</strong>
            </span>
            {data.accumulation.target_up !== null && (
              <span className="text-sm font-mono text-teal-500">
                Breakout target: {cur}{data.accumulation.target_up}
              </span>
            )}
            {data.accumulation.target_down !== null && (
              <span className="text-sm font-mono text-red-500">
                Breakdown target: {cur}{data.accumulation.target_down}
              </span>
            )}
          </div>
          <div className="px-4 py-2 bg-white/80 dark:bg-gray-900/40 text-xs">
            <p className="mb-2">{data.accumulation.summary}</p>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-[11px]">
              <div>
                <span className="text-gray-500">Range:</span>{" "}
                <span className="font-mono">
                  {cur}{data.accumulation.range_low}–{data.accumulation.range_high}
                </span>{" "}
                <span className="text-gray-500">({data.accumulation.range_pct}%)</span>
              </div>
              <div>
                <span className="text-gray-500">Volume:</span>{" "}
                <span className={clsx(
                  "font-mono",
                  data.accumulation.volume_ratio >= 1.2 ? "text-emerald-400" :
                  data.accumulation.volume_ratio >= 0.8 ? "text-yellow-400" :
                  "text-gray-500",
                )}>
                  {data.accumulation.volume_ratio}x
                </span>{" "}
                <span className="text-gray-500">vs prior</span>
              </div>
              <div>
                <span className="text-gray-500">Tests:</span>{" "}
                <span className="font-mono">
                  {data.accumulation.support_tests}↓ / {data.accumulation.resistance_tests}↑
                </span>
              </div>
              <div>
                <span className="text-gray-500">Pre-trend:</span>{" "}
                <span className={clsx(
                  "font-mono",
                  data.accumulation.pre_trend_pct < 0 ? "text-red-400" : "text-emerald-400",
                )}>
                  {data.accumulation.pre_trend_pct > 0 ? "+" : ""}{data.accumulation.pre_trend_pct}%
                </span>
              </div>
            </div>
          </div>
        </div>
      )}

      <div className="bg-white dark:bg-gray-900/30 rounded-lg border border-gray-300 dark:border-gray-700/50 p-2 relative">
        <div
          ref={containerRef}
          className="w-full"
          style={{ minHeight: CHART_HEIGHT }}
        />
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-white/70 dark:bg-gray-900/70 rounded-lg pointer-events-none">
            <div className="text-center">
              <RefreshCw className="w-8 h-8 animate-spin mx-auto text-emerald-500 mb-2" />
              <p className="text-sm text-gray-500">Loading {symbol}...</p>
            </div>
          </div>
        )}
        {!loading && error && (
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="text-center max-w-md p-4">
              <AlertTriangle className="w-8 h-8 mx-auto text-red-500 mb-2" />
              <p className="text-sm text-red-400 mb-2">
                Couldn't load {symbol}
              </p>
              <p className="text-xs text-gray-500 mb-3">{error}</p>
              <button
                onClick={handleRefresh}
                className="px-3 py-1 text-xs bg-emerald-500/20 text-emerald-500 rounded hover:bg-emerald-500/30"
              >
                Retry
              </button>
            </div>
          </div>
        )}
        {!loading && !error && !data && (
          <div className="absolute inset-0 flex items-center justify-center text-red-400 text-sm">
            No data for {symbol}
          </div>
        )}
      </div>

      {/* Sub-pane indicators */}
      {toggles.rsi && (
        <div className="mt-2 bg-white dark:bg-gray-900/30 rounded-lg border border-gray-300 dark:border-gray-700/50 p-2">
          <div className="text-[11px] uppercase tracking-wide text-purple-400 mb-1 px-1">RSI(14)</div>
          <div ref={rsiContainerRef} className="w-full" style={{ minHeight: 140 }} />
        </div>
      )}
      {toggles.macd && (
        <div className="mt-2 bg-white dark:bg-gray-900/30 rounded-lg border border-gray-300 dark:border-gray-700/50 p-2">
          <div className="text-[11px] uppercase tracking-wide text-blue-400 mb-1 px-1">MACD (12, 26, 9)</div>
          <div ref={macdContainerRef} className="w-full" style={{ minHeight: 140 }} />
        </div>
      )}
      {toggles.stoch && (
        <div className="mt-2 bg-white dark:bg-gray-900/30 rounded-lg border border-gray-300 dark:border-gray-700/50 p-2">
          <div className="text-[11px] uppercase tracking-wide text-cyan-400 mb-1 px-1">Stochastic %K %D</div>
          <div ref={stochContainerRef} className="w-full" style={{ minHeight: 140 }} />
        </div>
      )}

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
              {data.fvgs
                .slice(-12)
                .reverse()
                .map((f, i) => (
                  <div
                    key={i}
                    className="text-xs flex items-center justify-between border-b border-gray-200 dark:border-gray-700/30 py-1"
                  >
                    <span
                      className={clsx(
                        f.type === "bullish"
                          ? "text-emerald-600 dark:text-emerald-400"
                          : "text-red-500 dark:text-red-400",
                        f.mitigated && "opacity-50",
                      )}
                    >
                      {f.type === "bullish" ? "↑" : "↓"} {f.type.toUpperCase()}
                      {f.mitigated && " ·mit"}
                    </span>
                    <span className="font-mono text-gray-500 dark:text-gray-400">
                      {f.bottom.toFixed(1)} – {f.top.toFixed(1)} {cur}
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
              {data.structure
                .slice(-12)
                .reverse()
                .map((s, i) => {
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
                        {s.price.toFixed(1)} {cur}
                      </span>
                      <span className="text-gray-500">{s.time}</span>
                    </div>
                  );
                })}
            </div>
          </div>
        </div>
      )}

      {/* Support/Resistance panel */}
      {data?.support_resistance && data.support_resistance.length > 0 && (
        <div className="mt-4 bg-white dark:bg-gray-800/30 rounded-lg p-4 border border-gray-300 dark:border-gray-700/50">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-bold text-teal-600 dark:text-teal-300">
              Support / Resistance Levels ({data.support_resistance.length})
            </h3>
            <span className="text-[10px] text-gray-500">
              Strength = recency × touches • More ⭐ = more reliable
            </span>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2 max-h-64 overflow-y-auto">
            {data.support_resistance
              .slice()
              .sort((a, b) => b.price - a.price)
              .map((lvl, i) => {
                const isSupport = lvl.role === "support";
                const distance = data.current_price > 0
                  ? ((lvl.price - data.current_price) / data.current_price) * 100
                  : 0;
                return (
                  <div
                    key={i}
                    className={clsx(
                      "text-xs flex items-center justify-between gap-2 border rounded px-2 py-1.5",
                      isSupport
                        ? "border-emerald-500/30 bg-emerald-500/5"
                        : "border-red-500/30 bg-red-500/5",
                    )}
                  >
                    <div className="flex items-center gap-2">
                      <span
                        className={clsx(
                          "font-bold",
                          isSupport ? "text-emerald-500" : "text-red-500",
                        )}
                      >
                        {isSupport ? "↓" : "↑"} {lvl.role.toUpperCase()}
                      </span>
                      <span className="font-mono">{cur}{lvl.price.toFixed(1)}</span>
                    </div>
                    <div className="flex items-center gap-2 text-gray-500 dark:text-gray-400">
                      <span>{lvl.touches} touches</span>
                      <span className="text-yellow-400">{"⭐".repeat(lvl.strength)}</span>
                      <span className={clsx(
                        "font-mono w-12 text-right",
                        distance > 0 ? "text-red-400" : "text-emerald-400",
                      )}>
                        {distance > 0 ? "+" : ""}{distance.toFixed(1)}%
                      </span>
                    </div>
                  </div>
                );
              })}
          </div>
        </div>
      )}

      {/* Candle Patterns panel */}
      {data?.candle_patterns && data.candle_patterns.length > 0 && (
        <div className="mt-4 bg-white dark:bg-gray-800/30 rounded-lg p-4 border border-gray-300 dark:border-gray-700/50">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-bold text-cyan-600 dark:text-cyan-300">
              Candlestick Patterns ({data.candle_patterns.length})
            </h3>
            <span className="text-[10px] text-gray-500">
              Most recent first • Strength: ⭐ weak → ⭐⭐⭐ strong
            </span>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2 max-h-72 overflow-y-auto">
            {data.candle_patterns
              .slice()
              .reverse()
              .map((p, i) => (
                <div
                  key={i}
                  className={clsx(
                    "text-xs flex items-start gap-2 border rounded px-2 py-1.5",
                    p.bias === "bullish" && "border-emerald-500/30 bg-emerald-500/5",
                    p.bias === "bearish" && "border-red-500/30 bg-red-500/5",
                    p.bias === "neutral" && "border-purple-500/30 bg-purple-500/5",
                  )}
                >
                  <span
                    className={clsx(
                      "font-bold whitespace-nowrap",
                      p.bias === "bullish" && "text-emerald-500",
                      p.bias === "bearish" && "text-red-500",
                      p.bias === "neutral" && "text-purple-500",
                    )}
                  >
                    {p.bias === "bullish" ? "↑" : p.bias === "bearish" ? "↓" : "•"}{" "}
                    {p.type}
                  </span>
                  <span className="flex-1 text-gray-500 dark:text-gray-400">
                    {p.description}
                  </span>
                  <span className="font-mono text-gray-500 text-[11px] whitespace-nowrap">
                    {"⭐".repeat(p.strength)} {p.time}
                  </span>
                </div>
              ))}
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
