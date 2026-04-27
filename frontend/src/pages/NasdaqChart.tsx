import { useEffect, useRef, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  createChart,
  CandlestickSeries,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  type Time,
} from "lightweight-charts";
import { ArrowLeft, RefreshCw } from "lucide-react";
import { fetchNasdaqChart, type NasdaqChartData } from "../api/client";

export default function NasdaqChart() {
  const { symbol = "NVDA" } = useParams();
  const nav = useNavigate();
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  const [data, setData] = useState<NasdaqChartData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [period, setPeriod] = useState<string>("1y");

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const d = await fetchNasdaqChart(symbol, period);
      setData(d);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, [symbol, period]);

  useEffect(() => {
    if (!containerRef.current) return;
    const isDark = document.documentElement.classList.contains("dark");
    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height: 600,
      layout: {
        background: { color: isDark ? "#0a0a0a" : "#ffffff" },
        textColor: isDark ? "#cbd5e1" : "#1f2937",
      },
      grid: {
        vertLines: { color: isDark ? "#1f2937" : "#e5e7eb" },
        horzLines: { color: isDark ? "#1f2937" : "#e5e7eb" },
      },
      timeScale: { borderColor: isDark ? "#374151" : "#d1d5db" },
      rightPriceScale: { borderColor: isDark ? "#374151" : "#d1d5db" },
    });
    chartRef.current = chart;
    candleRef.current = chart.addSeries(CandlestickSeries, {
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderUpColor: "#26a69a",
      borderDownColor: "#ef5350",
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350",
    });
    volumeRef.current = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "vol",
    });
    chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } });
    const ro = new ResizeObserver(() => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: containerRef.current.clientWidth });
      }
    });
    ro.observe(containerRef.current);
    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!data || !candleRef.current || !volumeRef.current) return;
    candleRef.current.setData(data.candles.map((c) => ({ ...c, time: c.time as Time })));
    if (data.volumes) {
      volumeRef.current.setData(data.volumes.map((v) => ({ ...v, time: v.time as Time })));
    }
    chartRef.current?.timeScale().fitContent();
  }, [data]);

  const a = data?.analysis;

  return (
    <div className="max-w-[1440px] mx-auto px-3 sm:px-4 lg:px-8 py-6">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <div className="flex items-center gap-3">
          <button
            onClick={() => nav("/nasdaq/signals")}
            className="p-2 rounded border border-[var(--border)] hover:bg-[var(--hover)]"
          >
            <ArrowLeft className="h-4 w-4" />
          </button>
          <h1 className="text-xl font-bold flex items-center gap-2">
            {data?.symbol || symbol}
            {data && (
              <span className="text-emerald-500 font-mono text-base">
                ${data.current_price.toFixed(2)}
              </span>
            )}
          </h1>
        </div>
        <div className="flex items-center gap-1 bg-[var(--surface-active)] rounded p-1">
          {["3mo", "6mo", "1y", "2y"].map((p) => (
            <button
              key={p}
              onClick={() => setPeriod(p)}
              className={`px-2.5 py-1 rounded text-xs ${
                period === p
                  ? "bg-[var(--surface)] text-[var(--text)]"
                  : "text-[var(--text-muted)] hover:bg-[var(--hover)]"
              }`}
            >
              {p}
            </button>
          ))}
          <button
            onClick={load}
            disabled={loading}
            className="ml-2 p-1 rounded hover:bg-[var(--hover)] disabled:opacity-50"
          >
            <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
          </button>
        </div>
      </div>

      {error && (
        <div className="rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-500 mb-3">
          {error}
        </div>
      )}

      {a && (
        <div
          className="mb-4 rounded-lg border-2 px-4 py-3"
          style={{
            borderColor:
              a.action_color === "green"
                ? "#10b981"
                : a.action_color === "yellow"
                ? "#f59e0b"
                : a.action_color === "orange"
                ? "#f97316"
                : a.action_color === "red"
                ? "#ef4444"
                : "#6b7280",
            background:
              a.action_color === "green"
                ? "rgba(16,185,129,0.08)"
                : a.action_color === "red"
                ? "rgba(239,68,68,0.08)"
                : "rgba(107,114,128,0.06)",
          }}
        >
          <div className="flex items-center gap-3 flex-wrap mb-2">
            <span className="font-bold text-sm">
              BIAS: <span className="text-amber-400">{a.bias}</span>
            </span>
            <span className="text-xs text-[var(--text-muted)]">
              Confidence: {a.confidence}
            </span>
            <span className="text-sm font-bold">→ {a.action}</span>
          </div>
          <div className="text-sm text-[var(--text-muted)] mb-2">{a.summary}</div>
          {a.reasons?.length > 0 && (
            <ul className="text-xs text-[var(--text-muted)] space-y-0.5 mb-2">
              {a.reasons.map((r, i) => (
                <li key={i}>• {r}</li>
              ))}
            </ul>
          )}
          {a.entry !== null && (
            <div className="grid grid-cols-2 sm:grid-cols-5 gap-2 mt-2 text-xs">
              <Field label="Entry" value={`$${a.entry?.toFixed(2)}`} />
              <Field label="Stop" value={`$${a.stop_loss?.toFixed(2)}`} color="red" />
              <Field label="T1" value={`$${a.target1?.toFixed(2)}`} color="green" />
              <Field label="T2" value={`$${a.target2?.toFixed(2)}`} color="green" />
              <Field
                label="R:R"
                value={a.risk_reward !== null ? `1:${a.risk_reward?.toFixed(1)}` : "—"}
              />
            </div>
          )}
        </div>
      )}

      <div ref={containerRef} className="rounded-lg border border-[var(--border)]" />
    </div>
  );
}

function Field({ label, value, color }: { label: string; value: string; color?: "red" | "green" }) {
  return (
    <div className="rounded border border-[var(--border)] px-2 py-1.5">
      <div className="text-[10px] text-[var(--text-muted)] uppercase">{label}</div>
      <div
        className={`font-mono text-sm ${
          color === "red" ? "text-red-500" : color === "green" ? "text-emerald-500" : ""
        }`}
      >
        {value}
      </div>
    </div>
  );
}
