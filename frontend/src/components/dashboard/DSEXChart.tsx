import { useEffect, useRef } from "react";
import {
  createChart,
  AreaSeries,
  HistogramSeries,
  LineStyle,
  type IChartApi,
  type Time,
} from "lightweight-charts";
import { BarChart3 } from "lucide-react";
import type { DSEXBar } from "../../api/client.ts";

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
  data: DSEXBar[];
}

export default function DSEXChart({ data }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const theme = "dark" as const;

  // Create chart on mount
  useEffect(() => {
    if (!containerRef.current) return;

    const colors = getChartColors();
    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height: 220,
      layout: {
        background: { color: colors.bg },
        textColor: colors.text,
        fontSize: 10,
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
        scaleMargins: { top: 0.05, bottom: 0.25 },
      },
      timeScale: {
        borderColor: colors.border,
        timeVisible: false,
      },
    });

    chartRef.current = chart;

    const ro = new ResizeObserver(() => {
      if (containerRef.current) {
        chart.applyOptions({ width: containerRef.current.clientWidth });
      }
    });
    ro.observe(containerRef.current);

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
    };
  }, []);

  // Update colors on theme change
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    requestAnimationFrame(() => {
      const colors = getChartColors();
      chart.applyOptions({
        layout: { background: { color: colors.bg }, textColor: colors.text },
        grid: { vertLines: { color: colors.grid }, horzLines: { color: colors.grid } },
        crosshair: {
          vertLine: { color: colors.crosshair, labelBackgroundColor: colors.crosshairLabel },
          horzLine: { color: colors.crosshair, labelBackgroundColor: colors.crosshairLabel },
        },
        rightPriceScale: { borderColor: colors.border },
        timeScale: { borderColor: colors.border },
      });
    });
  }, [theme]);

  // Render data
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !data.length) return;

    // Determine trend color
    const first = data[0].dsex;
    const last = data[data.length - 1].dsex;
    const isUp = last >= first;
    const lineColor = isUp ? "#22c55e" : "#ef4444";
    const topColor = isUp ? "rgba(34,197,94,0.3)" : "rgba(239,68,68,0.3)";
    const bottomColor = isUp ? "rgba(34,197,94,0.02)" : "rgba(239,68,68,0.02)";

    // Clear any existing series
    try {
      for (const s of (chart as any).getSeries?.() ?? []) {
        chart.removeSeries(s);
      }
    } catch { /* fresh chart */ }

    // DSEX area series
    const areaSeries = chart.addSeries(AreaSeries, {
      topColor,
      bottomColor,
      lineColor,
      lineWidth: 2,
    });
    areaSeries.setData(
      data.map((b) => ({ time: b.date as Time, value: b.dsex })),
    );

    // Volume histogram
    const volSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "vol",
    });
    chart.priceScale("vol").applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });
    volSeries.setData(
      data.map((b, i) => ({
        time: b.date as Time,
        value: b.volume,
        color: i > 0 && data[i].dsex >= data[i - 1].dsex ? "#22c55e40" : "#ef444440",
      })),
    );

    chart.timeScale().fitContent();
  }, [data]);

  if (!data.length) return null;

  const last = data[data.length - 1];
  const first = data[0];
  const change = last.dsex - first.dsex;
  const changePct = first.dsex ? (change / first.dsex) * 100 : 0;

  return (
    <section className="bg-[var(--surface)] border border-[var(--border)] rounded-lg overflow-hidden">
      <div className="px-4 py-2.5 border-b border-[var(--border)] flex items-center gap-2">
        <BarChart3 className="h-3.5 w-3.5 text-blue-500" />
        <h2 className="text-xs font-semibold uppercase tracking-wider text-[var(--text-muted)]">
          DSEX Index
        </h2>
        <span className="text-xs tabular-nums text-[var(--text)] font-medium ml-2">
          {last.dsex.toFixed(2)}
        </span>
        <span
          className={`text-[11px] tabular-nums font-medium ${change >= 0 ? "text-green-500" : "text-red-500"}`}
        >
          {change >= 0 ? "+" : ""}{change.toFixed(2)} ({changePct >= 0 ? "+" : ""}{changePct.toFixed(2)}%)
        </span>
        <span className="text-[10px] text-[var(--text-dim)] ml-auto">
          {data.length} days
        </span>
      </div>
      <div ref={containerRef} className="w-full" />
    </section>
  );
}
