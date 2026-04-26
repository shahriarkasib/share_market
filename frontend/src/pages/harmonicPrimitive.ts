/**
 * HarmonicPrimitive — draws XABCD harmonic patterns:
 * Butterfly, Gartley, Bat, Crab, Shark.
 * Each pattern has 4 connected legs (X-A, A-B, B-C, C-D) plus the X-D dashed line,
 * with point labels and ratio annotations.
 */

import type {
  IChartApi,
  ISeriesApi,
  ISeriesPrimitive,
  IPrimitivePaneRenderer,
  IPrimitivePaneView,
  Time,
  SeriesAttachedParameter,
} from "lightweight-charts";

export interface HarmonicPattern {
  type: string;          // e.g. "harmonic_butterfly"
  bias: "bullish" | "bearish";
  x_time?: string; x_price?: number;
  a_time?: string; a_price?: number;
  b_time?: string; b_price?: number;
  c_time?: string; c_price?: number;
  d_time?: string; d_price?: number;
  ratios?: Record<string, number>;
}

interface Scope {
  context: CanvasRenderingContext2D;
  horizontalPixelRatio: number;
  verticalPixelRatio: number;
}
interface Target { useBitmapCoordinateSpace: (cb: (s: Scope) => void) => void; }

class HarmonicRenderer implements IPrimitivePaneRenderer {
  private patterns: HarmonicPattern[];
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;
  constructor(patterns: HarmonicPattern[], chart: IChartApi, series: ISeriesApi<"Candlestick">) {
    this.patterns = patterns;
    this.chart = chart;
    this.series = series;
  }

  draw(target: Target) {
    target.useBitmapCoordinateSpace((scope) => {
      const ctx = scope.context;
      const ts = this.chart.timeScale();
      const hpr = scope.horizontalPixelRatio;
      const vpr = scope.verticalPixelRatio;

      this.patterns.forEach((p) => {
        const points: [string, string | undefined, number | undefined][] = [
          ["X", p.x_time, p.x_price],
          ["A", p.a_time, p.a_price],
          ["B", p.b_time, p.b_price],
          ["C", p.c_time, p.c_price],
          ["D", p.d_time, p.d_price],
        ];
        const coords: { label: string; x: number; y: number }[] = [];
        for (const [label, t, price] of points) {
          if (!t || price == null) return;
          const x = ts.timeToCoordinate(t as Time);
          const y = this.series.priceToCoordinate(price);
          if (x == null || y == null) return;
          coords.push({ label, x, y });
        }
        if (coords.length !== 5) return;

        const isBull = p.bias === "bullish";
        const color = isBull ? "rgba(38, 166, 154, 0.95)" : "rgba(239, 83, 80, 0.95)";

        ctx.strokeStyle = color;
        ctx.lineWidth = 1.5 * hpr;
        ctx.setLineDash([]);

        // Connect X→A→B→C→D
        ctx.beginPath();
        ctx.moveTo(coords[0].x * hpr, coords[0].y * vpr);
        for (let i = 1; i < 5; i++) {
          ctx.lineTo(coords[i].x * hpr, coords[i].y * vpr);
        }
        ctx.stroke();

        // X→D dashed projection (the harmonic completion line)
        ctx.setLineDash([4 * hpr, 4 * hpr]);
        ctx.beginPath();
        ctx.moveTo(coords[0].x * hpr, coords[0].y * vpr);
        ctx.lineTo(coords[4].x * hpr, coords[4].y * vpr);
        ctx.stroke();
        ctx.setLineDash([]);

        // Point labels
        ctx.fillStyle = color;
        ctx.font = `bold ${11 * hpr}px monospace`;
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        coords.forEach(({ label, x, y }, i) => {
          const isHigh = i % 2 === (isBull ? 1 : 0);
          // Place label above for highs, below for lows
          const offset = isHigh ? -10 : 10;
          ctx.fillText(label, x * hpr, (y + offset) * vpr);
        });

        // Pattern type label near point D (the entry point)
        const D = coords[4];
        const typeLabel = p.type.replace("harmonic_", "").toUpperCase();
        ctx.font = `bold ${10 * hpr}px monospace`;
        ctx.textAlign = "left";
        ctx.textBaseline = "middle";
        ctx.fillText(`${typeLabel} (${p.bias})`, (D.x + 6) * hpr, D.y * vpr);

        // Ratio annotation between B and C (mid)
        if (p.ratios) {
          const midBC_x = (coords[2].x + coords[3].x) / 2;
          const midBC_y = (coords[2].y + coords[3].y) / 2;
          ctx.font = `${9 * hpr}px monospace`;
          ctx.fillStyle = isBull ? "rgba(110, 231, 183, 0.85)" : "rgba(252, 165, 165, 0.85)";
          ctx.textAlign = "center";
          const ratioStr = `AD/XA=${p.ratios["AD/XA"]?.toFixed(2)}`;
          ctx.fillText(ratioStr, midBC_x * hpr, (midBC_y + 14) * vpr);
        }
      });
    });
  }
}

class HarmonicPaneView implements IPrimitivePaneView {
  private patterns: HarmonicPattern[];
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;
  constructor(patterns: HarmonicPattern[], chart: IChartApi, series: ISeriesApi<"Candlestick">) {
    this.patterns = patterns;
    this.chart = chart;
    this.series = series;
  }
  zOrder() { return "top" as const; }
  renderer() { return new HarmonicRenderer(this.patterns, this.chart, this.series); }
}

export class HarmonicPrimitive implements ISeriesPrimitive<Time> {
  private chart: IChartApi | null = null;
  private series: ISeriesApi<"Candlestick"> | null = null;
  private patterns: HarmonicPattern[];
  constructor(patterns: HarmonicPattern[]) {
    this.patterns = patterns;
  }
  attached(param: SeriesAttachedParameter<Time>) {
    this.chart = param.chart;
    this.series = param.series as ISeriesApi<"Candlestick">;
  }
  detached() { this.chart = null; this.series = null; }
  paneViews(): readonly IPrimitivePaneView[] {
    if (!this.chart || !this.series) return [];
    return [new HarmonicPaneView(this.patterns, this.chart, this.series)];
  }
  updateAllViews() { /* */ }
}
