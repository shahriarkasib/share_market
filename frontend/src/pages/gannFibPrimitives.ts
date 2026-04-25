/**
 * Gann Fan and Fibonacci Circle primitives for lightweight-charts v5.
 * Both use canvas drawing via timeScale + series coordinate conversion.
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

interface BitmapScope {
  context: CanvasRenderingContext2D;
  bitmapSize: { width: number; height: number };
  mediaSize: { width: number; height: number };
  horizontalPixelRatio: number;
  verticalPixelRatio: number;
}

interface RenderTarget {
  useBitmapCoordinateSpace: (cb: (scope: BitmapScope) => void) => void;
}

// ═══════════════════════════════════════════════════════════
// GANN FAN
// ═══════════════════════════════════════════════════════════

export interface GannLine {
  label: string;
  start_time: string;
  start_price: number;
  end_time: string;
  end_price: number;
}

export interface GannFanData {
  pivot_time: string;
  pivot_price: number;
  direction: "up" | "down";
  lines: GannLine[];
}

class GannRenderer implements IPrimitivePaneRenderer {
  private fan: GannFanData;
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;

  constructor(fan: GannFanData, chart: IChartApi, series: ISeriesApi<"Candlestick">) {
    this.fan = fan;
    this.chart = chart;
    this.series = series;
  }

  draw(target: RenderTarget) {
    target.useBitmapCoordinateSpace((scope) => {
      const ctx = scope.context;
      const ts = this.chart.timeScale();
      const hpr = scope.horizontalPixelRatio;
      const vpr = scope.verticalPixelRatio;

      // Color palette for Gann angles — 1x1 (most important) is brightest
      const colors: Record<string, string> = {
        "1x1": "rgba(250, 204, 21, 0.9)",      // yellow (the key angle)
        "1x2": "rgba(96, 165, 250, 0.6)",
        "2x1": "rgba(96, 165, 250, 0.6)",
        "1x3": "rgba(168, 85, 247, 0.5)",
        "3x1": "rgba(168, 85, 247, 0.5)",
        "1x4": "rgba(244, 114, 182, 0.4)",
        "4x1": "rgba(244, 114, 182, 0.4)",
        "1x8": "rgba(156, 163, 175, 0.3)",
        "8x1": "rgba(156, 163, 175, 0.3)",
      };

      this.fan.lines.forEach((line) => {
        const x1 = ts.timeToCoordinate(line.start_time as Time);
        const x2 = ts.timeToCoordinate(line.end_time as Time);
        const y1 = this.series.priceToCoordinate(line.start_price);
        const y2 = this.series.priceToCoordinate(line.end_price);

        if (x1 === null || x2 === null || y1 === null || y2 === null) return;

        ctx.strokeStyle = colors[line.label] ?? "rgba(156, 163, 175, 0.5)";
        ctx.lineWidth = (line.label === "1x1" ? 1.5 : 1) * hpr;
        ctx.beginPath();
        ctx.moveTo(x1 * hpr, y1 * vpr);
        ctx.lineTo(x2 * hpr, y2 * vpr);
        ctx.stroke();

        // Label at the right end of each line
        ctx.fillStyle = colors[line.label] ?? "rgba(156, 163, 175, 0.7)";
        ctx.font = `${10 * hpr}px monospace`;
        ctx.textBaseline = "middle";
        ctx.fillText(line.label, x2 * hpr + 4 * hpr, y2 * vpr);
      });

      // Pivot marker
      const px = ts.timeToCoordinate(this.fan.pivot_time as Time);
      const py = this.series.priceToCoordinate(this.fan.pivot_price);
      if (px !== null && py !== null) {
        ctx.fillStyle = "rgba(250, 204, 21, 0.9)";
        ctx.beginPath();
        ctx.arc(px * hpr, py * vpr, 4 * hpr, 0, Math.PI * 2);
        ctx.fill();
      }
    });
  }
}

class GannPaneView implements IPrimitivePaneView {
  private fan: GannFanData;
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;

  constructor(fan: GannFanData, chart: IChartApi, series: ISeriesApi<"Candlestick">) {
    this.fan = fan;
    this.chart = chart;
    this.series = series;
  }

  zOrder() {
    return "normal" as const;
  }

  renderer() {
    return new GannRenderer(this.fan, this.chart, this.series);
  }
}

export class GannPrimitive implements ISeriesPrimitive<Time> {
  private fan: GannFanData;
  private chart: IChartApi | null = null;
  private series: ISeriesApi<"Candlestick"> | null = null;

  constructor(fan: GannFanData) {
    this.fan = fan;
  }

  attached(param: SeriesAttachedParameter<Time>) {
    this.chart = param.chart;
    this.series = param.series as ISeriesApi<"Candlestick">;
  }

  detached() {
    this.chart = null;
    this.series = null;
  }

  paneViews(): readonly IPrimitivePaneView[] {
    if (!this.chart || !this.series) return [];
    return [new GannPaneView(this.fan, this.chart, this.series)];
  }

  updateAllViews() {
    /* repaint */
  }
}

// ═══════════════════════════════════════════════════════════
// FIBONACCI CIRCLES
// ═══════════════════════════════════════════════════════════

export interface FibCircle {
  ratio: number;
  radius: number;
}

export interface FibCirclesData {
  center_time: string;
  center_price: number;
  base_radius: number;
  circles: FibCircle[];
}

class FibCircleRenderer implements IPrimitivePaneRenderer {
  private data: FibCirclesData;
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;

  constructor(data: FibCirclesData, chart: IChartApi, series: ISeriesApi<"Candlestick">) {
    this.data = data;
    this.chart = chart;
    this.series = series;
  }

  draw(target: RenderTarget) {
    target.useBitmapCoordinateSpace((scope) => {
      const ctx = scope.context;
      const ts = this.chart.timeScale();
      const hpr = scope.horizontalPixelRatio;
      const vpr = scope.verticalPixelRatio;

      const cx = ts.timeToCoordinate(this.data.center_time as Time);
      const cy = this.series.priceToCoordinate(this.data.center_price);
      if (cx === null || cy === null) return;

      // To draw a circle proportionally we need a price→pixel and time→pixel scale.
      // Approx: take the y-pixel distance for base_radius price units.
      const refY = this.series.priceToCoordinate(
        this.data.center_price + this.data.base_radius,
      );
      if (refY === null) return;
      const yScale = Math.abs(refY - cy); // pixels per base_radius price units

      // For x, assume one bar ≈ uniform spacing — approximate by using yScale (square circle in price space)
      // This is the convention Fibonacci circles use: equal in price terms.

      const ratioColors: Record<number, string> = {
        0.382: "rgba(251, 191, 36, 0.5)",
        0.5: "rgba(168, 85, 247, 0.5)",
        0.618: "rgba(244, 114, 182, 0.6)",
        1.0: "rgba(96, 165, 250, 0.6)",
        1.272: "rgba(251, 146, 60, 0.5)",
        1.618: "rgba(239, 68, 68, 0.6)",
        2.618: "rgba(156, 163, 175, 0.4)",
      };

      this.data.circles.forEach((c) => {
        const r = c.ratio * yScale;
        ctx.strokeStyle = ratioColors[c.ratio] ?? "rgba(156, 163, 175, 0.4)";
        ctx.lineWidth = 1 * hpr;
        ctx.setLineDash([4 * hpr, 4 * hpr]);
        ctx.beginPath();
        ctx.arc(cx * hpr, cy * vpr, r, 0, Math.PI * 2);
        ctx.stroke();
        ctx.setLineDash([]);

        // Label on top of each circle
        ctx.fillStyle = ratioColors[c.ratio] ?? "rgba(156, 163, 175, 0.7)";
        ctx.font = `${10 * hpr}px monospace`;
        ctx.textBaseline = "bottom";
        ctx.textAlign = "center";
        ctx.fillText(`${c.ratio}`, cx * hpr, cy * vpr - r - 4 * hpr);
      });

      // Center dot
      ctx.fillStyle = "rgba(96, 165, 250, 0.9)";
      ctx.beginPath();
      ctx.arc(cx * hpr, cy * vpr, 4 * hpr, 0, Math.PI * 2);
      ctx.fill();

      ctx.textAlign = "left"; // reset
    });
  }
}

class FibCirclePaneView implements IPrimitivePaneView {
  private data: FibCirclesData;
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;

  constructor(data: FibCirclesData, chart: IChartApi, series: ISeriesApi<"Candlestick">) {
    this.data = data;
    this.chart = chart;
    this.series = series;
  }

  zOrder() {
    return "normal" as const;
  }

  renderer() {
    return new FibCircleRenderer(this.data, this.chart, this.series);
  }
}

export class FibCirclesPrimitive implements ISeriesPrimitive<Time> {
  private data: FibCirclesData;
  private chart: IChartApi | null = null;
  private series: ISeriesApi<"Candlestick"> | null = null;

  constructor(data: FibCirclesData) {
    this.data = data;
  }

  attached(param: SeriesAttachedParameter<Time>) {
    this.chart = param.chart;
    this.series = param.series as ISeriesApi<"Candlestick">;
  }

  detached() {
    this.chart = null;
    this.series = null;
  }

  paneViews(): readonly IPrimitivePaneView[] {
    if (!this.chart || !this.series) return [];
    return [new FibCirclePaneView(this.data, this.chart, this.series)];
  }

  updateAllViews() {
    /* repaint */
  }
}
