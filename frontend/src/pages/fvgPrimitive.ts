/**
 * FVGPrimitive — custom canvas primitive for lightweight-charts v5
 * Draws clean bounded rectangles for Fair Value Gap zones.
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

export interface FVGZone {
  type: "bullish" | "bearish";
  top: number;
  bottom: number;
  start_time: string;
  end_time: string;
  mitigated?: boolean;
}

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

class FVGPaneRenderer implements IPrimitivePaneRenderer {
  private zones: FVGZone[];
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;

  constructor(zones: FVGZone[], chart: IChartApi, series: ISeriesApi<"Candlestick">) {
    this.zones = zones;
    this.chart = chart;
    this.series = series;
  }

  draw(target: RenderTarget) {
    target.useBitmapCoordinateSpace((scope) => {
      const ctx = scope.context;
      const ts = this.chart.timeScale();

      this.zones.forEach((z) => {
        const x1 = ts.timeToCoordinate(z.start_time as Time);
        const x2 = ts.timeToCoordinate(z.end_time as Time);
        const yTop = this.series.priceToCoordinate(z.top);
        const yBot = this.series.priceToCoordinate(z.bottom);

        if (x1 === null || x2 === null || yTop === null || yBot === null) return;

        // Mitigated zones rendered with much lower opacity
        const mit = z.mitigated === true;
        const fillAlpha = mit ? 0.06 : 0.2;
        const strokeAlpha = mit ? 0.25 : 0.65;
        const textAlpha = mit ? 0.5 : 0.95;

        const fillColor =
          z.type === "bullish"
            ? `rgba(38, 166, 154, ${fillAlpha})`
            : `rgba(239, 83, 80, ${fillAlpha})`;
        const strokeColor =
          z.type === "bullish"
            ? `rgba(38, 166, 154, ${strokeAlpha})`
            : `rgba(239, 83, 80, ${strokeAlpha})`;
        const textColor =
          z.type === "bullish"
            ? `rgba(110, 231, 183, ${textAlpha})`
            : `rgba(252, 165, 165, ${textAlpha})`;

        const hpr = scope.horizontalPixelRatio;
        const vpr = scope.verticalPixelRatio;
        const px1 = x1 * hpr;
        const px2 = x2 * hpr;
        const py1 = yTop * vpr;
        const py2 = yBot * vpr;

        ctx.fillStyle = fillColor;
        ctx.fillRect(px1, py1, px2 - px1, py2 - py1);

        ctx.strokeStyle = strokeColor;
        ctx.lineWidth = 1 * hpr;
        if (mit) ctx.setLineDash([3 * hpr, 3 * hpr]);
        ctx.strokeRect(px1, py1, px2 - px1, py2 - py1);
        ctx.setLineDash([]);

        // "FVG" label inside the zone (top-left corner). Skip if zone too small.
        const w = px2 - px1;
        const hgt = Math.abs(py2 - py1);
        if (w >= 28 * hpr && hgt >= 12 * vpr) {
          ctx.fillStyle = textColor;
          ctx.font = `${10 * hpr}px monospace`;
          ctx.textBaseline = "top";
          ctx.textAlign = "left";
          const labelY = Math.min(py1, py2) + 2 * vpr;
          const labelX = px1 + 3 * hpr;
          const label = mit ? "FVG·mit" : "FVG";
          ctx.fillText(label, labelX, labelY);
        }
      });
    });
  }
}

class FVGPaneView implements IPrimitivePaneView {
  private zones: FVGZone[];
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;

  constructor(zones: FVGZone[], chart: IChartApi, series: ISeriesApi<"Candlestick">) {
    this.zones = zones;
    this.chart = chart;
    this.series = series;
  }

  zOrder() {
    return "bottom" as const;
  }

  renderer() {
    return new FVGPaneRenderer(this.zones, this.chart, this.series);
  }
}

export class FVGPrimitive implements ISeriesPrimitive<Time> {
  private zones: FVGZone[];
  private chart: IChartApi | null = null;
  private series: ISeriesApi<"Candlestick"> | null = null;
  private requestUpdate?: () => void;

  constructor(zones: FVGZone[]) {
    this.zones = zones;
  }

  attached(param: SeriesAttachedParameter<Time>) {
    this.chart = param.chart;
    this.series = param.series as ISeriesApi<"Candlestick">;
    this.requestUpdate = param.requestUpdate;
  }

  detached() {
    this.chart = null;
    this.series = null;
  }

  paneViews(): readonly IPrimitivePaneView[] {
    if (!this.chart || !this.series) return [];
    return [new FVGPaneView(this.zones, this.chart, this.series)];
  }

  updateAllViews() {
    // Pane views rebuild on every paint.
  }

  setZones(zones: FVGZone[]) {
    this.zones = zones;
    if (this.requestUpdate) this.requestUpdate();
  }
}
