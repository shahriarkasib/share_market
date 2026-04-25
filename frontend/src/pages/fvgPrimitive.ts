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

        const fillColor =
          z.type === "bullish"
            ? "rgba(38, 166, 154, 0.18)"
            : "rgba(239, 83, 80, 0.18)";
        const strokeColor =
          z.type === "bullish"
            ? "rgba(38, 166, 154, 0.55)"
            : "rgba(239, 83, 80, 0.55)";

        const px1 = x1 * scope.horizontalPixelRatio;
        const px2 = x2 * scope.horizontalPixelRatio;
        const py1 = yTop * scope.verticalPixelRatio;
        const py2 = yBot * scope.verticalPixelRatio;

        ctx.fillStyle = fillColor;
        ctx.fillRect(px1, py1, px2 - px1, py2 - py1);

        ctx.strokeStyle = strokeColor;
        ctx.lineWidth = 1 * scope.horizontalPixelRatio;
        ctx.strokeRect(px1, py1, px2 - px1, py2 - py1);
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
