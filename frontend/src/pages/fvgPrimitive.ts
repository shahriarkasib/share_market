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
  private showMitigated: boolean;

  constructor(zones: FVGZone[], chart: IChartApi, series: ISeriesApi<"Candlestick">, showMitigated: boolean = false) {
    this.zones = zones;
    this.chart = chart;
    this.series = series;
    this.showMitigated = showMitigated;
  }

  draw(target: RenderTarget) {
    target.useBitmapCoordinateSpace((scope) => {
      const ctx = scope.context;
      const ts = this.chart.timeScale();

      // Render fresh FVGs only by default (matches TradingView SMC indicator).
      // Skip mitigated unless explicitly enabled.
      const visible = this.zones.filter((z) => !z.mitigated || this.showMitigated);

      visible.forEach((z) => {
        const x1 = ts.timeToCoordinate(z.start_time as Time);
        const x2 = ts.timeToCoordinate(z.end_time as Time);
        const yTop = this.series.priceToCoordinate(z.top);
        const yBot = this.series.priceToCoordinate(z.bottom);
        if (x1 === null || x2 === null || yTop === null || yBot === null) return;

        const mit = z.mitigated === true;
        // Distinct palette to match TradingView SMC visuals:
        //   bullish FVG  → teal / sea-green
        //   bearish FVG  → pink / magenta
        const fillAlpha = mit ? 0.05 : 0.22;
        const strokeAlpha = mit ? 0.20 : 0.60;
        const textAlpha = mit ? 0.5 : 0.95;

        const fillColor =
          z.type === "bullish"
            ? `rgba(20, 184, 166, ${fillAlpha})`     // teal-500
            : `rgba(217, 70, 239, ${fillAlpha})`;     // fuchsia-500
        const strokeColor =
          z.type === "bullish"
            ? `rgba(20, 184, 166, ${strokeAlpha})`
            : `rgba(217, 70, 239, ${strokeAlpha})`;
        const textColor =
          z.type === "bullish"
            ? `rgba(94, 234, 212, ${textAlpha})`     // teal-300
            : `rgba(240, 171, 252, ${textAlpha})`;   // fuchsia-300

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

        // "FVG" label centered (matches TradingView style)
        const w = px2 - px1;
        const hgt = Math.abs(py2 - py1);
        if (w >= 28 * hpr && hgt >= 12 * vpr) {
          ctx.fillStyle = textColor;
          ctx.font = `bold ${11 * hpr}px monospace`;
          ctx.textBaseline = "middle";
          ctx.textAlign = "center";
          const cx = (px1 + px2) / 2;
          const cy = (py1 + py2) / 2;
          ctx.fillText("FVG", cx, cy);
        }
      });
    });
  }
}

class FVGPaneView implements IPrimitivePaneView {
  private zones: FVGZone[];
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;
  private showMitigated: boolean;

  constructor(zones: FVGZone[], chart: IChartApi, series: ISeriesApi<"Candlestick">, showMitigated: boolean = false) {
    this.zones = zones;
    this.chart = chart;
    this.series = series;
    this.showMitigated = showMitigated;
  }

  zOrder() {
    return "bottom" as const;
  }

  renderer() {
    return new FVGPaneRenderer(this.zones, this.chart, this.series, this.showMitigated);
  }
}

export class FVGPrimitive implements ISeriesPrimitive<Time> {
  private zones: FVGZone[];
  private chart: IChartApi | null = null;
  private series: ISeriesApi<"Candlestick"> | null = null;
  private requestUpdate?: () => void;
  private showMitigated: boolean;

  constructor(zones: FVGZone[], showMitigated: boolean = false) {
    this.zones = zones;
    this.showMitigated = showMitigated;
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
    return [new FVGPaneView(this.zones, this.chart, this.series, this.showMitigated)];
  }

  updateAllViews() {
    // Pane views rebuild on every paint.
  }

  setZones(zones: FVGZone[]) {
    this.zones = zones;
    if (this.requestUpdate) this.requestUpdate();
  }
}
