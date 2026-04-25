/**
 * OrderBlockPrimitive — canvas-rendered rectangles for institutional order blocks.
 * Distinct from FVG via color (purple/violet for bullish, orange for bearish)
 * and styling (solid border for fresh, dashed for tested, dotted for mitigated).
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

export type OrderBlockStatus = "fresh" | "tested" | "mitigated";

export interface OrderBlockZone {
  type: "bullish" | "bearish";
  top: number;
  bottom: number;
  start_time: string;
  end_time: string;
  status: OrderBlockStatus;
  break_type?: string;
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

class OBPaneRenderer implements IPrimitivePaneRenderer {
  private zones: OrderBlockZone[];
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;

  constructor(zones: OrderBlockZone[], chart: IChartApi, series: ISeriesApi<"Candlestick">) {
    this.zones = zones;
    this.chart = chart;
    this.series = series;
  }

  draw(target: RenderTarget) {
    target.useBitmapCoordinateSpace((scope) => {
      const ctx = scope.context;
      const ts = this.chart.timeScale();
      const hpr = scope.horizontalPixelRatio;
      const vpr = scope.verticalPixelRatio;

      this.zones.forEach((z) => {
        const x1 = ts.timeToCoordinate(z.start_time as Time);
        const x2 = ts.timeToCoordinate(z.end_time as Time);
        const yTop = this.series.priceToCoordinate(z.top);
        const yBot = this.series.priceToCoordinate(z.bottom);

        if (x1 === null || x2 === null || yTop === null || yBot === null) return;

        // Color palette — violet for bullish, orange for bearish
        const fresh = z.status === "fresh";
        const tested = z.status === "tested";
        const mit = z.status === "mitigated";

        const fillAlpha = fresh ? 0.22 : tested ? 0.12 : 0.05;
        const strokeAlpha = fresh ? 0.85 : tested ? 0.55 : 0.25;
        const textAlpha = fresh ? 1.0 : tested ? 0.7 : 0.4;

        const fillColor =
          z.type === "bullish"
            ? `rgba(167, 139, 250, ${fillAlpha})`  // violet-400
            : `rgba(251, 146, 60, ${fillAlpha})`;   // orange-400
        const strokeColor =
          z.type === "bullish"
            ? `rgba(139, 92, 246, ${strokeAlpha})`  // violet-500
            : `rgba(249, 115, 22, ${strokeAlpha})`; // orange-500
        const textColor =
          z.type === "bullish"
            ? `rgba(196, 181, 253, ${textAlpha})`
            : `rgba(254, 215, 170, ${textAlpha})`;

        const px1 = x1 * hpr;
        const px2 = x2 * hpr;
        const py1 = yTop * vpr;
        const py2 = yBot * vpr;

        ctx.fillStyle = fillColor;
        ctx.fillRect(px1, py1, px2 - px1, py2 - py1);

        ctx.strokeStyle = strokeColor;
        ctx.lineWidth = (fresh ? 1.5 : 1) * hpr;
        if (tested) ctx.setLineDash([4 * hpr, 3 * hpr]);
        else if (mit) ctx.setLineDash([2 * hpr, 4 * hpr]);
        else ctx.setLineDash([]);
        ctx.strokeRect(px1, py1, px2 - px1, py2 - py1);
        ctx.setLineDash([]);

        // Label "OB" + status badge inside the zone
        const w = px2 - px1;
        const hgt = Math.abs(py2 - py1);
        if (w >= 30 * hpr && hgt >= 14 * vpr) {
          ctx.fillStyle = textColor;
          ctx.font = `bold ${10 * hpr}px monospace`;
          ctx.textBaseline = "top";
          ctx.textAlign = "left";
          const labelY = Math.min(py1, py2) + 2 * vpr;
          const labelX = px1 + 3 * hpr;
          const arrow = z.type === "bullish" ? "↑" : "↓";
          const statusTag = fresh ? "" : tested ? "·tested" : "·mit";
          ctx.fillText(`${arrow} OB${statusTag}`, labelX, labelY);
        }
      });
    });
  }
}

class OBPaneView implements IPrimitivePaneView {
  private zones: OrderBlockZone[];
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;

  constructor(zones: OrderBlockZone[], chart: IChartApi, series: ISeriesApi<"Candlestick">) {
    this.zones = zones;
    this.chart = chart;
    this.series = series;
  }

  zOrder() {
    return "bottom" as const;
  }

  renderer() {
    return new OBPaneRenderer(this.zones, this.chart, this.series);
  }
}

export class OrderBlockPrimitive implements ISeriesPrimitive<Time> {
  private zones: OrderBlockZone[];
  private chart: IChartApi | null = null;
  private series: ISeriesApi<"Candlestick"> | null = null;

  constructor(zones: OrderBlockZone[]) {
    this.zones = zones;
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
    return [new OBPaneView(this.zones, this.chart, this.series)];
  }

  updateAllViews() {
    /* repaint */
  }
}
