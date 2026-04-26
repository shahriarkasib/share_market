/**
 * ChartPatternPrimitive — draws auto-detected chart patterns:
 * Double Top/Bottom, Triangle, Flag, Cup & Handle.
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

export interface ChartPattern {
  type: string;
  bias: "bullish" | "bearish" | "neutral";
  neckline?: number;
  target?: number;
  // Double top/bottom
  p1_time?: string; p1_price?: number;
  p2_time?: string; p2_price?: number;
  // Triangle
  upper_start_time?: string; upper_start_price?: number;
  upper_end_time?: string; upper_end_price?: number;
  lower_start_time?: string; lower_start_price?: number;
  lower_end_time?: string; lower_end_price?: number;
  // Flag
  pole_start_time?: string; pole_high?: number; pole_low?: number;
  flag_top_time?: string; flag_top?: number; flag_bottom?: number;
  // Cup & Handle
  left_rim_time?: string; left_rim_price?: number;
  cup_bottom_time?: string; cup_bottom_price?: number;
  right_rim_time?: string; right_rim_price?: number;
  handle_end_time?: string; handle_low?: number;
}

interface Scope {
  context: CanvasRenderingContext2D;
  horizontalPixelRatio: number;
  verticalPixelRatio: number;
}
interface Target { useBitmapCoordinateSpace: (cb: (s: Scope) => void) => void; }

class PatternRenderer implements IPrimitivePaneRenderer {
  private patterns: ChartPattern[];
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;
  constructor(patterns: ChartPattern[], chart: IChartApi, series: ISeriesApi<"Candlestick">) {
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

      const toX = (t?: string) => t ? ts.timeToCoordinate(t as Time) : null;
      const toY = (p?: number) => p != null ? this.series.priceToCoordinate(p) : null;

      this.patterns.forEach((p) => {
        const isBull = p.bias === "bullish";
        const isBear = p.bias === "bearish";
        const color = isBull ? "rgba(38, 166, 154, 0.85)"
                    : isBear ? "rgba(239, 83, 80, 0.85)"
                    : "rgba(168, 85, 247, 0.85)";
        const fillColor = isBull ? "rgba(38, 166, 154, 0.08)"
                        : isBear ? "rgba(239, 83, 80, 0.08)"
                        : "rgba(168, 85, 247, 0.08)";

        ctx.strokeStyle = color;
        ctx.fillStyle = fillColor;
        ctx.lineWidth = 1.5 * hpr;
        ctx.setLineDash([]);

        const labelText = p.type.replace(/_/g, " ").toUpperCase();

        if (p.type === "double_top" || p.type === "double_bottom") {
          const x1 = toX(p.p1_time); const x2 = toX(p.p2_time);
          const y1 = toY(p.p1_price); const y2 = toY(p.p2_price);
          const yN = toY(p.neckline);
          if (x1 == null || x2 == null || y1 == null || y2 == null || yN == null) return;
          // Connect peaks/troughs
          ctx.beginPath();
          ctx.moveTo(x1 * hpr, y1 * vpr);
          ctx.lineTo(x2 * hpr, y2 * vpr);
          ctx.stroke();
          // Neckline (dashed)
          ctx.setLineDash([6 * hpr, 4 * hpr]);
          ctx.beginPath();
          ctx.moveTo(x1 * hpr, yN * vpr);
          ctx.lineTo((x2 + 30) * hpr, yN * vpr);
          ctx.stroke();
          ctx.setLineDash([]);
          // Label at midpoint
          ctx.fillStyle = color;
          ctx.font = `bold ${11 * hpr}px monospace`;
          ctx.textAlign = "center";
          ctx.textBaseline = "bottom";
          const midX = (x1 + x2) / 2;
          const labelY = p.type === "double_top" ? Math.min(y1, y2) - 8 : Math.max(y1, y2) + 18;
          ctx.fillText(labelText, midX * hpr, labelY * vpr);
        }

        else if (p.type.startsWith("triangle_")) {
          const xUS = toX(p.upper_start_time); const xUE = toX(p.upper_end_time);
          const yUS = toY(p.upper_start_price); const yUE = toY(p.upper_end_price);
          const xLS = toX(p.lower_start_time); const xLE = toX(p.lower_end_time);
          const yLS = toY(p.lower_start_price); const yLE = toY(p.lower_end_price);
          if ([xUS, xUE, yUS, yUE, xLS, xLE, yLS, yLE].some((v) => v == null)) return;
          ctx.beginPath();
          ctx.moveTo(xUS! * hpr, yUS! * vpr);
          ctx.lineTo(xUE! * hpr, yUE! * vpr);
          ctx.stroke();
          ctx.beginPath();
          ctx.moveTo(xLS! * hpr, yLS! * vpr);
          ctx.lineTo(xLE! * hpr, yLE! * vpr);
          ctx.stroke();
          // Label
          ctx.fillStyle = color;
          ctx.font = `bold ${11 * hpr}px monospace`;
          ctx.textAlign = "left";
          ctx.textBaseline = "middle";
          ctx.fillText(labelText, xUE! * hpr + 4 * hpr, ((yUE! + yLE!) / 2) * vpr);
        }

        else if (p.type === "bull_flag" || p.type === "bear_flag") {
          const xS = toX(p.pole_start_time); const xE = toX(p.flag_top_time);
          const yPH = toY(p.pole_high); const yPL = toY(p.pole_low);
          const yFT = toY(p.flag_top); const yFB = toY(p.flag_bottom);
          if ([xS, xE, yPH, yPL, yFT, yFB].some((v) => v == null)) return;
          // Pole line
          ctx.beginPath();
          ctx.moveTo(xS! * hpr, p.type === "bull_flag" ? yPL! * vpr : yPH! * vpr);
          ctx.lineTo(xS! * hpr, p.type === "bull_flag" ? yPH! * vpr : yPL! * vpr);
          ctx.stroke();
          // Flag rectangle
          ctx.fillRect(xS! * hpr, yFT! * vpr,
                       (xE! - xS!) * hpr, (yFB! - yFT!) * vpr);
          ctx.strokeRect(xS! * hpr, yFT! * vpr,
                         (xE! - xS!) * hpr, (yFB! - yFT!) * vpr);
          // Label
          ctx.fillStyle = color;
          ctx.font = `bold ${11 * hpr}px monospace`;
          ctx.textAlign = "left";
          ctx.textBaseline = "bottom";
          ctx.fillText(labelText, xS! * hpr + 4 * hpr, (yFT! - 4) * vpr);
        }

        else if (p.type === "cup_and_handle") {
          const xL = toX(p.left_rim_time); const xC = toX(p.cup_bottom_time);
          const xR = toX(p.right_rim_time); const xH = toX(p.handle_end_time);
          const yL = toY(p.left_rim_price); const yCB = toY(p.cup_bottom_price);
          const yR = toY(p.right_rim_price); const yH = toY(p.handle_low);
          if ([xL, xC, xR, xH, yL, yCB, yR, yH].some((v) => v == null)) return;
          // Cup curve (3-point arc approximation: Bezier through left → bottom → right)
          ctx.beginPath();
          ctx.moveTo(xL! * hpr, yL! * vpr);
          ctx.quadraticCurveTo(xC! * hpr, yCB! * vpr * 1.05, xR! * hpr, yR! * vpr);
          ctx.stroke();
          // Handle line
          ctx.beginPath();
          ctx.moveTo(xR! * hpr, yR! * vpr);
          ctx.lineTo(xH! * hpr, yH! * vpr);
          ctx.stroke();
          // Neckline
          if (p.neckline != null) {
            const yN = toY(p.neckline);
            if (yN != null) {
              ctx.setLineDash([6 * hpr, 4 * hpr]);
              ctx.beginPath();
              ctx.moveTo(xL! * hpr, yN * vpr);
              ctx.lineTo((xH! + 30) * hpr, yN * vpr);
              ctx.stroke();
              ctx.setLineDash([]);
            }
          }
          // Label
          ctx.fillStyle = color;
          ctx.font = `bold ${11 * hpr}px monospace`;
          ctx.textAlign = "center";
          ctx.textBaseline = "bottom";
          ctx.fillText(labelText, xC! * hpr, (yCB! + 18) * vpr);
        }
      });
    });
  }
}

class PatternPaneView implements IPrimitivePaneView {
  private patterns: ChartPattern[];
  private chart: IChartApi;
  private series: ISeriesApi<"Candlestick">;
  constructor(patterns: ChartPattern[], chart: IChartApi, series: ISeriesApi<"Candlestick">) {
    this.patterns = patterns;
    this.chart = chart;
    this.series = series;
  }
  zOrder() { return "top" as const; }
  renderer() { return new PatternRenderer(this.patterns, this.chart, this.series); }
}

export class PatternPrimitive implements ISeriesPrimitive<Time> {
  private chart: IChartApi | null = null;
  private series: ISeriesApi<"Candlestick"> | null = null;
  private patterns: ChartPattern[];
  constructor(patterns: ChartPattern[]) {
    this.patterns = patterns;
  }

  attached(param: SeriesAttachedParameter<Time>) {
    this.chart = param.chart;
    this.series = param.series as ISeriesApi<"Candlestick">;
  }
  detached() { this.chart = null; this.series = null; }
  paneViews(): readonly IPrimitivePaneView[] {
    if (!this.chart || !this.series) return [];
    return [new PatternPaneView(this.patterns, this.chart, this.series)];
  }
  updateAllViews() { /* */ }
}
