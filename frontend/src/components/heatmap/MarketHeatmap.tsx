/**
 * MarketHeatmap -- treemap visualisation of market activity.
 *
 * Renders every stock as an absolutely-positioned, colour-coded rectangle
 * inside a relative container. Colour encodes change_pct (red/green) and
 * area encodes the chosen "size" metric (turnover, volume, or trades).
 */

import { useMemo, useRef, useState, useCallback, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { clsx } from "clsx";
import type { HeatmapSector } from "../../types/index.ts";
import { squarify, type TreemapRect } from "./squarify.ts";
import { formatNumber, formatCompact } from "../../lib/format.ts";

/* ------------------------------------------------------------------ */
/*  Colour helpers                                                     */
/* ------------------------------------------------------------------ */

/** Linearly interpolate between two hex colours. `t` in [0, 1]. */
function lerpColor(a: string, b: string, t: number): string {
  const parse = (hex: string) => [
    parseInt(hex.slice(1, 3), 16),
    parseInt(hex.slice(3, 5), 16),
    parseInt(hex.slice(5, 7), 16),
  ];
  const ca = parse(a);
  const cb = parse(b);
  const r = Math.round(ca[0] + (cb[0] - ca[0]) * t);
  const g = Math.round(ca[1] + (cb[1] - ca[1]) * t);
  const bl = Math.round(ca[2] + (cb[2] - ca[2]) * t);
  return `rgb(${r},${g},${bl})`;
}

const COLOR_RED = "#ef4444";
const COLOR_GRAY = "#6b7280";
const COLOR_GREEN = "#22c55e";

/** Map change_pct to a background colour. */
function changePctToColor(pct: number): string {
  const clamped = Math.max(-5, Math.min(5, pct));
  if (clamped < 0) {
    return lerpColor(COLOR_RED, COLOR_GRAY, (clamped + 5) / 5);
  }
  return lerpColor(COLOR_GRAY, COLOR_GREEN, clamped / 5);
}

/** Choose white or black text depending on background luminance. */
function textColorFor(bg: string): string {
  // bg is "rgb(r,g,b)"
  const m = bg.match(/\d+/g);
  if (!m) return "#fff";
  const [r, g, b] = m.map(Number);
  const lum = 0.299 * r + 0.587 * g + 0.114 * b;
  return lum > 160 ? "#111" : "#fff";
}

/* ------------------------------------------------------------------ */
/*  Tooltip                                                            */
/* ------------------------------------------------------------------ */

interface TooltipData {
  symbol: string;
  sector: string;
  ltp: number;
  change_pct: number;
  volume: number;
  x: number;
  y: number;
}

function Tooltip({ data }: { data: TooltipData }) {
  const sign = data.change_pct >= 0 ? "+" : "";
  return (
    <div
      className="pointer-events-none fixed z-[100] rounded-lg border border-[var(--border)] bg-[var(--surface-elevated)] px-3 py-2 shadow-xl"
      style={{ left: data.x + 12, top: data.y + 12 }}
    >
      <p className="text-xs font-semibold text-[var(--text)]">{data.symbol}</p>
      <p className="text-[10px] text-[var(--text-muted)]">{data.sector}</p>
      <div className="mt-1 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-[10px]">
        <span className="text-[var(--text-dim)]">LTP</span>
        <span className="text-[var(--text)] tabular-nums text-right">
          {formatNumber(data.ltp)}
        </span>
        <span className="text-[var(--text-dim)]">Change</span>
        <span
          className={clsx(
            "tabular-nums font-medium text-right",
            data.change_pct > 0
              ? "text-green-400"
              : data.change_pct < 0
                ? "text-red-400"
                : "text-slate-400",
          )}
        >
          {sign}{data.change_pct.toFixed(2)}%
        </span>
        <span className="text-[var(--text-dim)]">Volume</span>
        <span className="text-[var(--text)] tabular-nums text-right">
          {formatCompact(data.volume)}
        </span>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Main component                                                     */
/* ------------------------------------------------------------------ */

interface Props {
  data: HeatmapSector[];
}

export default function MarketHeatmap({ data }: Props) {
  const navigate = useNavigate();
  const containerRef = useRef<HTMLDivElement>(null);
  const [tooltip, setTooltip] = useState<TooltipData | null>(null);
  const [containerWidth, setContainerWidth] = useState(0);

  // Observe container width for responsive layout
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setContainerWidth(entry.contentRect.width);
      }
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Determine container height based on viewport
  const isMobile = containerWidth > 0 && containerWidth < 640;
  const containerHeight = isMobile ? 400 : 600;

  // Build the treemap layout
  const rects = useMemo(() => {
    if (containerWidth <= 0) return [];

    // Flatten all stocks, tagging each with its sector
    const items = data.flatMap((sector) =>
      sector.stocks
        .filter((s) => s.size_value > 0)
        .map((s) => ({
          key: s.symbol,
          value: s.size_value,
          change_pct: s.change_pct,
          ltp: s.ltp,
          volume: s.volume,
          sector: sector.sector,
        })),
    );

    // Sort descending by value (required by squarify)
    items.sort((a, b) => b.value - a.value);

    return squarify(items, containerWidth, containerHeight);
  }, [data, containerWidth, containerHeight]);

  const handleMouseMove = useCallback(
    (e: React.MouseEvent, rect: TreemapRect) => {
      setTooltip({
        symbol: rect.data.key,
        sector: rect.data.sector as string,
        ltp: rect.data.ltp as number,
        change_pct: rect.data.change_pct as number,
        volume: rect.data.volume as number,
        x: e.clientX,
        y: e.clientY,
      });
    },
    [],
  );

  const handleMouseLeave = useCallback(() => {
    setTooltip(null);
  }, []);

  const handleClick = useCallback(
    (symbol: string) => {
      navigate(`/stock/${symbol}`);
    },
    [navigate],
  );

  return (
    <div className="relative">
      <div
        ref={containerRef}
        className="relative overflow-hidden rounded-lg border border-[var(--border)] bg-[var(--surface)]"
        style={{ height: containerHeight }}
      >
        {rects.map((rect) => {
          const bg = changePctToColor(rect.data.change_pct as number);
          const fg = textColorFor(bg);
          const tooSmall = rect.w < 30 || rect.h < 30;
          const changePct = rect.data.change_pct as number;
          const sign = changePct >= 0 ? "+" : "";

          // Scale font size proportional to rect area
          const area = rect.w * rect.h;
          const symbolSize = Math.max(8, Math.min(16, Math.sqrt(area) / 5));
          const pctSize = Math.max(7, symbolSize - 2);

          return (
            <div
              key={rect.data.key}
              onClick={() => handleClick(rect.data.key)}
              onMouseMove={(e) => handleMouseMove(e, rect)}
              onMouseLeave={handleMouseLeave}
              className="absolute cursor-pointer transition-opacity hover:opacity-80"
              style={{
                left: rect.x,
                top: rect.y,
                width: rect.w,
                height: rect.h,
                backgroundColor: bg,
                // 1px gap between cells
                padding: 0.5,
              }}
            >
              <div
                className="flex h-full w-full flex-col items-center justify-center overflow-hidden rounded-[2px]"
                style={{ backgroundColor: bg }}
              >
                {!tooSmall && (
                  <>
                    <span
                      className="font-semibold leading-tight truncate max-w-full px-0.5"
                      style={{
                        fontSize: symbolSize,
                        color: fg,
                      }}
                    >
                      {rect.data.key}
                    </span>
                    <span
                      className="tabular-nums leading-tight"
                      style={{
                        fontSize: pctSize,
                        color: fg,
                        opacity: 0.85,
                      }}
                    >
                      {sign}{changePct.toFixed(1)}%
                    </span>
                  </>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* Floating tooltip */}
      {tooltip && <Tooltip data={tooltip} />}

      {/* Legend */}
      <div className="mt-2 flex items-center justify-center gap-2 text-[10px] text-[var(--text-dim)]">
        <span>-5%</span>
        <div
          className="h-2 w-40 rounded-sm"
          style={{
            background: `linear-gradient(to right, ${COLOR_RED}, ${COLOR_GRAY}, ${COLOR_GREEN})`,
          }}
        />
        <span>+5%</span>
      </div>
    </div>
  );
}
