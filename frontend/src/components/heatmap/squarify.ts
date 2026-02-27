/**
 * Squarified treemap layout algorithm.
 *
 * Produces rectangles with aspect ratios as close to 1:1 as possible,
 * following the Bruls-Huizing-van Wijk algorithm. Pure TypeScript, zero
 * external dependencies.
 */

export interface TreemapItem {
  key: string;
  value: number;
  [k: string]: unknown;
}

export interface TreemapRect {
  x: number;
  y: number;
  w: number;
  h: number;
  data: TreemapItem;
}

/**
 * Lay out `items` into a rectangle of the given `width` x `height`.
 *
 * Items MUST be sorted by value descending before calling.
 * Returns an array of positioned rectangles in pixel coordinates.
 */
export function squarify(
  items: TreemapItem[],
  width: number,
  height: number,
): TreemapRect[] {
  if (items.length === 0 || width <= 0 || height <= 0) return [];

  const totalValue = items.reduce((s, i) => s + i.value, 0);
  if (totalValue <= 0) return [];

  // Normalise values so they sum to total area
  const area = width * height;
  const normalized = items.map((item) => ({
    ...item,
    area: (item.value / totalValue) * area,
  }));

  const rects: TreemapRect[] = [];
  layoutStrip(normalized, 0, 0, width, height, rects);
  return rects;
}

/** Worst aspect ratio in a row laid along the shorter side of the remaining rect. */
function worst(row: { area: number }[], sideLen: number): number {
  const s = row.reduce((sum, r) => sum + r.area, 0);
  const s2 = s * s;
  const side2 = sideLen * sideLen;
  let maxRatio = 0;
  for (const r of row) {
    const ratio = Math.max(
      (side2 * r.area) / s2,
      s2 / (side2 * r.area),
    );
    if (ratio > maxRatio) maxRatio = ratio;
  }
  return maxRatio;
}

type NormalizedItem = TreemapItem & { area: number };

function layoutStrip(
  items: NormalizedItem[],
  x: number,
  y: number,
  w: number,
  h: number,
  out: TreemapRect[],
): void {
  if (items.length === 0) return;

  if (items.length === 1) {
    out.push({ x, y, w, h, data: items[0] });
    return;
  }

  // Lay out along the shorter side
  const shortSide = Math.min(w, h);

  const row: NormalizedItem[] = [items[0]];
  let remaining = items.slice(1);
  let currentWorst = worst(row, shortSide);

  // Greedily add items to the current row while aspect ratio improves
  while (remaining.length > 0) {
    const candidate = [...row, remaining[0]];
    const newWorst = worst(candidate, shortSide);
    if (newWorst <= currentWorst) {
      row.push(remaining[0]);
      remaining = remaining.slice(1);
      currentWorst = newWorst;
    } else {
      break;
    }
  }

  // Position the row
  const rowArea = row.reduce((s, r) => s + r.area, 0);

  if (w >= h) {
    // row goes along the left edge (vertical strip)
    const stripW = rowArea / h;
    let cy = y;
    for (const item of row) {
      const itemH = item.area / stripW;
      out.push({ x, y: cy, w: stripW, h: itemH, data: item });
      cy += itemH;
    }
    layoutStrip(remaining, x + stripW, y, w - stripW, h, out);
  } else {
    // row goes along the top edge (horizontal strip)
    const stripH = rowArea / w;
    let cx = x;
    for (const item of row) {
      const itemW = item.area / stripH;
      out.push({ x: cx, y, w: itemW, h: stripH, data: item });
      cx += itemW;
    }
    layoutStrip(remaining, x, y + stripH, w, h - stripH, out);
  }
}
