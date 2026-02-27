/**
 * Number formatting helpers for the DSE Trading Assistant.
 * All currency values are in BDT (Bangladeshi Taka).
 */

const bdtFormatter = new Intl.NumberFormat("en-BD", {
  style: "currency",
  currency: "BDT",
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});

const compactFormatter = new Intl.NumberFormat("en-BD", {
  notation: "compact",
  compactDisplay: "short",
  maximumFractionDigits: 1,
});

const numberFormatter = new Intl.NumberFormat("en-BD", {
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});

/** Format a number as BDT currency with commas (e.g. BDT 1,234.50). */
export function formatBDT(value: number | null | undefined): string {
  if (value == null) return "--";
  return bdtFormatter.format(value);
}

/** Format a large number in compact notation (e.g. 1.2M, 45K). */
export function formatCompact(value: number | null | undefined): string {
  if (value == null) return "--";
  return compactFormatter.format(value);
}

/** Format a number with commas, no currency symbol. */
export function formatNumber(value: number | null | undefined): string {
  if (value == null) return "--";
  return numberFormatter.format(value);
}

/** Format a percentage with 1 decimal place and sign (e.g. +2.3%, -1.0%). */
export function formatPct(value: number | null | undefined): string {
  if (value == null) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}%`;
}

/** Format a change value with sign (e.g. +12.5, -3.2). DSE min tick = 0.10. */
export function formatChange(value: number | null | undefined): string {
  if (value == null) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}`;
}

/** Return a CSS text-color class based on sign of value. */
export function colorBySign(value: number): string {
  if (value > 0) return "text-green-400";
  if (value < 0) return "text-red-400";
  return "text-slate-400";
}
