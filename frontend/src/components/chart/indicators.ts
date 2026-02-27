/**
 * Client-side technical indicator calculations for chart overlays.
 * Pure math — no React, no side effects.
 */

/** Exponential Moving Average. Returns null for indices before the period fills. */
export function computeEMA(
  closes: number[],
  period: number,
): (number | null)[] {
  const result: (number | null)[] = new Array(closes.length).fill(null);
  if (closes.length < period) return result;

  // Seed with SMA of first `period` values
  let sum = 0;
  for (let i = 0; i < period; i++) sum += closes[i];
  let ema = sum / period;
  result[period - 1] = ema;

  const k = 2 / (period + 1);
  for (let i = period; i < closes.length; i++) {
    ema = closes[i] * k + ema * (1 - k);
    result[i] = ema;
  }
  return result;
}

/** Simple Moving Average. Returns null for indices before the period fills. */
export function computeSMA(
  closes: number[],
  period: number,
): (number | null)[] {
  const result: (number | null)[] = new Array(closes.length).fill(null);
  if (closes.length < period) return result;

  let sum = 0;
  for (let i = 0; i < period; i++) sum += closes[i];
  result[period - 1] = sum / period;

  for (let i = period; i < closes.length; i++) {
    sum += closes[i] - closes[i - period];
    result[i] = sum / period;
  }
  return result;
}

/** Bollinger Bands: middle = SMA(period), upper/lower = middle ± mult * stddev. */
export function computeBollingerBands(
  closes: number[],
  period = 20,
  mult = 2,
): { upper: (number | null)[]; middle: (number | null)[]; lower: (number | null)[] } {
  const middle = computeSMA(closes, period);
  const upper: (number | null)[] = new Array(closes.length).fill(null);
  const lower: (number | null)[] = new Array(closes.length).fill(null);

  for (let i = period - 1; i < closes.length; i++) {
    const sma = middle[i]!;
    let variance = 0;
    for (let j = i - period + 1; j <= i; j++) {
      variance += (closes[j] - sma) ** 2;
    }
    const std = Math.sqrt(variance / period);
    upper[i] = sma + mult * std;
    lower[i] = sma - mult * std;
  }

  return { upper, middle, lower };
}

/** RSI (Relative Strength Index). Returns null before `period` bars are filled. */
export function computeRSI(
  closes: number[],
  period = 14,
): (number | null)[] {
  const result: (number | null)[] = new Array(closes.length).fill(null);
  if (closes.length < period + 1) return result;

  // Calculate initial average gain/loss from first `period` changes
  let avgGain = 0;
  let avgLoss = 0;
  for (let i = 1; i <= period; i++) {
    const delta = closes[i] - closes[i - 1];
    if (delta > 0) avgGain += delta;
    else avgLoss += -delta;
  }
  avgGain /= period;
  avgLoss /= period;

  result[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);

  // Exponential smoothing for subsequent bars
  for (let i = period + 1; i < closes.length; i++) {
    const delta = closes[i] - closes[i - 1];
    const gain = delta > 0 ? delta : 0;
    const loss = delta < 0 ? -delta : 0;
    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;
    result[i] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  }

  return result;
}

/** MACD: 12-EMA minus 26-EMA, signal = 9-EMA of MACD, histogram = MACD - signal. */
export function computeMACD(closes: number[]): {
  macd: (number | null)[];
  signal: (number | null)[];
  histogram: (number | null)[];
} {
  const ema12 = computeEMA(closes, 12);
  const ema26 = computeEMA(closes, 26);
  const len = closes.length;

  const macdLine: (number | null)[] = new Array(len).fill(null);
  // MACD line is valid once both EMAs are valid (from index 25 onward)
  for (let i = 0; i < len; i++) {
    if (ema12[i] != null && ema26[i] != null) {
      macdLine[i] = ema12[i]! - ema26[i]!;
    }
  }

  // Extract the non-null MACD values to compute signal (9-EMA of MACD)
  const macdValues: number[] = [];
  const macdIndices: number[] = [];
  for (let i = 0; i < len; i++) {
    if (macdLine[i] != null) {
      macdValues.push(macdLine[i]!);
      macdIndices.push(i);
    }
  }

  const signalRaw = computeEMA(macdValues, 9);
  const signalLine: (number | null)[] = new Array(len).fill(null);
  const histLine: (number | null)[] = new Array(len).fill(null);

  for (let j = 0; j < macdValues.length; j++) {
    if (signalRaw[j] != null) {
      const idx = macdIndices[j];
      signalLine[idx] = signalRaw[j];
      histLine[idx] = macdLine[idx]! - signalRaw[j]!;
    }
  }

  return { macd: macdLine, signal: signalLine, histogram: histLine };
}

/** Stochastic Oscillator. %K = kPeriod-period, %D = dPeriod-period SMA of %K. */
export function computeStochastic(
  highs: number[],
  lows: number[],
  closes: number[],
  kPeriod = 14,
  dPeriod = 3,
): { k: (number | null)[]; d: (number | null)[] } {
  const len = closes.length;
  const kLine: (number | null)[] = new Array(len).fill(null);

  for (let i = kPeriod - 1; i < len; i++) {
    let highest = -Infinity;
    let lowest = Infinity;
    for (let j = i - kPeriod + 1; j <= i; j++) {
      if (highs[j] > highest) highest = highs[j];
      if (lows[j] < lowest) lowest = lows[j];
    }
    const range = highest - lowest;
    kLine[i] = range === 0 ? 50 : ((closes[i] - lowest) / range) * 100;
  }

  // %D = SMA of %K over dPeriod
  const dLine: (number | null)[] = new Array(len).fill(null);
  for (let i = kPeriod - 1 + dPeriod - 1; i < len; i++) {
    let sum = 0;
    for (let j = i - dPeriod + 1; j <= i; j++) {
      sum += kLine[j]!;
    }
    dLine[i] = sum / dPeriod;
  }

  return { k: kLine, d: dLine };
}

/** VWAP (Volume Weighted Average Price). Cumulative for daily bars. */
export function computeVWAP(
  highs: number[],
  lows: number[],
  closes: number[],
  volumes: number[],
): (number | null)[] {
  const len = closes.length;
  const result: (number | null)[] = new Array(len).fill(null);

  let cumTPV = 0;
  let cumVol = 0;

  for (let i = 0; i < len; i++) {
    const tp = (highs[i] + lows[i] + closes[i]) / 3;
    cumTPV += tp * volumes[i];
    cumVol += volumes[i];
    result[i] = cumVol === 0 ? null : cumTPV / cumVol;
  }

  return result;
}
