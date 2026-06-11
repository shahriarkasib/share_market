import axios from "axios";
import type {
  MarketSummary,
  StockSignal,
  StockPrice,
  MatrixStock,
  SignalsSummary,
  ScreenerResult,
  WatchlistItem,
  OHLCVBar,
  Holding,
  PortfolioSummary,
  PortfolioAlert,
  Suggestions,
  SectorPerformance,
  HeatmapSector,
  DailyAnalysisResponse,
  AnalysisSummaryResponse,
  LiveScanResponse,
  LLMScanResponse,
  BuyRadarResponse,
} from "../types/index.ts";

const api = axios.create({
  baseURL: "/api/v1",
  timeout: 20_000,
  headers: { "Content-Type": "application/json" },
});

// Separate axios instance for NASDAQ backend (lives on a different host).
// Caddy reverse-proxies port 8001 over HTTPS at the nip.io subdomain so the
// browser can reach it without mixed-content blocks.
const NASDAQ_API_BASE = "https://nasdaq.34.126.141.16.nip.io";
const nasdaqApi = axios.create({
  baseURL: NASDAQ_API_BASE,
  timeout: 30_000,
  headers: { "Content-Type": "application/json" },
});

export interface NasdaqChartData {
  symbol: string;
  current_price: number;
  candles: { time: string; open: number; high: number; low: number; close: number }[];
  volumes?: { time: string; value: number; color: string }[];
  fvgs?: Array<{
    type: "bullish" | "bearish";
    top: number;
    bottom: number;
    start_time: string;
    end_time: string;
    mitigated: boolean;
    quality?: number;
    valid?: boolean;
    tier?: string;
  }>;
  structure?: Array<{
    type: string;
    price: number;
    from_price: number;
    time: string;
    from_time: string;
  }>;
  analysis?: {
    bias: string;
    confidence: string;
    action: string;
    action_color: string;
    summary: string;
    reasons: string[];
    entry: number | null;
    entry_label: string | null;
    stop_loss: number | null;
    target1: number | null;
    target2: number | null;
    risk_reward: number | null;
    triggers: { icon: string; text: string }[];
  };
  premium_discount?: unknown;
  bos_zones?: unknown;
  accumulation?: unknown;
}

export async function fetchNasdaqChart(
  symbol: string,
  period: string = "1y",
): Promise<NasdaqChartData> {
  const { data } = await nasdaqApi.get<NasdaqChartData>(
    `/api/v1/nasdaq/smc-chart/${symbol.toUpperCase()}`,
    { params: { period } },
  );
  return data;
}

export interface NasdaqScreenerCandidate {
  symbol: string;
  price: number;
  bias: string;
  confidence: string;
  action: string;
  summary: string;
  entry: number | null;
  stop_loss: number | null;
  target1: number | null;
  target2: number | null;
  risk_reward: number | null;
}

export async function fetchNasdaqScreener(): Promise<NasdaqScreenerCandidate[]> {
  const { data } = await nasdaqApi.get<NasdaqScreenerCandidate[]>(
    `/api/v1/nasdaq/smc-screener`,
  );
  return data;
}

export interface NasdaqTicker {
  symbol: string;
  name: string | null;
  sector: string | null;
  halal_status?: string;
}

export async function fetchNasdaqTickers(): Promise<NasdaqTicker[]> {
  const { data } = await nasdaqApi.get<NasdaqTicker[]>(
    `/api/v1/nasdaq/tickers`,
  );
  return data;
}

// GCP VM never sleeps — no keep-alive needed

api.interceptors.response.use(
  (response) => response,
  (error) => {
    const message =
      error.response?.data?.detail ??
      error.response?.data?.message ??
      error.message ??
      "An unexpected error occurred";
    console.error("[API Error]", message, error.config?.url);
    return Promise.reject(new Error(message));
  },
);

/* ========================== Dashboard ========================== */

export async function fetchMarketSummary(): Promise<MarketSummary> {
  const { data } = await api.get<MarketSummary>("/market/summary");
  return data;
}

export async function fetchTopBuySignals(
  limit = 10,
): Promise<StockSignal[]> {
  const { data } = await api.get<StockSignal[]>("/signals/top", {
    params: { type: "buy", limit },
  });
  return data;
}

export async function fetchTopSellSignals(
  limit = 10,
): Promise<StockSignal[]> {
  const { data } = await api.get<StockSignal[]>("/signals/top", {
    params: { type: "sell", limit },
  });
  return data;
}

export async function fetchAllPrices(category?: string): Promise<StockPrice[]> {
  const { data } = await api.get<StockPrice[]>("/market/all-prices", {
    params: category ? { category } : undefined,
  });
  return data;
}

export async function fetchMatrixData(): Promise<MatrixStock[]> {
  const { data } = await api.get("/market/matrix");
  return data;
}

export interface DSEXChartBar {
  date: string;
  value: number;
  volume: number;
  turnover: number;
}

export async function fetchDSEXChart(): Promise<DSEXChartBar[]> {
  const { data } = await api.get<DSEXChartBar[]>("/market/dsex-chart");
  return data;
}

export interface SMCCandle {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
}

export interface SMCVolume {
  time: string;
  value: number;
  color: string;
}

export interface SMCFvg {
  type: "bullish" | "bearish";
  top: number;
  bottom: number;
  start_time: string;
  end_time: string;
  mitigated?: boolean;
}

export interface SMCKeyLevel {
  label: string;
  price: number;
  color: string;
  purpose: "resistance" | "support" | "breakout_long" | "breakout_short";
}

export interface SMCOrderBlock {
  type: "bullish" | "bearish";
  top: number;
  bottom: number;
  start_time: string;
  end_time: string;
  status: "fresh" | "tested" | "mitigated";
  break_type?: string;
}

export interface SMCAnalysisTrigger {
  icon: string;
  text: string;
}

export interface SMCAnalysis {
  bias: "BULLISH" | "BEARISH" | "NEUTRAL" | "WHIPSAW";
  confidence: "HIGH" | "MEDIUM" | "LOW";
  action: string;
  action_color: "green" | "yellow" | "red" | "orange" | "gray";
  summary: string;
  reasons: string[];
  entry: number | null;
  entry_label: string | null;
  stop_loss: number | null;
  target1: number | null;
  target2: number | null;
  risk_reward: number | null;
  triggers: SMCAnalysisTrigger[];
}

export interface SMCScreenerCandidate {
  symbol: string;
  price: number;
  bias: string;
  confidence: string;
  action: string;
  summary: string;
  entry: number | null;
  entry_label: string | null;
  stop_loss: number | null;
  target1: number | null;
  target2: number | null;
  risk_reward: number | null;
  reasons: string[];
}

export async function fetchSMCScreener(
  minConfidence: "HIGH" | "MEDIUM" | "LOW" = "MEDIUM",
): Promise<SMCScreenerCandidate[]> {
  const { data } = await api.get<SMCScreenerCandidate[]>(
    "/market/smc-screener",
    { params: { min_confidence: minConfidence } },
  );
  return data;
}

export interface LiveCompositeSignal {
  id: number;
  symbol: string;
  first_triggered: string;
  last_seen: string;
  status: "active" | "hit_t1" | "completed" | "stopped_out" | "invalidated" | "expired";
  composite_score: number;
  signal_level: "STRONG_BUY" | "BUY" | "WATCH" | "NONE";
  risk_score: number;
  entry: number | null;
  stop_loss: number | null;
  target1: number | null;
  target2: number | null;
  bias: string | null;
  active_signals: string[];
  reasons: string[];
  current_price: number | null;
  triggered_high: number | null;
  triggered_low: number | null;
  closed_at: string | null;
  close_price: number | null;
  pl_pct: number | null;
  regime: "TRENDING_UP" | "TRENDING_DOWN" | "SIDEWAYS" | "VOLATILE_EXPANSION" | null;
  action_type:
    | "BUY_NOW" | "RECENT_TRIGGER" | "MISSED_ENTRY" | "RUNNING"
    | "BUY_LIMIT" | "SETUP_DEEP" | "STALE" | "BREAKOUT_PENDING"
    | "AVOID" | "WAITING" | null;
  entry_distance_pct: number | null;
  state_label: string | null;
  days_since_trigger: number | null;
  fvg_distance_pct: number | null;
  votes: Record<string, { score: number; vote: "BUY" | "HOLD" | "AVOID"; weight_in_regime: number }> | null;
  t_plus_2_friendly?: boolean | null;
  t_plus_2_reasons?: string[];
  t_plus_2_bonuses?: string[];
  buy_votes?: number | null;
  weighted_buy_pct?: number | null;
  // ── New SMC-aligned fields ──
  entry_label?: string | null;
  entry_status?: "AT_ENTRY" | "WAIT_PULLBACK" | "TOO_FAR" | "DISCOUNT_TRIGGERED" | null;
  chase_warning?: string | null;
  aggressive_entry?: number | null;
  aggressive_entry_label?: string | null;
  aggressive_entry_distance_pct?: number | null;
  confidence?: "HIGH" | "MEDIUM" | "LOW" | null;
  hedge_fund_verdict?: string | null;
  structure_verdict?: string | null;
  order_flow_verdict?: string | null;
  volume_verdict?: string | null;
  htf_bias?: { bias?: string | null; trend_pct?: number | null; weeks_analysed?: number | null } | null;
  liquidity_sweep?: string | null;
  // Entry zone (range) + technical trigger fields
  entry_zone_low?: number | null;
  entry_zone_high?: number | null;
  aggressive_entry_zone_low?: number | null;
  aggressive_entry_zone_high?: number | null;
  primary_trigger_date?: string | null;
  primary_trigger_bars_ago?: number | null;
  primary_trigger_max_profit_pct?: number | null;
  primary_trigger_max_drawdown_pct?: number | null;
  tier1_trigger_date?: string | null;
  tier1_trigger_bars_ago?: number | null;
  tier1_max_profit_pct?: number | null;
  tier2_trigger_date?: string | null;
  tier2_trigger_bars_ago?: number | null;
  tier2_max_profit_pct?: number | null;
  bucket?: "IN_ZONE" | "JUST_BOUNCED" | "PULLBACK_IN_PROGRESS" | "WATCHING" | "MISSED" | "WRONG_TRIGGER" | "STALE" | null;
  analyst_verdict?: {
    verdict?: "STRONG_BUY" | "BUY" | "WATCH" | "NEUTRAL" | "AVOID" | "STRONG_AVOID" | null;
    emoji?: string;
    score?: number;
    summary?: string;
    factors?: Array<{ factor: string; score: number; detail?: string }>;
  } | null;
  analyst_score?: number | null;
  today_candle_quality?: {
    type?: string;
    score?: number;
    reason?: string;
    is_green?: boolean;
  } | null;
  flow_divergence?: {
    type?: string;
    score?: number;
    reason?: string;
    tape_net?: number;
    magnitude?: string;
  } | null;
  pattern_failure?: {
    count?: number;
    score?: number;
    failures?: Array<{ pattern: string; date: string; original_bias: string; new_bias: string; reason: string }>;
  } | null;
  volume_signature?: {
    type?: string;
    score?: number;
    reason?: string;
    rvol?: number;
    today_volume?: number;
    avg_30d_volume?: number;
    regime?: "climactic" | "strong" | "normal" | "weak" | "dormant" | string;
    premium_zone?: string;
    premium_pct?: number;
  } | null;
  rvol?: number | null;
  absorption_pattern?: {
    type?: "BULLISH_ABSORPTION" | "BEARISH_DISTRIBUTION" | string;
    count?: number;
    volume_trend?: "declining" | "stable" | "rising" | string;
    today_active?: boolean;
    at_demand_zone?: boolean;
    span_days?: number;
    score?: number;
    reason?: string;
    events?: Array<{ time?: string; type?: string; strength?: number }>;
  } | null;
  absorption_score?: number | null;
  // Locked trigger snapshot
  actual_trigger_price?: number | null;
  trigger_locked?: boolean | null;
  // T+N OHLC tracking
  t1_high?: number | null; t1_low?: number | null; t1_close?: number | null; t1_date?: string | null;
  t2_high?: number | null; t2_low?: number | null; t2_close?: number | null; t2_date?: string | null;
  t3_high?: number | null; t3_low?: number | null; t3_close?: number | null;
  t5_high?: number | null; t5_low?: number | null; t5_close?: number | null;
  // Bid ladder
  bid_ladder?: Array<{
    price: number;
    size_pct: number;
    label: string;
    edge?: "max" | "very_high" | "high" | "good" | "medium" | string;
    risk_pct?: number | null;
    reward_pct?: number | null;
  }> | null;
  short_term_trend?: {
    slope_pct?: number | null;
    direction?: "UP" | "DOWN" | "SIDEWAYS" | null;
    consecutive_red?: number | null;
    last_close_vs_prior?: number | null;
    bounce_confirmed?: boolean | null;
    lookback_bars?: number | null;
  } | null;
}

export async function fetchLiveCompositeSignals(
  status: string = "active",
  minScore: number = 0,
  options?: { buy_only?: boolean; quality_filter?: boolean },
): Promise<LiveCompositeSignal[]> {
  const params: Record<string, string | number | boolean> = {
    status,
    min_score: minScore,
  };
  if (options?.buy_only !== undefined) params.buy_only = options.buy_only;
  if (options?.quality_filter !== undefined) params.quality_filter = options.quality_filter;
  const { data } = await api.get<LiveCompositeSignal[]>("/market/live-signals", { params });
  return data;
}


export interface AccuracyBucket {
  trades: number;
  wins: number;
  win_rate: number;
  avg_t1_days: number;
  avg_max_fav_pct?: number;
  avg_max_adv_pct?: number;
}

export interface StrategyVoteAccuracy {
  buy_signals: number;
  wins: number;
  win_rate: number;
}

export interface StockAccuracy extends AccuracyBucket {
  symbol: string;
}

export interface SignalAccuracyReport {
  overall: AccuracyBucket;
  by_regime: Record<string, AccuracyBucket>;
  by_score_bucket: Record<string, AccuracyBucket>;
  by_action_type: Record<string, AccuracyBucket>;
  by_strategy_vote: Record<string, StrategyVoteAccuracy>;
  by_stock: { top: StockAccuracy[] };
  total_closed: number;
}

export async function fetchCompositeSignalAccuracy(): Promise<SignalAccuracyReport> {
  const { data } = await api.get<SignalAccuracyReport>("/market/signal-accuracy");
  return data;
}

// NASDAQ versions — same shapes, different backend
export async function fetchNasdaqLiveSignals(
  status: string = "active",
  minScore: number = 0,
): Promise<LiveCompositeSignal[]> {
  const { data } = await nasdaqApi.get<LiveCompositeSignal[]>(
    "/api/v1/nasdaq/live-signals",
    { params: { status, min_score: minScore } },
  );
  return data;
}

export async function fetchNasdaqSignalAccuracy(): Promise<SignalAccuracyReport> {
  const { data } = await nasdaqApi.get<SignalAccuracyReport>("/api/v1/nasdaq/signal-accuracy");
  return data;
}

export interface SMCStructureEvent {
  type: "bullish_BOS" | "bearish_BOS" | "bullish_ChoCh" | "bearish_ChoCh";
  price: number;
  from_price: number;
  time: string;
  from_time: string;
}

export interface SMCFibLevel {
  label: string;
  price: number;
}

export interface SMCFibonacci {
  high: number;
  low: number;
  levels: SMCFibLevel[];
}

export interface SMCPivots {
  pivot: number;
  r1: number;
  r2: number;
  r3: number;
  s1: number;
  s2: number;
  s3: number;
}

export interface SMCMaLinePoint {
  time: string;
  value: number;
}

export interface SMCGannLine {
  label: string;
  start_time: string;
  start_price: number;
  end_time: string;
  end_price: number;
}

export interface SMCGannFan {
  pivot_time: string;
  pivot_price: number;
  direction: "up" | "down";
  lines: SMCGannLine[];
}

export interface SMCFibCircle {
  ratio: number;
  radius: number;
}

export interface SMCFibCircles {
  center_time: string;
  center_price: number;
  base_radius: number;
  circles: SMCFibCircle[];
}

export interface SMCChartData {
  symbol: string;
  current_price: number;
  candles: SMCCandle[];
  volumes: SMCVolume[];
  fvgs: SMCFvg[];
  structure: SMCStructureEvent[];
  fibonacci?: SMCFibonacci | null;
  pivots?: SMCPivots | null;
  moving_averages?: Record<string, SMCMaLinePoint[]>;
  gann_fan?: SMCGannFan | null;
  fib_circles?: SMCFibCircles | null;
  key_levels?: SMCKeyLevel[];
  order_blocks?: SMCOrderBlock[];
  analysis?: SMCAnalysis & {
    thesis?: string[];
    alignment?: string[];
    adx?: number | null;
    is_trendy?: boolean;
    confluence?: { bottom: number; top: number; support_touches: number } | null;
    // ── New SMC-aligned fields ──
    entry_status?: "AT_ENTRY" | "WAIT_PULLBACK" | "TOO_FAR" | "DISCOUNT_TRIGGERED" | null;
    entry_distance_pct?: number | null;
    chase_warning?: string | null;
    aggressive_entry?: number | null;
    aggressive_entry_label?: string | null;
    aggressive_entry_distance_pct?: number | null;
    hedge_fund_verdict?: string | null;
    structure_narrative?: string | null;
    structure_verdict?: string | null;
    order_flow_narrative?: string | null;
    order_flow_verdict?: string | null;
    volume_narrative?: string | null;
    volume_verdict?: string | null;
  };
  htf_bias?: { bias?: string | null; trend_pct?: number | null;
                weeks_analysed?: number | null; narrative?: string | null;
                weekly_swing_high?: number | null; weekly_swing_low?: number | null } | null;
  liquidity_sweeps?: {
    events?: Array<{
      type: string; idx: number; date: string; swing_price: number;
      wick_high?: number; wick_low?: number; close: number; interpretation: string;
    }>;
    latest?: {
      type: string; idx: number; date: string; swing_price: number;
      wick_high?: number; wick_low?: number; close: number; interpretation: string;
    } | null;
  } | null;
  fib_dealing_range?: {
    swing_low: number; swing_high: number; leg_size_pct: number;
    is_uptrend_leg: boolean; current_pct: number;
    current_zone: string; action_text: string; narrative: string; valid: boolean;
    levels: Array<{ ratio: number; price: number; label: string;
                    zone: string; action: string }>;
  } | null;
  elliott_triangle?: {
    type: string; kind: string; bias: string;
    points: Array<{ label: string; price: number; time: string }>;
    breakout_up_target: number; breakdown_target: number;
    narrative: string;
  } | null;
  demand_zones?: Array<{
    type: "DEMAND"; subtype: "RBR" | "DBR";
    top: number; bottom: number; base_time: string;
    impulse_pct: number; mitigated: boolean;
  }>;
  supply_zones?: Array<{
    type: "SUPPLY"; subtype: "DBD" | "RBD";
    top: number; bottom: number; base_time: string;
    impulse_pct: number; mitigated: boolean;
  }>;
  volatility_imbalances?: Array<{
    type: "VI_BULLISH" | "VI_BEARISH";
    top: number; bottom: number; time: string;
    mitigated: boolean; is_below_price?: boolean; is_above_price?: boolean;
  }>;
  rsi?: { time: string; value: number }[];
  macd?: {
    macd: { time: string; value: number }[];
    signal: { time: string; value: number }[];
    histogram: { time: string; value: number; color: string }[];
  };
  stochastic?: {
    k: { time: string; value: number }[];
    d: { time: string; value: number }[];
  };
  bollinger_bands?: {
    upper: { time: string; value: number }[];
    middle: { time: string; value: number }[];
    lower: { time: string; value: number }[];
  };
  chart_patterns?: Array<{
    type: string;
    bias: "bullish" | "bearish" | "neutral";
    neckline?: number;
    target?: number;
    [k: string]: unknown;
  }>;
  harmonic_patterns?: Array<{
    type: string;
    bias: "bullish" | "bearish";
    [k: string]: unknown;
  }>;
  candle_patterns?: Array<{
    time: string;
    type: string;
    bias: "bullish" | "bearish" | "neutral";
    strength: number;
    description: string;
    price_high: number;
    price_low: number;
  }>;
  support_resistance?: Array<{
    price: number;
    touches: number;
    role: "support" | "resistance";
    strength: number;
    last_touch_time: string | null;
  }>;
  accumulation?: {
    phase: "ACCUMULATION" | "DISTRIBUTION" | "CONSOLIDATION";
    bias: "bullish" | "bearish" | "neutral";
    confidence: "LOW" | "MEDIUM" | "HIGH";
    range_high: number;
    range_low: number;
    range_pct: number;
    target_up: number | null;
    target_down: number | null;
    volume_ratio: number;
    support_tests: number;
    resistance_tests: number;
    bars_inside: number;
    lookback: number;
    pre_trend_pct: number;
    summary: string;
  } | null;
  premium_discount?: {
    range_high: number;
    range_low: number;
    equilibrium: number;
    extreme_premium: number;
    extreme_discount: number;
    current_zone:
      | "premium"
      | "discount"
      | "equilibrium"
      | "extreme_premium"
      | "extreme_discount";
    current_pct: number;
    bias_action: string;
  } | null;
  bos_zones?: {
    bullish_trigger?: { price: number; from_idx: number; label: string };
    bearish_trigger?: { price: number; from_idx: number; label: string };
  } | null;
  vsa_events?: Array<{
    idx: number; time: string; type: string; bias: "bullish" | "bearish" | "neutral";
    strength: number; description: string; high?: number; low?: number;
  }>;
  obv?: { current: number; trend: "rising" | "falling"; divergence: "bullish" | "bearish" | null;
          series: { time: string; value: number }[]; impact?: string } | null;
  mfi?: { current: number; signal: "overbought" | "oversold" | "neutral";
          overbought_threshold: number; oversold_threshold: number;
          series: { time: string; value: number }[]; impact?: string } | null;
  ichimoku?: {
    tenkan: number | null; kijun: number | null;
    senkou_a: number | null; senkou_b: number | null;
    signal: string; tk_cross: string | null;
    series: Array<{ time: string; tenkan: number | null; kijun: number | null;
                    senkou_a: number | null; senkou_b: number | null }>;
    impact?: string;
  } | null;
  wyckoff_events?: Array<{
    idx: number; time: string; type: string; bias: "bullish" | "bearish";
    strength: number; description: string;
  }>;
  order_flow?: {
    volume_profile: {
      poc: number;
      vah: number;
      val: number;
      hvn: number[];
      lvn: number[];
      bins: { price: number; volume: number; pct: number }[];
    } | null;
    vwap: {
      value: number;
      upper_1sd: number;
      lower_1sd: number;
      upper_2sd: number;
      lower_2sd: number;
      anchor_time: string;
      series: {
        time: string;
        vwap: number;
        upper_1sd: number;
        lower_1sd: number;
        upper_2sd: number;
        lower_2sd: number;
      }[];
    } | null;
    volume_delta: {
      last_delta: number;
      last_cum: number;
      delta_5d: number;
      delta_20d: number;
      series: { time: string; delta: number; cumulative: number; color: string }[];
    } | null;
    absorption: {
      absorbed: boolean;
      strength: number;
      vol_ratio: number;
      lower_wick_ratio: number;
      upper_wick_ratio: number;
      close_strength: number;
      body_pct: number;
    } | null;
    orderbook_imbalance: {
      imbalance: number;
      imbalance_pct: number;
      bid_size: number;
      ask_size: number;
      verdict: string;
      snapshots: number;
    } | null;
  } | null;
}

export type SMCCandlePattern = NonNullable<SMCChartData["candle_patterns"]>[number];

const smcCache = new Map<string, { data: SMCChartData; ts: number }>();
// 15s — short enough to feel live, long enough to prevent burst requests when
// toggling indicators. Backend caches for 60s so most fetches are server-cached.
const SMC_CACHE_TTL = 15_000;

export interface FetchSMCChartOptions {
  /** Bypass the in-memory cache and re-fetch from server. */
  force?: boolean;
  /** AbortSignal so callers can cancel in-flight requests. */
  signal?: AbortSignal;
}

export interface TimeAndSalesTick {
  ts: string;
  price: number;
  size: number;
  side: "B" | "S" | "?";
  best_bid: number | null;
  best_ask: number | null;
}

export interface TimeAndSalesResponse {
  symbol: string;
  count: number;
  ticks: TimeAndSalesTick[];
}

export interface SmartMoneyStock {
  symbol: string;
  trades: number;
  buy_vol: number;
  sell_vol: number;
  net_delta: number;
  buy_pct: number;
  buy_sell_ratio: number;
  high_px: number | null;
  low_px: number | null;
  last_trade: string | null;
}

export interface SmartMoneyResponse {
  side: "buy" | "sell" | "all";
  since_minutes: number;
  min_trades: number;
  count: number;
  stocks: SmartMoneyStock[];
}

export async function fetchSmartMoneyRadar(
  side: "buy" | "sell" | "all" = "buy",
  sinceMinutes: number = 240,
  minTrades: number = 5,
  limit: number = 50,
): Promise<SmartMoneyResponse> {
  const { data } = await api.get<SmartMoneyResponse>("/market/smart-money-radar", {
    params: { side, since_minutes: sinceMinutes, min_trades: minTrades, limit },
  });
  return data;
}


export async function fetchTimeAndSales(
  symbol: string,
  limit: number = 100,
  sinceMinutes: number = 240,
): Promise<TimeAndSalesResponse> {
  const { data } = await api.get<TimeAndSalesResponse>(
    `/stock/${symbol.toUpperCase()}/time-and-sales`,
    { params: { limit, since_minutes: sinceMinutes } },
  );
  return data;
}


export async function fetchSMCChart(
  symbol: string,
  period: "1m" | "3m" | "6m" | "1y" | "2y" | "3y" | "5y" = "5y",
  timeframe: "daily" | "weekly" = "daily",
  options: FetchSMCChartOptions = {},
): Promise<SMCChartData> {
  const key = `${symbol.toUpperCase()}_${period}_${timeframe}`;
  if (!options.force) {
    const hit = smcCache.get(key);
    if (hit && Date.now() - hit.ts < SMC_CACHE_TTL) return hit.data;
  }

  const { data } = await api.get<SMCChartData>(
    `/stock/${symbol.toUpperCase()}/smc-chart`,
    {
      params: { period, interval: timeframe },
      signal: options.signal,
    },
  );
  smcCache.set(key, { data, ts: Date.now() });
  return data;
}

export async function fetchSignalsSummary(): Promise<SignalsSummary> {
  const { data } = await api.get<SignalsSummary>("/signals/summary");
  return data;
}

export interface DSEXBar {
  date: string;
  dsex: number;
  volume: number;
  trade: number;
}

export async function fetchDSEXHistory(): Promise<DSEXBar[]> {
  const { data } = await api.get<DSEXBar[]>("/market/index-history");
  return data;
}

export async function fetchTopMovers(
  type: "gainers" | "losers" = "gainers",
  limit = 20,
): Promise<StockPrice[]> {
  const { data } = await api.get<StockPrice[]>("/market/movers", {
    params: { type, limit },
  });
  return data;
}

/* ========================== Stock Detail ========================== */

export async function fetchStockSignal(
  symbol: string,
): Promise<StockSignal> {
  const { data } = await api.get<StockSignal>(`/signals/${symbol}`);
  return data;
}

export async function fetchStockPrice(
  symbol: string,
): Promise<StockPrice> {
  const { data } = await api.get<StockPrice>(`/stock/${symbol}`);
  return data;
}

export async function fetchOHLCV(
  symbol: string,
  period = "3m",
): Promise<OHLCVBar[]> {
  const { data } = await api.get<OHLCVBar[]>(`/stock/${symbol}/history`, {
    params: { period },
  });
  return data;
}

export interface PeerStock {
  symbol: string;
  ltp: number;
  change_pct: number;
  volume: number;
  value: number;
  company_name?: string;
}

export async function fetchStockPeers(
  symbol: string,
): Promise<{ sector: string | null; peers: PeerStock[] }> {
  const { data } = await api.get<{ sector: string | null; peers: PeerStock[] }>(
    `/stock/${symbol}/peers`,
  );
  return data;
}

/* ========================== Screener ========================== */

export interface ScreenerParams {
  signal_type?: string;
  rsi_min?: number;
  rsi_max?: number;
  price_min?: number;
  price_max?: number;
  sort_by?: string;
  limit?: number;
  t2_safe?: boolean;
  min_expected_return?: number;
  max_risk_score?: number;
  trend?: string;
  max_hold_days?: number;
}

export async function fetchScreener(
  params: ScreenerParams,
): Promise<ScreenerResult> {
  const { data } = await api.get<ScreenerResult>("/screener", { params });
  return data;
}

/* ========================== Watchlist ========================== */

export async function fetchWatchlist(): Promise<WatchlistItem[]> {
  const { data } = await api.get<WatchlistItem[]>("/watchlist");
  return data;
}

export async function addToWatchlist(
  symbol: string,
  notes?: string,
): Promise<void> {
  await api.post("/watchlist", { symbol, notes });
}

export async function removeFromWatchlist(symbol: string): Promise<void> {
  await api.delete(`/watchlist/${symbol}`);
}

/* ========================== Portfolio ========================== */

export async function fetchHoldings(): Promise<Holding[]> {
  const { data } = await api.get<Holding[]>("/portfolio");
  return data;
}

export async function addHolding(holding: {
  symbol: string;
  quantity: number;
  buy_price: number;
  buy_date: string;
  notes?: string;
}): Promise<{ id: number; maturity_date: string }> {
  const { data } = await api.post("/portfolio", holding);
  return data;
}

export async function sellHolding(
  holdingId: number,
  sell: { sell_price: number; sell_date: string; quantity: number },
): Promise<void> {
  await api.post(`/portfolio/${holdingId}/sell`, sell);
}

export async function fetchPortfolioSummary(): Promise<PortfolioSummary> {
  const { data } = await api.get<PortfolioSummary>("/portfolio/summary");
  return data;
}

export async function fetchPortfolioAlerts(): Promise<PortfolioAlert[]> {
  const { data } = await api.get<PortfolioAlert[]>("/portfolio/alerts");
  return data;
}

export async function deleteHolding(holdingId: number): Promise<void> {
  await api.delete(`/portfolio/${holdingId}`);
}

/* ========================== Signal History & Accuracy ========================== */

export interface SignalHistoryEntry {
  id: number;
  symbol: string;
  date: string;
  signal_type: string;
  ltp: number;
  target_price: number;
  stop_loss: number;
  confidence: number;
  short_term_score: number;
  predicted_day2: number | null;
  predicted_day7: number | null;
  expected_return_pct: number;
  actual_day2: number | null;
  actual_day7: number | null;
  target_hit: number;
  stop_hit: number;
  actual_return_pct: number | null;
  reasoning: string;
}

export interface SignalAccuracy {
  total_verified: number;
  correct_direction?: number;
  accuracy_pct?: number;
  by_signal_type?: {
    signal_type: string;
    count: number;
    avg_return: number;
    targets_hit: number;
    stops_hit: number;
    profitable: number;
  }[];
  best_calls?: { symbol: string; date: string; signal_type: string; actual_return_pct: number }[];
  worst_calls?: { symbol: string; date: string; signal_type: string; actual_return_pct: number }[];
  recent_daily?: { date: string; signals: number; avg_return: number; targets_hit: number }[];
  message?: string;
}

export async function fetchSignalHistory(
  symbol: string,
  limit = 30,
): Promise<SignalHistoryEntry[]> {
  const { data } = await api.get<SignalHistoryEntry[]>(
    `/signals/history/${symbol}`,
    { params: { limit } },
  );
  return data;
}

export async function fetchSignalAccuracy(): Promise<SignalAccuracy> {
  const { data } = await api.get<SignalAccuracy>("/signals/accuracy");
  return data;
}

/* ========================== Suggestions ========================== */

export async function fetchSuggestions(): Promise<Suggestions> {
  const { data } = await api.get<Suggestions>("/signals/suggestions");
  return data;
}

/* ========================== Sectors & Heatmap ========================== */

export async function fetchSectorPerformance(): Promise<SectorPerformance[]> {
  const { data } = await api.get<SectorPerformance[]>("/market/sectors");
  return data;
}

export async function fetchHeatmapData(
  sizeBy: "turnover" | "volume" | "trades" = "turnover",
): Promise<HeatmapSector[]> {
  const { data } = await api.get<HeatmapSector[]>("/market/heatmap", {
    params: { size_by: sizeBy },
  });
  return data;
}

export async function fetchSectorDetail(
  sectorName: string,
): Promise<{ sector: string; stocks: StockPrice[] }> {
  const { data } = await api.get<{ sector: string; stocks: StockPrice[] }>(
    `/market/sectors/${encodeURIComponent(sectorName)}`,
  );
  return data;
}

/* ========================== Daily Analysis ========================== */

export async function fetchDailyAnalysis(
  date?: string,
  action?: string,
): Promise<DailyAnalysisResponse> {
  const params: Record<string, string> = {};
  if (date) params.date = date;
  if (action) params.action = action;
  const { data } = await api.get<DailyAnalysisResponse>("/analysis/daily", { params });
  return data;
}

export async function fetchAnalysisDates(): Promise<{ dates: string[] }> {
  const { data } = await api.get<{ dates: string[] }>("/analysis/dates");
  return data;
}

export async function fetchAnalysisSummary(
  date?: string,
): Promise<AnalysisSummaryResponse> {
  const params = date ? { date } : undefined;
  const { data } = await api.get<AnalysisSummaryResponse>("/analysis/summary", { params });
  return data;
}

export async function triggerAnalysis(): Promise<{ status: string; message?: string }> {
  const { data } = await api.post<{ status: string; message?: string }>("/analysis/trigger");
  return data;
}

export async function fetchAnalysisStatus(): Promise<{ running: boolean }> {
  const { data } = await api.get<{ running: boolean }>("/analysis/status");
  return data;
}

export function getAnalysisExcelUrl(date?: string): string {
  const base = "/api/v1/analysis/excel";
  return date ? `${base}?date=${date}` : base;
}

export async function fetchLiveTracker(
  date?: string,
): Promise<import("../types/index.ts").LiveTrackerResponse> {
  const params: Record<string, string> = {};
  if (date) params.date = date;
  const { data } = await api.get("/analysis/live-tracker", { params });
  return data;
}

/* ========================== Live Scan ========================== */

export async function fetchLiveScan(): Promise<LiveScanResponse> {
  const { data } = await api.get<LiveScanResponse>("/analysis/live-scan");
  return data;
}

export function getLiveScanExcelUrl(date?: string): string {
  const base = "/api/v1/analysis/live-scan/excel";
  return date ? `${base}?date=${date}` : base;
}

export async function triggerLiveScan(): Promise<{ status: string; message?: string }> {
  const { data } = await api.post<{ status: string; message?: string }>("/analysis/live-scan/trigger");
  return data;
}

/* ========================== LLM Scan ========================== */

export async function fetchLLMScan(date?: string): Promise<LLMScanResponse> {
  const params = date ? { date } : undefined;
  const { data } = await api.get<LLMScanResponse>("/analysis/llm-scan", { params });
  return data;
}

/* ========================== Predictions & LLM Analysis ========================== */

export async function fetchLLMDailyAnalysis(
  date?: string,
  action?: string,
  symbol?: string,
): Promise<{ date: string; count: number; analysis: import("../types").LLMDailyAnalysis[]; message?: string }> {
  const params: Record<string, string> = {};
  if (date) params.date = date;
  if (action) params.action = action;
  if (symbol) params.symbol = symbol;
  const { data } = await api.get("/predictions/llm-analysis", { params });
  return data;
}

export async function fetchJudgeAnalysis(
  date?: string,
  disagreementOnly = false,
): Promise<{
  date: string;
  count: number;
  agreements: number;
  disagreements: number;
  agreement_pct: number;
  verdicts: import("../types").JudgeAnalysis[];
}> {
  const params: Record<string, string | boolean> = {};
  if (date) params.date = date;
  if (disagreementOnly) params.disagreement_only = true;
  const { data } = await api.get("/predictions/judge-analysis", { params });
  return data;
}

export async function fetchPredictionTracker(params?: {
  date?: string;
  symbol?: string;
  source?: string;
  outcome?: string;
  limit?: number;
}): Promise<{ count: number; predictions: import("../types").PredictionEntry[] }> {
  const { data } = await api.get("/predictions/tracker", { params });
  return data;
}

export async function fetchAccuracyComparison(
  period = "30d",
): Promise<{ period: string; date: string | null; data: import("../types").AccuracyData[]; message?: string }> {
  const { data } = await api.get("/predictions/accuracy", { params: { period } });
  return data;
}

export async function fetchAccuracyHistory(
  days = 30,
  source?: string,
): Promise<{ days: number; history: import("../types").AccuracyData[] }> {
  const params: Record<string, string | number> = { days };
  if (source) params.source = source;
  const { data } = await api.get("/predictions/accuracy/history", { params });
  return data;
}

export async function fetchStockPredictionHistory(
  symbol: string,
  limit = 30,
): Promise<{
  symbol: string;
  count: number;
  by_date: Record<string, Record<string, import("../types").PredictionEntry>>;
  raw: import("../types").PredictionEntry[];
}> {
  const { data } = await api.get(`/predictions/stock/${symbol}`, { params: { limit } });
  return data;
}

export async function fetchBuyRadar(categories = "A"): Promise<BuyRadarResponse> {
  const { data } = await api.get<BuyRadarResponse>("/analysis/buy-radar", {
    params: { categories },
    timeout: 90_000,
  });
  return data;
}

/* ========================== News & Events ========================== */

export interface NewsItem {
  id: number;
  category: string;
  title: string;
  url: string;
  date: string;
  source: string;
  content?: string;
  symbols_mentioned?: string[];
  impact?: string;        // HIGH, MEDIUM, LOW, NOISE
  sentiment?: string;     // BULLISH, BEARISH, NEUTRAL, MIXED
  market_impact?: string; // STOCK_SPECIFIC, SECTOR_WIDE, DSEX_MOVING, MACRO, DIVIDEND, NOISE
  affected_symbols?: string[];
  summary?: string;
}

export interface CorporateEvent {
  id: number;
  symbol: string;
  date: string;
  event_type: string;
  title: string;
  details: string;
  source: string;
}

export interface UpcomingDividend {
  symbol: string;
  record_date: string;
  dividend_type: string;
  cash_pct: number;
  stock_pct: number;
  year: string;
}

export interface MarketHoliday {
  id: number;
  date: string;
  name: string;
}

export async function fetchMarketNews(params?: {
  category?: string;
  impact?: string;
  page?: number;
  per_page?: number;
}): Promise<{ items: NewsItem[]; total: number; page: number; per_page: number; categories: string[]; impact_levels?: string[] }> {
  const { data } = await api.get("/events/news", { params });
  return data;
}

export async function fetchCorporateEvents(params?: {
  symbol?: string;
  event_type?: string;
  days?: number;
  page?: number;
  per_page?: number;
}): Promise<{ items: CorporateEvent[]; total: number; page: number; per_page: number }> {
  const { data } = await api.get("/events", { params });
  return data;
}

export async function fetchStockEvents(symbol: string): Promise<{ symbol: string; events: CorporateEvent[] }> {
  const { data } = await api.get(`/events/stock/${symbol}`);
  return data;
}

export async function fetchUpcomingDividends(): Promise<{ upcoming: UpcomingDividend[] }> {
  const { data } = await api.get("/events/dividends/upcoming");
  return data;
}

export async function fetchDividendCalendar(month?: string): Promise<{ month: string; events: Record<string, UpcomingDividend[]> }> {
  const { data } = await api.get("/events/dividends/calendar", { params: month ? { month } : undefined });
  return data;
}

export async function fetchMarketHolidays(): Promise<{ holidays: MarketHoliday[] }> {
  const { data } = await api.get("/events/holidays");
  return data;
}

/* ========================== Seasonality ========================== */

export interface MonthData {
  month: number;
  avg_return: number;
  median_return: number;
  trimmed_mean: number;
  win_rate: number;
  sample_size: number;
  bootstrap_p: number;
  cohens_d: number;
  volatility: number;
}

export interface SectorSeasonality {
  name: string;
  months: MonthData[];
}

export interface StockSeasonality {
  symbol: string;
  sector: string;
  months: {
    month: number; avg_return: number; up_pct: number; years_up: number; years_total: number;
    median_return: number; trimmed_mean: number; bootstrap_p: number; cohens_d: number;
    best_return: number; worst_return: number; volatility: number;
  }[];
}

export interface WeekPerformance {
  week_start: string;
  week_end: string;
  dsex_return: number;
  sectors: { name: string; return_pct: number; top_stocks: string[] }[];
}

export interface SeasonalOutlook {
  month: number;
  month_name: string;
  top_sectors: { sector: string; avg_return: number; median_return: number; trimmed_mean: number; win_rate: number; sample_size: number; bootstrap_p: number; cohens_d: number; volatility: number }[];
  bottom_sectors: { sector: string; avg_return: number; median_return: number; trimmed_mean: number; win_rate: number; sample_size: number; bootstrap_p: number; cohens_d: number; volatility: number }[];
  top_stocks: { symbol: string; avg_return: number; median_return: number; trimmed_mean: number; win_rate: number; sample_size: number; sector: string; bootstrap_p: number; cohens_d: number; volatility: number }[];
  bottom_stocks: { symbol: string; avg_return: number; median_return: number; trimmed_mean: number; win_rate: number; sample_size: number; sector: string; bootstrap_p: number; cohens_d: number; volatility: number }[];
  yearly: { year: number; avg_return: number; stocks_up: number; stocks_down: number; total_stocks: number }[];
}

export async function fetchMonthlySectorSeasonality(year?: number): Promise<{ sectors: SectorSeasonality[]; years: number[] }> {
  const params: Record<string, number> = {};
  if (year) params.year = year;
  const { data } = await api.get("/seasonality/monthly/sectors", { params, timeout: 30000 });
  return data;
}

export async function fetchMonthlyStockSeasonality(
  category = "A", year?: number, sector?: string
): Promise<{ stocks: StockSeasonality[]; sectors: string[]; years: number[] }> {
  const params: Record<string, string | number> = { category };
  if (year) params.year = year;
  if (sector) params.sector = sector;
  const { data } = await api.get("/seasonality/monthly/stocks", { params, timeout: 30000 });
  return data;
}

export interface SectorYearlyDetail {
  sectors: Record<string, Record<string, Record<string, number>>>;  // sector -> year -> month -> return
  years: number[];
}

export async function fetchSectorYearlyDetail(): Promise<SectorYearlyDetail> {
  const { data } = await api.get("/seasonality/monthly/sectors/yearly", { timeout: 30000 });
  return data;
}

export interface StockYearlyDetail {
  stocks: Record<string, Record<string, Record<string, number>>>;  // symbol -> year -> month -> return
  years: number[];
}

export async function fetchStockYearlyDetail(category = "A"): Promise<StockYearlyDetail> {
  const { data } = await api.get("/seasonality/monthly/stocks/yearly", { params: { category }, timeout: 30000 });
  return data;
}

export async function fetchWeeklyPerformance(weeks = 12): Promise<{ weeks: WeekPerformance[] }> {
  const { data } = await api.get("/seasonality/weekly", { params: { weeks }, timeout: 30000 });
  return data;
}

export async function fetchSeasonalOutlook(month?: number): Promise<SeasonalOutlook> {
  const params = month ? { month } : {};
  const { data } = await api.get("/seasonality/outlook", { params, timeout: 30000 });
  return data;
}

export interface RecordDateImpact {
  symbol: string;
  events: {
    record_date: string;
    dividend_pct: number;
    pre_close: number;
    ex_close: number;
    ex_drop_pct: number;
    expected_drop_pct: number;
    excess_drop_pct: number | null;
    bottom_day: number;
    bottom_price: number;
    bottom_drop_pct: number;
    day_7_pct: number | null;
    day_14_pct: number | null;
    day_20_pct: number | null;
  }[];
  averages: {
    avg_ex_drop_pct: number | null;
    avg_bottom_day: number | null;
    avg_bottom_drop_pct: number | null;
    avg_day_7_pct: number | null;
    avg_day_14_pct: number | null;
    avg_day_20_pct: number | null;
    event_count: number;
  };
}

export async function fetchRecordDateImpact(symbol: string): Promise<RecordDateImpact> {
  const { data } = await api.get(`/dividends/impact/${symbol}`);
  return data;
}

export interface PostDividendOpportunity {
  symbol: string;
  record_date: string;
  days_since: number;
  drop_pct: number;
  expected_drop: number;
  excess_drop: number;
  current_price: number;
  volume_ratio: number;
  rsi: number;
}

export async function fetchPostDividendOpportunities(days = 7): Promise<{ opportunities: PostDividendOpportunity[] }> {
  const { data } = await api.get("/dividends/opportunities", { params: { days } });
  return data;
}

export interface UpcomingRecordDate {
  symbol: string;
  record_date: string;
  days_until: number;
  current_price: number | null;
  expected_ex_price: number | null;
  dividend_pct: number;
  title: string;
  avg_historical_ex_drop_pct: number | null;
  avg_historical_bottom_day: number | null;
  historical_events: number;
}

export async function fetchUpcomingRecordDates(days = 30): Promise<{ upcoming: UpcomingRecordDate[] }> {
  const { data } = await api.get("/dividends/upcoming", { params: { days }, timeout: 30000 });
  return data;
}

/* ========================== Floor Detection ========================== */

export interface FloorStock {
  symbol: string;
  sector: string | null;
  ltp: number;
  rsi: number;
  stoch_rsi: number;
  macd_hist: number;
  rsi_floor: number;
  stoch_floor: number;
  macd_floor: number;
  rsi_high: number;
  stoch_high: number;
  rsi_proximity: number;
  stoch_proximity: number;
  rsi_pace: number;
  stoch_pace: number;
  macd_pace: number;
  rsi_days_to_floor: number | null;
  stoch_days_to_floor: number | null;
  macd_days_to_floor: number | null;
  rsi_approaching: boolean;
  stoch_approaching: boolean;
  macd_approaching: boolean;
  approaching_count: number;
  score: number;
}

export async function fetchFloorTable(
  months = 6, asOf?: string
): Promise<{ stocks: FloorStock[]; lookback_months: number; as_of: string | null }> {
  const params: Record<string, string | number> = { months };
  if (asOf) params.as_of = asOf;
  const { data } = await api.get("/floor", { params, timeout: 30000 });
  return data;
}

export async function fetchFloorDates(): Promise<{ dates: string[] }> {
  const { data } = await api.get("/floor/dates", { timeout: 15000 });
  return data;
}

/* ========================== Chat ========================== */

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

export interface ChatResponse {
  session_id: string;
  response: string;
  history: ChatMessage[];
}

// Chat calls GCP VM directly (bypasses Render's 30s timeout)
const CHAT_URL = import.meta.env.VITE_CHAT_URL || "https://34.126.128.18.nip.io";

export async function sendChatMessage(
  message: string,
  sessionId?: string,
  userEmail?: string,
): Promise<ChatResponse> {
  const res = await fetch(`${CHAT_URL}/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message, session_id: sessionId, user_email: userEmail }),
    signal: AbortSignal.timeout(300_000),
  });
  if (!res.ok) throw new Error(`Chat error: ${res.status}`);
  return res.json();
}

export async function clearChatSession(sessionId: string): Promise<void> {
  await fetch(`${CHAT_URL}/chat/sessions/${sessionId}`, { method: "DELETE" }).catch(() => {});
}

/* ========================== AI Analysis (V2) ========================== */

export interface AIStock {
  symbol: string;
  date: string;
  overall_signal: "BUY" | "SELL" | "HOLD" | "WATCH" | "AVOID";
  signal_strength: string | null;
  confidence: string | null;
  classification: string | null;
  position_type: string | null;
  score_overall: number | null;
  score_money_flow: number | null;
  score_momentum: number | null;
  score_price_action: number | null;
  score_volatility: number | null;
  score_fundamentals: number | null;
  score_news: number | null;
  one_liner: string;
  entry_low: number | null;
  entry_high: number | null;
  stop_loss: number | null;
  stop_loss_method: string | null;
  target_1: number | null;
  target_2: number | null;
  for_new_buyer: string;
  for_holder: string;
  ltp: number | null;
  change_pct: number | null;
  volume: number | null;
  sector: string | null;
  category: string | null;
  eps_ttm: number | null;
  pe_ratio: number | null;
  dividend_yield_pct: number | null;
  high_52w: number | null;
  low_52w: number | null;
  rsi_14: number | null;
  cmf_20: number | null;
  cmf_pos_streak: number | null;
  cmf_neg_streak: number | null;
  adx_14: number | null;
  macd_hist: number | null;
  ma_aligned: boolean | null;
  atr_pct: number | null;
  vol_ratio: number | null;
  chg_5d: number | null;
  chg_20d: number | null;
  support: number | null;
  resistance: number | null;
}

export interface AIMarket {
  dsex: number | null;
  dsex_change: number | null;
  dsex_change_pct: number | null;
  advances: number;
  declines: number;
  unchanged: number;
  turnover_cr: number;
  market_status: string;
  regime: string | null;
  ai_summary: string | null;
  is_good_day_to_buy: boolean | null;
  signal_distribution: Record<string, number>;
  dsex_history: { date: string; dsex: number }[];
}

export async function fetchAIStocks(signal?: string): Promise<{ stocks: AIStock[]; count: number }> {
  const params = signal ? { signal } : undefined;
  const { data } = await api.get<{ stocks: AIStock[]; count: number }>("/ai/stocks", { params });
  return data;
}

export async function fetchAIStockDetail(symbol: string): Promise<Record<string, unknown>> {
  const { data } = await api.get(`/ai/stocks/${symbol}`);
  return data;
}

export async function fetchAIMarket(): Promise<AIMarket> {
  const { data } = await api.get<AIMarket>("/ai/market");
  return data;
}

export interface LiveSignal {
  symbol: string;
  category: string | null;
  ltp: number;
  open: number;
  prev_close: number;
  change_pct: number;
  high: number;
  low: number;
  volume: number;
  gap_type: "GAP_UP" | "GAP_DOWN" | "FLAT";
  gap_pct: number;
  body: "BULLISH" | "BEARISH" | "DOJI";
  shadow_signal: string | null;
  vol_ratio: number;
  vol_signal: "VERY_HIGH" | "HIGH" | "NORMAL" | "LOW";
  momentum: string;
  pivot_p: number | null;
  pivot_r1: number | null;
  pivot_s1: number | null;
  pivot_position: string | null;
  swing_structure: string | null;
  yesterday_candle: string | null;
  rsi: number | null;
  cmf: number | null;
  mean_reversion_score: number | null;
}

export async function fetchLiveSignals(): Promise<{ signals: LiveSignal[]; count: number }> {
  const { data } = await api.get<{ signals: LiveSignal[]; count: number }>("/ai/live-signals");
  return data;
}

export interface BuySetup {
  symbol: string;
  ltp: number;
  change_pct: number | null;
  volume: number | null;
  rsi: number | null;
  cmf: number | null;
  chg_5d: number | null;
  chg_20d: number | null;
  support: { price: number; touches: number; strength: string } | null;
  resistance: { price: number; touches: number } | null;
  pivot_r1: number | null;
  pivot_s1: number | null;
  candle: string | null;
  candle_confirmed: boolean;
  mr_score: number | null;
  sector: string | null;
  category: string | null;
  swing: string | null;
  setup: string;
  win_rate: number;
  note: string;
  setups_matched?: string[];
}

export interface BuySetupsResponse {
  setups: {
    support_oversold: BuySetup[];
    rsi_extreme: BuySetup[];
    mean_reversion: BuySetup[];
    obv_divergence: BuySetup[];
    squeeze_forming: BuySetup[];
    multi_setup: BuySetup[];
  };
  total: number;
}

export async function fetchBuySetups(): Promise<BuySetupsResponse> {
  const { data } = await api.get<BuySetupsResponse>("/ai/buy-setups", { timeout: 30000 });
  return data;
}

export interface LiveAlert {
  id: number;
  symbol: string;
  time: string;
  alert_type: string;
  severity: string;
  price: number;
  level_name: string | null;
  level_price: number | null;
  message: string;
  extra: Record<string, unknown> | null;
}

export async function fetchLiveAlerts(symbol?: string): Promise<{ alerts: LiveAlert[]; count: number }> {
  const params = symbol ? { symbol } : undefined;
  const { data } = await api.get<{ alerts: LiveAlert[]; count: number }>("/ai/alerts", { params });
  return data;
}

export async function fetchStockSummary(symbol: string): Promise<{ symbol: string; summary: string; sections: Record<string, string>; data: Record<string, unknown> }> {
  const { data } = await api.get(`/ai/summary/${symbol}`);
  return data;
}

export default api;
