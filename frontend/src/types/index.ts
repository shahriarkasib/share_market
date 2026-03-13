export interface StockPrice {
  symbol: string;
  company_name?: string;
  ltp: number;
  change: number;
  change_pct: number;
  open: number;
  high: number;
  low: number;
  close_prev: number;
  volume: number;
  value: number;
  trade_count: number;
}

export interface StockSignal {
  symbol: string;
  company_name?: string;
  action?: string;
  ltp: number;
  change_pct: number;
  signal_type: 'STRONG_BUY' | 'BUY' | 'HOLD' | 'SELL' | 'STRONG_SELL';
  confidence: number;
  short_term_score: number;
  long_term_score: number;
  target_price: number;
  stop_loss: number;
  risk_reward_ratio: number;
  reasoning: string;
  timing: 'BUY_NOW' | 'WAIT_FOR_DIP' | 'ACCUMULATE' | 'SELL_NOW' | 'HOLD_TIGHT';
  indicators: {
    rsi?: number | null;
    macd_signal?: string;
    bb_position?: string;
    ema_crossover?: string;
    volume_signal?: string;
    momentum_3d?: number | null;
    stoch_k?: number | null;
  };
  created_at?: string;
  // Prediction fields
  predicted_prices?: { day_2: number; day_3: number; day_4: number; day_5: number; day_6: number; day_7: number };
  expected_return_pct?: number;
  hold_days?: number;
  entry_strategy?: string;
  exit_strategy?: string;
  support_level?: number;
  resistance_level?: number;
  trend_strength?: 'STRONG_UP' | 'UP' | 'SIDEWAYS' | 'DOWN' | 'STRONG_DOWN';
  volatility_level?: 'LOW' | 'MEDIUM' | 'HIGH';
  t2_safe?: boolean;
  price_range_next_3d?: { min: number; max: number };
  daily_ranges?: Record<string, { min: number; max: number }>;
  risk_score?: number;
  t2_maturity_date?: string;
}

export interface MarketSummary {
  dsex_index: number;
  dsex_change: number;
  dsex_change_pct: number;
  total_volume: number;
  total_value: number;
  total_trade: number;
  advances: number;
  declines: number;
  unchanged: number;
  market_status: string;
  last_updated?: string;
}

export interface SignalsSummary {
  total_stocks: number;
  strong_buy_count: number;
  buy_count: number;
  hold_count: number;
  sell_count: number;
  strong_sell_count: number;
  market_sentiment: string;
  last_updated?: string;
  is_computing?: boolean;
}

export interface OHLCVBar {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface ScreenerResult {
  stocks: StockSignal[];
  total_count: number;
  filters_applied: Record<string, unknown>;
}

export interface WatchlistItem {
  id: number;
  symbol: string;
  added_at: string;
  notes?: string;
}

export interface Holding {
  id: number;
  symbol: string;
  quantity: number;
  remaining_quantity: number;
  buy_price: number;
  buy_date: string;
  maturity_date: string;
  is_mature: boolean;
  current_price: number;
  unrealized_pnl: number;
  unrealized_pnl_pct: number;
  sell_recommendation: string;
  signal_type?: string;
  status: string;
  notes?: string;
}

export interface PortfolioSummary {
  total_invested: number;
  current_value: number;
  total_pnl: number;
  total_pnl_pct: number;
  active_holdings: number;
  mature_holdings: number;
  at_risk_holdings: number;
}

export interface PortfolioAlert {
  symbol: string;
  alert_type: 'MATURITY' | 'STOP_LOSS' | 'TARGET_REACHED' | 'SIGNAL_CHANGE';
  message: string;
  urgency: 'HIGH' | 'MEDIUM' | 'LOW';
  holding_id: number;
}

export interface ExitAlert {
  holding: Holding;
  signal?: StockSignal;
  reasons: string[];
  pnl_pct: number;
}

export interface Suggestions {
  entry: StockSignal[];
  exit: ExitAlert[];
}

export interface SectorPerformance {
  sector: string;
  stock_count: number;
  advances: number;
  declines: number;
  unchanged: number;
  avg_change_pct: number;
  total_turnover: number;
  total_volume: number;
  total_trades: number;
  top_gainer?: { symbol: string; change_pct: number };
  top_loser?: { symbol: string; change_pct: number };
}

export interface HeatmapSector {
  sector: string;
  stocks: { symbol: string; change_pct: number; size_value: number; ltp: number; volume: number }[];
  total_size: number;
  avg_change_pct: number;
}

/* ========================== Daily Analysis ========================== */

export interface DailyAnalysis {
  symbol: string;
  action: string;
  reasoning: string;
  ltp: number;
  entry_low: number;
  entry_high: number;
  sl: number;
  t1: number;
  t2: number;
  risk_pct: number;
  reward_pct: number;
  rsi: number;
  stoch_rsi: number;
  macd_line: number;
  macd_signal: number;
  macd_hist: number;
  macd_status: string;
  bb_pct: number;
  atr: number;
  atr_pct: number;
  volatility: number;
  max_dd: number;
  support: number;
  resistance: number;
  trend_50d: number;
  avg_vol: number;
  vol_ratio: number;
  wait_days: string;
  vol_entry: string;
  entry_start?: string;
  entry_end?: string;
  exit_t1_by?: string;
  exit_t2_by?: string;
  hold_days_t1?: number;
  hold_days_t2?: number;
  scenarios_json: string;
  last_5_json: string;
  score: number;
  sector?: string;
  category?: string;
}

export interface DailyAnalysisResponse {
  date: string;
  count: number;
  summary: Record<string, number>;
  analysis: DailyAnalysis[];
  message?: string;
}

export interface LiveTrackerStock {
  symbol: string;
  action: string;
  category?: string;
  sector?: string;
  score: number;
  entry_low: number;
  entry_high: number;
  sl: number;
  t1: number;
  t2: number;
  entry_start?: string;
  entry_end?: string;
  exit_t1_by?: string;
  exit_t2_by?: string;
  hold_days_t1?: number;
  hold_days_t2?: number;
  reasoning: string;
  rsi: number;
  stoch_rsi: number;
  macd_status: string;
  risk_pct: number;
  reward_pct: number;
  live_ltp: number;
  live_change_pct: number;
  live_volume: number;
  live_high: number;
  live_low: number;
  status: "ENTRY_ZONE" | "APPROACHING" | "BELOW_ENTRY" | "T1_HIT" | "T2_HIT" | "SL_HIT" | "WATCHING";
  distance_pct: number;
}

export interface LiveTrackerResponse {
  date: string;
  market_status: string;
  updated_at: string | null;
  count: number;
  stocks: LiveTrackerStock[];
}

export interface AnalysisSummaryResponse {
  date: string;
  total: number;
  by_action: Record<string, number>;
}

/* ========================== Live Scan ========================== */

export interface LiveScanResult {
  timestamp: string;
  symbol: string;
  action: string;
  category: string;
  sector: string;
  score: number;
  live_ltp: number;
  live_change_pct: number;
  live_volume: number;
  entry_low: number;
  entry_high: number;
  sl: number;
  t1: number;
  t2: number;
  status: string;
  distance_pct: number;
  total_buy_vol: number;
  total_sell_vol: number;
  buy_sell_ratio: number;
  best_bid: number;
  best_ask: number;
  spread_pct: number;
  buy_levels: number;
  sell_levels: number;
  recommendation: string;
  reasoning: string;
  rsi: number;
  macd_status: string;
  t2_risk: string;
  t2_risk_reason: string;
}

export interface LLMStockPick {
  symbol: string;
  recommendation: string;
  confidence: string;
  reasoning: string;
  entry_strategy: string;
  risk_note: string;
}

export interface LLMMarketOutlook {
  sentiment: string;
  summary: string;
  key_insights: {
    sector_insights?: string;
    timing_advice?: string;
    stocks_to_avoid?: { symbol: string; reason: string }[];
  };
  key_risks: string[];
}

export interface LLMScanResponse {
  date: string;
  scan_time: string | null;
  scan_count?: number;
  market_outlook: LLMMarketOutlook | null;
  top_picks: LLMStockPick[];
  message?: string;
}

export interface LiveScanResponse {
  timestamp: string | null;
  date?: string;
  results: LiveScanResult[];
  summary: Record<string, number>;
  total: number;
  excel_path?: string;
  message?: string;
}

/* ========================== LLM Daily Analysis ========================== */

export interface LLMDailyAnalysis {
  symbol: string;
  sector?: string;
  action: string;
  confidence: string;
  reasoning: string;
  wait_for: string;
  wait_days: string;
  entry_low: number;
  entry_high: number;
  sl: number;
  t1: number;
  t2: number;
  risk_factors: string[];
  catalysts: string[];
  score: number;
  // LLM plan fields
  how_to_buy?: string;
  volume_rule?: string;
  next_day_plan?: string;
  sell_plan?: string;
  // Judge fields (joined)
  algo_action?: string;
  llm_action?: string;
  final_action?: string;
  final_confidence?: string;
  agreement?: boolean;
  judge_reasoning?: string;
  algo_strengths?: string;
  llm_strengths?: string;
  key_risk?: string;
}

export interface JudgeAnalysis {
  symbol: string;
  sector?: string;
  algo_action: string;
  llm_action: string;
  final_action: string;
  final_confidence: string;
  agreement: boolean;
  reasoning: string;
  algo_strengths: string;
  llm_strengths: string;
  key_risk: string;
  wait_days: string;
  score: number;
}

/* ========================== Prediction Tracker ========================== */

export interface PredictionEntry {
  date: string;
  symbol: string;
  sector?: string;
  source: "algo" | "llm" | "judge";
  action: string;
  score: number;
  wait_days: string;
  wait_days_min: number;
  wait_days_max: number;
  ltp_at_prediction: number;
  entry_low: number;
  entry_high: number;
  sl: number;
  t1: number;
  t2: number;
  transitioned_to: string | null;
  transition_date: string | null;
  transition_days: number | null;
  transition_within_window: boolean | null;
  t1_hit_date: string | null;
  t1_hit_days: number | null;
  t2_hit_date: string | null;
  t2_hit_days: number | null;
  sl_hit_date: string | null;
  sl_hit_days: number | null;
  max_gain_pct: number | null;
  max_loss_pct: number | null;
  final_return_pct: number | null;
  outcome: "CORRECT" | "WRONG" | "PARTIAL" | "PENDING";
  outcome_reason: string | null;
}

export interface AccuracyData {
  date: string;
  source: string;
  period: string;
  total_predictions: number;
  correct: number;
  wrong: number;
  pending: number;
  accuracy_pct: number | null;
  avg_return_pct: number | null;
  buy_accuracy_pct: number | null;
  hold_transition_accuracy_pct: number | null;
  t1_hit_rate: number | null;
  sl_hit_rate: number | null;
}

export interface IndicatorReadiness {
  value: number;
  readiness: number;
}

export interface BuyRadarStock {
  symbol: string;
  price: number;
  sector: string;
  category: string;
  stage: "ENTRY_ZONE" | "READY" | "APPROACHING" | "BUILDING" | "WATCHING";
  overall_readiness: number;
  ready_count: number;
  ret_5d: number;
  volume: number;
  vol_ratio: number;
  indicators: {
    rsi: IndicatorReadiness;
    mfi: IndicatorReadiness;
    cmf: IndicatorReadiness;
    macd: IndicatorReadiness;
    stoch_rsi: IndicatorReadiness;
    bb_pct: IndicatorReadiness;
  };
  // Layer scores (0-100)
  layers: {
    leading: number;
    confirming: number;
    money_flow: number;
    positioning: number;
    ai_verdict: number;
  };
  signals: string[];
  red_flags: string[];
  entry_low: number | null;
  entry_high: number | null;
  sl: number | null;
  t1: number | null;
  t2: number | null;
  action: string;
  score: number | null;
  // AI context
  ai_action: string;
  ai_confidence: string;
  ai_reasoning: string;
  ai_how_to_buy: string;
  ai_key_risk: string;
  ai_wait_for: string;
  ai_catalysts: string[];
  ai_risk_factors: string[];
  ai_signals: string[];
  stage_reasoning: string;
  // Profit estimation
  expected_return_1w?: number;
  expected_return_2w?: number;
  expected_return_1m?: number;
  downside_risk?: number;
  // DSEX analysis
  dsex_dependency?: string;
  if_dsex_drops?: string;
  if_dsex_rises?: string;
  dsex_outlook?: string;
  // Tracking fields
  days_on_radar: number;
  first_seen: string;
  entry_price: number;
  price_change_pct: number;
  stage_history: string[];
  trend: "IMPROVING" | "STABLE" | "DETERIORATING";
  is_new: boolean;
}

export interface RemovedRadarStock {
  symbol: string;
  last_stage: string;
  last_price: number;
  last_readiness: number;
  reason: string;
  removed_date: string;
  days_tracked: number;
}

export interface MarketContext {
  regime: "OVERSOLD" | "WEAK" | "NEUTRAL" | "HEATED" | "OVERBOUGHT";
  dsex: number;
  dsex_rsi: number;
  dsex_change: number;
  adjustment: number;
  // Volume analysis
  total_value_cr: number;
  total_volume: number;
  total_trades: number;
  advances: number;
  declines: number;
  volume_verdict: "VERY_LOW" | "LOW" | "NORMAL" | "HIGH" | "VERY_HIGH";
  breadth_pct: number;
  signal: string;
}

export interface DsexDailyPrediction {
  day: number;
  direction: string;
  range_low: number;
  range_high: number;
  reasoning: string;
}

export interface DsexForecast {
  forecast: string;
  sentiment: string;
  support: number;
  resistance: number;
  expected_direction: string;
  confidence: string;
  key_factors: string;
  scenario_bull: string;
  scenario_bear: string;
  scenario_base: string;
  daily_predictions?: DsexDailyPrediction[];
}

export interface BuyRadarResponse {
  date: string;
  count: number;
  stages: Record<string, number>;
  market_ctx: MarketContext;
  dsex_forecast?: DsexForecast;
  stocks: BuyRadarStock[];
  removed: RemovedRadarStock[];
}
