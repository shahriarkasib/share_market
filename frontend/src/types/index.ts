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
