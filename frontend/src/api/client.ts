import axios from "axios";
import type {
  MarketSummary,
  StockSignal,
  StockPrice,
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
  timeout: 15_000,
  headers: { "Content-Type": "application/json" },
});

// Keep Render backend warm — ping every 10 minutes to prevent cold starts
setInterval(() => {
  fetch("/api/v1/market/summary", { method: "HEAD" }).catch(() => {});
}, 10 * 60 * 1000);

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
  const { data } = await api.get("/dividends/upcoming", { params: { days } });
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

export default api;
