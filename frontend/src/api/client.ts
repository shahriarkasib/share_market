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
} from "../types/index.ts";

const api = axios.create({
  baseURL: "/api/v1",
  timeout: 60_000,
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

export default api;
