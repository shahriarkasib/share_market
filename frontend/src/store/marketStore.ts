import { create } from "zustand";
import type {
  MarketSummary,
  StockSignal,
  StockPrice,
  SignalsSummary,
  Suggestions,
} from "../types/index.ts";
import {
  fetchMarketSummary,
  fetchTopBuySignals,
  fetchTopSellSignals,
  fetchAllPrices,
  fetchSignalsSummary,
  fetchSuggestions,
  fetchDSEXHistory,
  type DSEXBar,
} from "../api/client.ts";

/* ---- localStorage cache helpers ---- */
const CACHE_KEY = "dse_dashboard_cache";
const CACHE_MAX_AGE = 5 * 60 * 1000; // 5 minutes — keep fresh, DSEX updates often

interface CachedData {
  marketSummary: MarketSummary | null;
  topBuySignals: StockSignal[];
  topSellSignals: StockSignal[];
  allPrices: StockPrice[];
  signalsSummary: SignalsSummary | null;
  suggestions: Suggestions | null;
  dsexHistory: DSEXBar[];
  savedAt: number;
}

function loadCache(): Partial<CachedData> | null {
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    if (!raw) return null;
    const cached: CachedData = JSON.parse(raw);
    if (Date.now() - cached.savedAt > CACHE_MAX_AGE) {
      localStorage.removeItem(CACHE_KEY);
      return null;
    }
    return cached;
  } catch {
    return null;
  }
}

function saveCache(data: Omit<CachedData, "savedAt">) {
  try {
    localStorage.setItem(CACHE_KEY, JSON.stringify({ ...data, savedAt: Date.now() }));
  } catch {
    // Storage full or unavailable — ignore
  }
}

const cached = loadCache();

/* ---- Store ---- */

interface MarketState {
  /* ---- data ---- */
  marketSummary: MarketSummary | null;
  topBuySignals: StockSignal[];
  topSellSignals: StockSignal[];
  allPrices: StockPrice[];
  signalsSummary: SignalsSummary | null;
  suggestions: Suggestions | null;
  dsexHistory: DSEXBar[];

  /* ---- meta ---- */
  isLoading: boolean;
  error: string | null;
  lastUpdated: Date | null;

  /* ---- actions ---- */
  fetchDashboard: () => Promise<void>;
}

/**
 * Global Zustand store for market-wide data used on the Dashboard.
 * Initializes from localStorage cache for instant display, then
 * fetches fresh data from the API in the background.
 */
export const useMarketStore = create<MarketState>((set) => ({
  // Initialize from cache if available — shows data instantly on hard refresh
  marketSummary: cached?.marketSummary ?? null,
  topBuySignals: cached?.topBuySignals ?? [],
  topSellSignals: cached?.topSellSignals ?? [],
  allPrices: cached?.allPrices ?? [],
  signalsSummary: cached?.signalsSummary ?? null,
  suggestions: cached?.suggestions ?? null,
  dsexHistory: cached?.dsexHistory ?? [],
  isLoading: false,
  error: null,
  lastUpdated: null,

  fetchDashboard: async () => {
    set({ isLoading: true, error: null });

    // Use allSettled so partial data still shows — no all-or-nothing
    const results = await Promise.allSettled([
      fetchMarketSummary(),
      fetchTopBuySignals(10),
      fetchTopSellSignals(10),
      fetchAllPrices(),
      fetchSignalsSummary(),
      fetchSuggestions(),
      fetchDSEXHistory(),
    ]);

    const val = <T,>(r: PromiseSettledResult<T>, fallback: T): T =>
      r.status === "fulfilled" ? r.value : fallback;

    const failed = results.filter((r) => r.status === "rejected").length;

    set((state) => {
      const newState = {
        marketSummary: val(results[0], state.marketSummary),
        topBuySignals: val(results[1], state.topBuySignals),
        topSellSignals: val(results[2], state.topSellSignals),
        allPrices: val(results[3], state.allPrices),
        signalsSummary: val(results[4], state.signalsSummary),
        suggestions: val(results[5], state.suggestions),
        dsexHistory: val(results[6], state.dsexHistory),
      };

      // Persist to localStorage for instant load on next visit
      saveCache(newState);

      return {
        ...newState,
        isLoading: false,
        lastUpdated: new Date(),
        error: failed > 0 ? `${failed} data source(s) unavailable` : null,
      };
    });
  },
}));
