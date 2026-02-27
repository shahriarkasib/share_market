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
 * `fetchDashboard` fires all API calls in parallel, stores the
 * results, and records the timestamp.
 */
export const useMarketStore = create<MarketState>((set) => ({
  marketSummary: null,
  topBuySignals: [],
  topSellSignals: [],
  allPrices: [],
  signalsSummary: null,
  suggestions: null,
  dsexHistory: [],
  isLoading: false,
  error: null,
  lastUpdated: null,

  fetchDashboard: async () => {
    set({ isLoading: true, error: null });
    try {
      const [
        marketSummary,
        topBuySignals,
        topSellSignals,
        allPrices,
        signalsSummary,
        suggestions,
        dsexHistory,
      ] = await Promise.all([
        fetchMarketSummary(),
        fetchTopBuySignals(10),
        fetchTopSellSignals(10),
        fetchAllPrices(),
        fetchSignalsSummary(),
        fetchSuggestions(),
        fetchDSEXHistory(),
      ]);

      set({
        marketSummary,
        topBuySignals,
        topSellSignals,
        allPrices,
        signalsSummary,
        suggestions,
        dsexHistory,
        isLoading: false,
        lastUpdated: new Date(),
      });
    } catch (err) {
      const message =
        err instanceof Error ? err.message : "Failed to fetch dashboard data";
      set({ error: message, isLoading: false });
    }
  },
}));
