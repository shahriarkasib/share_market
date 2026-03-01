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
 * Backend keeps all caches warm (refreshed every 5 min), so API calls
 * always return instantly — no localStorage caching needed.
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

    set((state) => ({
      marketSummary: val(results[0], state.marketSummary),
      topBuySignals: val(results[1], state.topBuySignals),
      topSellSignals: val(results[2], state.topSellSignals),
      allPrices: val(results[3], state.allPrices),
      signalsSummary: val(results[4], state.signalsSummary),
      suggestions: val(results[5], state.suggestions),
      dsexHistory: val(results[6], state.dsexHistory),
      isLoading: false,
      lastUpdated: new Date(),
      error: failed > 0 ? `${failed} data source(s) unavailable` : null,
    }));
  },
}));
