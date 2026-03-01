import { useCallback, useEffect, useRef, useState } from "react";

const INTERVAL_MS = 300_000; // 5 minutes

interface UseAutoRefreshOptions {
  /** Async function to call on each refresh cycle. */
  fetchFn: () => Promise<void>;
  /** Interval in ms between refreshes. Defaults to 300 000 (5 min). */
  intervalMs?: number;
  /** Whether to fetch immediately on mount/enable. Defaults to true. */
  immediate?: boolean;
  /** Whether polling is active. When false, the timer is paused and no fetches occur. Defaults to true. */
  enabled?: boolean;
}

interface UseAutoRefreshReturn {
  /** Seconds remaining until the next automatic refresh. */
  secondsToRefresh: number;
  /** Trigger an immediate refresh and reset the timer. */
  refresh: () => void;
  /** True while the fetch function is executing. */
  isRefreshing: boolean;
}

/**
 * Custom hook that polls an async function on a fixed interval, shows a
 * countdown to the next refresh, and pauses while the browser tab is hidden
 * or when `enabled` is false.
 */
export function useAutoRefresh({
  fetchFn,
  intervalMs = INTERVAL_MS,
  immediate = true,
  enabled = true,
}: UseAutoRefreshOptions): UseAutoRefreshReturn {
  const intervalSecs = Math.floor(intervalMs / 1000);
  const [secondsToRefresh, setSecondsToRefresh] = useState(intervalSecs);
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Refs so the interval callbacks always see the latest values without
  // needing the values in the dependency arrays.
  const fetchRef = useRef(fetchFn);
  fetchRef.current = fetchFn;

  const remainingRef = useRef(intervalSecs);
  const pausedAtRef = useRef<number | null>(null);
  const tickRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const enabledRef = useRef(enabled);
  enabledRef.current = enabled;

  /* ---- helper: clear the ticker ---- */
  const clearTick = useCallback(() => {
    if (tickRef.current) {
      clearInterval(tickRef.current);
      tickRef.current = null;
    }
  }, []);

  /* ---- execute one refresh cycle ---- */
  const doRefresh = useCallback(async () => {
    setIsRefreshing(true);
    try {
      await fetchRef.current();
    } catch (err) {
      console.error("[useAutoRefresh] fetch failed", err);
    } finally {
      setIsRefreshing(false);
      // Reset countdown
      remainingRef.current = intervalSecs;
      setSecondsToRefresh(intervalSecs);
    }
  }, [intervalSecs]);

  // Wire doRefresh into startTick (they reference each other)
  const startTickWithRefresh = useCallback(() => {
    clearTick();
    tickRef.current = setInterval(() => {
      if (!enabledRef.current) return;
      remainingRef.current -= 1;
      if (remainingRef.current <= 0) {
        void doRefresh();
      } else {
        setSecondsToRefresh(remainingRef.current);
      }
    }, 1000);
  }, [clearTick, doRefresh]);

  /* ---- manual refresh callback exposed to consumers ---- */
  const refresh = useCallback(() => {
    void doRefresh();
  }, [doRefresh]);

  /* ---- start/stop ticker based on enabled ---- */
  useEffect(() => {
    if (enabled) {
      // Fetch immediately if requested
      if (immediate) {
        void doRefresh();
      }
      startTickWithRefresh();
    } else {
      // Disabled — stop the ticker and reset countdown display
      clearTick();
      remainingRef.current = intervalSecs;
      setSecondsToRefresh(intervalSecs);
    }

    return clearTick;
  }, [enabled, immediate, doRefresh, startTickWithRefresh, clearTick, intervalSecs]);

  /* ---- pause / resume on visibility change ---- */
  useEffect(() => {
    const onVisibility = () => {
      if (!enabledRef.current) return;

      if (document.hidden) {
        // Pause: record remaining seconds and clear ticker
        pausedAtRef.current = remainingRef.current;
        clearTick();
      } else {
        // Resume: if we were paused, decide whether to refresh or continue
        if (pausedAtRef.current !== null) {
          if (pausedAtRef.current <= 0) {
            void doRefresh();
          }
          pausedAtRef.current = null;
          startTickWithRefresh();
        }
      }
    };

    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [doRefresh, startTickWithRefresh, clearTick]);

  return { secondsToRefresh, refresh, isRefreshing };
}
