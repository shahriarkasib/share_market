import { useCallback, useEffect, useRef, useState } from "react";

const INTERVAL_MS = 300_000; // 5 minutes

interface UseAutoRefreshOptions {
  /** Async function to call on each refresh cycle. */
  fetchFn: () => Promise<void>;
  /** Interval in ms between refreshes. Defaults to 300 000 (5 min). */
  intervalMs?: number;
  /** Whether to fetch immediately on mount. Defaults to true. */
  immediate?: boolean;
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
 * countdown to the next refresh, and pauses while the browser tab is hidden.
 */
export function useAutoRefresh({
  fetchFn,
  intervalMs = INTERVAL_MS,
  immediate = true,
}: UseAutoRefreshOptions): UseAutoRefreshReturn {
  const [secondsToRefresh, setSecondsToRefresh] = useState(
    Math.floor(intervalMs / 1000),
  );
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Refs so the interval callbacks always see the latest values without
  // needing the values in the dependency arrays.
  const fetchRef = useRef(fetchFn);
  fetchRef.current = fetchFn;

  const remainingRef = useRef(Math.floor(intervalMs / 1000));
  const pausedAtRef = useRef<number | null>(null);
  const tickRef = useRef<ReturnType<typeof setInterval> | null>(null);

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
      const secs = Math.floor(intervalMs / 1000);
      remainingRef.current = secs;
      setSecondsToRefresh(secs);
    }
  }, [intervalMs]);

  /* ---- manual refresh callback exposed to consumers ---- */
  const refresh = useCallback(() => {
    void doRefresh();
  }, [doRefresh]);

  /* ---- countdown ticker (1 s) ---- */
  useEffect(() => {
    // Fetch once on mount if requested
    if (immediate) {
      void doRefresh();
    }

    tickRef.current = setInterval(() => {
      remainingRef.current -= 1;
      if (remainingRef.current <= 0) {
        void doRefresh();
      } else {
        setSecondsToRefresh(remainingRef.current);
      }
    }, 1000);

    return () => {
      if (tickRef.current) clearInterval(tickRef.current);
    };
  }, [doRefresh, immediate]);

  /* ---- pause / resume on visibility change ---- */
  useEffect(() => {
    const onVisibility = () => {
      if (document.hidden) {
        // Pause: record remaining seconds and clear ticker
        pausedAtRef.current = remainingRef.current;
        if (tickRef.current) {
          clearInterval(tickRef.current);
          tickRef.current = null;
        }
      } else {
        // Resume: if we were paused, decide whether to refresh or continue
        if (pausedAtRef.current !== null) {
          // If we would have already refreshed while hidden, do it now
          if (pausedAtRef.current <= 0) {
            void doRefresh();
          }
          pausedAtRef.current = null;
          // Restart the 1-s ticker
          tickRef.current = setInterval(() => {
            remainingRef.current -= 1;
            if (remainingRef.current <= 0) {
              void doRefresh();
            } else {
              setSecondsToRefresh(remainingRef.current);
            }
          }, 1000);
        }
      }
    };

    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [doRefresh]);

  return { secondsToRefresh, refresh, isRefreshing };
}
