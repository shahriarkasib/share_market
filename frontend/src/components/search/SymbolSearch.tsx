import { useState, useRef, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { Search } from "lucide-react";
import { clsx } from "clsx";
import { useMarketStore } from "../../store/marketStore.ts";
import { fetchAllPrices } from "../../api/client.ts";
import { formatNumber, colorBySign } from "../../lib/format.ts";
import type { StockPrice } from "../../types/index.ts";

interface Props {
  /** Called when a symbol is selected. */
  onSelect?: (symbol: string) => void;
  /** Navigate to /stock/{symbol} on select (default true). */
  navigateOnSelect?: boolean;
  placeholder?: string;
  /** Smaller styling for inline forms. */
  compact?: boolean;
  className?: string;
}

export default function SymbolSearch({
  onSelect,
  navigateOnSelect = true,
  placeholder = "Search ticker...",
  compact = false,
  className,
}: Props) {
  const navigate = useNavigate();
  const [query, setQuery] = useState("");
  const [matches, setMatches] = useState<StockPrice[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [activeIdx, setActiveIdx] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>();

  // Get prices from global store (already fetched on Dashboard load)
  const allPrices = useMarketStore((s) => s.allPrices);
  const [localPrices, setLocalPrices] = useState<StockPrice[]>([]);

  // Use store prices if available, otherwise fetch on-demand
  const prices = allPrices.length > 0 ? allPrices : localPrices;

  useEffect(() => {
    if (allPrices.length === 0 && localPrices.length === 0) {
      fetchAllPrices()
        .then(setLocalPrices)
        .catch(() => {});
    }
  }, [allPrices.length, localPrices.length]);

  // Filter logic
  const doFilter = useCallback(
    (q: string) => {
      if (!q.trim() || prices.length === 0) {
        setMatches([]);
        setIsOpen(false);
        return;
      }
      const upper = q.toUpperCase().trim();
      const filtered = prices
        .filter(
          (p) =>
            p.symbol.toUpperCase().includes(upper) ||
            (p.company_name && p.company_name.toUpperCase().includes(upper)),
        )
        .slice(0, 8);
      setMatches(filtered);
      setIsOpen(filtered.length > 0);
      setActiveIdx(-1);
    },
    [prices],
  );

  const handleInputChange = (value: string) => {
    setQuery(value);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => doFilter(value), 100);
  };

  const selectSymbol = (symbol: string) => {
    setQuery("");
    setMatches([]);
    setIsOpen(false);
    onSelect?.(symbol);
    if (navigateOnSelect) {
      navigate(`/stock/${symbol}`);
    }
  };

  // Keyboard navigation
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!isOpen) return;

    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActiveIdx((i) => (i < matches.length - 1 ? i + 1 : 0));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIdx((i) => (i > 0 ? i - 1 : matches.length - 1));
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (activeIdx >= 0 && activeIdx < matches.length) {
        selectSymbol(matches[activeIdx].symbol);
      }
    } else if (e.key === "Escape") {
      setIsOpen(false);
      setActiveIdx(-1);
    }
  };

  // Click outside to close
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (
        containerRef.current &&
        !containerRef.current.contains(e.target as Node)
      ) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  return (
    <div ref={containerRef} className={clsx("relative", className)}>
      <div className="relative">
        <Search
          className={clsx(
            "absolute left-2.5 top-1/2 -translate-y-1/2 text-[var(--text-dim)]",
            compact ? "h-3 w-3" : "h-3.5 w-3.5",
          )}
        />
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={(e) => handleInputChange(e.target.value)}
          onFocus={() => query.trim() && doFilter(query)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          className={clsx(
            "w-full bg-[var(--bg)] border border-[var(--border)] rounded-md text-[var(--text)] placeholder-[var(--text-dim)] focus:outline-none focus:border-blue-500 transition-colors",
            compact ? "pl-7 pr-2 py-1.5 text-xs" : "pl-8 pr-3 py-1.5 text-xs",
          )}
        />
      </div>

      {/* Dropdown */}
      {isOpen && matches.length > 0 && (
        <div className="absolute z-50 top-full mt-1 w-full min-w-[280px] max-h-[320px] overflow-y-auto bg-[var(--surface)] border border-[var(--border)] rounded-lg shadow-lg">
          {matches.map((p, idx) => (
            <button
              key={p.symbol}
              type="button"
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => selectSymbol(p.symbol)}
              onMouseEnter={() => setActiveIdx(idx)}
              className={clsx(
                "w-full flex items-center gap-2 px-3 py-2 text-left transition-colors",
                idx === activeIdx
                  ? "bg-[var(--surface-active)]"
                  : "hover:bg-[var(--hover)]",
              )}
            >
              <span className="text-xs font-semibold text-[var(--text)] w-20 shrink-0 tabular-nums">
                {p.symbol}
              </span>
              <span className="text-[11px] text-[var(--text-muted)] truncate flex-1">
                {p.company_name || ""}
              </span>
              <span className="text-[11px] text-[var(--text)] tabular-nums shrink-0">
                {formatNumber(p.ltp)}
              </span>
              {p.change_pct != null && (
                <span
                  className={clsx(
                    "text-[10px] font-medium tabular-nums shrink-0 w-14 text-right",
                    colorBySign(p.change_pct),
                  )}
                >
                  {p.change_pct >= 0 ? "+" : ""}
                  {p.change_pct.toFixed(2)}%
                </span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
