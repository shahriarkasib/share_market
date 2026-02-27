import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import SymbolSearch from "../components/search/SymbolSearch.tsx";

declare global {
  interface Window {
    TradingView?: {
      widget: new (config: Record<string, unknown>) => unknown;
    };
  }
}

const DEFAULT_SYMBOL = "DSE:ROBI";

export default function AdvancedChart() {
  const containerRef = useRef<HTMLDivElement>(null);
  const [searchParams, setSearchParams] = useSearchParams();
  const symbolParam = searchParams.get("symbol");
  const [currentSymbol, setCurrentSymbol] = useState(
    symbolParam ? `DSE:${symbolParam}` : DEFAULT_SYMBOL,
  );

  const handleSymbolSelect = (symbol: string) => {
    setCurrentSymbol(`DSE:${symbol}`);
    setSearchParams({ symbol });
  };

  useEffect(() => {
    if (!containerRef.current) return;

    // Clear previous widget
    containerRef.current.innerHTML = "";

    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/tv.js";
    script.async = true;
    script.onload = () => {
      if (window.TradingView && containerRef.current) {
        new window.TradingView.widget({
          container_id: containerRef.current.id,
          symbol: currentSymbol,
          interval: "D",
          timezone: "Asia/Dhaka",
          theme: "dark",
          style: "1", // Candlestick
          locale: "en",
          toolbar_bg: "#0f172a",
          enable_publishing: false,
          allow_symbol_change: true,
          hide_side_toolbar: false,
          studies: ["RSI@tv-basicstudies", "MACD@tv-basicstudies"],
          width: "100%",
          height: "100%",
          save_image: true,
          details: true,
          hotlist: true,
          calendar: false,
          show_popup_button: true,
          popup_width: "1000",
          popup_height: "650",
        });
      }
    };
    document.head.appendChild(script);

    return () => {
      try {
        document.head.removeChild(script);
      } catch {
        /* already removed */
      }
    };
  }, [currentSymbol]);

  return (
    <div className="space-y-3">
      {/* Header with symbol search */}
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <h1 className="text-sm font-bold text-[var(--text)]">
          Advanced Chart
          <span className="ml-2 text-xs font-normal text-[var(--text-muted)]">
            Powered by TradingView
          </span>
        </h1>
        <div className="w-full sm:w-64">
          <SymbolSearch
            placeholder="Change symbol..."
            compact
            navigateOnSelect={false}
            onSelect={handleSymbolSelect}
          />
        </div>
      </div>

      {/* TradingView chart container */}
      <div
        className="bg-[var(--surface)] border border-[var(--border)] rounded-lg overflow-hidden"
        style={{ height: "calc(100vh - 140px)", minHeight: "500px" }}
      >
        <div id="tradingview-chart" ref={containerRef} className="w-full h-full" />
      </div>
    </div>
  );
}
