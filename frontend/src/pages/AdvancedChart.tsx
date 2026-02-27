import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import SymbolSearch from "../components/search/SymbolSearch.tsx";

const DEFAULT_SYMBOL = "ROBI";

export default function AdvancedChart() {
  const containerRef = useRef<HTMLDivElement>(null);
  const [searchParams, setSearchParams] = useSearchParams();
  const symbolParam = searchParams.get("symbol");
  const [currentSymbol, setCurrentSymbol] = useState(
    symbolParam || DEFAULT_SYMBOL,
  );

  const handleSymbolSelect = (symbol: string) => {
    setCurrentSymbol(symbol);
    setSearchParams({ symbol });
  };

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    // Clear previous widget
    container.innerHTML = "";

    // Build the TradingView Advanced Chart widget embed
    const widgetDiv = document.createElement("div");
    widgetDiv.className = "tradingview-widget-container__widget";
    widgetDiv.style.height = "100%";
    widgetDiv.style.width = "100%";
    container.appendChild(widgetDiv);

    const script = document.createElement("script");
    script.src =
      "https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js";
    script.async = true;
    script.type = "text/javascript";
    script.textContent = JSON.stringify({
      autosize: true,
      symbol: `DSEBD:${currentSymbol}`,
      interval: "D",
      timezone: "Asia/Dhaka",
      theme: "dark",
      style: "1",
      locale: "en",
      backgroundColor: "rgba(15, 23, 42, 1)",
      gridColor: "rgba(42, 46, 57, 0.3)",
      allow_symbol_change: true,
      hide_top_toolbar: false,
      hide_legend: false,
      save_image: true,
      calendar: false,
      studies: ["RSI@tv-basicstudies", "MACD@tv-basicstudies"],
      support_host: "https://www.tradingview.com",
    });
    container.appendChild(script);

    return () => {
      container.innerHTML = "";
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
        ref={containerRef}
        className="tradingview-widget-container bg-[var(--surface)] border border-[var(--border)] rounded-lg overflow-hidden"
        style={{ height: "calc(100vh - 140px)", minHeight: "500px" }}
      />
    </div>
  );
}
