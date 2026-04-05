import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { clsx } from "clsx";
import {
  Loader2,
  TrendingDown,
  ChevronDown,
  ChevronUp,
  AlertTriangle,
  Eye,
  Ban,
  ShoppingCart,
  Briefcase,
} from "lucide-react";
import { fetchAIStocks, fetchAIMarket } from "../api/client.ts";
import type { AIStock, AIMarket } from "../api/client.ts";

const SIGNAL_TABS = [
  { key: "ALL", label: "All", icon: Eye },
  { key: "BUY", label: "Buy", icon: ShoppingCart },
  { key: "HOLD", label: "Hold", icon: Briefcase },
  { key: "SELL", label: "Sell", icon: TrendingDown },
  { key: "WATCH", label: "Watch", icon: Eye },
  { key: "AVOID", label: "Avoid", icon: Ban },
] as const;

const SIGNAL_COLORS: Record<string, string> = {
  BUY: "text-emerald-400 bg-emerald-400/10 border-emerald-400/30",
  SELL: "text-red-400 bg-red-400/10 border-red-400/30",
  HOLD: "text-amber-400 bg-amber-400/10 border-amber-400/30",
  WATCH: "text-blue-400 bg-blue-400/10 border-blue-400/30",
  AVOID: "text-gray-400 bg-gray-400/10 border-gray-400/30",
};

const CONFIDENCE_COLORS: Record<string, string> = {
  HIGH: "text-emerald-400",
  MEDIUM: "text-amber-400",
  LOW: "text-red-400",
};

function formatNum(v: number | null, d = 1): string {
  if (v == null) return "–";
  return v.toFixed(d);
}

function formatPct(v: number | null): string {
  if (v == null) return "–";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(1)}%`;
}

function CmfBadge({ value, streak }: { value: number | null; streak: number }) {
  if (value == null) return <span className="text-[var(--text-dim)]">–</span>;
  const positive = value > 0;
  return (
    <span className={clsx("text-xs font-mono", positive ? "text-emerald-400" : "text-red-400")}>
      {value > 0 ? "+" : ""}{value.toFixed(3)}
      {streak > 0 && <span className="text-[var(--text-dim)] ml-1">({streak}d)</span>}
    </span>
  );
}

function SignalBadge({ signal, strength }: { signal: string; strength?: string | null }) {
  const color = SIGNAL_COLORS[signal] || SIGNAL_COLORS.WATCH;
  return (
    <span className={clsx("px-2 py-0.5 rounded text-xs font-bold border", color)}>
      {signal}{strength && strength !== "MEDIUM" ? ` (${strength.toLowerCase()})` : ""}
    </span>
  );
}

/* ── Market Header ── */
function MarketHeader({ market }: { market: AIMarket | null }) {
  if (!market) return null;
  const dsexDown = (market.dsex_change ?? 0) < 0;
  const dist = market.signal_distribution || {};

  return (
    <div className="mb-4 p-3 rounded-lg bg-[var(--surface)] border border-[var(--border)]">
      <div className="flex flex-wrap items-center gap-4 text-sm">
        <div>
          <span className="text-[var(--text-muted)]">DSEX </span>
          <span className="font-bold text-lg">{market.dsex?.toFixed(2)}</span>
          <span className={clsx("ml-2 font-mono", dsexDown ? "text-red-400" : "text-emerald-400")}>
            {market.dsex_change && market.dsex_change > 0 ? "+" : ""}{market.dsex_change?.toFixed(2)}
            {" "}({market.dsex_change_pct && market.dsex_change_pct > 0 ? "+" : ""}{market.dsex_change_pct?.toFixed(2)}%)
          </span>
        </div>
        <div className="text-[var(--text-dim)]">
          <span className="text-emerald-400">{market.advances}</span>
          {" / "}
          <span className="text-red-400">{market.declines}</span>
          {" / "}
          <span>{market.unchanged}</span>
        </div>
        <div className="text-[var(--text-dim)]">
          Turnover: {market.turnover_cr} Cr
        </div>
        {market.market_status && (
          <span className={clsx(
            "px-2 py-0.5 rounded text-xs font-bold",
            market.market_status === "OPEN" ? "bg-emerald-400/10 text-emerald-400" : "bg-gray-400/10 text-gray-400"
          )}>
            {market.market_status}
          </span>
        )}
      </div>
      {Object.keys(dist).length > 0 && (
        <div className="mt-2 flex gap-3 text-xs">
          {Object.entries(dist).map(([sig, cnt]) => (
            <span key={sig} className={clsx("font-mono", SIGNAL_COLORS[sig]?.split(" ")[0] || "text-gray-400")}>
              {sig}: {cnt}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

/* ── Stock Card ── */
function StockCard({ stock, onClick }: { stock: AIStock; onClick: () => void }) {
  const [expanded, setExpanded] = useState(false);
  const chgDown = (stock.change_pct ?? 0) < 0;
  const cmfPositive = (stock.cmf_pos_streak ?? 0) > 0;

  return (
    <div className="rounded-lg border border-[var(--border)] bg-[var(--surface)] overflow-hidden">
      {/* Header row */}
      <div
        className="p-3 cursor-pointer hover:bg-[var(--hover)] transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 min-w-0">
            <button
              onClick={(e) => { e.stopPropagation(); onClick(); }}
              className="font-bold text-sm hover:text-blue-400 transition-colors"
            >
              {stock.symbol}
            </button>
            <span className="text-xs text-[var(--text-dim)]">{stock.category}</span>
            <SignalBadge signal={stock.overall_signal} strength={stock.signal_strength} />
            {stock.confidence && (
              <span className={clsx("text-xs", CONFIDENCE_COLORS[stock.confidence] || "")}>
                {stock.confidence}
              </span>
            )}
          </div>
          <div className="flex items-center gap-3 text-sm shrink-0">
            <span className="font-mono font-bold">৳{formatNum(stock.ltp)}</span>
            <span className={clsx("font-mono text-xs", chgDown ? "text-red-400" : "text-emerald-400")}>
              {formatPct(stock.change_pct)}
            </span>
            {expanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
          </div>
        </div>

        {/* One liner */}
        {stock.one_liner && (
          <p className="mt-1 text-xs text-[var(--text-muted)] line-clamp-2">{stock.one_liner}</p>
        )}

        {/* Key metrics row */}
        <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-[var(--text-dim)]">
          {stock.score_overall != null && (
            <span>Score: <span className="text-[var(--text)] font-mono">{stock.score_overall}</span></span>
          )}
          {stock.rsi_14 != null && (
            <span>RSI: <span className={clsx("font-mono", stock.rsi_14 < 35 ? "text-emerald-400" : stock.rsi_14 > 65 ? "text-red-400" : "text-[var(--text)]")}>{formatNum(stock.rsi_14)}</span></span>
          )}
          <span>CMF: <CmfBadge value={stock.cmf_20} streak={cmfPositive ? stock.cmf_pos_streak! : stock.cmf_neg_streak!} /></span>
          {stock.adx_14 != null && (
            <span>ADX: <span className={clsx("font-mono", stock.adx_14 < 15 ? "text-red-400" : stock.adx_14 > 25 ? "text-emerald-400" : "text-amber-400")}>{formatNum(stock.adx_14)}</span></span>
          )}
          {stock.position_type && (
            <span className="text-[var(--text-muted)]">{stock.position_type}</span>
          )}
        </div>
      </div>

      {/* Expanded content */}
      {expanded && (
        <div className="border-t border-[var(--border)] p-3 space-y-3 text-xs">
          {/* Action advice */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {stock.for_new_buyer && (
              <div>
                <div className="text-[var(--text-muted)] mb-1 font-semibold">For New Buyer:</div>
                <p className="text-[var(--text)]">{stock.for_new_buyer}</p>
              </div>
            )}
            {stock.for_holder && (
              <div>
                <div className="text-[var(--text-muted)] mb-1 font-semibold">For Holder:</div>
                <p className="text-[var(--text)]">{stock.for_holder}</p>
              </div>
            )}
          </div>

          {/* Price levels */}
          <div className="flex flex-wrap gap-x-6 gap-y-1">
            {(stock.entry_low || stock.entry_high) && (
              <span>Entry: <span className="font-mono text-emerald-400">৳{formatNum(stock.entry_low)}–{formatNum(stock.entry_high)}</span></span>
            )}
            {stock.stop_loss && (
              <span>SL: <span className="font-mono text-red-400">৳{formatNum(stock.stop_loss)}</span>
                {stock.stop_loss_method && <span className="text-[var(--text-dim)]"> ({stock.stop_loss_method})</span>}
              </span>
            )}
            {stock.target_1 && (
              <span>T1: <span className="font-mono text-emerald-400">৳{formatNum(stock.target_1)}</span></span>
            )}
            {stock.target_2 && (
              <span>T2: <span className="font-mono text-blue-400">৳{formatNum(stock.target_2)}</span></span>
            )}
          </div>

          {/* Indicators & Fundamentals */}
          <div className="flex flex-wrap gap-x-4 gap-y-1 text-[var(--text-dim)]">
            {stock.ma_aligned != null && (
              <span>MA Aligned: {stock.ma_aligned ? <span className="text-emerald-400">Yes</span> : <span className="text-red-400">No</span>}</span>
            )}
            {stock.vol_ratio != null && (
              <span>Vol×: <span className="font-mono">{formatNum(stock.vol_ratio, 2)}</span></span>
            )}
            {stock.atr_pct != null && (
              <span>ATR%: <span className="font-mono">{formatNum(stock.atr_pct, 2)}</span></span>
            )}
            {stock.chg_5d != null && (
              <span>5D: <span className={clsx("font-mono", stock.chg_5d > 0 ? "text-emerald-400" : "text-red-400")}>{formatPct(stock.chg_5d)}</span></span>
            )}
            {stock.chg_20d != null && (
              <span>20D: <span className={clsx("font-mono", stock.chg_20d > 0 ? "text-emerald-400" : "text-red-400")}>{formatPct(stock.chg_20d)}</span></span>
            )}
          </div>

          <div className="flex flex-wrap gap-x-4 gap-y-1 text-[var(--text-dim)]">
            {stock.pe_ratio && <span>P/E: <span className="font-mono">{formatNum(stock.pe_ratio)}</span></span>}
            {stock.dividend_yield_pct && <span>Div: <span className="font-mono">{formatNum(stock.dividend_yield_pct)}%</span></span>}
            {stock.sector && <span>{stock.sector}</span>}
            {stock.high_52w && stock.low_52w && (
              <span>52W: ৳{formatNum(stock.low_52w)}–{formatNum(stock.high_52w)}</span>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

/* ── Main Page ── */
export default function Analysis() {
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState<string>("ALL");
  const [stocks, setStocks] = useState<AIStock[]>([]);
  const [market, setMarket] = useState<AIMarket | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");

  useEffect(() => {
    loadData();
  }, []);

  async function loadData() {
    setLoading(true);
    setError(null);
    try {
      const [stocksRes, marketRes] = await Promise.all([
        fetchAIStocks(),
        fetchAIMarket(),
      ]);
      setStocks(stocksRes.stocks);
      setMarket(marketRes);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load");
    } finally {
      setLoading(false);
    }
  }

  const filtered = stocks.filter((s) => {
    if (activeTab !== "ALL" && s.overall_signal !== activeTab) return false;
    if (search && !s.symbol.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  // Count per signal
  const counts = stocks.reduce<Record<string, number>>((acc, s) => {
    acc[s.overall_signal] = (acc[s.overall_signal] || 0) + 1;
    return acc;
  }, {});

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="animate-spin text-blue-400" size={32} />
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-64 gap-2">
        <AlertTriangle className="text-red-400" size={32} />
        <p className="text-red-400">{error}</p>
        <button onClick={loadData} className="text-sm text-blue-400 hover:underline">Retry</button>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <MarketHeader market={market} />

      {/* Tab bar */}
      <div className="flex items-center gap-1 overflow-x-auto pb-1">
        {SIGNAL_TABS.map(({ key, label, icon: Icon }) => (
          <button
            key={key}
            onClick={() => setActiveTab(key)}
            className={clsx(
              "flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-colors whitespace-nowrap",
              activeTab === key
                ? "bg-blue-500/20 text-blue-400 border border-blue-500/30"
                : "text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)]"
            )}
          >
            <Icon size={14} />
            {label}
            {key !== "ALL" && counts[key] ? (
              <span className="text-xs opacity-60">({counts[key]})</span>
            ) : key === "ALL" ? (
              <span className="text-xs opacity-60">({stocks.length})</span>
            ) : null}
          </button>
        ))}

        {/* Search */}
        <input
          type="text"
          placeholder="Search symbol..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="ml-auto px-2 py-1 rounded bg-[var(--surface)] border border-[var(--border)] text-sm w-32 focus:outline-none focus:border-blue-400"
        />
      </div>

      {/* Stock cards */}
      {filtered.length === 0 ? (
        <div className="text-center text-[var(--text-dim)] py-8">
          {stocks.length === 0
            ? "AI analysis is being computed... Check back after market close."
            : `No ${activeTab} signals found.`}
        </div>
      ) : (
        <div className="space-y-2">
          {filtered.map((stock) => (
            <StockCard
              key={stock.symbol}
              stock={stock}
              onClick={() => navigate(`/chart?symbol=${stock.symbol}`)}
            />
          ))}
        </div>
      )}
    </div>
  );
}
