import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { clsx } from "clsx";
import {
  Loader2, ChevronDown, ChevronUp,
  Target, TrendingDown, Activity, Eye, Zap, Shield,
} from "lucide-react";
import { fetchBuySetups, fetchLiveAlerts, fetchAIMarket } from "../api/client.ts";
import type { BuySetup, BuySetupsResponse, LiveAlert, AIMarket } from "../api/client.ts";

const SETUP_TABS = [
  { key: "multi_setup", label: "Best Picks", icon: Zap, desc: "Multiple overlapping signals", color: "text-emerald-400" },
  { key: "support_oversold", label: "Support + Oversold", icon: Shield, desc: "79-83% win rate", color: "text-blue-400" },
  { key: "rsi_extreme", label: "RSI < 30", icon: TrendingDown, desc: "74% win rate", color: "text-amber-400" },
  { key: "mean_reversion", label: "3+ Red Days", icon: Activity, desc: "63% win rate", color: "text-purple-400" },
  { key: "obv_divergence", label: "OBV Divergence", icon: Eye, desc: "61% win rate", color: "text-cyan-400" },
  { key: "squeeze_forming", label: "Squeeze", icon: Activity, desc: "Big move coming — watch for setup", color: "text-orange-400" },
  { key: "alerts", label: "Live Alerts", icon: Target, desc: "Real-time events", color: "text-red-400" },
] as const;

function SetupCard({ stock, onClick }: { stock: BuySetup; onClick: () => void }) {
  const [open, setOpen] = useState(false);

  return (
    <div className="rounded-lg border border-[var(--border)] bg-[var(--surface)] overflow-hidden">
      <div className="p-3 cursor-pointer hover:bg-[var(--hover)]" onClick={() => setOpen(!open)}>
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <button onClick={(e) => { e.stopPropagation(); onClick(); }}
              className="font-bold text-sm hover:text-blue-400">{stock.symbol}</button>
            <span className="text-xs text-[var(--text-dim)]">{stock.category}</span>
            <span className={clsx("px-2 py-0.5 rounded text-xs font-bold border",
              stock.win_rate >= 80 ? "text-emerald-400 bg-emerald-400/10 border-emerald-400/30" :
              stock.win_rate >= 70 ? "text-blue-400 bg-blue-400/10 border-blue-400/30" :
              stock.win_rate >= 60 ? "text-amber-400 bg-amber-400/10 border-amber-400/30" :
              "text-gray-400 bg-gray-400/10 border-gray-400/30"
            )}>{stock.win_rate}% win</span>
          </div>
          <div className="flex items-center gap-3 text-sm shrink-0">
            <span className="font-mono font-bold">৳{stock.ltp?.toFixed(1)}</span>
            <span className={clsx("font-mono text-xs", (stock.change_pct ?? 0) < 0 ? "text-red-400" : "text-emerald-400")}>
              {(stock.change_pct ?? 0) > 0 ? "+" : ""}{stock.change_pct?.toFixed(1)}%
            </span>
            {open ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
          </div>
        </div>

        <p className="mt-1 text-xs text-[var(--text-muted)]">{stock.note}</p>

        <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-[var(--text-dim)]">
          <span>RSI: <span className={clsx("font-mono", (stock.rsi ?? 50) < 35 ? "text-emerald-400" : "text-[var(--text)]")}>{stock.rsi?.toFixed(1)}</span></span>
          <span>CMF: <span className={clsx("font-mono", (stock.cmf ?? 0) > 0 ? "text-emerald-400" : "text-red-400")}>{stock.cmf?.toFixed(3)}</span></span>
          {stock.support && <span>Support: <span className="text-emerald-400 font-mono">{stock.support.price} ({stock.support.touches}T)</span></span>}
          {stock.candle && <span>Candle: <span className="text-amber-400">{stock.candle}{stock.candle_confirmed ? " ✓" : ""}</span></span>}
          {stock.swing && <span className="text-[var(--text-muted)]">{stock.swing}</span>}
        </div>
      </div>

      {open && (
        <div className="border-t border-[var(--border)] p-3 space-y-2 text-xs">
          <div className="flex flex-wrap gap-x-5 gap-y-1">
            {stock.pivot_r1 && <span>Target (R1): <span className="font-mono text-emerald-400">৳{stock.pivot_r1}</span></span>}
            {stock.resistance && <span>Resistance: <span className="font-mono text-red-400">৳{stock.resistance.price} ({stock.resistance.touches}T)</span></span>}
            {stock.pivot_s1 && <span>SL (S1): <span className="font-mono text-red-400">৳{stock.pivot_s1}</span></span>}
          </div>
          <div className="flex flex-wrap gap-x-5 gap-y-1 text-[var(--text-dim)]">
            {stock.chg_5d != null && <span>5D: <span className={clsx("font-mono", stock.chg_5d > 0 ? "text-emerald-400" : "text-red-400")}>{stock.chg_5d > 0 ? "+" : ""}{stock.chg_5d}%</span></span>}
            {stock.chg_20d != null && <span>20D: <span className={clsx("font-mono", stock.chg_20d > 0 ? "text-emerald-400" : "text-red-400")}>{stock.chg_20d > 0 ? "+" : ""}{stock.chg_20d}%</span></span>}
            {stock.mr_score != null && <span>MR Score: {stock.mr_score}/100</span>}
            {stock.sector && <span>{stock.sector}</span>}
          </div>
          {stock.setups_matched && (
            <div className="text-[var(--text-muted)]">
              Overlapping signals: <span className="text-emerald-400 font-bold">{stock.setups_matched.join(" + ")}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function AlertCard({ alert }: { alert: LiveAlert }) {
  const sevColor = alert.severity === "HIGH" ? "text-red-400 bg-red-400/10 border-red-400/30" : "text-amber-400 bg-amber-400/10 border-amber-400/30";
  const time = new Date(alert.time).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });

  return (
    <div className="flex items-start gap-2 py-1.5 text-xs border-b border-[var(--border)]/20">
      <span className={clsx("px-1 py-0.5 rounded text-[10px] font-bold border shrink-0", sevColor)}>{alert.severity}</span>
      <span className="font-bold shrink-0">{alert.symbol}</span>
      <span className="text-[var(--text-muted)] flex-1">{alert.message}</span>
      <span className="text-[var(--text-dim)] shrink-0">{time}</span>
    </div>
  );
}

function MarketBar({ market }: { market: AIMarket | null }) {
  if (!market) return null;
  const down = (market.dsex_change ?? 0) < 0;
  return (
    <div className="flex flex-wrap items-center gap-4 text-sm p-2 rounded-lg bg-[var(--surface)] border border-[var(--border)] mb-3">
      <div>
        <span className="text-[var(--text-muted)]">DSEX </span>
        <span className="font-bold">{market.dsex?.toFixed(2)}</span>
        <span className={clsx("ml-1 font-mono text-xs", down ? "text-red-400" : "text-emerald-400")}>
          {market.dsex_change_pct && market.dsex_change_pct > 0 ? "+" : ""}{market.dsex_change_pct?.toFixed(2)}%
        </span>
      </div>
      <span className="text-xs text-[var(--text-dim)]">
        <span className="text-emerald-400">{market.advances}</span>/<span className="text-red-400">{market.declines}</span>
      </span>
      <span className="text-xs text-[var(--text-dim)]">{market.turnover_cr} Cr</span>
    </div>
  );
}

export default function Analysis() {
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState<string>("multi_setup");
  const [data, setData] = useState<BuySetupsResponse | null>(null);
  const [alerts, setAlerts] = useState<LiveAlert[]>([]);
  const [market, setMarket] = useState<AIMarket | null>(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const [setupsRes, alertsRes, marketRes] = await Promise.all([
          fetchBuySetups(),
          fetchLiveAlerts(),
          fetchAIMarket(),
        ]);
        setData(setupsRes);
        setAlerts(alertsRes.alerts);
        setMarket(marketRes);
      } catch (e) {
        console.error(e);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  if (loading) {
    return <div className="flex items-center justify-center h-64"><Loader2 className="animate-spin text-blue-400" size={32} /></div>;
  }

  const setupStocks = activeTab === "alerts" ? [] : (data?.setups?.[activeTab as keyof BuySetupsResponse["setups"]] || []);
  const filtered = setupStocks.filter(s => !search || s.symbol.toLowerCase().includes(search.toLowerCase()));
  const filteredAlerts = alerts.filter(a => !search || a.symbol.toLowerCase().includes(search.toLowerCase()));

  return (
    <div className="space-y-3">
      <MarketBar market={market} />

      <div className="flex items-center gap-1 overflow-x-auto pb-1">
        {SETUP_TABS.map(({ key, label, icon: Icon }) => {
          const count = key === "alerts" ? alerts.length : (data?.setups?.[key as keyof BuySetupsResponse["setups"]]?.length || 0);
          return (
            <button key={key} onClick={() => setActiveTab(key)}
              className={clsx(
                "flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium whitespace-nowrap",
                activeTab === key ? "bg-blue-500/20 text-blue-400 border border-blue-500/30" : "text-[var(--text-muted)] hover:bg-[var(--hover)]"
              )}>
              <Icon size={13} />
              {label}
              {count > 0 && <span className="opacity-60">({count})</span>}
            </button>
          );
        })}
        <input type="text" placeholder="Search..." value={search} onChange={(e) => setSearch(e.target.value)}
          className="ml-auto px-2 py-1 rounded bg-[var(--surface)] border border-[var(--border)] text-xs w-28 focus:outline-none focus:border-blue-400" />
      </div>

      {/* Setup description */}
      {activeTab !== "alerts" && (
        <div className="text-xs text-[var(--text-dim)] px-1">
          {SETUP_TABS.find(t => t.key === activeTab)?.desc} — Backtested on 277 stocks, 6 months of DSE data
        </div>
      )}

      {/* Content */}
      {activeTab === "alerts" ? (
        <div className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-3">
          {filteredAlerts.length === 0 ? (
            <p className="text-center text-[var(--text-dim)] py-4">No alerts today. Alerts fire every 5 min during trading hours.</p>
          ) : (
            filteredAlerts.map(a => <AlertCard key={a.id} alert={a} />)
          )}
        </div>
      ) : filtered.length === 0 ? (
        <p className="text-center text-[var(--text-dim)] py-8">
          No stocks match this setup right now. Check back after market close for updated scans.
        </p>
      ) : (
        <div className="space-y-2">
          {filtered.map(s => (
            <SetupCard key={`${s.symbol}-${s.setup}`} stock={s}
              onClick={() => navigate(`/chart?symbol=${s.symbol}`)} />
          ))}
        </div>
      )}
    </div>
  );
}
