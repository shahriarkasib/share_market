import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { RefreshCw, TrendingUp, TrendingDown } from "lucide-react";
import { fetchSmartMoneyRadar } from "../api/client";
import type { SmartMoneyStock } from "../api/client";

const WINDOWS = [
  { value: 30, label: "30m" },
  { value: 60, label: "1h" },
  { value: 120, label: "2h" },
  { value: 240, label: "4h" },
  { value: 480, label: "8h" },
  { value: 720, label: "Full day" },
] as const;

const REFRESH_MS = 60_000;

export default function SmartMoney() {
  const [buys, setBuys] = useState<SmartMoneyStock[]>([]);
  const [sells, setSells] = useState<SmartMoneyStock[]>([]);
  const [windowMin, setWindowMin] = useState<number>(240);
  const [minTrades, setMinTrades] = useState<number>(10);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const [b, s] = await Promise.all([
        fetchSmartMoneyRadar("buy", windowMin, minTrades, 30),
        fetchSmartMoneyRadar("sell", windowMin, minTrades, 30),
      ]);
      setBuys(b.stocks || []);
      setSells(s.stocks || []);
      setLastRefresh(new Date());
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, [windowMin, minTrades]);

  useEffect(() => {
    const t = window.setInterval(() => {
      const now = new Date();
      const bstMin = (now.getUTCHours() * 60 + now.getUTCMinutes() + 6 * 60) % (24 * 60);
      const bstDay = (now.getUTCDay() + (bstMin >= 24 * 60 ? 1 : 0)) % 7;
      const market = bstDay >= 0 && bstDay <= 4 && bstMin >= 10 * 60 && bstMin <= 15 * 60;
      if (market) void load();
    }, REFRESH_MS);
    return () => window.clearInterval(t);
  }, [windowMin, minTrades]);

  const stats = useMemo(() => {
    const bn = buys.reduce((a, b) => a + b.net_delta, 0);
    const sn = sells.reduce((a, b) => a + b.net_delta, 0);
    return { bn, sn };
  }, [buys, sells]);

  const fmtNum = (n: number) => n.toLocaleString();
  const fmtPx = (px: number | null) => px == null ? "—" : `৳${px.toFixed(2)}`;

  return (
    <div className="max-w-[1440px] mx-auto px-3 sm:px-4 lg:px-8 py-6">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <div>
          <h1 className="text-xl font-bold flex items-center gap-2">
            💰 Smart Money Radar
            <span className="text-xs font-normal text-[var(--text-muted)]">
              Lee-Ready tick classification · 709 stocks tracked
            </span>
          </h1>
          {lastRefresh && (
            <p className="text-xs text-[var(--text-muted)] mt-0.5">
              Last refresh: {lastRefresh.toLocaleTimeString()} · auto-refresh 60s during market
            </p>
          )}
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded text-xs border border-[var(--border)] hover:bg-[var(--hover)] disabled:opacity-50"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap items-center gap-3 mb-4 text-xs">
        <span className="text-[var(--text-muted)]">Window:</span>
        {WINDOWS.map((w) => (
          <button
            key={w.value}
            onClick={() => setWindowMin(w.value)}
            className={`px-2.5 py-1 rounded border ${
              windowMin === w.value
                ? "bg-blue-500/15 border-blue-500/50 text-blue-500"
                : "border-[var(--border)] text-[var(--text-muted)] hover:bg-[var(--hover)]"
            }`}
          >
            {w.label}
          </button>
        ))}
        <span className="ml-2 text-[var(--text-muted)]">Min trades:</span>
        <input
          type="number"
          value={minTrades}
          min={1}
          max={100}
          onChange={(e) => setMinTrades(Math.max(1, parseInt(e.target.value) || 1))}
          className="w-16 px-2 py-1 border border-[var(--border)] rounded bg-transparent"
        />
      </div>

      {error && (
        <div className="rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-500 mb-3">
          {error}
        </div>
      )}

      <div className="rounded border border-purple-500/30 bg-purple-500/5 px-3 py-2 text-xs mb-4">
        <p className="text-[var(--text)]">
          <strong className="text-purple-400">How to read:</strong> Each row shows actual TRADES classified by Lee-Ready —
          buyer hit ask = buy (🟢), seller hit bid = sell (🔴). Net Δ shows institutional intent.
          High buy% + high ratio = aggressive accumulation. Pair with order-book imbalance for confirmation.
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* TOP BUYING */}
        <div>
          <div className="text-sm font-bold mb-2 flex items-center gap-2">
            <TrendingUp className="h-4 w-4 text-emerald-500" />
            <span className="text-emerald-500">TOP ACCUMULATION</span>
            <span className="text-xs text-[var(--text-muted)] font-normal">
              ({buys.length}) · net +{fmtNum(stats.bn)}
            </span>
          </div>
          <div className="rounded-lg border border-emerald-500/20 overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead className="bg-emerald-500/10 text-[var(--text-muted)] text-[11px]">
                <tr>
                  <th className="text-left px-2 py-1.5">Symbol</th>
                  <th className="text-right px-2 py-1.5">Net Δ</th>
                  <th className="text-right px-2 py-1.5">Buy%</th>
                  <th className="text-right px-2 py-1.5">Ratio</th>
                  <th className="text-right px-2 py-1.5">Trades</th>
                  <th className="text-right px-2 py-1.5">Range</th>
                </tr>
              </thead>
              <tbody>
                {buys.map((s) => (
                  <tr key={s.symbol} className="border-t border-[var(--border)] hover:bg-[var(--hover)]">
                    <td className="px-2 py-1">
                      <Link
                        to={`/smc-chart/${s.symbol}`}
                        className="font-bold text-blue-500 hover:underline"
                      >
                        {s.symbol}
                      </Link>
                    </td>
                    <td className="text-right px-2 py-1 text-emerald-500 font-bold">
                      +{fmtNum(s.net_delta)}
                    </td>
                    <td className="text-right px-2 py-1">{s.buy_pct.toFixed(0)}%</td>
                    <td className="text-right px-2 py-1 text-emerald-500/80">
                      {s.buy_sell_ratio >= 99 ? "∞" : `${s.buy_sell_ratio.toFixed(1)}×`}
                    </td>
                    <td className="text-right px-2 py-1 text-[var(--text-muted)]">{s.trades}</td>
                    <td className="text-right px-2 py-1 text-[10px] text-[var(--text-muted)]">
                      {fmtPx(s.low_px)} - {fmtPx(s.high_px)}
                    </td>
                  </tr>
                ))}
                {buys.length === 0 && !loading && (
                  <tr>
                    <td colSpan={6} className="text-center py-6 text-[var(--text-muted)]">
                      No data in this window
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* TOP SELLING */}
        <div>
          <div className="text-sm font-bold mb-2 flex items-center gap-2">
            <TrendingDown className="h-4 w-4 text-red-500" />
            <span className="text-red-500">TOP DISTRIBUTION</span>
            <span className="text-xs text-[var(--text-muted)] font-normal">
              ({sells.length}) · net {fmtNum(stats.sn)}
            </span>
          </div>
          <div className="rounded-lg border border-red-500/20 overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead className="bg-red-500/10 text-[var(--text-muted)] text-[11px]">
                <tr>
                  <th className="text-left px-2 py-1.5">Symbol</th>
                  <th className="text-right px-2 py-1.5">Net Δ</th>
                  <th className="text-right px-2 py-1.5">Buy%</th>
                  <th className="text-right px-2 py-1.5">Ratio</th>
                  <th className="text-right px-2 py-1.5">Trades</th>
                  <th className="text-right px-2 py-1.5">Range</th>
                </tr>
              </thead>
              <tbody>
                {sells.map((s) => (
                  <tr key={s.symbol} className="border-t border-[var(--border)] hover:bg-[var(--hover)]">
                    <td className="px-2 py-1">
                      <Link
                        to={`/smc-chart/${s.symbol}`}
                        className="font-bold text-blue-500 hover:underline"
                      >
                        {s.symbol}
                      </Link>
                    </td>
                    <td className="text-right px-2 py-1 text-red-500 font-bold">
                      {fmtNum(s.net_delta)}
                    </td>
                    <td className="text-right px-2 py-1">{s.buy_pct.toFixed(0)}%</td>
                    <td className="text-right px-2 py-1 text-red-500/80">
                      {s.buy_sell_ratio.toFixed(2)}×
                    </td>
                    <td className="text-right px-2 py-1 text-[var(--text-muted)]">{s.trades}</td>
                    <td className="text-right px-2 py-1 text-[10px] text-[var(--text-muted)]">
                      {fmtPx(s.low_px)} - {fmtPx(s.high_px)}
                    </td>
                  </tr>
                ))}
                {sells.length === 0 && !loading && (
                  <tr>
                    <td colSpan={6} className="text-center py-6 text-[var(--text-muted)]">
                      No data in this window
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
