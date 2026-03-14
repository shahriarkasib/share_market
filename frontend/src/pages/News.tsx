import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  Loader2,
  ExternalLink,
  Search,
  Calendar,
  ChevronLeft,
  ChevronRight,
  TrendingUp,
  TrendingDown,
  Minus,
  AlertTriangle,
} from "lucide-react";
import { clsx } from "clsx";
import {
  fetchMarketNews,
  fetchCorporateEvents,
  fetchUpcomingDividends,
  fetchMarketHolidays,
  type NewsItem,
  type CorporateEvent,
  type UpcomingDividend,
  type MarketHoliday,
} from "../api/client.ts";

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function fmtDate(dateStr: string): string {
  const d = new Date(dateStr);
  return d.toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
    year: "numeric",
  });
}

function fmtDateShort(dateStr: string): string {
  const d = new Date(dateStr);
  return d.toLocaleDateString("en-GB", { day: "numeric", month: "short" });
}

const IMPACT_COLORS: Record<string, string> = {
  HIGH: "bg-red-500/15 text-red-400 border-red-500/30",
  MEDIUM: "bg-amber-500/15 text-amber-400 border-amber-500/30",
  LOW: "bg-blue-500/10 text-blue-400/70 border-blue-500/20",
  NOISE: "bg-gray-500/10 text-gray-500 border-gray-500/20",
};

const SENTIMENT_ICON: Record<string, typeof TrendingUp> = {
  BULLISH: TrendingUp,
  BEARISH: TrendingDown,
  NEUTRAL: Minus,
  MIXED: AlertTriangle,
};

const SENTIMENT_COLOR: Record<string, string> = {
  BULLISH: "text-green-400",
  BEARISH: "text-red-400",
  NEUTRAL: "text-[var(--text-dim)]",
  MIXED: "text-amber-400",
};

const MARKET_IMPACT_LABELS: Record<string, string> = {
  STOCK_SPECIFIC: "Stock",
  SECTOR_WIDE: "Sector",
  DSEX_MOVING: "DSEX",
  MACRO: "Macro",
  DIVIDEND: "Dividend",
  NOISE: "—",
};

const CATEGORY_COLORS: Record<string, string> = {
  Stock_Market: "bg-blue-500/15 text-blue-400",
  "Stock Market": "bg-blue-500/15 text-blue-400",
  Local_Economy: "bg-amber-500/15 text-amber-400",
  "Local Economy": "bg-amber-500/15 text-amber-400",
  "Business_&_Corporate": "bg-emerald-500/15 text-emerald-400",
  "Business & Corporate": "bg-emerald-500/15 text-emerald-400",
};

const EVENT_TYPE_COLORS: Record<string, string> = {
  AGM: "bg-purple-500/15 text-purple-400",
  EGM: "bg-pink-500/15 text-pink-400",
  Dividend: "bg-green-500/15 text-green-400",
  "Record Date": "bg-blue-500/15 text-blue-400",
  "Book Closure": "bg-amber-500/15 text-amber-400",
  Rights: "bg-cyan-500/15 text-cyan-400",
  IPO: "bg-red-500/15 text-red-400",
};

function Badge({ label, colorMap }: { label: string; colorMap: Record<string, string> }) {
  const color = colorMap[label] ?? "bg-gray-500/15 text-gray-400";
  return (
    <span
      className={clsx(
        "inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-medium whitespace-nowrap",
        color,
      )}
    >
      {label.replace(/_/g, " ")}
    </span>
  );
}

/* ------------------------------------------------------------------ */
/*  Tabs                                                               */
/* ------------------------------------------------------------------ */

type TabKey = "market_moving" | "all_news" | "events" | "dividends";

const TABS: { key: TabKey; label: string }[] = [
  { key: "market_moving", label: "Market Moving" },
  { key: "all_news", label: "All News" },
  { key: "events", label: "Corporate Events" },
  { key: "dividends", label: "Dividend Calendar" },
];

/* ------------------------------------------------------------------ */
/*  Pagination                                                         */
/* ------------------------------------------------------------------ */

function Pagination({
  page,
  total,
  perPage,
  onPage,
}: {
  page: number;
  total: number;
  perPage: number;
  onPage: (p: number) => void;
}) {
  const totalPages = Math.max(1, Math.ceil(total / perPage));
  if (totalPages <= 1) return null;
  return (
    <div className="flex items-center justify-center gap-3 mt-4">
      <button
        disabled={page <= 1}
        onClick={() => onPage(page - 1)}
        className="p-1.5 rounded-md text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)] disabled:opacity-30 disabled:pointer-events-none transition-colors"
      >
        <ChevronLeft className="h-4 w-4" />
      </button>
      <span className="text-xs text-[var(--text-muted)] tabular-nums">
        {page} / {totalPages}
      </span>
      <button
        disabled={page >= totalPages}
        onClick={() => onPage(page + 1)}
        className="p-1.5 rounded-md text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)] disabled:opacity-30 disabled:pointer-events-none transition-colors"
      >
        <ChevronRight className="h-4 w-4" />
      </button>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  News Item Card                                                     */
/* ------------------------------------------------------------------ */

function NewsCard({ item }: { item: NewsItem }) {
  const SentimentIcon = item.sentiment ? SENTIMENT_ICON[item.sentiment] || Minus : Minus;
  const sentimentColor = item.sentiment ? SENTIMENT_COLOR[item.sentiment] || "" : "";
  const impactColor = item.impact ? IMPACT_COLORS[item.impact] || "" : "";
  const hasClassification = !!item.impact;

  return (
    <div
      className={clsx(
        "p-3 rounded-lg border transition-colors",
        item.impact === "HIGH"
          ? "border-red-500/20 bg-red-500/3 hover:border-red-500/40"
          : item.impact === "MEDIUM"
            ? "border-amber-500/15 bg-amber-500/2 hover:border-amber-500/30"
            : "border-[var(--border)] bg-[var(--surface)] hover:border-[var(--text-dim)]",
      )}
    >
      <div className="flex items-start gap-3">
        {/* Date + sentiment icon */}
        <div className="shrink-0 w-14 text-right flex flex-col items-end gap-1">
          <span className="text-[11px] text-[var(--text-dim)] tabular-nums leading-tight">
            {fmtDateShort(item.date)}
          </span>
          {hasClassification && (
            <SentimentIcon className={clsx("h-3.5 w-3.5", sentimentColor)} />
          )}
        </div>

        {/* Content */}
        <div className="min-w-0 flex-1">
          <div className="flex items-start gap-2">
            <div className="min-w-0 flex-1">
              {item.url ? (
                <a
                  href={item.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-sm text-[var(--text)] hover:text-blue-400 transition-colors leading-snug inline-flex items-start gap-1"
                >
                  <span className="line-clamp-2">{item.title}</span>
                  <ExternalLink className="h-3 w-3 shrink-0 mt-0.5 opacity-50" />
                </a>
              ) : (
                <span className="text-sm text-[var(--text)] leading-snug line-clamp-2">
                  {item.title}
                </span>
              )}
            </div>
          </div>

          {/* AI summary */}
          {item.summary && item.summary !== "No market impact." && (
            <p className="text-xs text-[var(--text-muted)] mt-1 leading-relaxed">
              {item.summary}
            </p>
          )}

          {/* Tags row */}
          <div className="flex items-center gap-1.5 mt-1.5 flex-wrap">
            {hasClassification && (
              <span className={clsx("inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-semibold border", impactColor)}>
                {item.impact}
              </span>
            )}
            {item.market_impact && item.market_impact !== "NOISE" && (
              <span className="text-[10px] text-[var(--text-dim)] px-1.5 py-0.5 rounded bg-[var(--hover)]">
                {MARKET_IMPACT_LABELS[item.market_impact] || item.market_impact}
              </span>
            )}
            <Badge label={item.category} colorMap={CATEGORY_COLORS} />
            {/* Affected symbols */}
            {item.affected_symbols && item.affected_symbols.length > 0 && (
              <div className="flex items-center gap-1">
                {item.affected_symbols.slice(0, 4).map((sym) => (
                  <Link
                    key={sym}
                    to={`/stock/${sym}`}
                    className="text-[10px] font-medium text-blue-400 hover:text-blue-300 bg-blue-500/10 px-1.5 py-0.5 rounded"
                  >
                    {sym}
                  </Link>
                ))}
                {item.affected_symbols.length > 4 && (
                  <span className="text-[10px] text-[var(--text-dim)]">
                    +{item.affected_symbols.length - 4}
                  </span>
                )}
              </div>
            )}
            {item.source && (
              <span className="text-[10px] text-[var(--text-dim)] ml-auto">{item.source}</span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Tab 1: Market Moving News                                          */
/* ------------------------------------------------------------------ */

function MarketMovingTab() {
  const [items, setItems] = useState<NewsItem[]>([]);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [impactFilter, setImpactFilter] = useState("HIGH");
  const perPage = 30;

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const res = await fetchMarketNews({
        impact: impactFilter,
        page,
        per_page: perPage,
      });
      setItems(res.items);
      setTotal(res.total);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to load news");
    } finally {
      setLoading(false);
    }
  }, [impactFilter, page]);

  useEffect(() => { load(); }, [load]);

  return (
    <div>
      {/* Impact filter */}
      <div className="flex flex-wrap items-center gap-2 mb-4">
        {["HIGH", "MEDIUM", "LOW"].map((level) => (
          <button
            key={level}
            onClick={() => { setImpactFilter(level); setPage(1); }}
            className={clsx(
              "px-3 py-1.5 rounded-md text-xs font-medium transition-colors border",
              impactFilter === level
                ? IMPACT_COLORS[level]
                : "border-[var(--border)] text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)]",
            )}
          >
            {level}
          </button>
        ))}
        <span className="text-[10px] text-[var(--text-dim)] ml-2">
          {total} items
        </span>
      </div>

      {loading ? (
        <div className="flex justify-center py-16">
          <Loader2 className="h-5 w-5 animate-spin text-[var(--text-dim)]" />
        </div>
      ) : error ? (
        <p className="text-center text-red-400 text-sm py-8">{error}</p>
      ) : items.length === 0 ? (
        <p className="text-center text-[var(--text-dim)] text-sm py-8">
          No {impactFilter.toLowerCase()} impact news found. Run the classifier to categorize news.
        </p>
      ) : (
        <div className="space-y-2">
          {items.map((item) => <NewsCard key={item.id} item={item} />)}
        </div>
      )}

      <Pagination page={page} total={total} perPage={perPage} onPage={setPage} />
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Tab 2: All News Feed                                               */
/* ------------------------------------------------------------------ */

function AllNewsFeedTab() {
  const [items, setItems] = useState<NewsItem[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [category, setCategory] = useState("");
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const perPage = 20;

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const res = await fetchMarketNews({
        category: category || undefined,
        page,
        per_page: perPage,
      });
      setItems(res.items);
      setTotal(res.total);
      if (res.categories?.length) setCategories(res.categories);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to load news");
    } finally {
      setLoading(false);
    }
  }, [category, page]);

  useEffect(() => { load(); }, [load]);

  return (
    <div>
      <div className="flex flex-wrap items-center gap-2 mb-4">
        <button
          onClick={() => { setCategory(""); setPage(1); }}
          className={clsx(
            "px-3 py-1.5 rounded-md text-xs font-medium transition-colors",
            !category
              ? "bg-blue-500/15 text-blue-400"
              : "text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)]",
          )}
        >
          All
        </button>
        {categories.filter(c => c !== "All").map((cat) => (
          <button
            key={cat}
            onClick={() => { setCategory(cat); setPage(1); }}
            className={clsx(
              "px-3 py-1.5 rounded-md text-xs font-medium transition-colors",
              category === cat
                ? "bg-blue-500/15 text-blue-400"
                : "text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)]",
            )}
          >
            {cat.replace(/_/g, " ")}
          </button>
        ))}
      </div>

      {loading ? (
        <div className="flex justify-center py-16">
          <Loader2 className="h-5 w-5 animate-spin text-[var(--text-dim)]" />
        </div>
      ) : error ? (
        <p className="text-center text-red-400 text-sm py-8">{error}</p>
      ) : items.length === 0 ? (
        <p className="text-center text-[var(--text-dim)] text-sm py-8">No news found.</p>
      ) : (
        <div className="space-y-2">
          {items.map((item) => <NewsCard key={item.id} item={item} />)}
        </div>
      )}

      <Pagination page={page} total={total} perPage={perPage} onPage={setPage} />
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Tab 3: Corporate Events                                            */
/* ------------------------------------------------------------------ */

function CorporateEventsTab() {
  const [items, setItems] = useState<CorporateEvent[]>([]);
  const [symbolFilter, setSymbolFilter] = useState("");
  const [eventType, setEventType] = useState("");
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const perPage = 20;

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const res = await fetchCorporateEvents({
        symbol: symbolFilter || undefined,
        event_type: eventType || undefined,
        page,
        per_page: perPage,
      });
      setItems(res.items);
      setTotal(res.total);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to load events");
    } finally {
      setLoading(false);
    }
  }, [symbolFilter, eventType, page]);

  useEffect(() => { load(); }, [load]);

  const [symbolInput, setSymbolInput] = useState("");
  useEffect(() => {
    const t = setTimeout(() => {
      setSymbolFilter(symbolInput.trim().toUpperCase());
      setPage(1);
    }, 400);
    return () => clearTimeout(t);
  }, [symbolInput]);

  return (
    <div>
      <div className="flex flex-wrap items-center gap-3 mb-4">
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-[var(--text-dim)]" />
          <input
            type="text"
            placeholder="Filter by symbol..."
            value={symbolInput}
            onChange={(e) => setSymbolInput(e.target.value)}
            className="pl-8 pr-3 py-1.5 rounded-md text-xs bg-[var(--bg)] border border-[var(--border)] text-[var(--text)] placeholder:text-[var(--text-dim)] focus:outline-none focus:border-blue-500 w-44"
          />
        </div>
        <select
          value={eventType}
          onChange={(e) => { setEventType(e.target.value); setPage(1); }}
          className="px-3 py-1.5 rounded-md text-xs bg-[var(--bg)] border border-[var(--border)] text-[var(--text)] focus:outline-none focus:border-blue-500"
        >
          <option value="">All Event Types</option>
          {Object.keys(EVENT_TYPE_COLORS).map((t) => (
            <option key={t} value={t}>{t}</option>
          ))}
        </select>
      </div>

      {loading ? (
        <div className="flex justify-center py-16">
          <Loader2 className="h-5 w-5 animate-spin text-[var(--text-dim)]" />
        </div>
      ) : error ? (
        <p className="text-center text-red-400 text-sm py-8">{error}</p>
      ) : items.length === 0 ? (
        <p className="text-center text-[var(--text-dim)] text-sm py-8">No events found.</p>
      ) : (
        <div className="space-y-2">
          {items.map((ev) => (
            <div
              key={ev.id}
              className="p-3 rounded-lg bg-[var(--surface)] border border-[var(--border)] hover:border-[var(--text-dim)] transition-colors"
            >
              <div className="flex items-start gap-3">
                <div className="shrink-0 w-14 text-right">
                  <span className="text-[11px] text-[var(--text-dim)] tabular-nums leading-tight block">
                    {fmtDateShort(ev.date)}
                  </span>
                </div>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 mb-1">
                    <Link
                      to={`/stock/${ev.symbol}`}
                      className="text-sm font-semibold text-blue-400 hover:text-blue-300 transition-colors"
                    >
                      {ev.symbol}
                    </Link>
                    <Badge label={ev.event_type} colorMap={EVENT_TYPE_COLORS} />
                  </div>
                  <p className="text-sm text-[var(--text)] leading-snug">{ev.title}</p>
                  {ev.details && (
                    <p className="text-xs text-[var(--text-muted)] mt-1 line-clamp-2 leading-relaxed">
                      {ev.details}
                    </p>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      <Pagination page={page} total={total} perPage={perPage} onPage={setPage} />
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Tab 4: Dividend Calendar                                           */
/* ------------------------------------------------------------------ */

function DividendCalendarTab() {
  const [dividends, setDividends] = useState<UpcomingDividend[]>([]);
  const [holidays, setHolidays] = useState<MarketHoliday[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      setError("");
      try {
        const [divRes, holRes] = await Promise.all([
          fetchUpcomingDividends(),
          fetchMarketHolidays(),
        ]);
        if (!cancelled) {
          setDividends(divRes.upcoming);
          setHolidays(holRes.holidays);
        }
      } catch (e: unknown) {
        if (!cancelled) setError(e instanceof Error ? e.message : "Failed to load data");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, []);

  const grouped = dividends.reduce<Record<string, UpcomingDividend[]>>((acc, d) => {
    const key = d.record_date;
    if (!acc[key]) acc[key] = [];
    acc[key].push(d);
    return acc;
  }, {});
  const sortedDates = Object.keys(grouped).sort();

  if (loading) {
    return (
      <div className="flex justify-center py-16">
        <Loader2 className="h-5 w-5 animate-spin text-[var(--text-dim)]" />
      </div>
    );
  }

  if (error) {
    return <p className="text-center text-red-400 text-sm py-8">{error}</p>;
  }

  return (
    <div className="space-y-8">
      <section>
        <h3 className="text-sm font-semibold text-[var(--text)] mb-3 flex items-center gap-2">
          <Calendar className="h-4 w-4 text-green-400" />
          Upcoming Record Dates
        </h3>

        {sortedDates.length === 0 ? (
          <p className="text-sm text-[var(--text-dim)] py-4">No upcoming dividends found.</p>
        ) : (
          <div className="space-y-4">
            {sortedDates.map((date) => (
              <div key={date}>
                <div className="text-xs font-medium text-[var(--text-muted)] mb-2 px-1">
                  {fmtDate(date)}
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-[var(--text-dim)] border-b border-[var(--border)]">
                        <th className="text-left py-1.5 px-2 font-medium">Symbol</th>
                        <th className="text-right py-1.5 px-2 font-medium">Cash %</th>
                        <th className="text-right py-1.5 px-2 font-medium">Stock %</th>
                        <th className="text-left py-1.5 px-2 font-medium">Type</th>
                        <th className="text-left py-1.5 px-2 font-medium">Year</th>
                      </tr>
                    </thead>
                    <tbody>
                      {grouped[date].map((d, i) => (
                        <tr
                          key={`${d.symbol}-${i}`}
                          className="border-b border-[var(--border)] last:border-b-0 hover:bg-[var(--hover)] transition-colors"
                        >
                          <td className="py-1.5 px-2">
                            <Link
                              to={`/stock/${d.symbol}`}
                              className="text-blue-400 hover:text-blue-300 font-medium transition-colors"
                            >
                              {d.symbol}
                            </Link>
                          </td>
                          <td className="text-right py-1.5 px-2 text-[var(--text)] tabular-nums">
                            {d.cash_pct != null && d.cash_pct > 0 ? `${d.cash_pct}%` : "-"}
                          </td>
                          <td className="text-right py-1.5 px-2 text-[var(--text)] tabular-nums">
                            {d.stock_pct != null && d.stock_pct > 0 ? `${d.stock_pct}%` : "-"}
                          </td>
                          <td className="py-1.5 px-2 text-[var(--text-muted)]">{d.dividend_type}</td>
                          <td className="py-1.5 px-2 text-[var(--text-muted)]">{d.year}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      <section>
        <h3 className="text-sm font-semibold text-[var(--text)] mb-3 flex items-center gap-2">
          <Calendar className="h-4 w-4 text-amber-400" />
          Market Holidays
        </h3>

        {holidays.length === 0 ? (
          <p className="text-sm text-[var(--text-dim)] py-4">No holidays data available.</p>
        ) : (
          <div className="space-y-1">
            {holidays.map((h, i) => (
              <div
                key={i}
                className="flex items-center gap-3 px-3 py-2 rounded-lg bg-[var(--surface)] border border-[var(--border)]"
              >
                <span className="text-xs text-[var(--text-dim)] tabular-nums w-20 shrink-0">
                  {fmtDate(h.date)}
                </span>
                <span className="text-sm text-[var(--text)]">{h.name}</span>
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Main Page                                                          */
/* ------------------------------------------------------------------ */

export default function News() {
  const [tab, setTab] = useState<TabKey>("market_moving");

  return (
    <div className="max-w-4xl mx-auto px-3 sm:px-4 lg:px-8 py-4 sm:py-6">
      <h1 className="text-lg font-bold text-[var(--text)] mb-4">News & Events</h1>

      {/* Tab bar */}
      <div className="flex items-center gap-1 border-b border-[var(--border)] mb-5">
        {TABS.map(({ key, label }) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            className={clsx(
              "px-4 py-2 text-xs font-medium transition-colors relative",
              tab === key
                ? "text-[var(--text)]"
                : "text-[var(--text-muted)] hover:text-[var(--text)]",
            )}
          >
            {label}
            {tab === key && (
              <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-500 rounded-full" />
            )}
          </button>
        ))}
      </div>

      {tab === "market_moving" && <MarketMovingTab />}
      {tab === "all_news" && <AllNewsFeedTab />}
      {tab === "events" && <CorporateEventsTab />}
      {tab === "dividends" && <DividendCalendarTab />}
    </div>
  );
}
