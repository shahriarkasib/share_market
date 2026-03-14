"""Corporate events, market news, and holidays API routes."""

import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Query

from database import get_connection
from data.cache import cache

logger = logging.getLogger(__name__)
router = APIRouter()

# Cache TTLs (seconds)
CACHE_TTL_NEWS = 300        # 5 min
CACHE_TTL_EVENTS = 300      # 5 min
CACHE_TTL_HOLIDAYS = 3600   # 1 hour
CACHE_TTL_DIVIDENDS = 600   # 10 min

# Noise filters — these corporate events are not price-moving
_NOISE_PATTERNS = [
    "Net Asset Value (NAV)",
    "has reported Net Asset Value",
    "has reported a Net Asset Value",
    "Awareness Message for Investors",
    "BSEC News",
    "Government Securities will resume",
    "Government Securities will be",
    "price limit on the trading",
    "coupon rate of the bond",
    "Continuation of BSEC",
]

# Only show these event types for corporate events (skip OTHER noise)
_IMPORTANT_EVENT_TYPES = {
    "EARNINGS", "RECORD_DATE", "CASH_DIVIDEND", "STOCK_DIVIDEND",
    "AGM", "EGM", "IPO", "BONUS", "RIGHTS_ISSUE", "SUSPENSION",
}


def _is_noise(title: str, details: str) -> bool:
    """Check if an announcement is noise (NAV reports, BSEC messages, etc)."""
    text = (title or "") + " " + (details or "")
    return any(p.lower() in text.lower() for p in _NOISE_PATTERNS)


# ── Helpers ───────────────────────────────────────────────


def _rows_to_dicts(rows) -> list[dict]:
    """Convert DB rows to plain dicts (handles DictRow objects)."""
    return [dict(r) for r in rows]


def _serialize(items: list[dict]) -> list[dict]:
    """Ensure date/datetime values are ISO-formatted strings."""
    out = []
    for item in items:
        row = {}
        for k, v in item.items():
            if isinstance(v, (date, datetime)):
                row[k] = v.isoformat()
            else:
                row[k] = v
        out.append(row)
    return out


# ── 1. GET /news — Paginated market news ─────────────────


@router.get("/news")
async def get_news(
    category: Optional[str] = Query(None, description="Filter: Stock_Market, Business_&_Corporate, Local_Economy, All"),
    impact: Optional[str] = Query(None, description="Filter: HIGH, MEDIUM, LOW, NOISE, or ALL"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    """Paginated market news with AI classification."""
    effective_cat = category if category and category != "All" else None
    effective_impact = impact if impact and impact != "ALL" else None

    cache_key = f"news:{effective_cat}:{effective_impact}:{page}:{per_page}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    conn = get_connection()
    offset = (page - 1) * per_page

    categories = ["Stock_Market", "Business_&_Corporate", "Local_Economy", "All"]
    impact_levels = ["HIGH", "MEDIUM", "LOW", "NOISE", "ALL"]

    # Build WHERE clause
    conditions = []
    params: list = []
    if effective_cat:
        conditions.append("category = ?")
        params.append(effective_cat)
    if effective_impact:
        conditions.append("impact = ?")
        params.append(effective_impact)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    total_row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM market_news {where}", params
    ).fetchone()

    rows = conn.execute(
        f"SELECT id, date, source, category, title, content, url, symbols_mentioned, "
        f"impact, sentiment, market_impact, affected_symbols, summary "
        f"FROM market_news {where} ORDER BY date DESC, id DESC LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()

    total = total_row["cnt"]
    items = _serialize(_rows_to_dicts(rows))
    conn.close()

    result = {
        "items": items,
        "total": total,
        "page": page,
        "per_page": per_page,
        "categories": categories,
        "impact_levels": impact_levels,
    }
    cache.set(cache_key, result, CACHE_TTL_NEWS)
    return result


# ── 2. GET /events — Corporate events (paginated) ────────


@router.get("")
async def get_events(
    symbol: Optional[str] = Query(None, description="Filter by stock symbol"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    days: int = Query(30, ge=1, le=365),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    include_noise: bool = Query(False, description="Include NAV reports, BSEC messages, etc"),
):
    """Paginated corporate events — only price-moving events by default."""
    cache_key = f"events:{symbol}:{event_type}:{days}:{page}:{per_page}:{include_noise}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    conn = get_connection()
    offset = (page - 1) * per_page
    since = (date.today() - timedelta(days=days)).isoformat()

    # Build WHERE clause — filter noise by default
    conditions = ["date >= ?"]
    params: list = [since]

    if symbol:
        conditions.append("symbol = ?")
        params.append(symbol.upper())

    if event_type:
        conditions.append("event_type = ?")
        params.append(event_type.upper())
    elif not include_noise:
        # Only show important event types by default
        placeholders = ",".join(["?"] * len(_IMPORTANT_EVENT_TYPES))
        conditions.append(f"event_type IN ({placeholders})")
        params.extend(sorted(_IMPORTANT_EVENT_TYPES))

    where = " AND ".join(conditions)

    total_row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM corporate_events WHERE {where}", params
    ).fetchone()

    rows = conn.execute(
        f"SELECT id, date, symbol, event_type, title, details, amount, source "
        f"FROM corporate_events WHERE {where} ORDER BY date DESC, id DESC LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()

    total = total_row["cnt"]

    # Post-filter noise from OTHER type if symbol search returns mixed results
    items = _serialize(_rows_to_dicts(rows))
    if not include_noise:
        items = [i for i in items if not _is_noise(i.get("title", ""), i.get("details", ""))]

    conn.close()

    result = {
        "items": items,
        "total": total,
        "page": page,
        "per_page": per_page,
    }
    cache.set(cache_key, result, CACHE_TTL_EVENTS)
    return result


# ── 3. GET /events/stock/{symbol} — Events for a stock ───


@router.get("/stock/{symbol}")
async def get_stock_events(symbol: str):
    """Recent corporate events for a specific stock — noise filtered."""
    sym = symbol.upper()
    cache_key = f"stock_events:{sym}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    conn = get_connection()
    since = (date.today() - timedelta(days=90)).isoformat()

    rows = conn.execute(
        "SELECT id, date, symbol, event_type, title, details, amount, source "
        "FROM corporate_events WHERE symbol = ? AND date >= ? ORDER BY date DESC",
        (sym, since),
    ).fetchall()
    conn.close()

    items = _serialize(_rows_to_dicts(rows))
    # Filter out noise (NAV reports, BSEC messages)
    items = [i for i in items if not _is_noise(i.get("title", ""), i.get("details", ""))]
    result = {"symbol": sym, "items": items, "total": len(items)}
    cache.set(cache_key, result, CACHE_TTL_EVENTS)
    return result


# ── 4. GET /dividends/upcoming — Upcoming dividend dates ──


def _parse_dividend_details(details: str) -> dict:
    """Extract cash_pct, stock_pct, and year from event details text."""
    info: dict = {}
    if not details:
        return info

    cash = re.search(r"Cash(?:\s+Dividend)?[:\s]*(\d+(?:\.\d+)?)%", details, re.I)
    if cash:
        info["cash_pct"] = float(cash.group(1))

    stock = re.search(r"Stock(?:\s+Dividend)?[:\s]*(\d+(?:\.\d+)?)%", details, re.I)
    if stock:
        info["stock_pct"] = float(stock.group(1))

    year = re.search(r"\b(20\d{2}(?:-20\d{2})?)\b", details)
    if year:
        info["year"] = year.group(1)

    return info


@router.get("/dividends/upcoming")
async def get_upcoming_dividends():
    """Upcoming dividend record dates and declarations."""
    cached = cache.get("dividends_upcoming")
    if cached:
        return cached

    conn = get_connection()
    today = date.today().isoformat()

    # Record dates in the future
    rows = conn.execute(
        "SELECT date, symbol, event_type, title, details, amount "
        "FROM corporate_events "
        "WHERE event_type = 'RECORD_DATE' AND date >= ? "
        "ORDER BY date ASC",
        (today,),
    ).fetchall()

    # Also pull recent dividend declarations (last 60 days) that might have
    # future record dates embedded in their details text
    decl_rows = conn.execute(
        "SELECT date, symbol, event_type, title, details, amount "
        "FROM corporate_events "
        "WHERE event_type IN ('CASH_DIVIDEND', 'STOCK_DIVIDEND') "
        "AND date >= ? "
        "ORDER BY date DESC",
        ((date.today() - timedelta(days=60)).isoformat(),),
    ).fetchall()
    conn.close()

    upcoming = []
    for r in rows:
        d = dict(r)
        parsed = _parse_dividend_details(d.get("details", ""))
        upcoming.append({
            "symbol": d["symbol"],
            "record_date": d["date"].isoformat() if isinstance(d["date"], date) else d["date"],
            "dividend_type": d.get("title", ""),
            "cash_pct": parsed.get("cash_pct"),
            "stock_pct": parsed.get("stock_pct"),
            "year": parsed.get("year"),
        })

    # Deduplicate recent declarations by symbol
    seen_symbols = {u["symbol"] for u in upcoming}
    recent_declarations = []
    for r in decl_rows:
        d = dict(r)
        if d["symbol"] not in seen_symbols:
            parsed = _parse_dividend_details(d.get("details", ""))
            recent_declarations.append({
                "symbol": d["symbol"],
                "declared_date": d["date"].isoformat() if isinstance(d["date"], date) else d["date"],
                "event_type": d["event_type"],
                "title": d.get("title", ""),
                "cash_pct": parsed.get("cash_pct"),
                "stock_pct": parsed.get("stock_pct"),
                "amount": d.get("amount"),
            })
            seen_symbols.add(d["symbol"])

    result = {
        "upcoming": upcoming,
        "recent_declarations": recent_declarations,
    }
    cache.set("dividends_upcoming", result, CACHE_TTL_DIVIDENDS)
    return result


# ── 5. GET /dividends/calendar — Monthly dividend calendar ─


@router.get("/dividends/calendar")
async def get_dividend_calendar(
    month: Optional[str] = Query(None, description="Month in YYYY-MM format"),
):
    """Dividend events grouped by date for a given month."""
    if month:
        try:
            year_int, month_int = map(int, month.split("-"))
        except ValueError:
            return {"error": "month must be YYYY-MM format", "calendar": {}}
    else:
        today = date.today()
        year_int, month_int = today.year, today.month
        month = f"{year_int:04d}-{month_int:02d}"

    cache_key = f"div_calendar:{month}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    # Date range for the month
    start = f"{month}-01"
    if month_int == 12:
        end = f"{year_int + 1:04d}-01-01"
    else:
        end = f"{year_int:04d}-{month_int + 1:02d}-01"

    conn = get_connection()
    rows = conn.execute(
        "SELECT date, symbol, event_type, title, details, amount "
        "FROM corporate_events "
        "WHERE event_type IN ('RECORD_DATE', 'CASH_DIVIDEND', 'STOCK_DIVIDEND', 'AGM') "
        "AND date >= ? AND date < ? "
        "ORDER BY date ASC, symbol ASC",
        (start, end),
    ).fetchall()
    conn.close()

    # Group by date
    calendar: dict[str, list] = {}
    for r in rows:
        d = dict(r)
        dt = d["date"].isoformat() if isinstance(d["date"], date) else str(d["date"])
        parsed = _parse_dividend_details(d.get("details", ""))
        entry = {
            "symbol": d["symbol"],
            "event_type": d["event_type"],
            "title": d.get("title", ""),
            "amount": d.get("amount"),
            "cash_pct": parsed.get("cash_pct"),
            "stock_pct": parsed.get("stock_pct"),
        }
        calendar.setdefault(dt, []).append(entry)

    result = {"month": month, "calendar": calendar}
    cache.set(cache_key, result, CACHE_TTL_DIVIDENDS)
    return result


# ── 6. GET /holidays — Market holidays ────────────────────


@router.get("/holidays")
async def get_holidays():
    """All market holidays."""
    cached = cache.get("market_holidays")
    if cached:
        return cached

    conn = get_connection()
    rows = conn.execute(
        "SELECT date, name, type FROM market_holidays ORDER BY date ASC"
    ).fetchall()
    conn.close()

    holidays = []
    for r in rows:
        d = dict(r)
        holidays.append({
            "date": d["date"].isoformat() if isinstance(d["date"], date) else d["date"],
            "name": d["name"],
            "type": d.get("type", "PUBLIC"),
        })

    result = {"holidays": holidays, "total": len(holidays)}
    cache.set("market_holidays", result, CACHE_TTL_HOLIDAYS)
    return result
