"""DSE time-and-sales scraper from lankabd.com + Lee-Ready side inference.

Polls `lankabd.com/api/Company/MkSecondDataSymbol` (the new API used by
LankaBD's MinuteChartMatrix page) every poll_interval seconds during DSE
market hours and stores derived ticks into `dse_ticks`. Side is inferred
against the latest order book snapshot (Lee-Ready: print at/above best
ask = buyer-initiated, at/below best bid = seller-initiated, midpoint by
price-change tick rule).

The endpoint returns a per-second snapshot of cumulative trade count +
volume; we diff against the previous snapshot to extract new trades.

Run as a systemd service alongside the order book scraper. Results power
true cumulative delta + footprint analytics for DSE.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Optional

import requests

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_connection


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dse_tape")


LANKABD_BASE = "https://www.lankabd.com"
TICK_URL = f"{LANKABD_BASE}/api/Company/MkSecondDataSymbol"
COMPANY_LIST_PAGE = f"{LANKABD_BASE}/Home/MinuteChartMatrix"
TIMEOUT = 10
POLL_INTERVAL = int(os.environ.get("DSE_TAPE_POLL_SEC", "180"))


def ensure_schema():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dse_ticks (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            ts TIMESTAMPTZ NOT NULL,
            price NUMERIC(12,2) NOT NULL,
            size BIGINT NOT NULL,
            side CHAR(1) NOT NULL,
            best_bid NUMERIC(12,2),
            best_ask NUMERIC(12,2),
            UNIQUE(symbol, ts, price, size)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dse_ticks_symbol_ts ON dse_ticks (symbol, ts DESC)")
    conn.commit()
    conn.close()
    log.info("dse_ticks schema ready")


# ─── CID resolution ──────────────────────────────────────────────────────
# LankaBD's tick API needs a numeric company ID, not the ticker. We scrape
# the MinuteChartMatrix page once on startup and refresh once per day.

_CID_CACHE: dict[str, int] = {}
_CID_LAST_REFRESH: float = 0.0
_CID_TTL_SECONDS = 24 * 3600


def refresh_cid_cache(session: requests.Session) -> int:
    """Scrape the company dropdown to build symbol→cid map."""
    global _CID_CACHE, _CID_LAST_REFRESH
    try:
        resp = session.get(COMPANY_LIST_PAGE, timeout=TIMEOUT)
        resp.raise_for_status()
        # Pattern: <option value="361">KDSALTD</option>
        matches = re.findall(r'value="(\d+)">([A-Z0-9]+)<', resp.text)
        cache = {sym: int(cid) for cid, sym in matches}
        if cache:
            _CID_CACHE = cache
            _CID_LAST_REFRESH = time.time()
            log.info(f"cid cache refreshed: {len(cache)} symbols")
        return len(cache)
    except Exception as e:
        log.warning(f"cid refresh failed: {e}")
        return 0


def get_csrf_token(session: requests.Session) -> Optional[str]:
    """Fetch a fresh anti-forgery token from the company list page."""
    try:
        resp = session.get(COMPANY_LIST_PAGE, timeout=TIMEOUT)
        m = re.search(r'__RequestVerificationToken[^>]*value="([^"]+)"', resp.text)
        return m.group(1) if m else None
    except Exception:
        return None


# ─── Tick fetch + diff ──────────────────────────────────────────────────
# We store the LAST cumulative count + volume seen per symbol so that we
# only emit ticks that are NEW since the previous poll.

_LAST_CUM: dict[str, dict] = {}  # symbol → {"count": float, "volume": float, "ts_ms": float}


def fetch_latest_trades(
    symbol: str, session: requests.Session, token: Optional[str], trade_counts: int = 200
) -> list[dict]:
    """Fetch recent per-second snapshots, diff against last seen, emit new ticks.

    LankaBD returns a list of [ts_ms, price, cum_count, cum_volume, cum_value_lakhs, ltp]
    rows. We compute deltas between consecutive rows AFTER our last-seen
    cumulative count to extract individual trade groups.
    """
    cid = _CID_CACHE.get(symbol.upper())
    if not cid:
        return []
    headers = {"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"}
    if token:
        headers["RequestVerificationToken"] = token
    try:
        resp = session.get(
            TICK_URL,
            params={"cid": cid, "tradeCounts": trade_counts},
            headers=headers,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        log.warning(f"fetch {symbol} (cid {cid}): {e}")
        return []

    rows = payload.get("data") or []
    if not rows:
        return []

    last_seen = _LAST_CUM.get(symbol.upper(), {})
    last_count = last_seen.get("count", 0)
    last_volume = last_seen.get("volume", 0)

    # Sort by timestamp ascending so diffs are forward-moving
    try:
        rows = sorted(rows, key=lambda r: r[0])
    except Exception:
        pass

    out: list[dict] = []
    prev_count = last_count
    prev_volume = last_volume
    for r in rows:
        try:
            ts_ms = float(r[0])
            price = float(r[1])
            cum_count = float(r[2])
            cum_volume = float(r[3])
        except Exception:
            continue
        if cum_count <= last_count:  # already-seen window
            continue
        new_count = int(cum_count - prev_count)
        new_volume = int(cum_volume - prev_volume)
        if new_count > 0 and new_volume > 0:
            avg_size = max(1, new_volume // max(1, new_count))
            ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            # Emit ONE aggregate tick per second-snapshot (price is the
            # representative print). This collapses N micro-trades into one
            # row, which is fine for delta/VWAP analytics.
            out.append({
                "ts": ts,
                "price": round(price, 2),
                "size": int(new_volume),
                "trade_count": new_count,
                "avg_size": avg_size,
            })
        prev_count = cum_count
        prev_volume = cum_volume

    # Update cache with the latest cumulative values seen
    if rows:
        try:
            latest = rows[-1]
            _LAST_CUM[symbol.upper()] = {
                "count": float(latest[2]),
                "volume": float(latest[3]),
                "ts_ms": float(latest[0]),
            }
        except Exception:
            pass
    return out


# ─── Side inference (Lee-Ready) ─────────────────────────────────────────


def get_best_bid_ask(symbol: str) -> tuple[Optional[float], Optional[float]]:
    """Lookup latest best bid/ask from orderbook_snapshots populated by the
    orderbook scheduler job."""
    try:
        conn = get_connection()
        row = conn.execute(
            """SELECT best_bid, best_ask FROM orderbook_snapshots
               WHERE symbol = ? ORDER BY ts DESC LIMIT 1""",
            (symbol.upper(),),
        ).fetchone()
        conn.close()
        if not row:
            return None, None
        return (
            float(row["best_bid"]) if row["best_bid"] else None,
            float(row["best_ask"]) if row["best_ask"] else None,
        )
    except Exception:
        return None, None


def classify_side(price: float, bid: Optional[float], ask: Optional[float]) -> str:
    """Lee-Ready trade classification."""
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask <= bid:
        return "?"
    mid = (bid + ask) / 2
    if price >= ask:
        return "B"
    if price <= bid:
        return "S"
    if price > mid:
        return "B"
    if price < mid:
        return "S"
    return "?"


def insert_ticks(symbol: str, ticks: list[dict]) -> int:
    if not ticks:
        return 0
    bid, ask = get_best_bid_ask(symbol)
    conn = get_connection()
    inserted = 0
    for t in ticks:
        side = classify_side(t["price"], bid, ask)
        try:
            conn.execute(
                """INSERT INTO dse_ticks (symbol, ts, price, size, side, best_bid, best_ask)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (symbol, ts, price, size) DO NOTHING""",
                (symbol.upper(), t["ts"], t["price"], t["size"], side, bid, ask),
            )
            inserted += 1
        except Exception as e:
            log.debug(f"insert skip: {e}")
    conn.commit()
    conn.close()
    return inserted


# ─── Market hours + watchlist ──────────────────────────────────────────


def is_market_open() -> bool:
    """DSE: Sun-Thu, 10:00-14:30 BST (UTC+6)."""
    now_utc = datetime.now(timezone.utc)
    bst_minutes = (now_utc.hour * 60 + now_utc.minute + 6 * 60) % (24 * 60)
    bst_hour = bst_minutes // 60
    bst_day = (now_utc.weekday() + (1 if now_utc.hour + 6 >= 24 else 0)) % 7
    if bst_day not in (6, 0, 1, 2, 3):
        return False
    return 10 <= bst_hour < 15


def get_watchlist() -> list[str]:
    """ALL stocks with a known LankaBD CID — smart-money tracking needs
    full coverage (Z-cat pump-and-dump plays are often where the action is).
    Returns up to ~700 symbols."""
    # Just use the cid cache directly — that's everything LankaBD exposes
    return list(_CID_CACHE.keys())


def main():
    ensure_schema()
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (DSEAnalysisBot/2.0)",
        "Referer": COMPANY_LIST_PAGE,
    })

    refresh_cid_cache(session)
    if not _CID_CACHE:
        log.error("no CIDs available — cannot poll. Sleeping 5 min and retrying.")

    token = get_csrf_token(session)
    log.info(f"csrf token: {'OK' if token else 'MISSING'}")

    while True:
        if not is_market_open():
            log.info("market closed — sleeping 5 min")
            time.sleep(300)
            continue

        # Refresh CID cache once a day
        if (time.time() - _CID_LAST_REFRESH) > _CID_TTL_SECONDS:
            refresh_cid_cache(session)
            token = get_csrf_token(session)

        watchlist = get_watchlist()
        if not watchlist:
            log.warning("empty watchlist — sleeping 60s")
            time.sleep(60)
            continue

        log.info(f"polling {len(watchlist)} symbols")
        total_inserted = 0
        token_failures = 0
        for sym in watchlist:
            try:
                ticks = fetch_latest_trades(sym, session, token, trade_counts=200)
                n = insert_ticks(sym, ticks)
                total_inserted += n
            except Exception as e:
                log.warning(f"{sym}: {e}")
                token_failures += 1
                if token_failures > 5:
                    # Token likely expired — refresh
                    token = get_csrf_token(session)
                    token_failures = 0
            time.sleep(0.2)  # gentler since we now poll ~700 symbols

        log.info(f"polled {len(watchlist)}, inserted {total_inserted} new ticks")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
