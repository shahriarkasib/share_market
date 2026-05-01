"""DSE time-and-sales scraper from lankabd.com + Lee-Ready side inference.

Polls `lankabd.com/DataFeed/LatestTrade.aspx?symbol=X` every poll_interval
seconds during DSE market hours (10:00-14:30 BST, Sun-Thu) and stores each
print into `dse_ticks`. Side is inferred against the latest order book
snapshot (Lee-Ready: print at/above best ask = buyer-initiated, at/below
best bid = seller-initiated, midpoint by majority).

Run as a systemd service alongside the existing order book scraper. Results
power true cumulative delta + footprint analytics for DSE.
"""
from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import get_connection


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dse_tape")


LATEST_TRADE_URL = "https://lankabd.com/DataFeed/LatestTrade.aspx"
TIMEOUT = 10
POLL_INTERVAL = int(os.environ.get("DSE_TAPE_POLL_SEC", "20"))


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


def fetch_latest_trades(symbol: str, session: requests.Session) -> list[dict]:
    """Scrape the latest-trade table for a symbol. Returns list of dicts:
    [{"ts": datetime, "price": float, "size": int}, ...]"""
    try:
        resp = session.get(LATEST_TRADE_URL, params={"symbol": symbol.upper()},
                           timeout=TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"fetch {symbol}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if not table:
        return []

    out = []
    today = datetime.now(timezone.utc).date()
    for tr in table.find_all("tr")[1:]:
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(tds) < 3:
            continue
        try:
            time_str = tds[0]
            price_str = tds[1].replace(",", "")
            qty_str = tds[2].replace(",", "")
            # time is HH:MM:SS in BST
            tm = datetime.strptime(time_str, "%H:%M:%S").time()
            ts = datetime.combine(today, tm, tzinfo=timezone.utc)
            price = float(price_str)
            size = int(qty_str)
            if price <= 0 or size <= 0:
                continue
            out.append({"ts": ts, "price": price, "size": size})
        except Exception:
            continue
    return out


def get_best_bid_ask(symbol: str) -> tuple[Optional[float], Optional[float]]:
    """Lookup latest best bid/ask from order_book_snapshots populated by the
    existing orderbook_scraper.py."""
    try:
        conn = get_connection()
        row = conn.execute(
            """SELECT best_bid, best_ask FROM order_book_snapshots
               WHERE symbol = ? ORDER BY snapshot_time DESC LIMIT 1""",
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


def insert_ticks(symbol: str, ticks: list[dict]):
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


def is_market_open() -> bool:
    """DSE: Sun-Thu, 10:00-14:30 BST (UTC+6)."""
    now_utc = datetime.now(timezone.utc)
    bst_minutes = (now_utc.hour * 60 + now_utc.minute + 6 * 60) % (24 * 60)
    bst_hour = bst_minutes // 60
    bst_day = (now_utc.weekday() + (1 if now_utc.hour + 6 >= 24 else 0)) % 7  # Mon=0
    # Sun=6, Mon=0, Tue=1, Wed=2, Thu=3 → trading days
    if bst_day not in (6, 0, 1, 2, 3):
        return False
    return 10 <= bst_hour < 15  # 10:00-14:59 BST captures 14:30 close


def get_watchlist() -> list[str]:
    """Stocks we actively trade — pull from `fundamentals` A-category + portfolio."""
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT DISTINCT symbol FROM fundamentals WHERE category = 'A' "
            "ORDER BY symbol LIMIT 80"
        ).fetchall()
        conn.close()
        return [r[0] for r in rows]
    except Exception:
        return []


def main():
    ensure_schema()
    session = requests.Session()
    session.headers.update({"User-Agent": "DSEAnalysisBot/1.0"})

    while True:
        if not is_market_open():
            log.info("market closed — sleeping 5 min")
            time.sleep(300)
            continue

        watchlist = get_watchlist()
        if not watchlist:
            log.warning("empty watchlist — sleeping 60s")
            time.sleep(60)
            continue

        log.info(f"polling {len(watchlist)} symbols")
        total = 0
        for sym in watchlist:
            try:
                ticks = fetch_latest_trades(sym, session)
                n = insert_ticks(sym, ticks)
                total += n
            except Exception as e:
                log.warning(f"{sym}: {e}")
            # gentle pacing — don't hammer lankabd
            time.sleep(0.4)

        log.info(f"polled {len(watchlist)}, inserted ~{total} new ticks")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
