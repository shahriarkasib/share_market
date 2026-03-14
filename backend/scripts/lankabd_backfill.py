#!/usr/bin/env python3
"""LankaDB historical backfill for DSE A-category stocks.

Phase A: TradingView UDF API  (2015 -> present, OHLCV only)
Phase B: PriceArchive scraper (2020 -> present, 30-column rich data)

Usage:
    python scripts/lankabd_backfill.py            # Phase A only
    python scripts/lankabd_backfill.py --rich      # Phase B only
    python scripts/lankabd_backfill.py --both      # Phase A then B
    python scripts/lankabd_backfill.py --symbol GP # Single stock (Phase A)
"""

import sys
import os
import re
import time
import argparse
import logging
from datetime import datetime, date, timedelta
from html.parser import HTMLParser

import requests
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23"
    "@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres",
)
DATABASE_URL_DIRECT = os.getenv(
    "DATABASE_URL_DIRECT",
    "postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23"
    "@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres",
)

RATE_LIMIT_TVC = 0.3   # seconds between TradingView API calls
RATE_LIMIT_HTML = 1.0   # seconds between PriceArchive scrapes
BATCH_SIZE = 100        # rows per INSERT

# 2-year chunks for TradingView API
CHUNK_RANGES = [
    ("2015-01-01", "2016-12-31"),
    ("2017-01-01", "2018-12-31"),
    ("2019-01-01", "2020-12-31"),
    ("2021-01-01", "2022-12-31"),
    ("2023-01-01", "2024-12-31"),
    ("2025-01-01", "2026-12-31"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lankabd")


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_conn(direct=False):
    """Return a raw psycopg2 connection."""
    dsn = DATABASE_URL_DIRECT if direct else DATABASE_URL
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def get_a_category_symbols(conn):
    """Fetch all A-category symbols from the fundamentals table."""
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM fundamentals WHERE category = 'A' ORDER BY symbol")
        return [row[0] for row in cur.fetchall()]


def bulk_upsert_daily_prices(conn, rows):
    """Batch-upsert into daily_prices using execute_values.

    Each row: (symbol, date, open, high, low, close, volume)
    """
    if not rows:
        return
    sql = """
        INSERT INTO daily_prices (symbol, date, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (symbol, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low  = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=BATCH_SIZE)


# ---------------------------------------------------------------------------
# Phase A — TradingView UDF Historical Data
# ---------------------------------------------------------------------------
class TVCSession:
    """Manages a LankaBD TradingView chart session (cookies + CSRF token)."""

    BASE = "https://www.lankabd.com"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        })
        self.token = None

    def init(self):
        """Visit the chart page to acquire cookies and the CSRF token."""
        url = f"{self.BASE}/Home/AdvancedCharts?sn=GP"
        log.info("Initializing TVC session …")
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()

        # Extract __RequestVerificationToken from a hidden input
        match = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp.text
        )
        if match:
            self.token = match.group(1)
            log.info("Got CSRF token: %s…", self.token[:20])
        else:
            # Try meta tag variant
            match = re.search(
                r'<meta\s+name="__RequestVerificationToken"\s+content="([^"]+)"',
                resp.text,
            )
            if match:
                self.token = match.group(1)
                log.info("Got CSRF token (meta): %s…", self.token[:20])
            else:
                log.warning("No CSRF token found — requests may fail")

    def fetch_history(self, symbol, from_ts, to_ts):
        """Fetch OHLCV bars from the TVC history endpoint.

        Returns list of (date_str, open, high, low, close, volume) or None on error.
        """
        url = f"{self.BASE}/tvc/DataFeed/history"
        params = {
            "symbol": symbol,
            "resolution": "D",
            "from": int(from_ts),
            "to": int(to_ts),
        }
        headers = {"X-Requested-With": "XMLHttpRequest"}
        if self.token:
            headers["RequestVerificationToken"] = self.token

        resp = self.session.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        status = data.get("s", "")
        if status == "no_data":
            return []
        if status != "ok":
            log.warning("Unexpected status '%s' for %s", status, symbol)
            return None

        timestamps = data.get("t", [])
        opens = data.get("o", [])
        highs = data.get("h", [])
        lows = data.get("l", [])
        closes = data.get("c", [])
        volumes = data.get("v", [])

        seen_dates = set()
        bars = []
        for i in range(len(timestamps)):
            dt = datetime.fromtimestamp(timestamps[i], tz=__import__('datetime').timezone.utc).strftime("%Y-%m-%d")
            if dt in seen_dates:
                continue  # skip duplicate dates within same response
            seen_dates.add(dt)
            bars.append((
                dt,
                round(opens[i], 1) if opens[i] is not None else None,
                round(highs[i], 1) if highs[i] is not None else None,
                round(lows[i], 1) if lows[i] is not None else None,
                round(closes[i], 1) if closes[i] is not None else None,
                min(int(volumes[i]), 2_147_483_647) if volumes[i] is not None else 0,
            ))
        return bars


def run_phase_a(symbols, conn):
    """Backfill daily_prices from TradingView UDF API for all given symbols."""
    log.info("=" * 60)
    log.info("PHASE A: TradingView UDF Historical Backfill")
    log.info("Symbols: %d  |  Chunks: %d  |  Range: 2015-01-01 → today", len(symbols), len(CHUNK_RANGES))
    log.info("=" * 60)

    tvc = TVCSession()
    tvc.init()

    total_rows = 0
    failures = []
    earliest_date = None
    latest_date = None

    for idx, symbol in enumerate(symbols):
        symbol_rows = 0
        for start_str, end_str in CHUNK_RANGES:
            try:
                from_ts = datetime.strptime(start_str, "%Y-%m-%d").timestamp()
                to_ts = datetime.strptime(end_str, "%Y-%m-%d").timestamp()
                # Don't fetch future chunks
                if from_ts > datetime.now().timestamp():
                    continue

                bars = tvc.fetch_history(symbol, from_ts, to_ts)
                time.sleep(RATE_LIMIT_TVC)

                if bars is None:
                    log.warning("  %s chunk %s-%s failed, skipping", symbol, start_str, end_str)
                    continue
                if not bars:
                    continue

                # Build rows for upsert
                rows = [(symbol, b[0], b[1], b[2], b[3], b[4], b[5]) for b in bars]
                bulk_upsert_daily_prices(conn, rows)
                symbol_rows += len(rows)

                # Track date range
                for b in bars:
                    d = b[0]
                    if earliest_date is None or d < earliest_date:
                        earliest_date = d
                    if latest_date is None or d > latest_date:
                        latest_date = d

            except Exception as e:
                log.error("FAILED %s chunk %s-%s: %s", symbol, start_str, end_str, e)
                failures.append((symbol, str(e)))

        total_rows += symbol_rows

        if (idx + 1) % 10 == 0 or idx == len(symbols) - 1:
            log.info(
                "Progress: %d/%d stocks done  |  %s: %d rows  |  cumulative: %d rows",
                idx + 1, len(symbols), symbol, symbol_rows, total_rows,
            )

    log.info("=" * 60)
    log.info("PHASE A COMPLETE")
    log.info("  Total rows upserted : %d", total_rows)
    log.info("  Date range          : %s → %s", earliest_date, latest_date)
    log.info("  Failures            : %d", len(failures))
    if failures:
        for sym, err in failures:
            log.info("    %s: %s", sym, err)
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Phase B — PriceArchive Rich Data (stub — run with --rich)
# ---------------------------------------------------------------------------
EXTENDED_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS daily_prices_extended (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    ltp REAL, high REAL, low REAL, open REAL, close REAL, ycp REAL,
    change_pct REAL,
    weekly_change_pct REAL,
    biweekly_change_pct REAL,
    monthly_change_pct REAL,
    yearly_change_pct REAL,
    trades INTEGER,
    turnover_mn REAL,
    volume INTEGER,
    volume_change_1d_pct REAL,
    volume_change_2d_pct REAL,
    block_max_price REAL,
    block_min_price REAL,
    block_trades INTEGER,
    block_quantity INTEGER,
    block_value_mn REAL,
    market_cap_bn REAL,
    market_cap_usd_bn REAL,
    forward_pe REAL,
    audited_pe REAL,
    rsi REAL,
    beta REAL,
    turnover_velocity REAL,
    UNIQUE(symbol, date)
);
"""


class PriceArchiveParser(HTMLParser):
    """Parse PriceArchive HTML table rows into lists of values."""

    def __init__(self):
        super().__init__()
        self.rows = []
        self._in_table = False
        self._in_tbody = False
        self._in_row = False
        self._in_cell = False
        self._current_row = []
        self._current_cell = ""

    def handle_starttag(self, tag, attrs):
        attrs_d = dict(attrs)
        if tag == "table" and "dataTable" in attrs_d.get("class", ""):
            self._in_table = True
        if self._in_table and tag == "tbody":
            self._in_tbody = True
        if self._in_tbody and tag == "tr":
            self._in_row = True
            self._current_row = []
        if self._in_row and tag == "td":
            self._in_cell = True
            self._current_cell = ""

    def handle_endtag(self, tag):
        if self._in_cell and tag == "td":
            self._in_cell = False
            self._current_row.append(self._current_cell.strip())
        if self._in_row and tag == "tr":
            self._in_row = False
            if self._current_row:
                self.rows.append(self._current_row)
        if tag == "tbody":
            self._in_tbody = False
        if tag == "table":
            self._in_table = False

    def handle_data(self, data):
        if self._in_cell:
            self._current_cell += data


def _parse_num(val, allow_negative=True):
    """Parse a numeric string, stripping commas and %. Returns None on failure."""
    if not val or val.strip() in ("", "-", "N/A", "--"):
        return None
    val = val.strip().replace(",", "").replace("%", "")
    try:
        n = float(val)
        if not allow_negative and n < 0:
            return None
        return n
    except ValueError:
        return None


def _parse_int(val):
    n = _parse_num(val)
    return int(n) if n is not None else None


def fetch_price_archive(session, symbol, from_date, to_date):
    """Fetch PriceArchive HTML and parse into rows of data.

    Returns list of tuples ready for DB insert, or empty list.
    """
    url = "https://www.lankabd.com/Home/PriceArchive"
    params = {
        "symbol": symbol,
        "fromdate": from_date,   # format: YYYY-MM-DD
        "todate": to_date,
    }
    resp = session.get(url, params=params, timeout=60)
    resp.raise_for_status()

    parser = PriceArchiveParser()
    parser.feed(resp.text)

    db_rows = []
    for raw in parser.rows:
        if len(raw) < 5:
            continue
        # Column 0 is usually the date
        try:
            # Try common date formats
            date_str = raw[0].strip()
            for fmt in ("%d %b %Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%b-%Y"):
                try:
                    dt = datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue
            else:
                continue
        except Exception:
            continue

        # Map columns (order from LankaBD PriceArchive):
        # 0:Date 1:LTP 2:High 3:Low 4:Open 5:Close/YCP?  6:YCP 7:Change%
        # 8:Weekly% 9:BiWeekly% 10:Monthly% 11:Yearly%
        # 12:Trades 13:Turnover(Mn) 14:Volume 15:Vol1D% 16:Vol2D%
        # 17:BlockMax 18:BlockMin 19:BlockTrades 20:BlockQty 21:BlockVal(Mn)
        # 22:MCap(Bn) 23:MCap$(Bn) 24:FwdPE 25:AuditedPE 26:RSI 27:Beta 28:TV
        def g(i):
            return raw[i] if i < len(raw) else None

        row = (
            symbol, dt,
            _parse_num(g(1)), _parse_num(g(2)), _parse_num(g(3)),
            _parse_num(g(4)), _parse_num(g(5)), _parse_num(g(6)),
            _parse_num(g(7)),   # change_pct
            _parse_num(g(8)),   # weekly
            _parse_num(g(9)),   # biweekly
            _parse_num(g(10)),  # monthly
            _parse_num(g(11)),  # yearly
            _parse_int(g(12)),  # trades
            _parse_num(g(13)),  # turnover_mn
            _parse_int(g(14)),  # volume
            _parse_num(g(15)),  # vol 1d%
            _parse_num(g(16)),  # vol 2d%
            _parse_num(g(17)),  # block max
            _parse_num(g(18)),  # block min
            _parse_int(g(19)),  # block trades
            _parse_int(g(20)),  # block qty
            _parse_num(g(21)),  # block val mn
            _parse_num(g(22)),  # mcap bn
            _parse_num(g(23)),  # mcap usd bn
            _parse_num(g(24)),  # fwd pe
            _parse_num(g(25)),  # audited pe
            _parse_num(g(26)),  # rsi
            _parse_num(g(27)),  # beta
            _parse_num(g(28)),  # turnover velocity
        )
        db_rows.append(row)

    return db_rows


def bulk_upsert_extended(conn, rows):
    """Batch-upsert into daily_prices_extended."""
    if not rows:
        return
    cols = (
        "symbol, date, ltp, high, low, open, close, ycp, "
        "change_pct, weekly_change_pct, biweekly_change_pct, "
        "monthly_change_pct, yearly_change_pct, "
        "trades, turnover_mn, volume, "
        "volume_change_1d_pct, volume_change_2d_pct, "
        "block_max_price, block_min_price, block_trades, block_quantity, block_value_mn, "
        "market_cap_bn, market_cap_usd_bn, forward_pe, audited_pe, "
        "rsi, beta, turnover_velocity"
    )
    n_cols = len(cols.split(","))
    update_cols = [c.strip() for c in cols.split(",")[2:]]  # skip symbol, date
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO daily_prices_extended ({cols})
        VALUES %s
        ON CONFLICT (symbol, date) DO UPDATE SET {update_set}
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=BATCH_SIZE)


def run_phase_b(symbols, conn):
    """Backfill daily_prices_extended from PriceArchive scraping."""
    log.info("=" * 60)
    log.info("PHASE B: PriceArchive Rich Data Backfill")
    log.info("Symbols: %d  |  Range: 2020-01-01 → today", len(symbols))
    log.info("=" * 60)

    # Create table via direct connection
    direct_conn = get_conn(direct=True)
    try:
        with direct_conn.cursor() as cur:
            cur.execute(EXTENDED_TABLE_DDL)
        log.info("Table daily_prices_extended ensured.")
    finally:
        direct_conn.close()

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
    })

    # 6-month chunks from 2020-01-01 to today
    chunks = []
    start = date(2020, 1, 1)
    today = date.today()
    while start < today:
        end = min(start + timedelta(days=182), today)
        chunks.append((start.isoformat(), end.isoformat()))
        start = end + timedelta(days=1)

    total_rows = 0
    failures = []

    for idx, symbol in enumerate(symbols):
        symbol_rows = 0
        try:
            for start_str, end_str in chunks:
                rows = fetch_price_archive(session, symbol, start_str, end_str)
                time.sleep(RATE_LIMIT_HTML)
                if rows:
                    bulk_upsert_extended(conn, rows)
                    symbol_rows += len(rows)
        except Exception as e:
            log.error("FAILED %s: %s", symbol, e)
            failures.append((symbol, str(e)))

        total_rows += symbol_rows

        if (idx + 1) % 10 == 0 or idx == len(symbols) - 1:
            log.info(
                "Progress: %d/%d stocks  |  %s: %d rows  |  cumulative: %d",
                idx + 1, len(symbols), symbol, symbol_rows, total_rows,
            )

    log.info("=" * 60)
    log.info("PHASE B COMPLETE")
    log.info("  Total rows upserted : %d", total_rows)
    log.info("  Failures            : %d", len(failures))
    if failures:
        for sym, err in failures:
            log.info("    %s: %s", sym, err)
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="LankaDB DSE Backfill")
    parser.add_argument("--rich", action="store_true", help="Run Phase B (PriceArchive)")
    parser.add_argument("--both", action="store_true", help="Run Phase A then B")
    parser.add_argument("--symbol", type=str, help="Backfill a single symbol only")
    args = parser.parse_args()

    conn = get_conn()

    if args.symbol:
        symbols = [args.symbol.upper()]
        log.info("Single-symbol mode: %s", symbols[0])
    else:
        symbols = get_a_category_symbols(conn)
        log.info("Loaded %d A-category symbols", len(symbols))

    if not symbols:
        log.error("No symbols found. Exiting.")
        sys.exit(1)

    run_a = not args.rich or args.both
    run_b = args.rich or args.both

    if run_a:
        run_phase_a(symbols, conn)

    if run_b:
        run_phase_b(symbols, conn)
    elif not args.both and not args.rich:
        log.info("Phase B: Run with --rich flag to backfill extended data.")

    conn.close()


if __name__ == "__main__":
    main()
