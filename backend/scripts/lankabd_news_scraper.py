"""Scrape LankaDB corporate announcements, declarations, and news.

Stores data in PostgreSQL tables:
  - corporate_events (announcements + declarations)
  - market_news (news headlines)
  - market_holidays (Bangladesh market holidays 2024-2026)

Usage:
    source venv/bin/activate
    python scripts/lankabd_news_scraper.py
"""

import re
import time
import logging
from datetime import datetime, date, timedelta

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── DB connections ─────────────────────────────────────────
# Pooler for DML (port 6543)
DATABASE_URL = (
    "postgresql://postgres.iihlezpkpllacztoaguc:"
    "160021062Ss%23%23@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres"
)
# Direct for DDL (port 5432)
DATABASE_URL_DIRECT = (
    "postgresql://postgres.iihlezpkpllacztoaguc:"
    "160021062Ss%23%23@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"
)

BASE_URL = "https://www.lankabd.com"
RATE_LIMIT = 1.0  # seconds between requests


# ── DDL ────────────────────────────────────────────────────
DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS corporate_events (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        symbol TEXT NOT NULL,
        event_type TEXT NOT NULL,
        title TEXT,
        details TEXT,
        amount REAL,
        source TEXT DEFAULT 'lankabd',
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(date, symbol, event_type, title)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_news (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        source TEXT DEFAULT 'lankabd',
        category TEXT,
        title TEXT NOT NULL,
        content TEXT,
        url TEXT,
        symbols_mentioned TEXT[],
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(date, title)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_holidays (
        date DATE PRIMARY KEY,
        name TEXT NOT NULL,
        type TEXT DEFAULT 'PUBLIC'
    )
    """,
]


def create_tables():
    """Create tables using direct connection (DDL needs non-pooler)."""
    logger.info("Creating tables via direct connection...")
    conn = psycopg2.connect(DATABASE_URL_DIRECT)
    cur = conn.cursor()
    for ddl in DDL_STATEMENTS:
        cur.execute(ddl)
    conn.commit()
    conn.close()
    logger.info("Tables created/verified.")


def get_conn():
    """Get pooler connection for DML."""
    return psycopg2.connect(
        DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor
    )


# ── Session helper ─────────────────────────────────────────
def create_session():
    """Create requests session with LankaDB cookies."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
    })
    r = s.get(BASE_URL, timeout=15)
    r.raise_for_status()
    # Extract verification token
    tokens = re.findall(
        r'__RequestVerificationToken.*?value="([^"]+)"', r.text
    )
    token = tokens[0] if tokens else None
    return s, token


# ── Normalize event type ──────────────────────────────────
def normalize_event_type(text):
    """Classify announcement text into event types."""
    t = text.lower()
    if "cash dividend" in t:
        return "CASH_DIVIDEND"
    if "stock dividend" in t:
        return "STOCK_DIVIDEND"
    if "record date" in t:
        return "RECORD_DATE"
    if "agm" in t or "annual general meeting" in t:
        return "AGM"
    if "egm" in t or "extraordinary general" in t:
        return "EGM"
    if "rights" in t and ("issue" in t or "share" in t):
        return "RIGHTS_ISSUE"
    if "bonus" in t and ("share" in t or "issue" in t):
        return "BONUS"
    if "earning" in t or "eps" in t or "financial result" in t or "financial statement" in t:
        return "EARNINGS"
    if "ipo" in t:
        return "IPO"
    if "suspend" in t:
        return "SUSPENSION"
    return "OTHER"


def extract_amount(text):
    """Try to extract a monetary/percentage amount from text."""
    # Look for "XX% cash dividend" or "Tk. X.XX"
    m = re.search(r"(\d+(?:\.\d+)?)\s*%\s*(?:cash|stock)\s*dividend", text, re.I)
    if m:
        return float(m.group(1))
    m = re.search(r"Tk\.?\s*(\d+(?:,\d+)*(?:\.\d+)?)", text)
    if m:
        return float(m.group(1).replace(",", ""))
    return None


# ── A. Corporate Announcements ─────────────────────────────
def scrape_announcements(session, start_date="2024-01-01", end_date=None):
    """Scrape corporate announcements from LankaDB.

    Paginates through all pages for the given date range.
    """
    if end_date is None:
        end_date = date.today().isoformat()

    logger.info(f"Scraping announcements from {start_date} to {end_date}...")
    conn = get_conn()
    cur = conn.cursor()

    # Process in 3-month chunks to keep page counts manageable
    from_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    to_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

    total_inserted = 0
    total_skipped = 0

    while from_dt < to_dt:
        chunk_end = min(from_dt + timedelta(days=90), to_dt)
        logger.info(f"  Chunk: {from_dt} to {chunk_end}")

        page = 1
        while True:
            url = (
                f"{BASE_URL}/Home/MarketAnnouncements"
                f"?catName=All&sn=&fromdate={from_dt}&todate={chunk_end}"
                f"&page={page}&pageSize=100"
            )
            try:
                r = session.get(url, timeout=30)
                r.raise_for_status()
            except Exception as e:
                logger.warning(f"  Request failed page {page}: {e}")
                break

            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.find_all(
                "div",
                class_=lambda c: c and "list-group-item" in c and "hoverable" in c,
            )

            if not items:
                break

            for item in items:
                try:
                    # Symbol
                    sym_link = item.find("a", href=re.compile("/Company/"))
                    symbol = sym_link.get_text(strip=True).rstrip("\xa0") if sym_link else "EXCH"

                    # Date
                    date_span = item.find(
                        "span", class_="small text-dark font-weight-bold"
                    )
                    date_text = date_span.get_text(strip=True) if date_span else ""
                    try:
                        ann_date = datetime.strptime(date_text, "%d %b, %Y").date()
                    except ValueError:
                        ann_date = from_dt

                    # Details
                    p = item.find("p")
                    details = p.get_text(strip=True) if p else ""
                    if not details:
                        continue

                    event_type = normalize_event_type(details)
                    amount = extract_amount(details)
                    title = details[:200] if len(details) > 200 else details

                    cur.execute(
                        """
                        INSERT INTO corporate_events (date, symbol, event_type, title, details, amount, source)
                        VALUES (%s, %s, %s, %s, %s, %s, 'lankabd')
                        ON CONFLICT (date, symbol, event_type, title) DO NOTHING
                        """,
                        (ann_date, symbol, event_type, title, details, amount),
                    )
                    if cur.rowcount > 0:
                        total_inserted += 1
                    else:
                        total_skipped += 1

                except Exception as e:
                    logger.debug(f"  Error parsing item: {e}")
                    continue

            conn.commit()

            # Check for next page
            next_link = soup.find("a", class_="page-link", string=re.compile("NEXT", re.I))
            if next_link and len(items) == 100:
                page += 1
                time.sleep(RATE_LIMIT)
            else:
                break

        from_dt = chunk_end + timedelta(days=1)
        time.sleep(RATE_LIMIT)

    conn.close()
    logger.info(
        f"Announcements done: {total_inserted} inserted, {total_skipped} skipped (duplicates)"
    )
    return total_inserted


# ── B. Declarations ────────────────────────────────────────
def scrape_declarations(session):
    """Scrape latest declarations table from LankaDB.

    This provides dividend amounts, record dates, AGM dates etc.
    """
    logger.info("Scraping declarations...")
    conn = get_conn()
    cur = conn.cursor()

    url = f"{BASE_URL}/Details/GetLatestDeclarations"
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch declarations: {e}")
        return 0

    soup = BeautifulSoup(r.text, "html.parser")

    # Find the data table (the one with >10 rows)
    tables = soup.find_all("table")
    data_table = None
    for t in tables:
        rows = t.find_all("tr")
        if len(rows) > 10:
            data_table = t
            break

    if not data_table:
        logger.warning("No declaration table found")
        return 0

    rows = data_table.find_all("tr")
    # Skip header row
    header_cells = rows[0].find_all(["th", "td"])
    headers = [h.get_text(strip=True) for h in header_cells]
    logger.info(f"  Declaration headers: {headers}")

    total_inserted = 0
    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) < 6:
            continue

        try:
            values = [c.get_text(strip=True) for c in cells]
            # Columns: Publish Date, Symbol, Sector, Year, Dividend Type,
            #          Cash Dividend %, RIU/Stock Dividend %, EPS/EPU, NAV,
            #          Record Date, AGM Date, Year End
            pub_date_str = values[0] if len(values) > 0 else ""
            symbol = values[1] if len(values) > 1 else ""
            div_type = values[4] if len(values) > 4 else ""
            cash_div = values[5] if len(values) > 5 else ""
            stock_div = values[6] if len(values) > 6 else ""
            record_date = values[9] if len(values) > 9 else ""
            agm_date = values[10] if len(values) > 10 else ""

            if not symbol or symbol == "-":
                continue

            # Parse publish date
            try:
                pub_date = datetime.strptime(pub_date_str, "%Y-%m-%d").date()
            except ValueError:
                pub_date = date.today()

            # Insert cash dividend event
            if cash_div and cash_div != "0" and cash_div != "-":
                try:
                    cash_amt = float(cash_div)
                except ValueError:
                    cash_amt = None
                title = f"{div_type} Cash Dividend: {cash_div}%"
                details = f"Cash Dividend: {cash_div}%, Stock Dividend: {stock_div}%"
                if record_date and record_date != "-":
                    details += f", Record Date: {record_date}"
                if agm_date and agm_date != "-":
                    details += f", AGM: {agm_date}"

                cur.execute(
                    """
                    INSERT INTO corporate_events (date, symbol, event_type, title, details, amount, source)
                    VALUES (%s, %s, %s, %s, %s, %s, 'lankabd_decl')
                    ON CONFLICT (date, symbol, event_type, title) DO NOTHING
                    """,
                    (pub_date, symbol, "CASH_DIVIDEND", title, details, cash_amt),
                )
                if cur.rowcount > 0:
                    total_inserted += 1

            # Insert stock dividend event
            if stock_div and stock_div != "0" and stock_div != "-":
                try:
                    stock_amt = float(stock_div)
                except ValueError:
                    stock_amt = None
                title = f"{div_type} Stock Dividend: {stock_div}%"
                details = f"Stock Dividend: {stock_div}%, Cash Dividend: {cash_div}%"
                if record_date and record_date != "-":
                    details += f", Record Date: {record_date}"

                cur.execute(
                    """
                    INSERT INTO corporate_events (date, symbol, event_type, title, details, amount, source)
                    VALUES (%s, %s, %s, %s, %s, %s, 'lankabd_decl')
                    ON CONFLICT (date, symbol, event_type, title) DO NOTHING
                    """,
                    (pub_date, symbol, "STOCK_DIVIDEND", title, details, stock_amt),
                )
                if cur.rowcount > 0:
                    total_inserted += 1

            # Insert record date event
            if record_date and record_date != "-":
                try:
                    rd = datetime.strptime(record_date, "%Y-%m-%d").date()
                    title = f"Record Date for {div_type} dividend"
                    cur.execute(
                        """
                        INSERT INTO corporate_events (date, symbol, event_type, title, details, amount, source)
                        VALUES (%s, %s, %s, %s, %s, NULL, 'lankabd_decl')
                        ON CONFLICT (date, symbol, event_type, title) DO NOTHING
                        """,
                        (rd, symbol, "RECORD_DATE", title, f"Dividend: Cash {cash_div}%, Stock {stock_div}%"),
                    )
                    if cur.rowcount > 0:
                        total_inserted += 1
                except ValueError:
                    pass

            # Insert AGM event
            if agm_date and agm_date != "-":
                try:
                    ad = datetime.strptime(agm_date, "%Y-%m-%d").date()
                    title = f"AGM ({div_type})"
                    cur.execute(
                        """
                        INSERT INTO corporate_events (date, symbol, event_type, title, details, amount, source)
                        VALUES (%s, %s, %s, %s, %s, NULL, 'lankabd_decl')
                        ON CONFLICT (date, symbol, event_type, title) DO NOTHING
                        """,
                        (ad, symbol, "AGM", title, f"Dividend: Cash {cash_div}%, Stock {stock_div}%"),
                    )
                    if cur.rowcount > 0:
                        total_inserted += 1
                except ValueError:
                    pass

        except Exception as e:
            logger.debug(f"  Error parsing declaration row: {e}")
            continue

    conn.commit()
    conn.close()
    logger.info(f"Declarations done: {total_inserted} events inserted")
    return total_inserted


# ── C. News Headlines ─────────────────────────────────────
NEWS_CATEGORIES = {
    1: "Stock_Market",
    2: "Local_Economy",
    3: "Business_&_Corporate",
}


def extract_symbols_mentioned(text, known_symbols=None):
    """Extract DSE stock symbols mentioned in news text."""
    if known_symbols is None:
        return []
    # Look for uppercase words that match known symbols
    words = set(re.findall(r"\b([A-Z]{2,15})\b", text))
    return sorted(words & known_symbols)


def scrape_news(session, known_symbols=None, from_date="2024-01-01", max_pages=50):
    """Scrape news headlines from LankaDB."""
    logger.info(f"Scraping news headlines (from {from_date}, max {max_pages} pages/cat)...")
    conn = get_conn()
    cur = conn.cursor()

    total_inserted = 0

    for news_type, cat_name in NEWS_CATEGORIES.items():
        logger.info(f"  Category: {cat_name}")
        page = 1
        pages_scraped = 0

        while page <= max_pages:
            url = (
                f"{BASE_URL}/Home/news"
                f"?catName={cat_name}&newsType={news_type}"
                f"&fromDate={from_date}&toDate=&page={page}&pageSize=100"
            )
            try:
                r = session.get(url, timeout=30)
                r.raise_for_status()
            except Exception as e:
                logger.warning(f"  News request failed page {page}: {e}")
                break

            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.find_all(
                "div",
                class_=lambda c: c and "list-group-item" in c and "hoverable" in c,
            )

            if not items:
                break

            for item in items:
                try:
                    # Title
                    headline = item.find("div", class_="headline")
                    title = headline.get_text(strip=True) if headline else ""
                    if not title:
                        # Fallback: first text block
                        title = item.get_text(strip=True)[:200]

                    # Date + source
                    strong = item.find("strong")
                    date_source = strong.get_text(strip=True) if strong else ""
                    # Parse "12 Mar 2026; Source: The Business Standard"
                    date_match = re.match(
                        r"(\d{1,2}\s+\w+\s+\d{4})", date_source
                    )
                    try:
                        news_date = (
                            datetime.strptime(date_match.group(1), "%d %b %Y").date()
                            if date_match
                            else date.today()
                        )
                    except ValueError:
                        news_date = date.today()

                    source_match = re.search(r"Source:\s*(.+?)$", date_source)
                    news_source = (
                        source_match.group(1).strip() if source_match else "lankabd"
                    )

                    # Content (paragraph)
                    p = item.find("p")
                    content = p.get_text(strip=True) if p else ""

                    # URL
                    ext_link = item.find("a", href=re.compile("^https?://"))
                    news_url = ext_link.get("href") if ext_link else None

                    # Symbols mentioned
                    full_text = f"{title} {content}"
                    symbols = (
                        extract_symbols_mentioned(full_text, known_symbols)
                        if known_symbols
                        else []
                    )

                    if not title:
                        continue

                    cur.execute(
                        """
                        INSERT INTO market_news (date, source, category, title, content, url, symbols_mentioned)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date, title) DO NOTHING
                        """,
                        (
                            news_date,
                            news_source,
                            cat_name.replace("_", " "),
                            title,
                            content,
                            news_url,
                            symbols if symbols else None,
                        ),
                    )
                    if cur.rowcount > 0:
                        total_inserted += 1

                except Exception as e:
                    logger.debug(f"  Error parsing news item: {e}")
                    continue

            conn.commit()
            pages_scraped += 1

            # Check for next page
            next_link = soup.find(
                "a", class_="page-link", string=re.compile("NEXT", re.I)
            )
            if next_link:
                page += 1
                time.sleep(RATE_LIMIT)
            else:
                break

        logger.info(f"    {pages_scraped} pages scraped")
        time.sleep(RATE_LIMIT)

    conn.close()
    logger.info(f"News done: {total_inserted} headlines inserted")
    return total_inserted


# ── D. Market Holidays ─────────────────────────────────────
# Bangladesh market holidays 2024-2026 (manually compiled)
# Friday + Saturday are regular weekends (not included here)
HOLIDAYS = [
    # 2024
    ("2024-01-01", "New Year's Day", "PUBLIC"),
    ("2024-02-05", "Shab-e-Meraj", "PUBLIC"),
    ("2024-02-21", "International Mother Language Day / Shaheed Day", "PUBLIC"),
    ("2024-02-22", "Shab-e-Barat", "PUBLIC"),
    ("2024-03-17", "Sheikh Mujibur Rahman Birthday", "PUBLIC"),
    ("2024-03-26", "Independence Day", "PUBLIC"),
    ("2024-03-28", "Shab-e-Qadr", "PUBLIC"),
    ("2024-04-01", "Bank Holiday", "PUBLIC"),
    ("2024-04-11", "Eid ul-Fitr", "EID"),
    ("2024-04-12", "Eid ul-Fitr", "EID"),
    ("2024-04-13", "Eid ul-Fitr", "EID"),
    ("2024-04-14", "Bengali New Year (Pahela Boishakh)", "PUBLIC"),
    ("2024-04-15", "Eid ul-Fitr (extended)", "EID"),
    ("2024-05-01", "May Day", "PUBLIC"),
    ("2024-05-22", "Buddha Purnima", "PUBLIC"),
    ("2024-06-17", "Eid ul-Adha", "EID"),
    ("2024-06-18", "Eid ul-Adha", "EID"),
    ("2024-06-19", "Eid ul-Adha", "EID"),
    ("2024-07-08", "Muharram (Ashura)", "PUBLIC"),
    ("2024-07-17", "Muharram (Ashura)", "PUBLIC"),
    ("2024-08-15", "National Mourning Day", "PUBLIC"),
    ("2024-09-16", "Eid-e-Milad-un-Nabi", "PUBLIC"),
    ("2024-10-13", "Durga Puja (Bijoya Dashami)", "PUBLIC"),
    ("2024-10-14", "Durga Puja", "PUBLIC"),
    ("2024-11-04", "Shab-e-Meraj", "PUBLIC"),
    ("2024-12-16", "Victory Day", "PUBLIC"),
    ("2024-12-25", "Christmas Day", "PUBLIC"),
    # 2025
    ("2025-01-01", "New Year's Day", "PUBLIC"),
    ("2025-01-27", "Shab-e-Meraj", "PUBLIC"),
    ("2025-02-12", "Shab-e-Barat", "PUBLIC"),
    ("2025-02-21", "International Mother Language Day / Shaheed Day", "PUBLIC"),
    ("2025-03-26", "Independence Day", "PUBLIC"),
    ("2025-03-28", "Shab-e-Qadr", "PUBLIC"),
    ("2025-03-31", "Eid ul-Fitr", "EID"),
    ("2025-04-01", "Eid ul-Fitr", "EID"),
    ("2025-04-02", "Eid ul-Fitr", "EID"),
    ("2025-04-03", "Eid ul-Fitr (extended)", "EID"),
    ("2025-04-14", "Bengali New Year (Pahela Boishakh)", "PUBLIC"),
    ("2025-05-01", "May Day", "PUBLIC"),
    ("2025-05-12", "Buddha Purnima", "PUBLIC"),
    ("2025-06-07", "Eid ul-Adha", "EID"),
    ("2025-06-08", "Eid ul-Adha", "EID"),
    ("2025-06-09", "Eid ul-Adha", "EID"),
    ("2025-06-10", "Eid ul-Adha (extended)", "EID"),
    ("2025-07-06", "Muharram (Ashura)", "PUBLIC"),
    ("2025-08-15", "National Mourning Day", "PUBLIC"),
    ("2025-09-05", "Eid-e-Milad-un-Nabi", "PUBLIC"),
    ("2025-10-02", "Durga Puja (Bijoya Dashami)", "PUBLIC"),
    ("2025-10-03", "Durga Puja", "PUBLIC"),
    ("2025-12-16", "Victory Day", "PUBLIC"),
    ("2025-12-25", "Christmas Day", "PUBLIC"),
    # 2026
    ("2026-01-01", "New Year's Day", "PUBLIC"),
    ("2026-01-16", "Shab-e-Meraj", "PUBLIC"),
    ("2026-02-02", "Shab-e-Barat", "PUBLIC"),
    ("2026-02-21", "International Mother Language Day / Shaheed Day", "PUBLIC"),
    ("2026-03-17", "Shab-e-Qadr", "PUBLIC"),
    ("2026-03-20", "Eid ul-Fitr", "EID"),
    ("2026-03-21", "Eid ul-Fitr", "EID"),
    ("2026-03-22", "Eid ul-Fitr", "EID"),
    ("2026-03-23", "Eid ul-Fitr (extended)", "EID"),
    ("2026-03-26", "Independence Day", "PUBLIC"),
    ("2026-04-14", "Bengali New Year (Pahela Boishakh)", "PUBLIC"),
    ("2026-05-01", "May Day", "PUBLIC"),
    ("2026-05-02", "Buddha Purnima", "PUBLIC"),
    ("2026-05-28", "Eid ul-Adha", "EID"),
    ("2026-05-29", "Eid ul-Adha", "EID"),
    ("2026-05-30", "Eid ul-Adha", "EID"),
    ("2026-05-31", "Eid ul-Adha (extended)", "EID"),
    ("2026-06-26", "Muharram (Ashura)", "PUBLIC"),
    ("2026-08-15", "National Mourning Day", "PUBLIC"),
    ("2026-08-26", "Eid-e-Milad-un-Nabi", "PUBLIC"),
    ("2026-09-21", "Durga Puja (Bijoya Dashami)", "PUBLIC"),
    ("2026-09-22", "Durga Puja", "PUBLIC"),
    ("2026-12-16", "Victory Day", "PUBLIC"),
    ("2026-12-25", "Christmas Day", "PUBLIC"),
]


def insert_holidays():
    """Insert Bangladesh market holidays."""
    logger.info("Inserting market holidays...")
    conn = get_conn()
    cur = conn.cursor()

    inserted = 0
    for date_str, name, htype in HOLIDAYS:
        cur.execute(
            """
            INSERT INTO market_holidays (date, name, type)
            VALUES (%s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET name = EXCLUDED.name, type = EXCLUDED.type
            """,
            (date_str, name, htype),
        )
        if cur.rowcount > 0:
            inserted += 1

    conn.commit()
    conn.close()
    logger.info(f"Holidays: {inserted} inserted/updated out of {len(HOLIDAYS)} total")
    return inserted


# ── Main ───────────────────────────────────────────────────
def main():
    import argparse

    parser = argparse.ArgumentParser(description="LankaDB News & Events Scraper")
    parser.add_argument(
        "--quick", action="store_true",
        help="Quick mode: only last 7 days of announcements, 5 pages of news per category",
    )
    parser.add_argument(
        "--days", type=int, default=None,
        help="Lookback days for announcements and news (default: 7 for --quick, all for full)",
    )
    args = parser.parse_args()

    quick = args.quick
    lookback_days = args.days or (7 if quick else None)

    t_start = time.time()
    mode = "QUICK" if quick else "FULL"
    print("=" * 60)
    print(f"LankaDB News & Events Scraper ({mode})")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # 1. Create tables
    create_tables()

    # 2. Load known symbols for news matching
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM fundamentals")
    known_symbols = {r["symbol"] for r in cur.fetchall()}
    conn.close()
    logger.info(f"Known symbols: {len(known_symbols)}")

    # 3. Create session
    session, token = create_session()
    logger.info("Session created with cookies")

    # 4. Scrape announcements
    if lookback_days:
        start = (date.today() - timedelta(days=lookback_days)).isoformat()
    else:
        start = "2024-01-01"
    ann_count = scrape_announcements(session, start)

    # 5. Scrape declarations
    time.sleep(RATE_LIMIT)
    decl_count = scrape_declarations(session)

    # 6. Scrape news
    time.sleep(RATE_LIMIT)
    news_from = (date.today() - timedelta(days=lookback_days)).isoformat() if lookback_days else "2024-01-01"
    news_max_pages = 5 if quick else 50
    news_count = scrape_news(session, known_symbols, from_date=news_from, max_pages=news_max_pages)

    # 7. Insert holidays
    holiday_count = insert_holidays()

    # Summary
    elapsed = time.time() - t_start
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Mode:                   {mode}")
    print(f"  Announcements inserted: {ann_count}")
    print(f"  Declarations inserted:  {decl_count}")
    print(f"  News headlines inserted: {news_count}")
    print(f"  Holidays inserted:      {holiday_count}")
    print(f"  Total time: {elapsed:.1f}s")

    # Verify counts
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as cnt FROM corporate_events")
    print(f"\n  corporate_events total rows: {cur.fetchone()['cnt']}")
    cur.execute("SELECT COUNT(*) as cnt FROM market_news")
    print(f"  market_news total rows: {cur.fetchone()['cnt']}")
    cur.execute("SELECT MAX(date) as d FROM market_news")
    print(f"  market_news latest: {cur.fetchone()['d']}")
    cur.execute("SELECT MAX(date) as d FROM corporate_events")
    print(f"  corporate_events latest: {cur.fetchone()['d']}")
    conn.close()


if __name__ == "__main__":
    main()
