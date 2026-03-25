"""DSE Official News Scraper.

Scrapes company announcements directly from dsebd.org.
These are the FIRST source of news — published minutes after companies file.

Covers: board meetings, dividends, partnerships, earnings, record dates,
stock suspensions, name changes, and all other DSE announcements.

Usage:
    from scripts.dse_news_scraper import scrape_dse_news
    scrape_dse_news()  # fetches latest and stores in market_news
"""

import logging
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DSE_BASE = "https://www.dsebd.org"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}

# Known DSE stock symbols (loaded once)
_known_symbols: set[str] = set()


def _load_symbols():
    """Load known symbols from DB."""
    global _known_symbols
    if _known_symbols:
        return
    try:
        from database import get_connection
        conn = get_connection()
        rows = conn.execute("SELECT symbol FROM fundamentals").fetchall()
        conn.close()
        _known_symbols = {r["symbol"] for r in rows}
    except Exception:
        pass


def _extract_symbols(text: str) -> list[str]:
    """Extract stock symbols mentioned in text."""
    _load_symbols()
    found = []
    # Check if text starts with a known symbol (DSE format: "SYMBOL: announcement")
    for sym in _known_symbols:
        if text.upper().startswith(sym + ":") or text.upper().startswith(sym + " "):
            found.append(sym)
        elif f" {sym} " in f" {text.upper()} ":
            found.append(sym)
    return list(set(found))


def _categorize(title: str) -> str:
    """Categorize announcement by content."""
    t = title.upper()
    if any(w in t for w in ["DIVIDEND", "CASH DIVIDEND", "STOCK DIVIDEND"]):
        return "DIVIDEND"
    if any(w in t for w in ["AGM", "EGM", "ANNUAL GENERAL", "EXTRAORDINARY GENERAL"]):
        return "AGM"
    if any(w in t for w in ["RECORD DATE", "BOOK CLOSURE"]):
        return "RECORD_DATE"
    if any(w in t for w in ["BOARD MEETING", "BOARD OF DIRECTOR"]):
        return "BOARD_MEETING"
    if any(w in t for w in ["EPS", "EARNINGS", "FINANCIAL STATEMENT", "UN-AUDITED", "AUDITED"]):
        return "EARNINGS"
    if any(w in t for w in ["SPOT", "SUSPEND", "RESUME", "HALT"]):
        return "TRADING_STATUS"
    if any(w in t for w in ["NAV", "NET ASSET"]):
        return "NAV"
    if any(w in t for w in ["PARTNER", "MOU", "AGREEMENT", "JOINT VENTURE", "COLLABORATION"]):
        return "PARTNERSHIP"
    if any(w in t for w in ["RIGHT", "BONUS", "IPO", "OFFERING"]):
        return "CORPORATE_ACTION"
    if any(w in t for w in ["APPOINT", "RESIGN", "SECRETARY", "DIRECTOR"]):
        return "MANAGEMENT"
    if any(w in t for w in ["INSPECTION", "FACTORY", "VISIT"]):
        return "INSPECTION"
    return "OTHER"


def scrape_dse_news(max_pages: int = 3):
    """Scrape latest news from DSE official website.

    Fetches from:
    1. display_news.php (today's announcements on homepage)
    2. news_archive.php (recent days)

    Stores in market_news table with source='dse_official'.
    """
    from database import get_connection

    session = requests.Session()
    session.headers.update(HEADERS)

    total_inserted = 0
    today = datetime.now().strftime("%Y-%m-%d")

    # --- 1. Scrape today's news from main page ---
    try:
        r = session.get(f"{DSE_BASE}/", timeout=15)
        if r.ok:
            soup = BeautifulSoup(r.text, "html.parser")
            news_items = []

            # DSE puts news in display_news.php links
            for a in soup.find_all("a"):
                href = a.get("href", "")
                if "display_news.php" not in href:
                    continue
                text = a.get_text(strip=True)
                if not text or len(text) < 10:
                    continue
                # Skip generic DSE/BSEC awareness messages
                if any(skip in text for skip in ["Awareness Message", "Greetings Message", "Good morning"]):
                    continue

                symbols = _extract_symbols(text)
                category = _categorize(text)

                # Skip daily NAV reports (noise)
                if category == "NAV":
                    continue

                news_items.append({
                    "date": today,
                    "source": "dse_official",
                    "category": category,
                    "title": text[:500],
                    "content": text,
                    "symbols": symbols,
                })

            # Store in DB
            if news_items:
                conn = get_connection()
                for item in news_items:
                    try:
                        conn.execute(
                            """INSERT INTO market_news
                               (date, source, category, title, content, symbols_mentioned, created_at)
                               VALUES (%s, %s, %s, %s, %s, %s, NOW())
                               ON CONFLICT DO NOTHING""",
                            (item["date"], item["source"], item["category"],
                             item["title"], item["content"],
                             ",".join(item["symbols"]) if item["symbols"] else None),
                        )
                        total_inserted += 1
                    except Exception as e:
                        logger.debug(f"Insert failed: {e}")
                conn.execute("COMMIT")
                conn.close()
                logger.info(f"DSE homepage: {len(news_items)} announcements ({total_inserted} new)")

    except Exception as e:
        logger.error(f"DSE homepage scrape failed: {e}")

    # --- 2. Scrape news archive (last 7 days) ---
    try:
        r = session.get(f"{DSE_BASE}/news_archive_7days.php", timeout=15)
        if r.ok and len(r.text) > 1000:
            soup = BeautifulSoup(r.text, "html.parser")

            # Parse the archive table
            archive_count = 0
            current_date = today

            for row in soup.find_all("tr"):
                cells = row.find_all("td")
                if not cells:
                    continue

                text = row.get_text(strip=True)

                # Check if this is a date header
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', text)
                if date_match:
                    current_date = date_match.group(1)
                    continue

                # Check for news content
                if len(text) > 20:
                    symbols = _extract_symbols(text)
                    category = _categorize(text)
                    if category == "NAV":
                        continue

                    conn = get_connection()
                    try:
                        conn.execute(
                            """INSERT INTO market_news
                               (date, source, category, title, content, symbols_mentioned, created_at)
                               VALUES (%s, %s, %s, %s, %s, %s, NOW())
                               ON CONFLICT DO NOTHING""",
                            (current_date, "dse_official", category,
                             text[:500], text,
                             ",".join(symbols) if symbols else None),
                        )
                        archive_count += 1
                    except Exception:
                        pass
                    conn.execute("COMMIT")
                    conn.close()

            total_inserted += archive_count
            logger.info(f"DSE archive: {archive_count} items")

    except Exception as e:
        logger.error(f"DSE archive scrape failed: {e}")

    # --- 3. Scrape corporate announcements page ---
    try:
        r = session.get(f"{DSE_BASE}/corporate-announcement.php", timeout=15)
        if r.ok and len(r.text) > 1000:
            soup = BeautifulSoup(r.text, "html.parser")

            corp_count = 0
            for row in soup.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                # Try to extract date and content
                text = " ".join(c.get_text(strip=True) for c in cells)
                if len(text) < 20:
                    continue

                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', text)
                news_date = date_match.group(1) if date_match else today

                symbols = _extract_symbols(text)
                category = _categorize(text)
                if category == "NAV":
                    continue

                conn = get_connection()
                try:
                    conn.execute(
                        """INSERT INTO market_news
                           (date, source, category, title, content, symbols_mentioned, created_at)
                           VALUES (%s, %s, %s, %s, %s, %s, NOW())
                           ON CONFLICT DO NOTHING""",
                        (news_date, "dse_corporate", category,
                         text[:500], text,
                         ",".join(symbols) if symbols else None),
                    )
                    corp_count += 1
                except Exception:
                    pass
                conn.execute("COMMIT")
                conn.close()

            total_inserted += corp_count
            logger.info(f"DSE corporate: {corp_count} items")

    except Exception as e:
        logger.error(f"DSE corporate scrape failed: {e}")

    logger.info(f"DSE news scrape complete: {total_inserted} total items")
    return total_inserted


def run():
    """Entry point for cron/manual execution."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    count = scrape_dse_news()
    print(f"Scraped {count} DSE news items")


if __name__ == "__main__":
    run()
