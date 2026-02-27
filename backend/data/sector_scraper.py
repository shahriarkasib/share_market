"""Scrape DSE sector-to-company mapping from dsebd.org."""

import logging
import requests
from bs4 import BeautifulSoup
from database import get_connection

logger = logging.getLogger(__name__)

DSE_INDUSTRY_URL = "https://www.dsebd.org/by_industrylisting.php"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}

# Hardcoded fallback: sector name -> industryno on DSE site
SECTOR_IDS = {
    "Bank": 11, "Cement": 16, "Ceramics Sector": 18, "Corporate Bond": 46,
    "Debenture": 34, "Engineering": 1, "Financial Institutions": 12,
    "Food & Allied": 2, "Fuel & Power": 3, "Insurance": 13, "IT Sector": 15,
    "Jute": 4, "Miscellaneous": 14, "Mutual Funds": 19,
    "Paper & Printing": 5, "Pharmaceuticals & Chemicals": 6,
    "Services & Real Estate": 10, "Tannery Industries": 7,
    "Telecommunication": 20, "Textile": 8, "Travel & Leisure": 9,
    "G-Sec (T-Bond)": 48,
}


def scrape_sector_mapping() -> dict[str, list[str]]:
    """
    Scrape DSE website for sector → [symbol] mapping.
    Updates the fundamentals table with sector info.
    Returns dict of {sector_name: [symbols]}.
    """
    sector_map: dict[str, list[str]] = {}

    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        for sector_name, industry_no in SECTOR_IDS.items():
            try:
                url = f"https://www.dsebd.org/companylistbyindustry.php?industryno={industry_no}"
                resp = session.get(url, timeout=15)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")

                symbols = []
                table = soup.find("table", class_="table-responsive") or soup.find("table")
                if not table:
                    continue

                for tr in table.find_all("tr")[1:]:
                    cols = tr.find_all("td")
                    if len(cols) >= 2:
                        symbol = cols[1].get_text(strip=True)
                        company_name = cols[2].get_text(strip=True) if len(cols) >= 3 else ""
                        if symbol and symbol.isalpha() or "&" in symbol:
                            symbols.append((symbol, company_name))

                if symbols:
                    sector_map[sector_name] = [s[0] for s in symbols]
                    _upsert_sector_data(sector_name, symbols)
                    logger.info(f"Scraped {sector_name}: {len(symbols)} stocks")

            except Exception as e:
                logger.warning(f"Failed to scrape sector {sector_name}: {e}")
                continue

    except Exception as e:
        logger.error(f"Sector scraping failed: {e}")

    if not sector_map:
        logger.warning("Scraping returned empty, using fallback")
        sector_map = _load_from_db()

    return sector_map


def _upsert_sector_data(sector_name: str, stocks: list[tuple[str, str]]):
    """Insert/update sector and fundamentals data."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO sectors (name, stock_count, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
            (sector_name, len(stocks)),
        )
        for symbol, company_name in stocks:
            conn.execute(
                """INSERT INTO fundamentals (symbol, company_name, sector, updated_at)
                   VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(symbol) DO UPDATE SET
                     sector = excluded.sector,
                     company_name = COALESCE(excluded.company_name, fundamentals.company_name),
                     updated_at = CURRENT_TIMESTAMP""",
                (symbol, company_name or None, sector_name),
            )
        conn.commit()
    finally:
        conn.close()


def _load_from_db() -> dict[str, list[str]]:
    """Load existing sector mapping from DB as fallback."""
    conn = get_connection()
    rows = conn.execute("SELECT symbol, sector FROM fundamentals WHERE sector IS NOT NULL").fetchall()
    conn.close()
    result: dict[str, list[str]] = {}
    for r in rows:
        result.setdefault(r["sector"], []).append(r["symbol"])
    return result


def get_sector_map() -> dict[str, list[str]]:
    """Get sector mapping from DB (fast, no scraping)."""
    return _load_from_db()
