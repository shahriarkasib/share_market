#!/usr/bin/env python3
"""Seed the remote Render backend with historical prices and sector data.

Usage:
    python seed_remote.py [--url https://share-market-kk7e.onrender.com]

This script:
1. Fetches historical price data from DSE via bdshare (local access works)
2. Scrapes sector-to-company mapping from dsebd.org
3. POSTs data in chunks to the remote backend's admin endpoints
"""

import argparse
import json
import math
import requests
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_URL = "https://share-market-kk7e.onrender.com"
CHUNK_SIZE = 500  # rows per POST request


def safe_val(v):
    """Convert NaN/inf to None for JSON."""
    if v is None:
        return None
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return v


def seed_historical_prices(base_url: str, days: int = 120):
    """Fetch and push historical prices."""
    from bdshare import get_historical_data

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    logger.info(f"Fetching historical data {start_date} to {end_date}...")

    # Try batch download first
    try:
        df = get_historical_data(start=start_date, end=end_date)
        if df is not None and not df.empty:
            df = df.reset_index()
            logger.info(f"Got {len(df)} rows via batch download")
        else:
            df = None
    except Exception as e:
        logger.warning(f"Batch download failed: {e}")
        df = None

    # Fallback: fetch top stocks individually
    if df is None:
        logger.info("Trying per-stock download...")
        # Get symbols from live_prices endpoint
        try:
            resp = requests.get(f"{base_url}/api/v1/market/prices", timeout=30)
            prices = resp.json()
            symbols = [p["symbol"] for p in prices[:100]]
        except Exception:
            symbols = [
                "ROBI", "GP", "BATBC", "BXPHARMA", "BERGERPBL", "SQURPHARMA",
                "RENATA", "MARICO", "LHBL", "BSRMSTEEL", "ISLAMIBANK", "BRACBANK",
                "DUTCHBANGL", "EBL", "WALTONHIL", "BEXIMCO", "ICB", "OLYMPIC",
                "POWERGRID", "UPGDCL", "SUMMIT", "MJLBD", "DBH", "IDLC",
            ]
        import pandas as pd
        all_dfs = []
        for i, sym in enumerate(symbols):
            try:
                sdf = get_historical_data(start=start_date, end=end_date, code=sym)
                if sdf is not None and not sdf.empty:
                    sdf = sdf.reset_index()
                    all_dfs.append(sdf)
            except Exception:
                pass
            if (i + 1) % 20 == 0:
                logger.info(f"  Progress: {i + 1}/{len(symbols)}")
        if all_dfs:
            df = pd.concat(all_dfs, ignore_index=True)
            logger.info(f"Got {len(df)} rows via per-stock download")
        else:
            logger.error("No historical data fetched")
            return

    # Normalize columns
    if "ltp" in df.columns and "close" in df.columns:
        df = df.drop(columns=["close"])
    col_map = {"ltp": "close", "trade": "trade_count"}
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Convert to JSON-safe rows
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "symbol": str(row.get("symbol", "")),
            "date": str(row.get("date", ""))[:10],
            "open": safe_val(row.get("open")),
            "high": safe_val(row.get("high")),
            "low": safe_val(row.get("low")),
            "close": safe_val(row.get("close")),
            "volume": int(row.get("volume", 0) or 0),
            "value": safe_val(row.get("value")),
            "trade_count": int(row.get("trade_count", 0) or 0),
        })

    # POST in chunks
    total_inserted = 0
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        try:
            resp = requests.post(
                f"{base_url}/api/v1/admin/seed-prices",
                json={"rows": chunk},
                timeout=60,
            )
            result = resp.json()
            total_inserted += result.get("inserted", 0)
            logger.info(f"  Chunk {i // CHUNK_SIZE + 1}: {result}")
        except Exception as e:
            logger.error(f"  Chunk {i // CHUNK_SIZE + 1} failed: {e}")

    logger.info(f"Historical prices seeded: {total_inserted} rows")


def seed_sectors(base_url: str):
    """Scrape sector mapping from DSE and push to remote."""
    from bs4 import BeautifulSoup

    SECTOR_IDS = {
        "Bank": 11, "Cement": 16, "Ceramics Sector": 18, "Engineering": 1,
        "Financial Institutions": 12, "Food & Allied": 2, "Fuel & Power": 3,
        "Insurance": 13, "IT Sector": 15, "Jute": 4, "Miscellaneous": 14,
        "Mutual Funds": 19, "Paper & Printing": 5,
        "Pharmaceuticals & Chemicals": 6, "Services & Real Estate": 10,
        "Tannery Industries": 7, "Telecommunication": 20, "Textile": 8,
        "Travel & Leisure": 9,
    }

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    })

    sector_map = {}
    for sector_name, industry_no in SECTOR_IDS.items():
        try:
            url = f"https://www.dsebd.org/companylistbyindustry.php?industryno={industry_no}"
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            stocks = []
            table = soup.find("table", class_="table-responsive") or soup.find("table")
            if not table:
                continue

            for tr in table.find_all("tr")[1:]:
                cols = tr.find_all("td")
                if len(cols) >= 2:
                    symbol = cols[1].get_text(strip=True)
                    company_name = cols[2].get_text(strip=True) if len(cols) >= 3 else ""
                    if symbol and (symbol.isalpha() or "&" in symbol):
                        stocks.append({"symbol": symbol, "company_name": company_name})

            if stocks:
                sector_map[sector_name] = stocks
                logger.info(f"  {sector_name}: {len(stocks)} stocks")

        except Exception as e:
            logger.warning(f"  Failed {sector_name}: {e}")

    if not sector_map:
        logger.error("No sectors scraped")
        return

    # POST to remote
    try:
        resp = requests.post(
            f"{base_url}/api/v1/admin/seed-sectors",
            json={"sectors": sector_map},
            timeout=60,
        )
        logger.info(f"Sector seeding result: {resp.json()}")
    except Exception as e:
        logger.error(f"Sector POST failed: {e}")


def trigger_signals(base_url: str):
    """Trigger signal computation on remote."""
    try:
        resp = requests.post(f"{base_url}/api/v1/admin/init", timeout=30)
        logger.info(f"Signal computation trigger: {resp.json()}")
    except Exception as e:
        logger.error(f"Signal trigger failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Seed remote backend with DSE data")
    parser.add_argument("--url", default=DEFAULT_URL, help="Backend URL")
    parser.add_argument("--skip-prices", action="store_true", help="Skip historical prices")
    parser.add_argument("--skip-sectors", action="store_true", help="Skip sector mapping")
    parser.add_argument("--skip-signals", action="store_true", help="Skip signal trigger")
    args = parser.parse_args()

    logger.info(f"Seeding remote backend at {args.url}")

    if not args.skip_prices:
        seed_historical_prices(args.url)

    if not args.skip_sectors:
        seed_sectors(args.url)

    if not args.skip_signals:
        logger.info("Triggering signal computation...")
        trigger_signals(args.url)

    # Final health check
    try:
        resp = requests.get(f"{args.url}/health", timeout=15)
        logger.info(f"Final health: {resp.json()}")
    except Exception as e:
        logger.error(f"Health check failed: {e}")


if __name__ == "__main__":
    main()
