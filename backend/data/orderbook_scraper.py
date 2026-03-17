"""Order book / market depth scraper from LankaBD.

Fetches bid/ask prices and volumes for DSE stocks via LankaBD's
MarketDepthData AJAX endpoint. Stores snapshots every 5 minutes
during market hours.

Usage:
    from data.orderbook_scraper import fetch_market_depth, fetch_all_depths
"""

import logging
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

LANKABD_DEPTH_URL = "https://www.lankabd.com/Home/MarketDepthData"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "X-Requested-With": "XMLHttpRequest",
}


def _get_session() -> requests.Session:
    """Get a session with LankaBD cookies."""
    s = requests.Session()
    s.headers.update(HEADERS)
    # Visit homepage to get session cookie
    try:
        s.get("https://www.lankabd.com", timeout=10)
    except Exception:
        pass
    return s


def fetch_market_depth(session: requests.Session, symbol: str) -> dict | None:
    """Fetch order book for a single stock.

    Returns: {
        "symbol": "GP",
        "ltp": 254.5,
        "open": 253.0,
        "high": 255.2,
        "low": 252.0,
        "ycp": 253.0,
        "close": 254.5,
        "trades": 923,
        "volume": 89748,
        "value": 22753000,
        "bids": [{"price": 254.0, "volume": 500}, {"price": 253.5, "volume": 1200}, ...],
        "asks": [{"price": 255.0, "volume": 300}, {"price": 255.5, "volume": 800}, ...],
        "total_bid_volume": 5000,
        "total_ask_volume": 3200,
        "bid_ask_ratio": 1.56,
    }
    """
    try:
        r = session.post(
            LANKABD_DEPTH_URL,
            data={"Symbol": symbol, "Exchange": "DSE"},
            timeout=10,
        )
        if not r.ok:
            # Try GET
            r = session.get(
                LANKABD_DEPTH_URL,
                params={"Symbol": symbol, "Exchange": "DSE"},
                timeout=10,
            )
        if not r.ok or not r.text:
            return None

        data = r.json()

        # Parse buy/sell price tables from HTML
        bids = _parse_depth_table(data.get("buyPriceTable", ""))
        asks = _parse_depth_table(data.get("sellPriceTable", ""))

        total_bid = sum(b["volume"] for b in bids)
        total_ask = sum(a["volume"] for a in asks)

        return {
            "symbol": symbol,
            "ltp": _safe_float(data.get("lastTradePrice")),
            "open": _safe_float(data.get("openPrice")),
            "high": _safe_float(data.get("daysHigh")),
            "low": _safe_float(data.get("daysLow")),
            "ycp": _safe_float(data.get("yesterdayClosePrice")),
            "close": _safe_float(data.get("closePrice")),
            "trades": _safe_int(data.get("noOfTrade")),
            "volume": _safe_int(data.get("totalVolume")),
            "value": _safe_float(data.get("totalValueMN", 0)) * 1_000_000,  # MN → raw
            "bids": bids,
            "asks": asks,
            "total_bid_volume": total_bid,
            "total_ask_volume": total_ask,
            "bid_ask_ratio": round(total_bid / total_ask, 2) if total_ask > 0 else 0,
            "best_bid": bids[0]["price"] if bids else 0,
            "best_ask": asks[0]["price"] if asks else 0,
            "spread": round(asks[0]["price"] - bids[0]["price"], 1) if bids and asks else 0,
            "bid_levels": len(bids),
            "ask_levels": len(asks),
        }
    except Exception as e:
        logger.debug(f"Depth fetch failed for {symbol}: {e}")
        return None


def _parse_depth_table(html: str) -> list[dict]:
    """Parse bid/ask table HTML into [{price, volume}, ...]."""
    if not html:
        return []
    try:
        soup = BeautifulSoup(html, "html.parser")
        rows = []
        for tr in soup.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) >= 2:
                price = _safe_float(cells[0])
                volume = _safe_int(cells[1])
                if price > 0:
                    rows.append({"price": price, "volume": volume})
        return rows
    except Exception:
        return []


def _safe_float(val, default=0.0) -> float:
    if val is None:
        return default
    try:
        s = str(val).replace(",", "").strip()
        return float(s) if s else default
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0) -> int:
    if val is None:
        return default
    try:
        s = str(val).replace(",", "").strip()
        return int(float(s)) if s else default
    except (ValueError, TypeError):
        return default


def fetch_all_depths(symbols: list[str], delay: float = 0.3) -> list[dict]:
    """Fetch order book for multiple stocks with rate limiting.

    Args:
        symbols: List of stock symbols
        delay: Seconds between requests (rate limit)

    Returns: List of depth dicts (only successful ones)
    """
    session = _get_session()
    results = []
    for i, sym in enumerate(symbols):
        depth = fetch_market_depth(session, sym)
        if depth:
            results.append(depth)
        if i < len(symbols) - 1:
            time.sleep(delay)
        if (i + 1) % 50 == 0:
            logger.info(f"  Depth progress: {i + 1}/{len(symbols)}")
    return results
