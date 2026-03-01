"""Unified DSE data fetcher with fallback chain."""

import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, time as dtime
import logging
import time as time_module
import pytz

logger = logging.getLogger(__name__)

# DSE website URLs for scraping
DSE_LATEST_PRICE_URL = "https://www.dsebd.org/latest_share_price_scroll_l.php"
DSE_MARKET_SUMMARY_URL = "https://www.dsebd.org/"
DSE_COMPANY_URL = "https://www.dsebd.org/displayCompany.php?name="

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


class DSEDataFetcher:
    """Fetches stock data from DSE using web scraping with bdshare as primary source."""

    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update(HEADERS)
        self._bdshare_available = self._check_bdshare()

    def _check_bdshare(self) -> bool:
        """Check if bdshare package is available."""
        try:
            from bdshare import get_current_trade_data
            return True
        except ImportError:
            logger.warning("bdshare not available, using scraping fallback")
            return False

    def get_live_prices(self) -> pd.DataFrame:
        """
        Get current prices for all DSE listed stocks.
        Returns DataFrame with: symbol, ltp, high, low, open, close_prev, change, change_pct, volume, value, trade_count
        """
        if self._bdshare_available:
            try:
                return self._get_live_prices_bdshare()
            except Exception as e:
                logger.error(f"bdshare failed: {e}, falling back to scraping")

        return self._get_live_prices_scrape()

    def _get_live_prices_bdshare(self) -> pd.DataFrame:
        """Fetch live prices using bdshare."""
        from bdshare import get_current_trade_data
        df = get_current_trade_data()
        if df is None or df.empty:
            raise ValueError("bdshare returned empty data")

        # Standardize column names
        column_map = {
            "TRADING CODE": "symbol",
            "trading_code": "symbol",
            "LTP": "ltp",
            "ltp": "ltp",
            "HIGH": "high",
            "high": "high",
            "LOW": "low",
            "low": "low",
            "OPENP": "open",
            "openp": "open",
            "CLOSEP": "close_prev",
            "closep": "close_prev",
            "YCP": "close_prev",
            "ycp": "close_prev",
            "CHANGE": "change",
            "change": "change",
            "TRADE": "trade_count",
            "trade": "trade_count",
            "VALUE": "value",
            "value": "value",
            "VOLUME": "volume",
            "volume": "volume",
        }

        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

        # Ensure numeric columns
        numeric_cols = ["ltp", "high", "low", "open", "close_prev", "change", "volume", "value", "trade_count"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Compute signed change and change_pct from LTP vs close_prev
        # (bdshare 'change' column is unsigned / unreliable)
        if "ltp" in df.columns and "close_prev" in df.columns:
            df["change"] = (df["ltp"] - df["close_prev"]).round(2)
            mask = df["close_prev"] > 0
            df.loc[mask, "change_pct"] = ((df.loc[mask, "ltp"] - df.loc[mask, "close_prev"]) / df.loc[mask, "close_prev"] * 100).round(2)
            df.loc[~mask, "change_pct"] = 0.0

        # bdshare doesn't return 'open' — use close_prev (YCP) as proxy
        if "open" not in df.columns or df["open"].isna().all() or (df["open"] == 0).all():
            df["open"] = df.get("close_prev", 0)

        return df

    def _get_live_prices_scrape(self) -> pd.DataFrame:
        """Fallback: scrape live prices from dsebd.org."""
        try:
            resp = self._session.get(DSE_LATEST_PRICE_URL, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            table = soup.find("table", {"class": "table-responsive"})
            if not table:
                table = soup.find("table")
            if not table:
                logger.error("Could not find price table on DSE website")
                return pd.DataFrame()

            rows = []
            for tr in table.find_all("tr")[1:]:  # Skip header
                cols = [td.get_text(strip=True) for td in tr.find_all("td")]
                if len(cols) >= 10:
                    try:
                        symbol = cols[1]
                        ltp = float(cols[2].replace(",", "")) if cols[2] else 0
                        high = float(cols[3].replace(",", "")) if cols[3] else 0
                        low = float(cols[4].replace(",", "")) if cols[4] else 0
                        close_prev = float(cols[5].replace(",", "")) if cols[5] else 0
                        change = float(cols[6].replace(",", "")) if cols[6] else 0
                        trade_count = int(cols[7].replace(",", "")) if cols[7] else 0
                        value_str = cols[8].replace(",", "") if cols[8] else "0"
                        value = float(value_str)
                        volume = int(cols[9].replace(",", "")) if cols[9] else 0

                        change_pct = (change / close_prev * 100) if close_prev else 0

                        rows.append({
                            "symbol": symbol,
                            "ltp": ltp,
                            "high": high,
                            "low": low,
                            "open": high,  # DSE doesn't always show open
                            "close_prev": close_prev,
                            "change": change,
                            "change_pct": round(change_pct, 2),
                            "volume": volume,
                            "value": value,
                            "trade_count": trade_count,
                        })
                    except (ValueError, IndexError) as e:
                        continue

            return pd.DataFrame(rows)

        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            return pd.DataFrame()

    def get_historical(self, symbol: str, days: int = 365) -> pd.DataFrame:
        """
        Get historical OHLCV data for a symbol.
        Returns DataFrame with: date, open, high, low, close, volume
        """
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        if self._bdshare_available:
            try:
                return self._get_historical_bdshare(symbol, start_date, end_date)
            except Exception as e:
                logger.error(f"bdshare historical failed for {symbol}: {e}")

        return self._get_historical_scrape(symbol, start_date, end_date)

    def _get_historical_bdshare(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """Fetch historical data using bdshare."""
        from bdshare import get_historical_data
        df = get_historical_data(start=start, end=end, code=symbol)

        if df is None or df.empty:
            raise ValueError(f"No historical data for {symbol}")

        # bdshare returns: index=date, columns: symbol, ltp, high, low, open, close, ycp, trade, value, volume
        # Reset index to get date as column
        df = df.reset_index()

        # Standardize columns
        column_map = {
            "date": "date",
            "ltp": "close",  # Use LTP as close price
            "high": "high",
            "low": "low",
            "open": "open",
            "close": "close_price",  # DSE close might be different from LTP
            "ycp": "close_prev",
            "trade": "trade_count",
            "value": "value",
            "volume": "volume",
        }
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

        # If 'open' column has no data, use close_price or close
        if "open" in df.columns:
            df["open"] = pd.to_numeric(df["open"], errors="coerce")
            # Fill missing opens with close
            if df["open"].isna().all() and "close" in df.columns:
                df["open"] = df["close"]

        numeric_cols = ["open", "high", "low", "close", "volume", "value"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)

        return df

    def _get_historical_scrape(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """Fallback: scrape historical from DSE data archive."""
        # DSE data archive page
        try:
            url = f"https://www.dsebd.org/data_archive/data_archive.php"
            # This is complex to scrape, return empty for now
            logger.warning(f"Historical scraping not implemented for {symbol}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Historical scraping failed: {e}")
            return pd.DataFrame()

    def get_market_summary(self) -> dict:
        """Get DSEX index and market statistics.

        Priority: scrape live DSEX from dsebd.org, enrich with live DB data.
        Fallback: bdshare historical summary.
        """
        # Always try live scrape first — it has real-time DSEX
        try:
            summary = self._scrape_live_dsex()
            if summary.get("dsex_index", 0) > 0:
                return summary
        except Exception as e:
            logger.warning(f"Live DSEX scrape failed: {e}")

        # Fallback to bdshare (historical, may be stale)
        if self._bdshare_available:
            try:
                return self._get_market_summary_bdshare()
            except Exception as e:
                logger.error(f"bdshare market summary failed: {e}")

        return self._get_market_summary_scrape()

    def _scrape_live_dsex(self) -> dict:
        """Scrape real-time DSEX index from dsebd.org homepage."""
        import re

        resp = self._session.get(DSE_MARKET_SUMMARY_URL, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        full_text = soup.get_text(" ", strip=True)

        summary = {
            "dsex_index": 0, "dsex_change": 0, "dsex_change_pct": 0,
            "total_volume": 0, "total_value": 0, "total_trade": 0,
            "advances": 0, "declines": 0, "unchanged": 0,
            "market_status": "CLOSED",
        }

        # Pattern: "DSE X Index 5478.529 -121.738 -2.17%"
        # The page has two matches: "last update" (live) and "Preceding Trade Date" (prev close)
        # We want the "last update" one
        match = re.search(
            r"last update.*?DSE\s*X\s*Index\s+([\d.]+)\s+([-\d.]+)\s+([-\d.]+)%",
            full_text, re.IGNORECASE,
        )
        if match:
            summary["dsex_index"] = round(float(match.group(1)), 2)
            summary["dsex_change"] = round(float(match.group(2)), 2)
            summary["dsex_change_pct"] = round(float(match.group(3)), 2)
        else:
            # Broader fallback
            match2 = re.search(
                r"DSE\s*X\s*Index\s+([\d.]+)\s+([-\d.]+)\s+([-\d.]+)%",
                full_text,
            )
            if match2:
                summary["dsex_index"] = round(float(match2.group(1)), 2)
                summary["dsex_change"] = round(float(match2.group(2)), 2)
                summary["dsex_change_pct"] = round(float(match2.group(3)), 2)

        if summary["dsex_index"] <= 0:
            raise ValueError("Could not parse DSEX from DSE homepage")

        # Extract today's trade/volume/value from page
        # Pattern: "Issues ... <trades> <volume> <value> Issues"
        tv_match = re.search(r"(\d+)\s+(\d+)\s+([\d.]+)\s+Issues\s+Advanced", full_text)
        if tv_match:
            summary["total_trade"] = int(tv_match.group(1))
            summary["total_volume"] = int(tv_match.group(2))
            summary["total_value"] = float(tv_match.group(3))

        # Enrich advances/declines and live totals from DB
        from database import get_connection
        conn = get_connection()
        live_agg = conn.execute(
            "SELECT COALESCE(SUM(volume),0) as vol, COALESCE(SUM(value),0) as val, "
            "COALESCE(SUM(trade_count),0) as trades, "
            "SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) as adv, "
            "SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) as dec, "
            "SUM(CASE WHEN change_pct = 0 AND trade_count > 0 THEN 1 ELSE 0 END) as unch "
            "FROM live_prices WHERE ltp > 0"
        ).fetchone()
        conn.close()

        if live_agg:
            summary["advances"] = int(live_agg["adv"])
            summary["declines"] = int(live_agg["dec"])
            summary["unchanged"] = int(live_agg["unch"])
            # Use DB totals if scrape missed them
            if summary["total_volume"] == 0:
                summary["total_volume"] = int(live_agg["vol"])
            if summary["total_value"] == 0:
                summary["total_value"] = float(live_agg["val"])
            if summary["total_trade"] == 0:
                summary["total_trade"] = int(live_agg["trades"])

        # Market status from time
        dse_tz = pytz.timezone("Asia/Dhaka")
        now = datetime.now(dse_tz)
        market_days = [6, 0, 1, 2, 3]  # Sun-Thu
        if now.weekday() in market_days and dtime(10, 0) <= now.time() <= dtime(14, 30):
            summary["market_status"] = "OPEN"

        logger.info(f"Live DSEX scraped: {summary['dsex_index']} ({summary['dsex_change']:+.2f})")
        return summary

    def _get_market_summary_bdshare(self) -> dict:
        """Get market summary via bdshare."""
        from bdshare import market_summary
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            data = market_summary()

        if data is None or (isinstance(data, pd.DataFrame) and data.empty):
            raise ValueError("No market summary data")

        summary = {
            "dsex_index": 0,
            "dsex_change": 0,
            "dsex_change_pct": 0,
            "total_volume": 0,
            "total_value": 0,
            "total_trade": 0,
            "advances": 0,
            "declines": 0,
            "unchanged": 0,
            "market_status": "CLOSED",
        }

        if isinstance(data, pd.DataFrame) and not data.empty:
            # Columns: Date, Total Trade, Total Volume, Total Value, DSEX Index, DSES Index, DS30 Index, DGEN Index
            latest = data.iloc[0]

            dsex_index = float(latest.get("DSEX Index", 0) or 0)
            summary["dsex_index"] = round(dsex_index, 2)
            summary["total_trade"] = int(latest.get("Total Trade", 0) or 0)
            summary["total_volume"] = int(latest.get("Total Volume", 0) or 0)
            # bdshare uses "Total Value (mn)" column name
            total_val = latest.get("Total Value (mn)") or latest.get("Total Value") or 0
            summary["total_value"] = float(total_val or 0)

            # Calculate change from previous day
            if len(data) > 1:
                prev_dsex = float(data.iloc[1].get("DSEX Index", 0) or 0)
                if prev_dsex > 0:
                    change = dsex_index - prev_dsex
                    summary["dsex_change"] = round(change, 2)
                    summary["dsex_change_pct"] = round((change / prev_dsex) * 100, 2)

            # Enrich advances/declines and live totals from DB
            from database import get_connection
            conn = get_connection()
            adv = conn.execute("SELECT COUNT(*) FROM live_prices WHERE change_pct > 0").fetchone()[0]
            dec = conn.execute("SELECT COUNT(*) FROM live_prices WHERE change_pct < 0").fetchone()[0]
            unch = conn.execute("SELECT COUNT(*) FROM live_prices WHERE change_pct = 0").fetchone()[0]
            summary["advances"] = adv
            summary["declines"] = dec
            summary["unchanged"] = unch

            # Use live DB totals if bdshare summary is stale (e.g. from previous day)
            live_agg = conn.execute(
                "SELECT COALESCE(SUM(volume),0) as vol, COALESCE(SUM(value),0) as val, COALESCE(SUM(trade_count),0) as trades FROM live_prices WHERE ltp > 0"
            ).fetchone()
            conn.close()
            if live_agg:
                live_vol = int(live_agg["vol"])
                live_val = float(live_agg["val"])
                live_trades = int(live_agg["trades"])
                # Prefer live totals when they're larger (bdshare summary may be from prev day)
                if live_vol > summary["total_volume"]:
                    summary["total_volume"] = live_vol
                if live_val > summary["total_value"]:
                    summary["total_value"] = live_val
                if live_trades > summary["total_trade"]:
                    summary["total_trade"] = live_trades

            # Determine market status based on time
            dse_tz = pytz.timezone("Asia/Dhaka")
            now = datetime.now(dse_tz)
            market_open = dtime(10, 0)
            market_close = dtime(14, 30)
            market_days = [6, 0, 1, 2, 3]  # Sun-Thu
            if now.weekday() in market_days:
                if market_open <= now.time() <= market_close:
                    summary["market_status"] = "OPEN"
                else:
                    summary["market_status"] = "CLOSED"
            else:
                summary["market_status"] = "CLOSED"

        return summary

    def _get_market_summary_scrape(self) -> dict:
        """Scrape market summary from DSE homepage."""
        try:
            resp = self._session.get(DSE_MARKET_SUMMARY_URL, timeout=10)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            summary = {
                "dsex_index": 0,
                "dsex_change": 0,
                "dsex_change_pct": 0,
                "total_volume": 0,
                "total_value": 0,
                "total_trade": 0,
                "advances": 0,
                "declines": 0,
                "unchanged": 0,
                "market_status": "UNKNOWN",
            }

            # Try to find DSEX index value
            index_elements = soup.find_all(string=lambda s: s and "DSEX" in str(s))
            for elem in index_elements:
                parent = elem.parent
                if parent:
                    siblings = parent.find_next_siblings()
                    for sib in siblings:
                        text = sib.get_text(strip=True)
                        try:
                            val = float(text.replace(",", ""))
                            if val > 1000:  # DSEX is typically 4000-7000
                                summary["dsex_index"] = val
                                break
                        except ValueError:
                            continue

            return summary

        except Exception as e:
            logger.error(f"Market summary scraping failed: {e}")
            return {
                "dsex_index": 0, "dsex_change": 0, "dsex_change_pct": 0,
                "total_volume": 0, "total_value": 0, "total_trade": 0,
                "advances": 0, "declines": 0, "unchanged": 0,
                "market_status": "UNKNOWN",
            }

    def get_top_movers(self, limit: int = 20) -> dict:
        """Get top gainers and losers."""
        df = self.get_live_prices()
        if df.empty:
            return {"gainers": [], "losers": []}

        # Filter out stocks with zero or NaN values
        df = df.dropna(subset=["ltp", "change_pct"])
        df = df[df["ltp"] > 0]

        gainers = df.nlargest(limit, "change_pct").to_dict("records")
        losers = df.nsmallest(limit, "change_pct").to_dict("records")

        return {"gainers": gainers, "losers": losers}

    def get_sector_performance(self) -> list:
        """Get sector-wise performance."""
        if self._bdshare_available:
            try:
                from bdshare import get_market_summary
                # bdshare may have sector data
                pass
            except Exception:
                pass
        return []

    @staticmethod
    def scrape_stock_category(symbol: str) -> str | None:
        """Scrape market category (A/B/N/Z) for a stock from DSE website."""
        try:
            url = f"https://www.dsebd.org/displayCompany.php?name={symbol}"
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            for td in soup.find_all("td"):
                if "Market Category" in td.get_text():
                    next_td = td.find_next_sibling("td")
                    if next_td:
                        cat = next_td.get_text(strip=True)
                        if cat in ("A", "B", "N", "Z"):
                            return cat
        except Exception as e:
            logger.debug(f"Category scrape failed for {symbol}: {e}")
        return None

    @staticmethod
    def scrape_all_categories(symbols: list[str]) -> dict[str, str]:
        """Scrape categories for a list of symbols. Rate-limited to ~3 req/sec."""
        results = {}
        for i, sym in enumerate(symbols):
            cat = DSEDataFetcher.scrape_stock_category(sym)
            if cat:
                results[sym] = cat
            if (i + 1) % 3 == 0:
                time_module.sleep(1)  # Rate limit
            if (i + 1) % 50 == 0:
                logger.info(f"  Category scrape progress: {i + 1}/{len(symbols)}, found {len(results)}")
        return results
