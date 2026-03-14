"""Seasonality analysis for DSE stocks and sectors.

Two modes:
  1. precompute_seasonality() — heavy CTE, run once daily after market close.
     Writes results to `seasonality_monthly` table (~4000 rows).
  2. monthly_sector_performance / monthly_stock_performance / current_month_outlook
     — instant SELECTs from the precomputed table.
"""

import calendar
import logging
import os
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23"
    "@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres",
)


def _get_conn():
    """Open a new psycopg2 connection with RealDictCursor."""
    conn = psycopg2.connect(
        DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor
    )
    conn.autocommit = False
    return conn


# ════════════════════════════════════════════════════════════════
#  Precompute (run once daily)
# ════════════════════════════════════════════════════════════════


DDL = """
CREATE TABLE IF NOT EXISTS seasonality_monthly (
    symbol      TEXT NOT NULL,
    sector      TEXT,
    category    TEXT,
    month       INT  NOT NULL,
    avg_return  NUMERIC(10,6),
    win_rate    NUMERIC(6,4),
    years_up    INT,
    years_total INT,
    PRIMARY KEY (symbol, month)
);

CREATE TABLE IF NOT EXISTS seasonality_yearly (
    symbol      TEXT NOT NULL,
    sector      TEXT,
    category    TEXT,
    year        INT  NOT NULL,
    month       INT  NOT NULL,
    return      NUMERIC(10,6),
    PRIMARY KEY (symbol, year, month)
);
"""


def precompute_seasonality():
    """Heavy CTE: compute monthly seasonality for every stock, write to table."""
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute(DDL)
    conn.commit()

    sql = """
    WITH monthly_bounds AS (
        SELECT
            dp.symbol,
            f.sector,
            UPPER(COALESCE(f.category, 'A')) AS cat,
            EXTRACT(YEAR  FROM dp.date)::int AS yr,
            EXTRACT(MONTH FROM dp.date)::int AS mo,
            (ARRAY_AGG(dp.open  ORDER BY dp.date ASC))[1]  AS first_open,
            (ARRAY_AGG(dp.close ORDER BY dp.date DESC))[1] AS last_close
        FROM daily_prices dp
        JOIN fundamentals f ON f.symbol = dp.symbol
        WHERE dp.open > 0 AND dp.close > 0
        GROUP BY dp.symbol, f.sector, cat, yr, mo
    ),
    monthly_returns AS (
        SELECT
            symbol, sector, cat, mo,
            CASE WHEN (last_close / NULLIF(first_open, 0) - 1) > 0
                 THEN 1 ELSE 0 END AS is_up,
            (last_close / NULLIF(first_open, 0)) - 1 AS ret
        FROM monthly_bounds
        WHERE first_open > 0
    )
    SELECT
        symbol, sector, cat,
        mo                                              AS month,
        ROUND(AVG(ret)::numeric, 6)                     AS avg_return,
        ROUND(
            SUM(is_up)::numeric / NULLIF(COUNT(*), 0), 4
        )                                               AS win_rate,
        SUM(is_up)::int                                 AS years_up,
        COUNT(*)::int                                   AS years_total
    FROM monthly_returns
    GROUP BY symbol, sector, cat, mo;
    """
    cur.execute(sql)
    rows = cur.fetchall()
    logger.info("precompute_seasonality: computed %d rows", len(rows))

    # Truncate and reload
    cur.execute("DELETE FROM seasonality_monthly")
    if rows:
        insert_sql = """
            INSERT INTO seasonality_monthly
                (symbol, sector, category, month, avg_return, win_rate, years_up, years_total)
            VALUES %s
        """
        values = [
            (r["symbol"], r["sector"], r["cat"], r["month"],
             r["avg_return"], r["win_rate"], r["years_up"], r["years_total"])
            for r in rows
        ]
        psycopg2.extras.execute_values(cur, insert_sql, values, page_size=500)

    # Also precompute per-year data for expandable views
    yearly_sql = """
    WITH monthly_bounds AS (
        SELECT
            dp.symbol,
            f.sector,
            UPPER(COALESCE(f.category, 'A')) AS cat,
            EXTRACT(YEAR  FROM dp.date)::int AS yr,
            EXTRACT(MONTH FROM dp.date)::int AS mo,
            (ARRAY_AGG(dp.open  ORDER BY dp.date ASC))[1]  AS first_open,
            (ARRAY_AGG(dp.close ORDER BY dp.date DESC))[1] AS last_close
        FROM daily_prices dp
        JOIN fundamentals f ON f.symbol = dp.symbol
        WHERE dp.open > 0 AND dp.close > 0
        GROUP BY dp.symbol, f.sector, cat, yr, mo
    )
    SELECT symbol, sector, cat, yr AS year, mo AS month,
           ROUND(((last_close / NULLIF(first_open, 0)) - 1)::numeric, 6) AS return
    FROM monthly_bounds WHERE first_open > 0;
    """
    cur.execute(yearly_sql)
    yearly_rows = cur.fetchall()

    cur.execute("DELETE FROM seasonality_yearly")
    if yearly_rows:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO seasonality_yearly (symbol, sector, category, year, month, return) VALUES %s",
            [(r["symbol"], r["sector"], r["cat"], r["year"], r["month"], r["return"])
             for r in yearly_rows],
            page_size=1000,
        )
    logger.info("precompute_seasonality: wrote %d yearly rows", len(yearly_rows))

    conn.commit()
    conn.close()
    logger.info("precompute_seasonality: wrote %d avg rows + %d yearly rows",
                len(rows), len(yearly_rows))
    return len(rows)


# ════════════════════════════════════════════════════════════════
#  1. Monthly sector performance (instant read)
# ════════════════════════════════════════════════════════════════


def monthly_sector_performance(year: int | None = None) -> dict:
    """Average monthly return per sector.

    year=None → overall average from precomputed table.
    year=2025 → actual performance for that year (live query, fast for 1 year).
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        if year:
            cur.execute("""
                WITH monthly_bounds AS (
                    SELECT
                        f.sector,
                        EXTRACT(MONTH FROM dp.date)::int AS mo,
                        (ARRAY_AGG(dp.open  ORDER BY dp.date ASC))[1]  AS first_open,
                        (ARRAY_AGG(dp.close ORDER BY dp.date DESC))[1] AS last_close
                    FROM daily_prices dp
                    JOIN fundamentals f ON f.symbol = dp.symbol
                    WHERE f.sector IS NOT NULL
                      AND EXTRACT(YEAR FROM dp.date) = %s
                      AND dp.open > 0 AND dp.close > 0
                    GROUP BY f.sector, mo
                )
                SELECT sector, mo AS month,
                       ROUND(((last_close / NULLIF(first_open, 0)) - 1)::numeric, 6) AS avg_return,
                       CASE WHEN last_close > first_open THEN 1.0 ELSE 0.0 END AS win_rate,
                       1 AS sample_size
                FROM monthly_bounds WHERE first_open > 0
                ORDER BY sector, mo
            """, (year,))
        else:
            cur.execute("""
                SELECT sector, month,
                       AVG(avg_return)::numeric(10,6) AS avg_return,
                       AVG(win_rate)::numeric(6,4)    AS win_rate,
                       SUM(years_total)::int          AS sample_size
                FROM seasonality_monthly
                WHERE sector IS NOT NULL
                GROUP BY sector, month
                ORDER BY sector, month
            """)
        rows = cur.fetchall()

        # Also return available years
        cur.execute("""
            SELECT DISTINCT EXTRACT(YEAR FROM date)::int AS yr
            FROM daily_prices ORDER BY yr DESC
        """)
        years = [r["yr"] for r in cur.fetchall()]
    finally:
        conn.close()

    sector_map: dict[str, list[dict]] = {}
    for r in rows:
        name = r["sector"]
        sector_map.setdefault(name, []).append({
            "month": r["month"],
            "avg_return": float(r["avg_return"] or 0),
            "win_rate": float(r["win_rate"] or 0),
            "sample_size": int(r["sample_size"] or 0),
        })

    sectors = [
        {"name": name, "months": months}
        for name, months in sorted(sector_map.items())
    ]
    logger.info("monthly_sector_performance(year=%s): %d sectors", year, len(sectors))
    return {"sectors": sectors, "years": years}


def sector_yearly_detail() -> dict:
    """Per-sector per-year per-month returns for expandable heatmap.

    Returns { sectors: { "Bank": { 2025: { 1: 0.03, 2: -0.01, ... }, ... } }, years: [...] }
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT sector, year, month,
                   AVG(return)::numeric(10,6) AS avg_return
            FROM seasonality_yearly
            WHERE sector IS NOT NULL
            GROUP BY sector, year, month
            ORDER BY sector, year, month
        """)
        rows = cur.fetchall()

        cur.execute("SELECT DISTINCT year FROM seasonality_yearly ORDER BY year DESC")
        years = [r["year"] for r in cur.fetchall()]
    finally:
        conn.close()

    # Shape: { sector: { year: { month: return } } }
    data: dict[str, dict[int, dict[int, float]]] = {}
    for r in rows:
        s = r["sector"]
        y = int(r["year"])
        m = int(r["month"])
        data.setdefault(s, {}).setdefault(y, {})[m] = float(r["avg_return"] or 0)

    return {"sectors": data, "years": years}


def stock_yearly_detail(category: str = "A") -> dict:
    """Per-stock per-year per-month returns for expandable stock patterns table.

    Returns { stocks: { "BEXIMCO": { "2025": { "1": 0.03, ... }, ... } }, years: [...] }
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, year, month, return
            FROM seasonality_yearly
            WHERE category = UPPER(%s)
            ORDER BY symbol, year, month
            """,
            (category,),
        )
        rows = cur.fetchall()

        cur.execute("SELECT DISTINCT year FROM seasonality_yearly ORDER BY year DESC")
        years = [r["year"] for r in cur.fetchall()]
    finally:
        conn.close()

    # Shape: { symbol: { year: { month: return } } }
    data: dict[str, dict[int, dict[int, float]]] = {}
    for r in rows:
        sym = r["symbol"]
        y = int(r["year"])
        m = int(r["month"])
        data.setdefault(sym, {}).setdefault(y, {})[m] = float(r["return"] or 0)

    logger.info("stock_yearly_detail(category=%s): %d stocks", category, len(data))
    return {"stocks": data, "years": years}


# ════════════════════════════════════════════════════════════════
#  2. Monthly stock performance (instant read)
# ════════════════════════════════════════════════════════════════


def monthly_stock_performance(category: str = "A", year: int | None = None,
                              sector: str | None = None) -> dict:
    """Per-stock monthly seasonality.

    year=None → overall average. year=2025 → that year only.
    sector filter narrows results to a specific sector.
    Also returns list of available sectors for the filter dropdown.
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()

        if year:
            # Live query for a specific year
            conditions = ["EXTRACT(YEAR FROM dp.date) = %s",
                          "UPPER(COALESCE(f.category, 'A')) = UPPER(%s)",
                          "dp.open > 0", "dp.close > 0"]
            params: list = [year, category]
            if sector:
                conditions.append("f.sector = %s")
                params.append(sector)
            where = " AND ".join(conditions)
            cur.execute(f"""
                WITH monthly_bounds AS (
                    SELECT
                        dp.symbol, f.sector,
                        EXTRACT(MONTH FROM dp.date)::int AS mo,
                        (ARRAY_AGG(dp.open  ORDER BY dp.date ASC))[1]  AS first_open,
                        (ARRAY_AGG(dp.close ORDER BY dp.date DESC))[1] AS last_close
                    FROM daily_prices dp
                    JOIN fundamentals f ON f.symbol = dp.symbol
                    WHERE {where}
                    GROUP BY dp.symbol, f.sector, mo
                )
                SELECT symbol, sector, mo AS month,
                       ROUND(((last_close / NULLIF(first_open, 0)) - 1)::numeric, 6) AS avg_return,
                       CASE WHEN last_close > first_open THEN 1.0 ELSE 0.0 END AS up_pct,
                       CASE WHEN last_close > first_open THEN 1 ELSE 0 END AS years_up,
                       1 AS years_total
                FROM monthly_bounds WHERE first_open > 0
                ORDER BY symbol, mo
            """, params)
        else:
            # Precomputed averages
            conditions = ["category = UPPER(%s)"]
            params = [category]
            if sector:
                conditions.append("sector = %s")
                params.append(sector)
            where = " AND ".join(conditions)
            cur.execute(f"""
                SELECT symbol, sector, month, avg_return, win_rate AS up_pct,
                       years_up, years_total
                FROM seasonality_monthly
                WHERE {where}
                ORDER BY symbol, month
            """, params)

        rows = cur.fetchall()

        # Get distinct sectors for filter dropdown
        cur.execute("""
            SELECT DISTINCT sector FROM seasonality_monthly
            WHERE category = UPPER(%s) AND sector IS NOT NULL
            ORDER BY sector
        """, (category,))
        sectors = [r["sector"] for r in cur.fetchall()]

        # Available years
        cur.execute("""
            SELECT DISTINCT EXTRACT(YEAR FROM date)::int AS yr
            FROM daily_prices ORDER BY yr DESC
        """)
        years = [r["yr"] for r in cur.fetchall()]
    finally:
        conn.close()

    stock_map: dict[str, dict] = {}
    for r in rows:
        sym = r["symbol"]
        if sym not in stock_map:
            stock_map[sym] = {"symbol": sym, "sector": r["sector"], "months": []}
        stock_map[sym]["months"].append({
            "month": r["month"],
            "avg_return": float(r["avg_return"] or 0),
            "up_pct": float(r["up_pct"] or 0),
            "years_up": int(r["years_up"] or 0),
            "years_total": int(r["years_total"] or 0),
        })

    stocks = sorted(stock_map.values(), key=lambda s: s["symbol"])
    logger.info("monthly_stock_performance(category=%s, year=%s, sector=%s): %d stocks",
                category, year, sector, len(stocks))
    return {"stocks": stocks, "sectors": sectors, "years": years}


# ════════════════════════════════════════════════════════════════
#  3. Weekly performance (last N weeks) — still live query
# ════════════════════════════════════════════════════════════════


def weekly_performance(weeks_back: int = 52) -> list[dict]:
    """Sector-level weekly returns for the last *weeks_back* weeks."""
    cutoff = (datetime.now() - timedelta(weeks=weeks_back)).strftime("%Y-%m-%d")

    sector_sql = """
    WITH week_bounds AS (
        SELECT
            f.sector,
            dp.symbol,
            DATE_TRUNC('week', dp.date)::date AS wk,
            (ARRAY_AGG(dp.open  ORDER BY dp.date ASC))[1]  AS first_open,
            (ARRAY_AGG(dp.close ORDER BY dp.date DESC))[1] AS last_close
        FROM daily_prices dp
        JOIN fundamentals f ON f.symbol = dp.symbol
        WHERE f.sector IS NOT NULL
          AND dp.date >= %s
          AND dp.open > 0 AND dp.close > 0
        GROUP BY f.sector, dp.symbol, wk
    ),
    stock_returns AS (
        SELECT
            sector, symbol, wk,
            (last_close / NULLIF(first_open, 0)) - 1 AS ret
        FROM week_bounds
        WHERE first_open > 0
    ),
    ranked AS (
        SELECT
            sector, symbol, wk, ret,
            ROW_NUMBER() OVER (PARTITION BY sector, wk ORDER BY ret DESC) AS rn
        FROM stock_returns
    )
    SELECT
        sector,
        wk,
        ROUND(AVG(ret)::numeric, 6)        AS sector_return,
        ARRAY_AGG(symbol ORDER BY ret DESC) FILTER (WHERE rn <= 3) AS top_stocks
    FROM ranked
    GROUP BY sector, wk
    ORDER BY wk DESC, sector;
    """

    dsex_sql = """
    WITH dsex_weeks AS (
        SELECT
            DATE_TRUNC('week', date)::date AS wk,
            (ARRAY_AGG(dsex_index ORDER BY date ASC))[1]  AS first_val,
            (ARRAY_AGG(dsex_index ORDER BY date DESC))[1] AS last_val,
            MIN(date)::text AS week_start,
            MAX(date)::text AS week_end
        FROM dsex_history
        WHERE date >= %s AND dsex_index > 0
        GROUP BY wk
    )
    SELECT
        wk, week_start, week_end,
        ROUND(((last_val / NULLIF(first_val, 0)) - 1)::numeric, 6) AS dsex_return
    FROM dsex_weeks
    ORDER BY wk DESC;
    """

    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sector_sql, (cutoff,))
        sector_rows = cur.fetchall()
        cur.execute(dsex_sql, (cutoff,))
        dsex_rows = cur.fetchall()
    finally:
        conn.close()

    dsex_by_wk: dict[str, dict] = {}
    for r in dsex_rows:
        wk_key = str(r["wk"])
        dsex_by_wk[wk_key] = {
            "week_start": r["week_start"],
            "week_end": r["week_end"],
            "dsex_return": float(r["dsex_return"] or 0),
        }

    week_sectors: dict[str, list[dict]] = {}
    for r in sector_rows:
        wk_key = str(r["wk"])
        week_sectors.setdefault(wk_key, []).append({
            "name": r["sector"],
            "return_pct": float(r["sector_return"] or 0),
            "top_stocks": list(r["top_stocks"] or []),
        })

    all_weeks = sorted(
        set(list(dsex_by_wk.keys()) + list(week_sectors.keys())), reverse=True
    )
    result = []
    for wk in all_weeks:
        dsex_info = dsex_by_wk.get(wk, {})
        result.append({
            "week_start": dsex_info.get("week_start", wk),
            "week_end": dsex_info.get("week_end", wk),
            "dsex_return": dsex_info.get("dsex_return", 0),
            "sectors": sorted(
                week_sectors.get(wk, []),
                key=lambda s: s["return_pct"],
                reverse=True,
            ),
        })

    logger.info("weekly_performance(weeks_back=%d): %d weeks", weeks_back, len(result))
    return result


# ════════════════════════════════════════════════════════════════
#  4. Current month outlook (instant read)
# ════════════════════════════════════════════════════════════════


def month_outlook(month: int | None = None) -> dict:
    """Top/bottom sectors and stocks for a given month, plus yearly breakdown.

    If *month* is None, uses the current calendar month.
    Returns top-5/bottom-5 sectors, top-25/bottom-25 stocks, and per-year data.
    """
    if month is None:
        month = datetime.now().month
    month_name = calendar.month_name[month]

    conn = _get_conn()
    try:
        cur = conn.cursor()

        # Sector outlook
        cur.execute("""
            SELECT sector,
                   AVG(avg_return)::numeric(10,6) AS avg_return,
                   AVG(win_rate)::numeric(6,4)    AS win_rate,
                   SUM(years_total)::int          AS sample_size
            FROM seasonality_monthly
            WHERE sector IS NOT NULL AND month = %s
            GROUP BY sector
            ORDER BY avg_return DESC
        """, (month,))
        sector_rows = cur.fetchall()

        # Stock outlook (category A, at least 2 years of data)
        cur.execute("""
            SELECT symbol, sector, avg_return, win_rate, years_total AS sample_size
            FROM seasonality_monthly
            WHERE category = 'A' AND month = %s AND years_total >= 2
            ORDER BY avg_return DESC
        """, (month,))
        stock_rows = cur.fetchall()

        # Yearly breakdown: how did each year actually perform in this month?
        cur.execute("""
            WITH monthly_bounds AS (
                SELECT
                    EXTRACT(YEAR FROM dp.date)::int AS yr,
                    (ARRAY_AGG(dp.close ORDER BY dp.date DESC))[1] AS last_close,
                    (ARRAY_AGG(dp.open  ORDER BY dp.date ASC))[1]  AS first_open
                FROM daily_prices dp
                JOIN fundamentals f ON f.symbol = dp.symbol
                WHERE EXTRACT(MONTH FROM dp.date) = %s
                  AND dp.open > 0 AND dp.close > 0
                  AND f.sector IS NOT NULL
                GROUP BY dp.symbol, yr
            )
            SELECT yr AS year,
                   ROUND(AVG((last_close / NULLIF(first_open, 0)) - 1)::numeric, 6) AS avg_return,
                   COUNT(*) FILTER (WHERE last_close > first_open) AS stocks_up,
                   COUNT(*) FILTER (WHERE last_close <= first_open) AS stocks_down,
                   COUNT(*)::int AS total_stocks
            FROM monthly_bounds
            WHERE first_open > 0
            GROUP BY yr
            ORDER BY yr DESC
        """, (month,))
        yearly_rows = cur.fetchall()
    finally:
        conn.close()

    def _fmt_sector(r):
        return {
            "sector": r["sector"],
            "avg_return": float(r["avg_return"] or 0),
            "win_rate": float(r["win_rate"] or 0),
            "sample_size": r["sample_size"],
        }

    def _fmt_stock(r):
        return {
            "symbol": r["symbol"],
            "sector": r["sector"],
            "avg_return": float(r["avg_return"] or 0),
            "win_rate": float(r["win_rate"] or 0),
            "sample_size": r["sample_size"],
        }

    top_sectors = [_fmt_sector(r) for r in sector_rows[:5]]
    bottom_sectors = [_fmt_sector(r) for r in sector_rows[-5:]][::-1]
    top_stocks = [_fmt_stock(r) for r in stock_rows[:25]]
    bottom_stocks = [_fmt_stock(r) for r in stock_rows[-25:]][::-1]

    yearly = [{
        "year": int(r["year"]),
        "avg_return": float(r["avg_return"] or 0),
        "stocks_up": int(r["stocks_up"]),
        "stocks_down": int(r["stocks_down"]),
        "total_stocks": int(r["total_stocks"]),
    } for r in yearly_rows]

    return {
        "month": month,
        "month_name": month_name,
        "top_sectors": top_sectors,
        "bottom_sectors": bottom_sectors,
        "top_stocks": top_stocks,
        "bottom_stocks": bottom_stocks,
        "yearly": yearly,
    }


# Keep old name as alias
def current_month_outlook() -> dict:
    return month_outlook()
