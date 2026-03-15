"""Seasonality analysis for DSE stocks and sectors.

Two modes:
  1. precompute_seasonality() — heavy CTE, run once daily after market close.
     Writes results to `seasonality_monthly` table (~4000 rows).
  2. monthly_sector_performance / monthly_stock_performance / current_month_outlook
     — instant SELECTs from the precomputed table.
"""

import calendar
import logging
import math
import os
import random
from datetime import datetime, timedelta
from statistics import median

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
    symbol        TEXT NOT NULL,
    sector        TEXT,
    category      TEXT,
    month         INT  NOT NULL,
    avg_return    NUMERIC(10,6),
    median_return NUMERIC(10,6),
    trimmed_mean  NUMERIC(10,6),
    win_rate      NUMERIC(6,4),
    years_up      INT,
    years_total   INT,
    bootstrap_p   NUMERIC(6,4),
    cohens_d      NUMERIC(8,4),
    best_return   NUMERIC(10,6),
    worst_return  NUMERIC(10,6),
    volatility    NUMERIC(10,6),
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


def _trimmed_mean(vals: list[float], trim_pct: float = 0.1) -> float:
    """Mean after removing top/bottom trim_pct of values."""
    if len(vals) < 3:
        return sum(vals) / len(vals) if vals else 0.0
    s = sorted(vals)
    cut = max(1, int(len(s) * trim_pct))
    trimmed = s[cut:-cut] if cut < len(s) // 2 else s
    return sum(trimmed) / len(trimmed) if trimmed else 0.0


def _bootstrap_p(vals: list[float], n_iter: int = 5000) -> float:
    """Bootstrap p-value: probability that mean return is different from zero.
    Returns p-value (low = statistically significant seasonal effect)."""
    if len(vals) < 3:
        return 1.0
    n = len(vals)
    obs_mean = sum(vals) / n
    count_extreme = 0
    for _ in range(n_iter):
        sample = [random.choice(vals) for _ in range(n)]
        boot_mean = sum(sample) / n
        # Two-tailed: count if bootstrap mean is as extreme as observed
        if abs(boot_mean) >= abs(obs_mean):
            count_extreme += 1
    # p-value under null hypothesis (mean = 0): fraction of resampled centered means >= observed
    # Actually: resample from centered distribution
    centered = [v - obs_mean for v in vals]
    count_extreme = 0
    for _ in range(n_iter):
        sample = [random.choice(centered) for _ in range(n)]
        boot_mean = sum(sample) / n
        if abs(boot_mean + obs_mean) >= abs(obs_mean):
            count_extreme += 1
    return count_extreme / n_iter


def _cohens_d(vals: list[float]) -> float:
    """Cohen's d effect size: mean / stdev. Measures how large the effect is."""
    if len(vals) < 2:
        return 0.0
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)
    sd = math.sqrt(var) if var > 0 else 1e-9
    return mean / sd


def precompute_seasonality():
    """Heavy computation: monthly seasonality with advanced statistics for every stock."""
    conn = _get_conn()
    cur = conn.cursor()

    # Add new columns if they don't exist (for existing tables)
    for col, dtype in [
        ("median_return", "NUMERIC(10,6)"),
        ("trimmed_mean", "NUMERIC(10,6)"),
        ("bootstrap_p", "NUMERIC(6,4)"),
        ("cohens_d", "NUMERIC(8,4)"),
        ("best_return", "NUMERIC(10,6)"),
        ("worst_return", "NUMERIC(10,6)"),
        ("volatility", "NUMERIC(10,6)"),
    ]:
        try:
            cur.execute(f"ALTER TABLE seasonality_monthly ADD COLUMN IF NOT EXISTS {col} {dtype}")
        except Exception:
            conn.rollback()
    conn.commit()

    cur.execute(DDL)
    conn.commit()

    # Step 1: Get per-year returns for all (symbol, month) pairs
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
    SELECT symbol, sector, cat, yr, mo,
           (last_close / NULLIF(first_open, 0)) - 1 AS ret
    FROM monthly_bounds WHERE first_open > 0
    ORDER BY symbol, mo, yr;
    """
    cur.execute(yearly_sql)
    all_yearly = cur.fetchall()
    logger.info("precompute_seasonality: fetched %d yearly rows", len(all_yearly))

    # Step 2: Group by (symbol, month) and compute all stats in Python
    from collections import defaultdict
    groups: dict[tuple, dict] = {}  # (symbol, month) -> {sector, cat, returns: []}
    for r in all_yearly:
        key = (r["symbol"], r["mo"])
        if key not in groups:
            groups[key] = {"sector": r["sector"], "cat": r["cat"], "returns": []}
        ret = float(r["ret"]) if r["ret"] is not None else 0.0
        groups[key]["returns"].append(ret)

    random.seed(42)  # Reproducible bootstrap
    rows = []
    for (symbol, month), g in groups.items():
        rets = g["returns"]
        n = len(rets)
        if n == 0:
            continue
        avg = sum(rets) / n
        med = median(rets) if n > 0 else 0.0
        tmean = _trimmed_mean(rets)
        wr = sum(1 for r in rets if r > 0) / n
        yup = sum(1 for r in rets if r > 0)
        bp = _bootstrap_p(rets) if n >= 3 else 1.0
        cd = _cohens_d(rets) if n >= 2 else 0.0
        best = max(rets)
        worst = min(rets)
        vol = math.sqrt(sum((r - avg) ** 2 for r in rets) / max(n - 1, 1))

        rows.append((
            symbol, g["sector"], g["cat"], month,
            round(avg, 6), round(med, 6), round(tmean, 6),
            round(wr, 4), yup, n,
            round(bp, 4), round(cd, 4),
            round(best, 6), round(worst, 6), round(vol, 6),
        ))

    logger.info("precompute_seasonality: computed stats for %d (symbol, month) groups", len(rows))

    # Step 3: Write to table
    cur.execute("DELETE FROM seasonality_monthly")
    if rows:
        insert_sql = """
            INSERT INTO seasonality_monthly
                (symbol, sector, category, month,
                 avg_return, median_return, trimmed_mean,
                 win_rate, years_up, years_total,
                 bootstrap_p, cohens_d,
                 best_return, worst_return, volatility)
            VALUES %s
        """
        psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=500)

    # Also write per-year data for expandable views (reuse already-fetched data)
    cur.execute("DELETE FROM seasonality_yearly")
    if all_yearly:
        yearly_values = [
            (r["symbol"], r["sector"], r["cat"], r["yr"], r["mo"],
             round(float(r["ret"]), 6) if r["ret"] is not None else 0)
            for r in all_yearly
        ]
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO seasonality_yearly (symbol, sector, category, year, month, return) VALUES %s",
            yearly_values, page_size=1000,
        )
    logger.info("precompute_seasonality: wrote %d yearly rows", len(all_yearly))

    conn.commit()
    conn.close()
    logger.info("precompute_seasonality: wrote %d avg rows + %d yearly rows",
                len(rows), len(all_yearly))
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
                       AVG(avg_return)::numeric(10,6)    AS avg_return,
                       AVG(median_return)::numeric(10,6) AS median_return,
                       AVG(trimmed_mean)::numeric(10,6)  AS trimmed_mean,
                       AVG(win_rate)::numeric(6,4)       AS win_rate,
                       SUM(years_total)::int             AS sample_size,
                       AVG(bootstrap_p)::numeric(6,4)    AS bootstrap_p,
                       AVG(cohens_d)::numeric(8,4)       AS cohens_d,
                       AVG(volatility)::numeric(10,6)    AS volatility
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
            "median_return": float(r.get("median_return") or r.get("avg_return") or 0),
            "trimmed_mean": float(r.get("trimmed_mean") or r.get("avg_return") or 0),
            "win_rate": float(r["win_rate"] or 0),
            "sample_size": int(r["sample_size"] or 0),
            "bootstrap_p": float(r.get("bootstrap_p") or 1.0),
            "cohens_d": float(r.get("cohens_d") or 0),
            "volatility": float(r.get("volatility") or 0),
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
                       years_up, years_total,
                       median_return, trimmed_mean, bootstrap_p, cohens_d,
                       best_return, worst_return, volatility
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
            "median_return": float(r.get("median_return") or r.get("avg_return") or 0),
            "trimmed_mean": float(r.get("trimmed_mean") or r.get("avg_return") or 0),
            "bootstrap_p": float(r.get("bootstrap_p") or 1.0),
            "cohens_d": float(r.get("cohens_d") or 0),
            "best_return": float(r.get("best_return") or 0),
            "worst_return": float(r.get("worst_return") or 0),
            "volatility": float(r.get("volatility") or 0),
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
                   AVG(avg_return)::numeric(10,6)    AS avg_return,
                   AVG(median_return)::numeric(10,6) AS median_return,
                   AVG(trimmed_mean)::numeric(10,6)  AS trimmed_mean,
                   AVG(win_rate)::numeric(6,4)       AS win_rate,
                   SUM(years_total)::int             AS sample_size,
                   AVG(bootstrap_p)::numeric(6,4)    AS bootstrap_p,
                   AVG(cohens_d)::numeric(8,4)       AS cohens_d,
                   AVG(volatility)::numeric(10,6)    AS volatility
            FROM seasonality_monthly
            WHERE sector IS NOT NULL AND month = %s
            GROUP BY sector
            ORDER BY avg_return DESC
        """, (month,))
        sector_rows = cur.fetchall()

        # Stock outlook (category A, at least 2 years of data)
        cur.execute("""
            SELECT symbol, sector, avg_return, median_return, trimmed_mean,
                   win_rate, years_total AS sample_size,
                   bootstrap_p, cohens_d, volatility
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
            "median_return": float(r.get("median_return") or r.get("avg_return") or 0),
            "trimmed_mean": float(r.get("trimmed_mean") or r.get("avg_return") or 0),
            "win_rate": float(r["win_rate"] or 0),
            "sample_size": r["sample_size"],
            "bootstrap_p": float(r.get("bootstrap_p") or 1.0),
            "cohens_d": float(r.get("cohens_d") or 0),
            "volatility": float(r.get("volatility") or 0),
        }

    def _fmt_stock(r):
        return {
            "symbol": r["symbol"],
            "sector": r["sector"],
            "avg_return": float(r["avg_return"] or 0),
            "median_return": float(r.get("median_return") or r.get("avg_return") or 0),
            "trimmed_mean": float(r.get("trimmed_mean") or r.get("avg_return") or 0),
            "win_rate": float(r["win_rate"] or 0),
            "sample_size": r["sample_size"],
            "bootstrap_p": float(r.get("bootstrap_p") or 1.0),
            "cohens_d": float(r.get("cohens_d") or 0),
            "volatility": float(r.get("volatility") or 0),
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
