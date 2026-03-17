#!/usr/bin/env python3
"""Migrate all data from Supabase to local PostgreSQL."""

import json
import psycopg2
import psycopg2.extras

SRC_URL = "postgresql://postgres.iihlezpkpllacztoaguc:160021062Ss%23%23@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres"
DST_URL = "postgresql://dse:dse_trading_2026@localhost/dse_trading"

# Tables to migrate
TABLES = [
    "dsex_forecast", "dsex_history", "fundamentals", "holdings",
    "intraday_alerts", "intraday_snapshots", "judge_daily_analysis",
    "live_prices", "llm_daily_analysis", "llm_scan_results",
    "market_holidays", "market_news", "market_summary",
    "orderbook_snapshots", "prediction_tracker", "radar_precomputed",
    "radar_snapshots", "scan_decisions", "seasonality_monthly",
    "seasonality_yearly", "sectors", "signal_history", "signal_patterns",
    "signals", "watchlist", "chat_users",
]

CHUNK_SIZE = 2000


def migrate_table(tbl):
    try:
        src = psycopg2.connect(SRC_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        dst = psycopg2.connect(DST_URL)
        src_cur = src.cursor()
        dst_cur = dst.cursor()

        # Get columns
        src_cur.execute(
            "SELECT column_name, data_type, column_default "
            "FROM information_schema.columns "
            f"WHERE table_name = '{tbl}' AND table_schema = 'public' "
            "ORDER BY ordinal_position"
        )
        cols = src_cur.fetchall()
        if not cols:
            print(f"  {tbl}: no columns found (skip)")
            src.close(); dst.close()
            return

        # Build CREATE TABLE
        col_defs = []
        serial_cols = set()
        for c in cols:
            name = c["column_name"]
            dtype = c["data_type"]
            if dtype == "ARRAY":
                dtype = "TEXT"
            elif dtype == "USER-DEFINED":
                dtype = "JSONB"
            if c["column_default"] and "nextval" in str(c["column_default"]):
                dtype = "SERIAL"
                serial_cols.add(name)
            col_defs.append(f"{name} {dtype}")

        create_sql = f"CREATE TABLE IF NOT EXISTS {tbl} ({', '.join(col_defs)})"
        dst_cur.execute(create_sql)
        dst.commit()

        # Count rows
        src_cur.execute(f"SELECT COUNT(*) as c FROM {tbl}")
        count = src_cur.fetchone()["c"]
        if count == 0:
            print(f"  {tbl}: 0 rows")
            src.close(); dst.close()
            return

        # Column names (skip serial/id columns)
        col_names = [c["column_name"] for c in cols if c["column_name"] not in serial_cols]
        cols_str = ", ".join(col_names)
        placeholders = ", ".join(["%s"] * len(col_names))

        # Fetch and insert in chunks
        offset = 0
        total = 0
        while offset < count:
            src_cur.execute(f"SELECT {cols_str} FROM {tbl} LIMIT {CHUNK_SIZE} OFFSET {offset}")
            rows = src_cur.fetchall()
            if not rows:
                break

            for row in rows:
                vals = []
                for c in col_names:
                    v = row[c]
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v)
                    vals.append(v)
                try:
                    dst_cur.execute(
                        f"INSERT INTO {tbl} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                        vals,
                    )
                    total += 1
                except Exception:
                    dst.rollback()

            dst.commit()
            offset += CHUNK_SIZE

        print(f"  {tbl}: {total}/{count} rows")
        src.close()
        dst.close()
    except Exception as e:
        print(f"  {tbl}: ERROR {str(e)[:120]}")
        try:
            src.close()
        except Exception:
            pass
        try:
            dst.close()
        except Exception:
            pass


if __name__ == "__main__":
    print("=== Migrating Supabase → Local PostgreSQL ===")
    for tbl in TABLES:
        migrate_table(tbl)
    print("=== Migration complete ===")
