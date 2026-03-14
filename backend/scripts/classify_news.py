#!/usr/bin/env python3
"""Classify market news using Claude — impact level, sentiment, affected symbols.

Adds AI-generated tags to market_news rows that haven't been classified yet.
Designed to run after the daily LankaBD news scrape.

Usage:
    python3 scripts/classify_news.py           # classify unclassified news
    python3 scripts/classify_news.py --days 7  # only last 7 days
    python3 scripts/classify_news.py --all     # reclassify everything
"""

import json
import logging
import os
import time
from datetime import date, timedelta

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

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
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514")

BATCH_SIZE = 30  # news items per Claude call
BATCH_DELAY = 5  # seconds between API calls


def get_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    return conn


def ensure_columns():
    """Add classification columns to market_news if missing."""
    conn = psycopg2.connect(DATABASE_URL_DIRECT)
    cur = conn.cursor()
    columns = [
        ("impact", "TEXT"),           # HIGH, MEDIUM, LOW, NOISE
        ("sentiment", "TEXT"),        # BULLISH, BEARISH, NEUTRAL, MIXED
        ("market_impact", "TEXT"),    # STOCK_SPECIFIC, SECTOR_WIDE, DSEX_MOVING, MACRO, DIVIDEND, NOISE
        ("affected_symbols", "TEXT[]"),  # AI-detected symbols (more accurate than regex)
        ("summary", "TEXT"),         # 1-line AI summary
        ("classified_at", "TIMESTAMP"),
    ]
    for col, dtype in columns:
        try:
            cur.execute(f"ALTER TABLE market_news ADD COLUMN IF NOT EXISTS {col} {dtype}")
        except Exception:
            conn.rollback()
    conn.commit()
    conn.close()
    logger.info("Classification columns ensured")


def load_unclassified(days=None, force_all=False):
    """Load news items that haven't been classified yet."""
    conn = get_conn()
    cur = conn.cursor()

    if force_all:
        where = ""
        params = []
    elif days:
        since = (date.today() - timedelta(days=days)).isoformat()
        where = "AND date >= %s"
        params = [since]
    else:
        where = "AND classified_at IS NULL"
        params = []

    cur.execute(f"""
        SELECT id, date, category, title, content, source, symbols_mentioned
        FROM market_news
        WHERE 1=1 {where}
        ORDER BY date DESC, id DESC
    """, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def build_prompt(news_batch):
    """Build Claude prompt to classify a batch of news items."""
    items_text = ""
    for n in news_batch:
        symbols = ", ".join(n["symbols_mentioned"]) if n["symbols_mentioned"] else "none"
        items_text += (
            f'\n---\nID: {n["id"]}\n'
            f'Date: {n["date"]}\n'
            f'Category: {n["category"]}\n'
            f'Source: {n["source"]}\n'
            f'Title: {n["title"]}\n'
            f'Content: {(n["content"] or "")[:300]}\n'
            f'Mentioned symbols: {symbols}\n'
        )

    return f"""You are a Bangladesh stock market (DSE) analyst. Classify each news item below by its market impact.

For each item, determine:

1. **impact**: How much this affects stock prices
   - HIGH = Will directly move specific stock prices or DSEX index (earnings reports, dividend announcements, major company events, regulatory changes affecting listed companies)
   - MEDIUM = Indirectly affects market sentiment or specific sectors (economic policy, sector trends, forex/interest rate changes)
   - LOW = General information, minor relevance to stock prices
   - NOISE = No market relevance (NAV reports, generic government notices)

2. **sentiment**: Market sentiment direction
   - BULLISH = Positive for stock prices / market
   - BEARISH = Negative for stock prices / market
   - NEUTRAL = Informational, no clear direction
   - MIXED = Contains both positive and negative elements

3. **market_impact**: What area of the market is affected
   - STOCK_SPECIFIC = Affects one or a few specific stocks (earnings, dividends, company news)
   - SECTOR_WIDE = Affects an entire sector (banking regulation, textile exports, pharma policy)
   - DSEX_MOVING = Could move the overall index (macro policy, foreign investment flows, major regulatory change)
   - MACRO = Economy-wide but indirect market effect (GDP, inflation, forex reserves)
   - DIVIDEND = Dividend declaration, record date, or ex-date related
   - NOISE = Not market relevant

4. **affected_symbols**: List of DSE stock symbols directly affected. Use trading symbols (e.g., "GP" not "Grameenphone", "LHBL" not "LafargeHolcim"). Empty array if none specific.

5. **summary**: One concise sentence summarizing the market relevance. If NOISE, just say "No market impact."

Return a JSON array:
```json
[
  {{"id": 123, "impact": "HIGH", "sentiment": "BULLISH", "market_impact": "STOCK_SPECIFIC", "affected_symbols": ["LHBL"], "summary": "LHBL profit up 34% YoY — strong earnings beat"}}
]
```

NEWS ITEMS:
{items_text}

Return ONLY the JSON array, no other text."""


def call_claude(prompt):
    """Call Claude via the same SDK/CLI fallback as llm_daily_analyzer."""
    # Reuse the existing call_claude from llm_daily_analyzer
    # which supports both ANTHROPIC_API_KEY and claude CLI fallback
    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from scripts.llm_daily_analyzer import call_claude as _call
        return _call(prompt, timeout=120)
    except ImportError:
        pass

    # Direct fallback: use claude CLI (same as data-audit pipeline)
    import subprocess
    import tempfile
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, prefix="classify_") as f:
            f.write(prompt)
            f.write("\n\nRespond with ONLY the JSON array. Start your response with [")
            prompt_file = f.name

        result = subprocess.run(
            ["bash", "-c", f'cat "{prompt_file}" | claude -p --model sonnet --max-turns 1'],
            capture_output=True, text=True, timeout=120,
        )
        os.unlink(prompt_file)

        if result.returncode != 0:
            logger.error(f"Claude CLI error: {(result.stderr or '')[:300]}")
            return None
        return result.stdout.strip()
    except FileNotFoundError:
        logger.error("Claude CLI not found")
        return None
    except subprocess.TimeoutExpired:
        logger.error("Claude CLI timed out")
        return None


def parse_response(raw):
    """Parse Claude's JSON response."""
    if not raw:
        return []
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("[")
        end = text.rfind("]") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass
    logger.error(f"Failed to parse: {text[:200]}")
    return []


def save_classifications(results):
    """Save classification results to database."""
    conn = get_conn()
    cur = conn.cursor()
    updated = 0

    for r in results:
        try:
            symbols = r.get("affected_symbols", [])
            if not isinstance(symbols, list):
                symbols = []
            cur.execute("""
                UPDATE market_news SET
                    impact = %s,
                    sentiment = %s,
                    market_impact = %s,
                    affected_symbols = %s,
                    summary = %s,
                    classified_at = NOW()
                WHERE id = %s
            """, (
                r.get("impact", "LOW"),
                r.get("sentiment", "NEUTRAL"),
                r.get("market_impact", "MACRO"),
                symbols if symbols else None,
                r.get("summary", ""),
                r["id"],
            ))
            updated += cur.rowcount
        except Exception as e:
            logger.warning(f"Failed to save classification for id={r.get('id')}: {e}")
            conn.rollback()
            conn = get_conn()
            cur = conn.cursor()

    conn.commit()
    conn.close()
    return updated


def classify_news(days=None, force_all=False):
    """Main: classify unclassified news items."""
    ensure_columns()

    news = load_unclassified(days=days, force_all=force_all)
    if not news:
        logger.info("No news to classify")
        return 0

    logger.info(f"Classifying {len(news)} news items in batches of {BATCH_SIZE}...")

    batches = [news[i:i + BATCH_SIZE] for i in range(0, len(news), BATCH_SIZE)]
    total_updated = 0

    for i, batch in enumerate(batches, 1):
        logger.info(f"Batch {i}/{len(batches)} ({len(batch)} items)...")

        prompt = build_prompt(batch)
        raw = call_claude(prompt)
        results = parse_response(raw)

        if results:
            updated = save_classifications(results)
            total_updated += updated
            logger.info(f"  Saved {updated} classifications")
        else:
            logger.warning(f"  No results from batch {i}")

        if i < len(batches):
            time.sleep(BATCH_DELAY)

    logger.info(f"Done: {total_updated} news items classified")
    return total_updated


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Classify market news using Claude")
    parser.add_argument("--days", type=int, default=None, help="Only classify last N days")
    parser.add_argument("--all", action="store_true", help="Reclassify all news")
    args = parser.parse_args()

    classify_news(days=args.days, force_all=args.all)
