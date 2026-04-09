# DSE Trading Assistant V2 — Complete Implementation Spec
# ========================================================
# This is the Claude Code prompt for upgrading the website.
# Goal: Make it 100% accurate, user-friendly, and genuinely AI-powered.
#
# PRINCIPLE: Don't hardcode analysis logic in the frontend.
# The data pipeline (Python + pandas_ta) computes indicators.
# Claude AI analyzes and decides. The website DISPLAYS results.


## ARCHITECTURE OVERVIEW

```
DATA LAYER (GCloud VM — already exists):
├── daily_prices table (OHLCV for all stocks, scraped daily)
├── Per-stock computed data:
│   ├── daily.csv   (500 days × 25+ indicators via pandas_ta)
│   ├── weekly.csv  (104 weeks × 25+ indicators)
│   ├── monthly.csv (24 months × 25+ indicators)
│   └── fundamentals.json (EPS, P/E, debt, dividends, etc.)
├── DSEX index data (same format)
├── News/events (already being collected)
├── Seasonal data (already computed)
└── Market breadth / sector data

AI LAYER (Claude API — runs daily after market close):
├── Reads all computed data for each stock
├── Analyzes using full context (no hardcoded scoring)
├── Produces structured JSON output per stock
├── Produces market-level analysis
└── Stores results in database

DISPLAY LAYER (React Vite on Vercel + FastAPI on GCloud VM):
├── FastAPI serves AI analysis results from database
├── React frontend renders charts, tables, cards
├── NO analysis logic in frontend — pure display
└── User-friendly, informative, accurate
```


## THE AI ANALYSIS PROMPT (runs daily for each stock)

This is the core prompt that replaces hardcoded scoring.
Feed Claude the multi-timeframe data + fundamentals + news.
Claude thinks and outputs structured JSON.

```
You are analyzing {TICKER} on the Dhaka Stock Exchange.

Here is ALL available data:

=== DAILY DATA (last 60 days) ===
{daily_csv_last_60_rows}

=== WEEKLY DATA (last 26 weeks) ===
{weekly_csv_last_26_rows}

=== MONTHLY DATA (last 12 months) ===
{monthly_csv_last_12_rows}

=== FUNDAMENTALS ===
{fundamentals_json}

=== DSEX MARKET CONTEXT ===
{dsex_daily_last_30_rows}
Market breadth today: {advances} advances, {declines} declines
DSEX turnover: {turnover} crore

=== RECENT NEWS ABOUT THIS STOCK ===
{stock_specific_news_json}

=== SECTOR CONTEXT ===
Sector: {sector}
Sector 30-day performance: {sector_performance}
Sector seasonal pattern for current month: {seasonal_data}

=== CURRENT POSITION (if held) ===
{position_info_if_any}

---

ANALYZE THIS STOCK COMPLETELY. Consider:

1. MULTI-TIMEFRAME ALIGNMENT
   - What does daily say? (short-term momentum)
   - What does weekly say? (medium-term trend)
   - What does monthly say? (long-term direction)
   - Are they aligned or conflicting?

2. MONEY FLOW & SMART MONEY
   - CMF trend: is it positive? How long has it been positive/negative?
   - OBV: is volume accumulating or distributing?
   - Up-day vs down-day volume ratio
   - MFI confirmation

3. DIVERGENCES
   - Check RSI, MACD, CMF, OBV for divergences from price
   - Use window comparison: split last 20 daily bars into 2 halves
   - Also check weekly timeframe for longer-term divergences

4. TREND & REGIME
   - ADX: is there a trend? How strong?
   - Moving average alignment: EMA9 > EMA21 > EMA50 > SMA200?
   - Golden cross or death cross?
   - Ichimoku: price vs cloud position?

5. CANDLESTICK PATTERNS
   - Check last 3 daily candles for reversal/continuation patterns
   - Volume confirmation on patterns?

6. FUNDAMENTALS
   - P/E vs sector average
   - Debt/Equity — is it safe?
   - Dividend yield and sustainability (FCF vs dividend payout)
   - Revenue/earnings growth trend
   - ROE quality

7. NEWS & EVENTS
   - Any upcoming dividend record date?
   - Earnings announcement coming?
   - Corporate actions (rights, bonus, split)?
   - Negative news (fraud, BSEC action, management issues)?

8. RISK ASSESSMENT
   - T+2 lockup risk (how much can it drop in 2 days?)
   - Liquidity risk (avg volume, can you exit?)
   - Market regime risk (DSEX conditions)
   - Sector rotation risk

IMPORTANT CONTEXT FROM EXPERIENCE:
- CMF > 0 is required for any buy. But CMF must stay positive 5+ consecutive days.
- Divergences matter more than absolute indicator levels.
- MACD histogram slope matters more than the cross itself. Never buy the cross day.
- Volume confirms everything. Price up + volume down = fake rally.
- ADX < 15 means choppy — most signals are unreliable.
- Position type matters: TREND stocks hold with trailing SL, RANGE stocks exit at breakeven.
- Ex-dividend drops can take weeks to recover on range-bound stocks.

OUTPUT THIS EXACT JSON STRUCTURE:
```

### AI Output Schema (per stock, stored in database)

```json
{
  "ticker": "ACMELAB",
  "date": "2026-04-02",
  "ltp": 75.5,
  "sector": "Pharmaceuticals & Chemicals",
  "category": "A",

  "overall_signal": "HOLD",
  "signal_strength": "WEAK",
  "confidence": "MEDIUM",

  "classification": "BUILDING",
  "position_type": "RANGE",

  "score": {
    "overall": 32,
    "money_flow": 20,
    "momentum": 35,
    "price_action": 25,
    "volatility": 50,
    "fundamentals": 55,
    "news_sentiment": 50
  },

  "timeframe_alignment": {
    "daily": "NEUTRAL — RSI 47, CMF -0.11, no trend",
    "weekly": "BEARISH — below weekly EMA21, MACD negative",
    "monthly": "NEUTRAL — range-bound 70-82 for 18 months",
    "aligned": false,
    "summary": "No alignment across timeframes. Stock is directionless."
  },

  "indicators": {
    "rsi": { "value": 47.25, "zone": "neutral", "trend": "flat" },
    "stoch_rsi": { "k": 95.0, "d": 69.3, "zone": "overbought", "signal": "K > D but overextended" },
    "macd": {
      "line": -0.34, "signal": 0.08, "histogram": -0.42,
      "status": "bearish",
      "hist_direction": "worsening",
      "note": "MACD bearish, histogram negative and not converging"
    },
    "cmf": {
      "value": -0.109,
      "zone": "distribution",
      "consecutive_positive_days": 0,
      "consecutive_negative_days": 1,
      "trend": "flipping — was positive 2 days ago, now negative again",
      "reliability_note": "CMF crosses zero 24x/year on this stock. 42% win rate. Unreliable."
    },
    "mfi": { "value": 43.2, "zone": "neutral" },
    "adx": { "value": 11.8, "regime": "CHOPPY", "note": "Below 15 — no trend. Signals unreliable." },
    "di": { "plus": 21.5, "minus": 16.9, "advantage": "bulls", "note": "But meaningless with ADX < 15" },
    "obv": { "slope": "negative", "divergence": "none" },
    "bb": { "pct": 0.06, "zone": "near lower band", "squeeze": false },
    "ema_alignment": {
      "ema9": 76.62, "ema21": 77.24, "ema50": 76.65, "sma200": null,
      "aligned": false,
      "note": "All MAs bunched within 76-77 range — no clear trend direction"
    },
    "ichimoku": {
      "price_vs_cloud": "at boundary",
      "cloud_color": "red turning neutral",
      "note": "Price tangled in cloud — no clear signal"
    }
  },

  "divergences": {
    "rsi": { "detected": false, "type": "none" },
    "macd": { "detected": true, "type": "bullish", "detail": "histogram converging on daily" },
    "cmf": { "detected": false, "type": "none" },
    "obv": { "detected": false, "type": "none" },
    "summary": "Only MACD convergence detected, but weak — no multi-indicator confirmation"
  },

  "candlestick": {
    "pattern": null,
    "note": "No significant pattern in last 3 candles"
  },

  "volume_analysis": {
    "current": 215090,
    "avg_20d": 270178,
    "ratio": 0.80,
    "up_down_ratio": 2.76,
    "signal": "Below average volume. But green days have 2.76x more volume than red days — some accumulation happening beneath the surface.",
    "volume_price": "NEUTRAL"
  },

  "fundamentals": {
    "pe_ratio": 6.31,
    "pe_vs_market": "Cheap (market 18.2x)",
    "eps": 11.36,
    "dividend_yield": 4.68,
    "dividend_per_share": 3.50,
    "dividend_sustainable": false,
    "debt_equity": 0.96,
    "roe": 9.2,
    "revenue_growth": 12.5,
    "net_margin": 7.3,
    "margin_trend": "declining (was 8.3%)",
    "fcf": "negative",
    "current_ratio": 1.08,
    "summary": "Fundamentally cheap by P/E but debt-heavy with negative FCF. Dividend may not be sustainable. Revenue growing but margins shrinking."
  },

  "news": {
    "has_catalyst": false,
    "recent_events": [],
    "upcoming_events": [],
    "sentiment": "neutral",
    "risk_flags": []
  },

  "support_resistance": {
    "immediate_support": 75.4,
    "major_support": 72.5,
    "immediate_resistance": 80.7,
    "major_resistance": 82.0,
    "note": "Heavy volume zone at 80-84 — bagholders selling. Support at 72.5 held multiple times."
  },

  "risk": {
    "t2_risk": "MEDIUM — ATR 1.67%, max 2-day drop ~3.3% from entry",
    "liquidity_risk": "LOW — avg volume 270K, can exit easily",
    "market_risk": "DSEX breadth weak (73 up, 290 down). Bearish context.",
    "sector_risk": "Pharma sector -2.5% this month. Defensive but not immune.",
    "overall_risk": "MEDIUM"
  },

  "action": {
    "for_new_buyer": "DON'T BUY. Range-bound on all timeframes. CMF unreliable (42% win rate historically). ADX < 15 = choppy. No catalyst. Wait for: weekly MACD cross + CMF positive 10+ days + ADX > 20.",
    "for_holder": "HOLD cautiously at ৳76.40 entry. You're at -1.2%. StochRSI at 95 = short bounce likely to ৳77-78. This is a RANGE stock — exit goal is breakeven, not profit. SL: ৳72.50.",
    "entry_range": null,
    "stop_loss": 72.5,
    "target_1": 78.0,
    "target_2": 82.0,
    "hold_days": "Exit on next bounce to 77-78",
    "position_sizing": "Small — max 10% of portfolio for range stocks"
  },

  "one_liner": "Range-bound pharma with cheap P/E but unreliable technicals. Hold if in, don't enter fresh.",

  "ai_reasoning": "Multi-timeframe analysis shows no alignment: daily neutral, weekly bearish, monthly flat. CMF has been unreliable on this stock all year (24 flips, 42% accuracy). The up/down volume ratio of 2.76 is the one bullish signal, but it hasn't translated to price movement because institutional flow (CMF) isn't sustaining. Fundamentally undervalued at P/E 6.3x but debt of ৳25.1B with negative FCF explains the discount — it's cheap for a reason. ADX at 11.8 means there's no trend to trade. Best classified as a dividend hold (4.68% yield) for patient investors, not a trading candidate."
}
```


## PAGE-BY-PAGE V2 UPGRADE PLAN

### Page 1: DASHBOARD (/)

**Current:** DSEX chart + breadth bar + top entry picks
**Keep:** Everything — this page is good
**Add/Improve:**

```
TOP SECTION (market pulse):
├── DSEX price + change (keep)
├── Market breadth bar (keep)
├── ADD: Market regime badge — "TRENDING UP" / "CHOPPY" etc
│   with multiplier shown (e.g., "CHOPPY 0.3x — be cautious")
├── ADD: DSEX AI summary — 2-3 sentences from Claude about today's market
│   Example: "DSEX down 1% with 290 declines. Broad selling pressure.
│   Turnover 257M is below average. Not a day to buy."
└── ADD: Global context — S&P 500, Oil, USD/BDT in small badges

DSEX CHART (keep TradingView widget)

TOP ENTRY PICKS SECTION:
├── Keep the current list format
├── ADD: confidence color coding (HIGH=green border, LOW=gray)
├── ADD: "AI Reasoning" expandable — why this stock was picked
├── ADD: fundamentals mini-line (P/E, Div%, Debt/E)
├── CHANGE: Sort by AI confidence, not just score
└── ADD: "Held Positions Alert" section at top if user has portfolio
    showing sell signals for held stocks

NEW SECTION: "Today's Sells" — stocks where AI detected sell signals
├── Bearish divergence detected
├── CMF flipped negative
├── Overbought + fading volume
└── Death cross occurred
```

### Page 2: HEATMAP (/heatmap)

**Current:** Treemap by turnover showing daily movers
**Keep:** Everything — great visual
**Improve:**

```
├── ADD: Toggle between "Today's Change" and "CMF Status"
│   CMF view: green = CMF positive, red = CMF negative, brightness = CMF magnitude
│   This immediately shows WHERE smart money is flowing sector by sector
├── ADD: Toggle for "Weekly Change" and "Monthly Change"
├── ADD: Sector border/grouping — visually group by sector
├── ADD: Click stock → shows mini popup with: signal, CMF, RSI, one-liner
└── ADD: "AI Regime" banner at top: "Market regime: CHOPPY — 0.3x multiplier active"
```

### Page 3: MATRIX (/matrix)

**Current:** Sortable table with Score, Rank, RSI, MACD, Volume, etc.
**Keep:** Core table structure
**Improve:**

```
COLUMNS UPGRADE:
├── KEEP: Symbol, Cat, LTP, Chg%, AI Signal, Score, Rank, RSI, MACD, Volx, Volume
├── ADD: CMF column (color-coded: green > 0, red < 0, with value)
├── ADD: CMF Streak (days positive/negative) — critical for our 5-day rule
├── ADD: ADX column (red < 15, yellow 15-25, green 25-40)
├── ADD: Position Type (TREND / RANGE / CHOPPY)
├── ADD: Timeframe alignment icon (✓ aligned, ✗ conflicting, ~ mixed)
├── ADD: Fundamentals mini-columns (P/E, Div%)
├── CHANGE: "AI Signal" should show the full signal: BUY / HOLD / SELL / AVOID / WATCH
│   with color coding and strength (strong/medium/weak)
├── ADD: One-liner tooltip on hover showing AI reasoning
└── ADD: Filter by signal type: "Show only BUY signals" / "Show only SELL" etc

SORT PRESETS:
├── Best Opportunities (current) — sort by score
├── Strongest CMF — sort by CMF descending (where is smart money?)
├── Most Oversold — sort by RSI ascending
├── Trending Stocks — sort by ADX descending (only ADX > 25)
├── Dividend Plays — sort by dividend yield descending
├── DANGER Zone — sort by CMF ascending (most distribution)
└── NEW: "AI Picks" — Claude's top 5-10 with reasoning
```

### Page 4: NEWS (/news)

**Current:** News with Market Moving / All News / Corporate Events / Dividend Calendar tabs
**Keep:** Everything — already good
**Improve:**

```
├── ADD: AI impact analysis per news item
│   Not just "HIGH" tag — but "Impact: ACMELAB may drop 3-5% if
│   bond default confirmed. Avoid buying."
├── ADD: Affected stocks list per news item
│   "Beximco bond crisis → affects: BXPHARMA, BEXIMCO, BXSYNTH"
├── ADD: Stock-specific news tab when viewing a stock's analysis page
└── ADD: Macro news section — Bangladesh Bank decisions, inflation data,
    remittance updates, forex reserve changes
```

### Page 5: CHART (/chart?symbol=KBPPWBIL)

**Current:** TradingView candlestick + indicator overlays + tooltips explaining indicators
**Keep:** Everything — the educational tooltips are great
**Improve:**

```
├── ADD: AI Analysis panel on the right side (or below chart):
│   Shows the full AI analysis JSON rendered beautifully:
│   - Signal badge (BUY/HOLD/SELL)
│   - Score breakdown (money flow / momentum / price action / volatility)
│   - Divergence alerts with visual markers ON the chart
│   - Support/resistance lines drawn on chart automatically
│   - One-liner summary
│   - Full AI reasoning (expandable)
├── ADD: Multi-timeframe toggle that shows weekly/monthly charts
│   with their own indicator values
├── ADD: "What AI sees" section — highlights specific patterns:
│   "CMF divergence from price (bullish)" with arrow on chart
│   "Volume spike on March 15 — institutional entry" with marker
├── ADD: Historical signal accuracy for this stock:
│   "Past 12 months: 8 buy signals, 5 profitable (62.5% win rate)"
└── ADD: Fundamentals tab below chart showing P/E, dividends, debt, etc.
```

### Page 6: ANALYSIS / BUY RADAR (/radar)

**Current:** Pipeline view — 5 In Entry Zone, 60 Near Entry, 112 Moved Past, 1 Watching
**THIS IS THE PAGE TO REDESIGN.** It's the core product.

```
NEW LAYOUT:

TOP: MARKET CONTEXT (always visible)
├── Regime badge + multiplier
├── DSEX summary (2 lines from AI)
├── Breadth bar
├── Global context (S&P, oil, BDT)
└── "AI says: [Overall market recommendation in 1 sentence]"

TAB 1: "BUY" — stocks to buy
├── Sub-tabs: Entry Zone | Near Entry | Approaching
├── Each stock card shows:
│   ├── Ticker + price + 5d change
│   ├── Signal: "BUY on dip" / "BUY now" / "WAIT for pullback"
│   ├── Confidence: HIGH / MEDIUM / LOW with color
│   ├── Entry range, Target 1, Target 2, Stop Loss
│   ├── CMF value + streak (with color)
│   ├── Key reason (1 line): "Weekly MACD just crossed bullish + CMF 0.15"
│   ├── Position type: TREND (hold for 5-7%) or RANGE (take 3% and exit)
│   ├── Risk tags: "T+2 Risk" / "High DSEX dependency" / "Low volume"
│   ├── Fundamentals mini: P/E | Div% | Debt/E
│   ├── Timeframe alignment: Daily ✓ Weekly ✓ Monthly ~
│   └── EXPANDABLE: Full AI reasoning (3-5 paragraphs)
│       + all indicator values
│       + divergence details
│       + volume analysis
│       + what could go wrong

TAB 2: "SELL" — stocks showing sell signals (NEW)
├── Stocks where AI detected:
│   - Bearish divergence (RSI, MACD)
│   - CMF flipped negative after being positive
│   - RSI > 70 + volume declining
│   - Death cross (SMA50 < SMA200)
│   - Bearish engulfing at resistance
├── Each card shows:
│   ├── Why to sell (specific signal)
│   ├── How urgent (sell now vs sell on bounce)
│   └── Suggested exit price

TAB 3: "WATCH" — on radar but not ready yet (NEW)
├── Stocks building setups — not ready to buy
├── What's needed: "Wait for CMF to cross 0" / "Wait for weekly MACD cross"
├── Days until potential signal
└── Alert: "Notify me when this stock enters buy zone"

TAB 4: "AVOID" — stocks to stay away from (NEW)
├── CMF < -0.15 (heavy distribution)
├── Death cross stocks
├── ADX < 15 + no CMF support (ACMELAB type)
├── News-driven danger (fraud, BSEC action)
└── Each shows WHY to avoid

TAB 5: "PORTFOLIO" — your held positions (NEW)
├── Input your positions (ticker + entry price)
├── Shows P/L for each
├── AI re-evaluates daily: HOLD / SELL / ADD
├── Sell signal alerts highlighted in red
├── Position type: is this a TREND hold or RANGE escape?
├── Days held + expected timeline
└── Dividend calendar for held stocks

TAB 6: "SECTORS" — sector-level analysis (NEW)
├── Which sectors AI recommends (based on CMF, momentum, seasonal)
├── Sector rotation map: money flowing FROM banking TO pharma, etc.
├── Current month seasonal tendency for each sector
└── Top pick per sector with reasoning
```

### Page 7: DIVIDENDS (/dividends)

**Current:** Upcoming record dates with ex-price calculator + historical drop
**Keep:** Everything — excellent feature
**Add:**

```
├── ADD: "Should I buy for dividend?" AI verdict per stock
│   Considers: will ex-drop recover? How fast? Is CMF positive?
│   "PIONEERINS: 25% dividend but historical avg drop -28.8%.
│   Recovery typically takes 45 days. CMF negative. AVOID."
├── ADD: Dividend yield screener — sort all stocks by yield
├── ADD: "Sustainable dividends" filter — FCF positive + payout ratio < 60%
└── ADD: Calendar view (visual timeline of upcoming record dates)
```

### Page 8: SEASONAL (/seasonality)

**Current:** Sector × month heatmap showing historical avg returns
**Keep:** Everything — unique and valuable
**Add:**

```
├── ADD: "This month's playbook" — AI summary of what sectors
│   historically do well THIS month + whether current conditions align
│   "April historically bearish for Banks (-4.3%) and IT (-4.6%).
│   Currently: Bank sector CMF negative, confirming seasonal weakness."
├── ADD: Stock-level seasonal patterns (not just sector)
├── ADD: Best month to buy each sector (highlight row)
└── ADD: Seasonal pattern reliability % (how often does the pattern hold)
```

### Page 9: FLOOR DETECTION (/floor)

**Current:** RSI/StochRSI/MACD floor detection with approach scores
**Keep:** Everything — smart concept
**Improve:**

```
├── ADD: CMF column — is smart money buying at this floor?
│   Floor without CMF support = false floor (trap)
├── ADD: Multi-timeframe floor — is it a daily floor AND weekly floor?
│   Daily floor + weekly floor = much more reliable
├── ADD: Historical floor accuracy per stock
│   "ORIONINFU: 4 floor approaches in 6mo, 3 bounced (75% accurate)"
├── ADD: Volume confirmation — is volume increasing at the floor?
└── CHANGE: Rename "Score" to "Floor Confidence" for clarity
```


## NEW PAGES TO ADD

### Page 10: STOCK DEEP DIVE (/stock/ACMELAB) — NEW

```
A comprehensive single-stock page combining everything:

HEADER:
├── Ticker, price, change, signal badge
├── One-liner AI summary
├── Score breakdown visual (radar chart or bar chart)
└── Fundamentals strip: P/E | EPS | Div% | Debt/E | MCap

SECTION 1: Multi-timeframe chart
├── Daily / Weekly / Monthly toggle
├── AI-detected patterns marked on chart
├── Support/resistance lines auto-drawn
└── Divergence markers

SECTION 2: AI Analysis (the full JSON rendered beautifully)
├── Signal + reasoning
├── Indicator cards (CMF, RSI, MACD, ADX, etc.)
├── Divergence status
├── Volume analysis
└── Risk assessment

SECTION 3: Fundamentals
├── Financial summary (revenue, profit, margins)
├── Dividend history
├── Debt analysis
├── Peer comparison (vs sector)

SECTION 4: News & Events
├── Stock-specific news
├── Upcoming corporate events
├── Dividend calendar

SECTION 5: Historical Signal Performance
├── Past buy/sell signals from AI
├── Win rate, avg return
├── Chart showing signal points
```

### Page 11: DSEX ANALYSIS (/dsex) — NEW

```
Dedicated DSEX index analysis page:

├── DSEX chart (daily + weekly + monthly)
├── Market regime with history (when did it change?)
├── Breadth analysis (advances/declines trend over time)
├── DSEX indicators (RSI, MACD, CMF, ADX)
├── Sector rotation map (money flow between sectors)
├── Global correlation (DSEX vs S&P 500 chart)
├── AI market summary (3-5 paragraphs)
├── "Is it a good day to buy?" verdict
└── Macro context (remittance, forex, inflation data)
```


## DATA PIPELINE (runs daily on GCloud VM)

```
CRON SCHEDULE: Every day at 3:00 PM BST (after market close)

STEP 1: Scrape today's data (15 min)
├── Fetch OHLCV for all 350+ stocks from DSE
├── Fetch DSEX index data
├── Fetch news from configured sources
└── Store raw data in database

STEP 2: Compute indicators (10 min)
├── For each stock:
│   ├── Append today's data to daily.csv
│   ├── Recompute all indicators using pandas_ta
│   ├── Aggregate to weekly.csv and monthly.csv
│   └── Recompute weekly/monthly indicators
├── Compute DSEX indicators
└── Compute sector aggregates

STEP 3: AI Analysis (60-90 min for 178 A-cat stocks)
├── For each stock, send data to Claude API
│   ├── Daily (60 rows) + Weekly (26 rows) + Monthly (12 rows)
│   ├── Fundamentals JSON
│   ├── DSEX context
│   ├── Stock-specific news
│   └── Sector context
├── Claude returns structured JSON (the schema above)
├── Store result in database
├── Generate market-level analysis
└── Generate sector-level analysis

STEP 4: Generate derived views (5 min)
├── Sort stocks into BUY / SELL / WATCH / AVOID categories
├── Update heatmap data
├── Update floor detection
├── Update seasonal analysis
└── Update portfolio alerts

STEP 5: Serve via API to frontend
├── /api/dashboard — market summary + top picks
├── /api/matrix — all stocks with scores
├── /api/stock/{ticker} — full analysis for one stock
├── /api/sectors — sector analysis
├── /api/signals/buy — buy signals
├── /api/signals/sell — sell signals
├── /api/dsex — index analysis
├── /api/news — news with AI impact
├── /api/dividends — dividend calendar
├── /api/seasonal — seasonal data
└── /api/floor — floor detection
```


## CLAUDE API BATCHING STRATEGY

178 A-category stocks × ~2000 tokens per analysis = ~356K tokens output.
Plus input tokens (data per stock ~3000-5000 tokens) = ~700K-900K input.

```
Option A: Batch API (cheapest, 50% discount)
├── Submit all 178 stocks as a batch
├── Results in ~30 minutes
├── Best for daily pipeline
└── Cost: ~$3-5/day with Sonnet

Option B: Individual calls
├── 178 sequential API calls
├── ~1 min per stock = ~3 hours
├── More expensive
└── Only use if batch isn't available

Option C: Tiered analysis
├── Top 50 by volume: full analysis (daily)
├── Next 50: medium analysis (daily indicators only, weekly on weekends)
├── Remaining 78: light scan (weekly only)
├── Reduces cost by 50%
└── Focus AI budget on stocks people actually trade

RECOMMENDED: Option C for cost efficiency.
Full analysis for top 50 = ~$1.50/day
Medium for next 50 = ~$0.50/day
Light for rest = ~$0.25/day
Total: ~$2.25/day = ~$70/month
```


## KEY DESIGN PRINCIPLES FOR V2

1. **AI decides, frontend displays.** No hardcoded `if RSI < 30 then "oversold"` in JavaScript. Claude produces the analysis, frontend renders it.

2. **Show reasoning, not just signals.** Users should understand WHY a stock is rated "BUY" or "AVOID." The AI reasoning is the product.

3. **CMF is king.** Make CMF visible everywhere — in the matrix, on charts, in analysis cards. Color-code it. Show the streak. This is the most important indicator.

4. **Multi-timeframe always.** Never show a signal based on daily alone. Always show daily + weekly + monthly alignment.

5. **Be honest about uncertainty.** If ADX < 15, say "choppy — signals unreliable." Don't pretend to have confidence when the data doesn't support it.

6. **Dividends are not free money.** Always show ex-dividend drop analysis alongside dividend opportunities.

7. **Position type determines strategy.** Clearly label every stock as TREND or RANGE and show the appropriate exit strategy for each.

8. **Historical accuracy builds trust.** Show "this signal type has been 65% accurate over the last year" wherever possible.

9. **Mobile-friendly.** Most DSE traders check on their phones during trading hours. Every page must work on mobile.

10. **Fast.** Pre-compute everything. The website should load in < 2 seconds. No real-time API calls for analysis — everything is cached from the daily pipeline run.
```
