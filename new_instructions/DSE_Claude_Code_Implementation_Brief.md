# DSE Trading Assistant V2 — Claude Code Implementation Brief
# =============================================================
# Give this to Claude Code. It tells it WHAT to build, not how RSI works.


## WHAT EXISTS

- Frontend: React (Vite) deployed on Vercel at dse-trading.vercel.app
- Backend: FastAPI on GCloud VM
- Database: on GCloud VM (daily_prices, stocks, news tables)
- Current pipeline: Claude Opus analyzes 178 A-cat stocks daily via CLI on GCloud VM
- Data: daily OHLCV scraped for all DSE stocks, stored in VM database
- Pages: Dashboard, Heatmap, Matrix, News, Chart, Analysis/Radar, Dividends, Seasonal, Floor Detection
- Stack: React (Vite) + FastAPI + GCloud VM (NOT Next.js)


## WHAT TO BUILD

### 1. Multi-Timeframe Data Pipeline

For each stock, generate 3 CSVs from daily OHLCV using pandas_ta:

```
/stock_data/{TICKER}/
├── daily.csv      (500 rows, 2 years)
├── weekly.csv     (104 rows, 2 years, aggregated from daily)
├── monthly.csv    (24 rows, 2 years, aggregated from daily)
└── fundamentals.json
```

**Weekly aggregation:** Group by week (Sun-Thu for DSE). Open = first day's open, High = max high, Low = min low, Close = last day's close, Volume = sum.

**Monthly aggregation:** Same logic for calendar month.

**Indicators to compute via pandas_ta on ALL three timeframes:**
RSI(14), StochRSI(14,14,3,3), MACD(12,26,9), CMF(20), MFI(14), ADX(14), +DI(14), -DI(14), BBands(20,2), ATR(14), EMA(9), EMA(21), EMA(50), SMA(50), SMA(200), EMA(200), Williams%R(14), OBV, Ichimoku(9,26,52,26), CCI(20), VWAP if possible.

**Additional computed columns on daily:**
- atr_pct = ATR / close × 100
- vol_ratio = volume / avg_volume_20
- chg_5d, chg_10d, chg_20d (% price changes)
- macd_hist_slope = (hist_today - hist_3d_ago) / 3
- obv_slope_10d = linear regression slope of OBV over 10 days
- price_slope_10d = linear regression slope of close over 10 days
- cmf_slope_10d = linear regression slope of CMF over 10 days
- swing_low_20d = min close in 20 days
- pct_from_swing_low = (close - swing_low) / swing_low × 100
- days_since_swing_low
- up_down_vol_ratio = avg vol on green days / avg vol on red days (10-day)
- bb_width_percentile = percentile rank of BB width vs last 50 days
- cmf_consecutive_positive_days = streak count of CMF > 0 from latest
- cmf_consecutive_negative_days = same for < 0
- ma_aligned = bool (EMA9 > EMA21 > EMA50 > SMA200)
- golden_cross = bool (SMA50 just crossed above SMA200)
- death_cross = bool (SMA50 just crossed below SMA200)

**Fundamentals JSON per stock** (scrape from DSE/amarstock/stockbd or manually maintain):
```json
{
  "ticker": "ACMELAB",
  "sector": "Pharmaceuticals & Chemicals",
  "category": "A",
  "market_cap": 16290000000,
  "shares_outstanding": 211600000,
  "eps_ttm": 11.36,
  "pe_ratio": 6.31,
  "nav_per_share": 120.5,
  "dividend_yield_pct": 4.68,
  "dividend_per_share": 3.50,
  "last_dividend_date": "2025-10-15",
  "next_agm_date": null,
  "debt_equity": 0.96,
  "current_ratio": 1.08,
  "roe_pct": 9.2,
  "revenue_ttm": 35940000000,
  "revenue_growth_pct": 12.5,
  "net_profit_ttm": 2400000000,
  "net_margin_pct": 7.3,
  "free_cash_flow": -1500000000,
  "high_52w": 85.9,
  "low_52w": 68.0
}
```

**DSEX index data:** Same format as stocks. Compute same indicators. Also compute:
- advances, declines, unchanged (from daily market data)
- total turnover in crore
- market regime: derive from ADX, SMA50 slope, price vs SMA50

**Cron schedule:** Run daily at 3:00 PM BST after market close.


### 2. AI Analysis Pipeline

For each stock (start with 178 A-category), feed Claude the full data and get back structured JSON.

**Input to Claude (per stock):**
- daily.csv (full 500 rows)
- weekly.csv (full 104 rows)
- monthly.csv (full 24 rows)
- fundamentals.json
- DSEX daily data (last 60 rows)
- Today's market breadth (advances/declines/turnover)
- Recent news for this stock (from news table)
- Sector performance data
- Seasonal pattern for current month
- User's held position if any

**Claude's system prompt context** (the DSE-specific things Claude wouldn't know):
- T+2 settlement rule and its implications for SL
- Circuit breaker at ±10%
- Volume < 10K = untradeable
- Tick size 0.10 BDT
- CMF must stay positive 5+ consecutive days for real accumulation (learned from ACMELAB analysis where CMF crosses zero 24x/year with 42% win rate)
- Never buy the day of MACD cross (40-60% fakeout rate on DSE)
- Day 2 Rule for market dips
- Dividends drop stock price by dividend amount on ex-date — range-bound stocks may not recover
- Position types: TREND (hold with trailing SL) vs RANGE (exit at breakeven)
- Market regime (DSEX ADX < 15 = choppy, signals unreliable)
- Bangladesh macro context matters: remittance, forex reserves, interest rates

**Output JSON schema** (Claude returns this per stock):
```json
{
  "ticker": "",
  "date": "",
  "ltp": 0,
  "sector": "",
  "category": "",
  "overall_signal": "BUY | HOLD | SELL | AVOID | WATCH",
  "signal_strength": "STRONG | MEDIUM | WEAK",
  "confidence": "HIGH | MEDIUM | LOW",
  "classification": "ENTRY_ZONE | READY | APPROACHING | BUILDING | WATCHING",
  "position_type": "STRONG_TREND | TREND | EMERGING | RANGE | CHOPPY",
  "one_liner": "",
  "score": {
    "overall": 0, "money_flow": 0, "momentum": 0,
    "price_action": 0, "volatility": 0, "fundamentals": 0, "news_sentiment": 0
  },
  "timeframe_alignment": {
    "daily": "", "weekly": "", "monthly": "",
    "aligned": false, "summary": ""
  },
  "indicators": {
    "rsi": { "value": 0, "zone": "", "divergence": "" },
    "stoch_rsi": { "k": 0, "d": 0, "zone": "", "signal": "" },
    "macd": { "line": 0, "signal": 0, "histogram": 0, "status": "", "hist_slope": "", "divergence": "" },
    "cmf": { "value": 0, "zone": "", "consecutive_positive_days": 0, "trend": "", "reliability_note": "" },
    "mfi": { "value": 0, "zone": "" },
    "adx": { "value": 0, "regime": "" },
    "di": { "plus": 0, "minus": 0, "advantage": "" },
    "obv": { "slope": "", "divergence": "" },
    "bb": { "pct": 0, "zone": "", "squeeze": false },
    "moving_averages": {
      "ema9": 0, "ema21": 0, "ema50": 0, "sma50": 0, "sma200": 0, "ema200": 0,
      "aligned": false, "golden_cross": false, "death_cross": false,
      "price_vs_sma200": "", "trend_ma": ""
    },
    "ichimoku": { "price_vs_cloud": "", "cloud_color": "", "support_level": 0 },
    "atr": { "value": 0, "pct": 0 },
    "williams_r": { "value": 0, "zone": "" }
  },
  "divergences": {
    "rsi": { "detected": false, "type": "", "timeframe": "", "detail": "" },
    "macd": { "detected": false, "type": "", "detail": "" },
    "cmf": { "detected": false, "type": "", "detail": "" },
    "obv": { "detected": false, "type": "", "detail": "" },
    "summary": ""
  },
  "candlestick": { "pattern": null, "volume_confirmed": false },
  "volume_analysis": {
    "current": 0, "avg_20d": 0, "ratio": 0,
    "up_down_ratio": 0, "volume_price_signal": "", "analysis": ""
  },
  "fundamentals": {
    "pe_ratio": 0, "pe_vs_sector": "", "pe_vs_market": "",
    "eps": 0, "eps_growth": "",
    "nav_per_share": 0, "price_vs_nav": "",
    "dividend_yield": 0, "dividend_per_share": 0,
    "dividend_sustainable": false, "dividend_sustainability_reason": "",
    "next_dividend_record_date": null, "ex_dividend_impact": "",
    "debt_equity": 0, "debt_assessment": "",
    "free_cash_flow": "", "roe": 0, "current_ratio": 0,
    "revenue_growth": "", "net_margin": 0, "margin_trend": "",
    "market_cap": 0,
    "summary": "", "cheap_or_expensive": "", "why": ""
  },
  "support_resistance": {
    "immediate_support": 0, "major_support": 0,
    "immediate_resistance": 0, "major_resistance": 0,
    "method_used": "", "volume_nodes": []
  },
  "news": {
    "has_catalyst": false, "positive": [], "negative": [],
    "upcoming_events": [], "sentiment": "", "risk_flags": []
  },
  "risk": {
    "t2_risk": "", "liquidity_risk": "", "market_risk": "",
    "sector_risk": "", "fundamental_risk": "", "overall_risk": ""
  },
  "action": {
    "for_new_buyer": "", "for_holder": "",
    "entry_range": "", "stop_loss": 0,
    "stop_loss_method": "", "stop_loss_reasoning": "",
    "target_1": 0, "target_2": 0, "target_method": "",
    "hold_period": "", "position_sizing": "", "what_to_wait_for": ""
  },
  "historical_accuracy": {
    "similar_setups_found": 0, "win_rate": 0, "avg_return": 0, "note": ""
  },
  "ai_reasoning": ""
}
```

Store this JSON in the database per stock per date.


### 3. Market-Level Analysis

After all stocks are analyzed, generate a daily market analysis:

```json
{
  "date": "",
  "dsex": { "close": 0, "change_pct": 0, "regime": "", "regime_multiplier": 0 },
  "breadth": { "advances": 0, "declines": 0, "ratio": 0, "signal": "" },
  "turnover_cr": 0,
  "ai_market_summary": "2-3 sentences about today's market",
  "is_good_day_to_buy": false,
  "global_context": { "sp500": "", "oil": "", "usd_bdt": "" },
  "sector_rotation": { "hot_sectors": [], "cold_sectors": [], "analysis": "" },
  "top_buy_signals": ["TICKER1", "TICKER2"],
  "top_sell_signals": ["TICKER3"],
  "warning_flags": []
}
```


### 4. Frontend Upgrades

Refer to the V2 spec document (DSE_V2_Complete_Spec.md) for page-by-page details. Priority order:

1. **Analysis/Radar page** — redesign into 6 tabs (BUY/SELL/WATCH/AVOID/PORTFOLIO/SECTORS)
2. **Matrix page** — add CMF, CMF streak, ADX, position type, timeframe alignment columns
3. **Dashboard** — add regime badge, AI market summary, global context
4. **Chart page** — add AI analysis panel alongside chart
5. **Stock Deep Dive** — new page at /stock/{TICKER}
6. **DSEX Analysis** — new page at /dsex
7. **Heatmap** — add CMF toggle view
8. **Other pages** — incremental improvements per spec

### 5. API Endpoints

```
GET /api/market          — daily market analysis JSON
GET /api/stocks          — all stocks with latest AI analysis (for matrix)
GET /api/stock/{ticker}  — full analysis JSON for one stock
GET /api/signals/buy     — stocks with BUY signal, sorted by confidence
GET /api/signals/sell    — stocks with SELL signal
GET /api/signals/watch   — stocks on watchlist
GET /api/signals/avoid   — stocks to avoid
GET /api/sectors         — sector-level analysis
GET /api/dsex            — DSEX index analysis
GET /api/dividends       — dividend calendar with AI verdicts
GET /api/seasonal        — seasonal patterns
GET /api/floor           — floor detection data
GET /api/portfolio       — user portfolio with AI re-evaluation
GET /api/heatmap         — heatmap data (daily change + CMF view)
```


## IMPLEMENTATION ORDER

```
Phase 1: Data Pipeline (Week 1)
├── pandas_ta indicator computation for all stocks
├── Weekly/monthly aggregation
├── DSEX indicator computation
├── Store computed CSVs on VM
└── Cron job to update daily

Phase 2: AI Analysis Pipeline (Week 2)
├── Build the prompt template
├── Feed stock data to Claude Desktop via CLI
├── Parse JSON output
├── Store results in database
├── Build market-level analysis
└── Test with 10 stocks first, then scale to 178

Phase 3: API Layer (Week 3)
├── Build all API endpoints
├── Serve AI analysis results from database
├── Ensure fast response times (pre-computed, cached)
└── Test all endpoints

Phase 4: Frontend V2 (Week 3-4)
├── Analysis page redesign (priority 1)
├── Matrix page upgrades
├── Dashboard upgrades
├── New stock deep dive page
├── New DSEX page
└── Other page improvements
```


## KEY PRINCIPLES

1. **No hardcoded analysis logic in frontend or backend.** Python computes indicators. Claude analyzes. Website displays.
2. **Full data to Claude.** Send 500 days daily + 104 weeks + 24 months. Don't compress.
3. **Claude knows finance.** Don't explain RSI or MACD in the prompt. Only explain DSE-specific quirks and lessons from our trading.
4. **Pre-compute everything.** Website loads cached results. No real-time AI calls.
5. **Mobile-first.** Most users check on phones during trading hours.
6. **Accuracy over features.** Better to have 5 accurate pages than 15 half-baked ones.
