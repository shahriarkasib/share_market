# DSE Trading Assistant V2 — UI/UX Design System
# =================================================
# Give this to Claude Code alongside the other spec files.
# This covers: visual design, component library, layout patterns,
# page-specific designs, responsive behavior, and UX principles.


## DESIGN PHILOSOPHY

The website should feel like a **professional trading terminal** — clean, information-dense but not cluttered, dark-themed, and fast. Think Bloomberg Terminal meets modern fintech. Users are DSE traders checking this on their phones during market hours and on desktop after market close.

**Core principles:**
- Information density > whitespace. Traders want data, not decoration.
- Color = meaning. Green = bullish/positive. Red = bearish/negative. Yellow/amber = caution. Blue = neutral/info. Don't use color decoratively.
- Every pixel earns its place. If a UI element doesn't help the trader make a decision, remove it.
- Mobile-first. Most users check during trading hours on phones.
- Fast. Pre-rendered, no loading spinners. Data should feel instant.
- AI reasoning should be accessible but not overwhelming. Show signal first, reasoning on demand (expandable).


## EXISTING DESIGN LANGUAGE (preserve this)

Based on the current site, the design system uses:

### Colors
```css
/* Background */
--bg-primary: #0d1117;        /* Main background - very dark */
--bg-secondary: #161b22;      /* Cards, panels */
--bg-tertiary: #1c2333;       /* Elevated cards, hover states */
--bg-input: #21262d;          /* Input fields, dropdowns */

/* Text */
--text-primary: #e6edf3;      /* Primary text - off-white */
--text-secondary: #8b949e;    /* Secondary/muted text */
--text-tertiary: #6e7681;     /* Hints, labels */

/* Signals */
--green: #3fb950;             /* Bullish, positive, buy */
--green-bg: rgba(63,185,80,0.12);
--red: #f85149;               /* Bearish, negative, sell */
--red-bg: rgba(248,81,73,0.12);
--yellow: #d29922;            /* Caution, watch, hold */
--yellow-bg: rgba(210,153,34,0.12);
--blue: #58a6ff;              /* Neutral, info */
--blue-bg: rgba(88,166,255,0.12);
--cyan: #39d2c0;              /* Accent, entry zone */
--purple: #bc8cff;            /* Special indicators */
--orange: #f0883e;            /* Warning, approaching */

/* Borders */
--border: rgba(240,246,252,0.1);
--border-emphasis: rgba(240,246,252,0.2);

/* Chart specific */
--chart-candle-green: #26a69a;
--chart-candle-red: #ef5350;
--chart-line-blue: #2962ff;
--chart-volume-green: rgba(38,166,154,0.4);
--chart-volume-red: rgba(239,83,80,0.4);
```

### Typography
```css
/* Use system font stack for performance */
font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans', Helvetica, Arial, sans-serif;

/* Sizes */
--text-xs: 11px;    /* Small labels, timestamps */
--text-sm: 12px;    /* Table cells, secondary info */
--text-base: 13px;  /* Body text, descriptions */
--text-md: 14px;    /* Primary content */
--text-lg: 16px;    /* Section headers */
--text-xl: 20px;    /* Page titles */
--text-2xl: 24px;   /* Big numbers (prices, scores) */
--text-3xl: 32px;   /* Hero numbers (DSEX value) */

/* Weights */
--font-normal: 400;
--font-medium: 500;
--font-semibold: 600;
--font-bold: 700;

/* Monospace for numbers */
font-variant-numeric: tabular-nums;  /* Align numbers in columns */
```

### Spacing & Radius
```css
--radius-sm: 4px;    /* Badges, pills */
--radius-md: 6px;    /* Buttons, inputs */
--radius-lg: 8px;    /* Cards */
--radius-xl: 12px;   /* Large cards, modals */

--space-xs: 4px;
--space-sm: 8px;
--space-md: 12px;
--space-lg: 16px;
--space-xl: 24px;
--space-2xl: 32px;
```


## COMPONENT LIBRARY

### Signal Badges
Used everywhere to show BUY/SELL/HOLD/WATCH/AVOID status.

```
BUY          → green bg, green text, bold
SELL         → red bg, red text, bold
HOLD         → yellow bg, yellow text
WATCH        → blue bg, blue text
AVOID        → red border, red text (outline style, not filled)
ENTRY_ZONE   → cyan bg, dark text, glowing border
APPROACHING  → orange bg, orange text
BUILDING     → purple bg, purple text
WATCHING     → gray bg, gray text (muted)
```

Size variants:
- Small (matrix table): 10px font, 2px 6px padding
- Medium (cards): 11px font, 3px 10px padding
- Large (page headers): 13px font, 4px 14px padding

### Confidence Indicators
```
HIGH   → solid green dot + "HIGH" text
MEDIUM → solid yellow dot + "MEDIUM" text  
LOW    → solid red dot + "LOW" text
```

### Score Display
The overall score (0-100) with category breakdown.

```
Layout:
┌──────────────────────────────────────┐
│  Score: 72        [READY] badge      │
│                                      │
│  Money Flow    ████████████░░ 65     │
│  Momentum      ██████████░░░░ 55     │
│  Price Action  ████████████████ 80   │
│  Volatility    ██████████░░░░ 50     │
│  Fundamentals  ████████████░░ 70     │
└──────────────────────────────────────┘

Bar colors:
- 0-25: red
- 26-50: yellow
- 51-75: blue
- 76-100: green
```

### Indicator Pills
Small inline indicators showing key values with color coding.

```
CMF +0.18     → green text, green-bg pill
CMF -0.09     → red text, red-bg pill
RSI 45        → gray text (neutral)
RSI 28        → green text (oversold = opportunity)
RSI 75        → red text (overbought = danger)
ADX 12        → red text with "CHOPPY" label
ADX 32        → green text with "TRENDING" label
```

### Stock Cards (for Analysis/Radar page)
```
┌─────────────────────────────────────────────────────────┐
│ [Score] TICKER   price  ▼ -2.3% (5d)    [SIGNAL] badge │
│         Sector            0.8x vol                      │
│                                                         │
│ ● BUY on dip  [MEDIUM confidence]                       │
│ "Wait for CMF to sustain positive 5+ days"              │
│                                                         │
│ Entry: 74.0-76.5  T1: 83.0  T2: 87.0  SL: 69.5        │
│                                                         │
│ CMF: -0.06  RSI: 47  MACD: Converging  ADX: 12 CHOPPY  │
│ Daily: Neutral  Weekly: Bearish  Monthly: Flat          │
│                                                         │
│ P/E: 6.3x  Div: 4.7%  D/E: 0.96                       │
│                                                         │
│ ▸ Full AI Analysis (expandable)                         │
└─────────────────────────────────────────────────────────┘

Card border color = signal color (green for BUY, red for SELL, etc.)
Left border accent: 3px solid signal-color
```

### Data Tables (Matrix page)
```
- Sticky header with sort arrows
- Row hover: bg slightly lighter
- Alternating row shading: subtle (every other row 2% lighter)
- Number columns: right-aligned, tabular-nums
- Signal/badge columns: center-aligned
- Clickable rows → navigate to /stock/{ticker}
- Color-code CMF column: gradient from red (negative) to green (positive)
- Color-code RSI: green < 30, gray 30-70, red > 70
- Color-code ADX: red < 15, yellow 15-25, green 25-40, orange > 40
- Compact mode: fit as many rows as possible on screen
- Pagination or virtual scroll for 387 rows
```

### Charts
```
- Use TradingView Lightweight Charts (already using)
- Dark theme matching site colors
- Candlestick: green/red as defined above
- Volume bars: semi-transparent below chart
- Moving averages: distinct colors
  - EMA9: white (thin)
  - EMA21: orange
  - EMA50: blue  
  - SMA200: purple (thick)
- Support/resistance: dashed horizontal lines
- AI signal markers: colored dots on chart (green=buy, red=sell)
- Responsive: full width, 400px height on desktop, 250px on mobile
```

### Expandable AI Reasoning
```
Used on stock cards and deep dive pages.

Collapsed state:
  ▸ Full AI Analysis

Expanded state:
  ▾ Full AI Analysis
  ┌────────────────────────────────────────────────┐
  │ Multi-timeframe analysis shows no alignment... │
  │ CMF has been unreliable on this stock...       │
  │ The up/down volume ratio of 2.76 suggests...   │
  │                                                │
  │ Fundamentals: P/E 6.3x is cheap but...         │
  │ Debt of ৳25.1B with negative FCF...            │
  │                                                │
  │ Risk: T+2 exposure ~3.3% downside...           │
  └────────────────────────────────────────────────┘

- Font: 13px, line-height 1.7
- Color: text-secondary
- Background: bg-tertiary (slightly elevated from card)
- Max-height with scroll if very long
- Highlight key terms: bold for ticker names, colored for signals
```

### Regime Badge (shown on dashboard and analysis pages)
```
TRENDING UP   → green bg, upward arrow icon, "1.5x" multiplier shown
TRENDING DOWN → red bg, downward arrow icon, "0.5x"
RANGING       → blue bg, horizontal arrow, "1.0x"
CHOPPY        → yellow bg, zigzag icon, "0.3x — be cautious"
```

### Market Breadth Bar (already exists, keep)
```
Green section (advances) | Gray (unchanged) | Red (declines)
With counts below: "73 (18.9%)" | "24 (6.2%)" | "290 (74.9%)"
Full width of container
```

### Dividend Calendar Card
```
┌──────────────────────────────────────────────────┐
│ PIONEERINS    Record: Apr 5    ⏰ 1d              │
│ Price: ৳52.9  Div: 25%  Ex-Price: ~৳50.4         │
│ Hist Avg Drop: -28.8%                             │
│                                                   │
│ AI Verdict: AVOID — drop exceeds dividend.         │
│ Range-bound stock, CMF negative. Won't recover.    │
└──────────────────────────────────────────────────┘

Color: red border if AVOID, green if OPPORTUNITY
```


## PAGE LAYOUTS

### Dashboard (/)
```
┌─ HEADER BAR (sticky) ──────────────────────────────────┐
│ 🔥 DSE Trading  | Dashboard | Heatmap | Matrix | ...   │
│ DSEX 5,219.7 ▼-53.0 (-1.0%)  Vol: 257.3M  ● CLOSED   │
└────────────────────────────────────────────────────────┘

┌─ MARKET PULSE ─────────────────────────────────────────┐
│ [CHOPPY 0.3x] badge   ↑73  ↓290  Vol: 257M  195K trades│
│                                                         │
│ AI: "Broad selling pressure with 290 declines.          │
│ Turnover below average. Not a day to buy."              │
│                                                         │
│ 🌍 S&P 500: -0.8%  |  Oil: $82  |  USD/BDT: 121.5     │
└────────────────────────────────────────────────────────┘

┌─ MARKET BREADTH BAR ──────────────────────────────────┐
│ ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░████████████│
│ 73 (18.9%)           24 (6.2%)           290 (74.9%)    │
└────────────────────────────────────────────────────────┘

┌─ DSEX CHART (TradingView) ───────────────────────────┐
│ [49 days line chart with volume bars below]            │
└──────────────────────────────────────────────────────┘

┌─ TWO COLUMN LAYOUT ──────────────────────────────────┐
│                           │                           │
│  🟢 TOP BUY SIGNALS       │  🔴 SELL ALERTS            │
│  (5 stocks)               │  (stocks with sell signals)│
│  [Stock card]             │  [Stock card]              │
│  [Stock card]             │  [Stock card]              │
│  [Stock card]             │                            │
│                           │  ⚠️ HELD POSITION ALERTS   │
│  → View all buy signals   │  [Portfolio alerts]        │
│                           │                            │
└──────────────────────────────────────────────────────┘
```

### Analysis / Radar (/ radar) — COMPLETE REDESIGN
```
┌─ MARKET CONTEXT (always visible) ────────────────────┐
│ [REGIME badge]  DSEX: 5219 ▼-1.0%  ↑73 ↓290         │
│ AI: "Not a buying day. Wait for breadth > 50%."       │
└──────────────────────────────────────────────────────┘

┌─ TABS ──────────────────────────────────────────────┐
│ [🟢 BUY (8)]  [🔴 SELL (3)]  [👀 WATCH (15)]         │
│ [⛔ AVOID (45)] [📊 PORTFOLIO] [📈 SECTORS]           │
└────────────────────────────────────────────────────┘

TAB: BUY
┌─ CATEGORY PILLS ──────────────────────────────────────┐
│ [5 Entry Zone]  [12 Near Entry]  [38 Approaching]      │
└──────────────────────────────────────────────────────┘

┌─ STOCK CARDS (list view) ────────────────────────────┐
│ Each card as defined in component library above       │
│ Sorted by confidence (HIGH first), then score         │
│                                                       │
│ Toggle: [Pipeline view] [List view] [Compact table]   │
└──────────────────────────────────────────────────────┘

TAB: SELL
┌─────────────────────────────────────────────────────┐
│ Stocks where AI detected sell signals:               │
│                                                      │
│ FINEFOODS ৳482  [SELL — take profit]                 │
│ "RSI 71 + StochRSI 95 = overbought. CMF 0.37 is     │
│ strong but short-term pullback likely. Sell 50%."     │
│                                                      │
│ [Stock card with sell reasoning]                      │
└─────────────────────────────────────────────────────┘

TAB: PORTFOLIO
┌─────────────────────────────────────────────────────┐
│ Total invested: ৳4,00,000  |  Current: ৳3,95,000    │
│ P/L: ▼ -৳5,000 (-1.25%)                              │
│                                                      │
│ FINEFOODS  ৳485 → ৳482  ▼-0.6%  [HOLD] TREND        │
│ ACMELAB    ৳76.4 → ৳75.5 ▼-1.2%  [EXIT] RANGE       │
│ SPCERAMICS ৳21.1 → ৳20.0 ▼-5.3%  [HOLD] EMERGING    │
│ SAPORTL    ৳50.5 → ৳50.5  0.0%   [HOLD] EMERGING    │
│ KBPPWBIL   ৳51.5 → ৳50.8 ▼-1.4%  [HOLD] RANGE       │
│                                                      │
│ Each row expandable → full AI re-evaluation           │
└─────────────────────────────────────────────────────┘

TAB: SECTORS
┌─────────────────────────────────────────────────────┐
│ Sector rotation map:                                  │
│ Money flowing INTO: Pharma (+0.08 avg CMF), Cement    │
│ Money flowing OUT: Banking (-0.12 avg CMF), IT        │
│                                                      │
│ [Sector cards with top pick per sector]                │
│ Seasonal note: "April historically bearish for Banks" │
└─────────────────────────────────────────────────────┘
```

### Stock Deep Dive (/stock/{TICKER}) — NEW PAGE
```
┌─ HEADER ────────────────────────────────────────────┐
│ ACMELAB  ৳75.5  ▼-1.2%     [BUILDING] [RANGE]       │
│ The ACME Laboratories Limited                        │
│ Pharmaceuticals & Chemicals  |  Category A           │
│                                                      │
│ "Range-bound pharma with cheap P/E but unreliable    │
│ technicals. Hold if in, don't enter fresh."          │
└────────────────────────────────────────────────────┘

┌─ SCORE VISUAL ──────────────────────────────────────┐
│ [Score breakdown bars — money flow / momentum /      │
│  price action / volatility / fundamentals]           │
│                                                      │
│ Confidence: MEDIUM  |  Position type: RANGE          │
└────────────────────────────────────────────────────┘

┌─ TABS ──────────────────────────────────────────────┐
│ [📊 Chart] [🤖 AI Analysis] [📋 Fundamentals]        │
│ [📰 News] [📈 History] [🎯 Signals]                   │
└────────────────────────────────────────────────────┘

CHART TAB:
- TradingView chart with Daily / Weekly / Monthly toggle
- Indicator overlay buttons (same as current chart page)
- AI markers on chart (buy/sell signal points)
- Support/resistance lines auto-drawn

AI ANALYSIS TAB:
- Full AI reasoning (rendered nicely, not raw JSON)
- Indicator cards in grid layout
- Divergence status with visual
- Volume analysis
- Timeframe alignment summary
- Risk assessment

FUNDAMENTALS TAB:
- Financial metrics in grid
- Dividend history
- Debt analysis
- Peer comparison table (vs sector)

NEWS TAB:
- Stock-specific news
- Corporate events timeline
- Upcoming events (dividend, AGM, earnings)

HISTORY TAB:
- Past AI signals for this stock
- Win rate, avg return
- Chart showing where signals occurred

SIGNALS TAB:
- What conditions would trigger a BUY
- What conditions would trigger a SELL
- "Waiting for: CMF > 0 for 5+ days AND weekly MACD cross"
```


## RESPONSIVE DESIGN

### Breakpoints
```css
/* Mobile first */
@media (min-width: 640px)  { /* sm — tablet portrait */ }
@media (min-width: 768px)  { /* md — tablet landscape */ }
@media (min-width: 1024px) { /* lg — desktop */ }
@media (min-width: 1280px) { /* xl — wide desktop */ }
```

### Mobile Adaptations
```
- Navigation: hamburger menu on mobile, horizontal tabs on desktop
- Stock cards: full width, stacked vertically
- Matrix table: horizontal scroll with sticky first column (symbol)
- Charts: reduced height (250px), full width
- Score bars: simplified (number only, no bar on mobile)
- Tabs: scrollable horizontal on mobile if too many
- AI reasoning: collapsed by default, tap to expand
- Heatmap: may not work well on small screens — show list view alternative
- Dashboard two-column → single column on mobile
- Entry/Target/SL: stacked vertically on mobile instead of horizontal
```


## INTERACTION PATTERNS

### Navigation
- Click stock anywhere → goes to /stock/{TICKER} deep dive
- Search bar in header: fuzzy search by ticker or company name
- Keyboard shortcut: "/" to focus search
- Back button always works (proper routing)

### Data Freshness
- Show "Last updated: 3:15 PM BST" timestamp on dashboard
- Show "● CLOSED" or "● LIVE" market status indicator
- If data is > 24 hours old, show warning banner

### Loading States
- Skeleton loaders matching card/table shapes
- Never show empty states — show "No stocks match this filter" instead

### Filtering & Sorting
- Matrix: multi-column sort, filter by sector/category/signal
- Analysis: filter by confidence level, sector, score range
- All filters via URL params (shareable links)

### User Preferences (localStorage)
- Preferred view (pipeline vs list vs compact)
- Portfolio positions (ticker + entry price)
- Watchlist
- Hidden columns in matrix


## ANIMATIONS (subtle, purposeful)

```css
/* Card hover */
transition: background-color 0.15s, border-color 0.15s;

/* Score bar fill */
transition: width 0.6s ease-out;

/* Expandable sections */
transition: max-height 0.3s ease-out;

/* Number changes (price updates) */
/* Brief green/red flash on value change */
@keyframes flash-green { 0% { color: var(--green); } 100% { color: var(--text-primary); } }
@keyframes flash-red { 0% { color: var(--red); } 100% { color: var(--text-primary); } }
```

No loading spinners. No bounce animations. No decorative motion.
Only functional transitions that help the user track changes.


## ACCESSIBILITY

- All interactive elements have focus states (2px blue outline)
- Color is never the ONLY way to convey information (always paired with text/icon)
- Min touch target: 44px × 44px on mobile
- Tables have proper `<thead>`, `<tbody>`, `scope` attributes
- Images/charts have alt text with the key data point
- Sufficient contrast ratio (4.5:1 minimum for text on dark bg)
