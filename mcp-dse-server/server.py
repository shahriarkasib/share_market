"""
MCP Server for DSE Trading Assistant

Connects to the Render-hosted backend API. Lets Claude Desktop answer
questions like "what should I buy tomorrow?" using today's automated analysis.

Tools:
- get_daily_analysis: Today's BUY/WAIT/AVOID picks with entry/exit prices
- get_stock_detail: Full technical analysis for a specific stock
- get_market_summary: DSEX index, breadth, top movers
- query_screener: Filter stocks by RSI, MACD, price range, etc.
- get_analysis_history: Past daily analyses for a stock (trend tracking)
"""

import json
import httpx
from typing import Any
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

server = Server("dse-trading")

API_BASE = "https://share-market-kk7e.onrender.com/api/v1"
TIMEOUT = 30.0


# ── helpers ──────────────────────────────────────────────────────────

def _fmt_num(v, decimals=1):
    """Format a number for display."""
    if v is None:
        return "-"
    try:
        return f"{float(v):,.{decimals}f}"
    except (ValueError, TypeError):
        return str(v)


def _fmt_pct(v):
    if v is None:
        return "-"
    try:
        return f"{float(v):+.1f}%"
    except (ValueError, TypeError):
        return str(v)


def _action_emoji(action: str) -> str:
    m = {
        "BUY": "🟢", "BUY on dip": "🟡", "BUY (wait for MACD cross)": "🟠",
        "HOLD/WAIT": "🔵", "SELL/AVOID": "🔴", "AVOID": "🔴",
    }
    return m.get(action, "⚪")


# ── tool definitions ─────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="get_daily_analysis",
            description=(
                "Get today's (or a specific date's) automated stock analysis for DSE. "
                "Returns BUY, BUY-on-dip, WAIT-for-MACD, HOLD/WAIT, and AVOID picks "
                "with entry ranges, stop-loss, targets, risk/reward, and reasoning. "
                "Use this when the user asks 'what should I buy?', 'what are today's picks?', "
                "'show me BUY stocks', etc."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format. Defaults to today.",
                    },
                    "action": {
                        "type": "string",
                        "description": "Filter by action: BUY, BUY on dip, WAIT for MACD, HOLD/WAIT, SELL/AVOID, AVOID. Leave empty for all.",
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="get_stock_detail",
            description=(
                "Get full technical analysis for a specific DSE stock symbol. "
                "Returns price, indicators (RSI, MACD, StochRSI, BB), entry/exit levels, "
                "support/resistance, scenarios, and trading recommendation. "
                "Use when user asks about a specific stock like 'tell me about BSRMSTEEL' or 'should I buy GP?'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "DSE stock symbol (e.g., BSRMSTEEL, GP, BEXIMCO)",
                    },
                    "date": {
                        "type": "string",
                        "description": "Date YYYY-MM-DD. Defaults to today.",
                    },
                },
                "required": ["symbol"],
            },
        ),
        Tool(
            name="get_market_summary",
            description=(
                "Get current DSE market summary: DSEX index, change, breadth (advances/declines), "
                "total volume and value, market status. Use when user asks 'how is the market today?' "
                "or 'what is DSEX at?'."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="query_screener",
            description=(
                "Screen DSE stocks by technical criteria. Filter by RSI range, MACD status, "
                "price range, volume, etc. Use when user asks 'show me oversold stocks', "
                "'which stocks have bullish MACD crossover?', 'stocks under 100 BDT with high volume'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "rsi_max": {
                        "type": "number",
                        "description": "Maximum RSI (e.g., 30 for oversold)",
                    },
                    "rsi_min": {
                        "type": "number",
                        "description": "Minimum RSI",
                    },
                    "macd_status": {
                        "type": "string",
                        "description": "MACD status filter: 'Bullish crossover', 'Bearish crossover', 'Converging bullish', 'Converging bearish'",
                    },
                    "price_min": {
                        "type": "number",
                        "description": "Minimum LTP (BDT)",
                    },
                    "price_max": {
                        "type": "number",
                        "description": "Maximum LTP (BDT)",
                    },
                    "action": {
                        "type": "string",
                        "description": "Analysis action filter: BUY, BUY on dip, WAIT for MACD, HOLD/WAIT, AVOID",
                    },
                    "sort_by": {
                        "type": "string",
                        "description": "Sort field: score, rsi, volume, change_pct. Default: score",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 20)",
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="get_analysis_history",
            description=(
                "Get past daily analyses for a stock to track how its recommendation changed over time. "
                "Use when user asks 'how has BSRMSTEEL been rated this week?' or "
                "'show me the trend for GP's analysis'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "DSE stock symbol",
                    },
                    "days": {
                        "type": "integer",
                        "description": "Number of past days to look back (default 7)",
                    },
                },
                "required": ["symbol"],
            },
        ),
    ]


# ── tool handlers ────────────────────────────────────────────────────

@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    try:
        if name == "get_daily_analysis":
            return await handle_daily_analysis(arguments)
        elif name == "get_stock_detail":
            return await handle_stock_detail(arguments)
        elif name == "get_market_summary":
            return await handle_market_summary()
        elif name == "query_screener":
            return await handle_screener(arguments)
        elif name == "get_analysis_history":
            return await handle_analysis_history(arguments)
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def handle_daily_analysis(args: dict) -> list[TextContent]:
    """Fetch today's daily analysis from the backend."""
    params = {}
    if args.get("date"):
        params["date"] = args["date"]
    if args.get("action"):
        params["action"] = args["action"]

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{API_BASE}/analysis/daily", params=params, timeout=TIMEOUT
        )
        if resp.status_code != 200:
            return [TextContent(type="text", text=f"API error {resp.status_code}: {resp.text}")]

        data = resp.json()

    analysis = data.get("analysis", [])
    count = data.get("count", 0)
    date = data.get("date", "?")
    summary = data.get("summary", {})

    if count == 0:
        return [TextContent(type="text", text=f"No analysis available for {date}. The daily analysis may not have run yet today.")]

    # Build summary
    out = f"## DSE Daily Analysis — {date}\n\n"
    out += f"**{count} stocks analyzed**\n"
    for action, cnt in sorted(summary.items(), key=lambda x: -x[1]):
        out += f"- {_action_emoji(action)} {action}: {cnt}\n"
    out += "\n"

    # Group by action
    groups: dict[str, list] = {}
    for item in analysis:
        act = item.get("action", "UNKNOWN")
        groups.setdefault(act, []).append(item)

    # Show BUY stocks first with full detail
    for action in ["BUY", "BUY on dip", "BUY (wait for MACD cross)", "HOLD/WAIT", "SELL/AVOID", "AVOID"]:
        items = groups.get(action, [])
        if not items:
            continue

        out += f"### {_action_emoji(action)} {action} ({len(items)} stocks)\n\n"

        if action in ("BUY", "BUY on dip"):
            # Full detail for actionable picks
            for s in items[:15]:
                out += f"**{s.get('symbol')}** — LTP: {_fmt_num(s.get('ltp'))} BDT\n"
                out += f"  Entry: {_fmt_num(s.get('entry_low'))}–{_fmt_num(s.get('entry_high'))} | "
                out += f"SL: {_fmt_num(s.get('sl'))} | T1: {_fmt_num(s.get('t1'))} | T2: {_fmt_num(s.get('t2'))}\n"
                out += f"  Risk: {_fmt_pct(s.get('risk_pct'))} | Reward: {_fmt_pct(s.get('reward_pct'))} | "
                out += f"RSI: {_fmt_num(s.get('rsi'))} | MACD: {s.get('macd_status', '-')}\n"
                if s.get("reasoning"):
                    out += f"  {s['reasoning'][:200]}\n"
                out += "\n"
        else:
            # Compact list for non-actionable
            for s in items[:10]:
                out += f"- **{s.get('symbol')}** LTP: {_fmt_num(s.get('ltp'))} | RSI: {_fmt_num(s.get('rsi'))} | {s.get('macd_status', '-')}"
                if s.get("reasoning"):
                    out += f" — {s['reasoning'][:100]}"
                out += "\n"
            if len(items) > 10:
                out += f"  ... and {len(items) - 10} more\n"
            out += "\n"

    return [TextContent(type="text", text=out)]


async def handle_stock_detail(args: dict) -> list[TextContent]:
    """Get full analysis detail for a specific stock."""
    symbol = args.get("symbol", "").upper().strip()
    if not symbol:
        return [TextContent(type="text", text="Please provide a stock symbol.")]

    params = {"date": args["date"]} if args.get("date") else {}

    async with httpx.AsyncClient() as client:
        # Get daily analysis for this stock
        params["action"] = ""  # no filter
        resp = await client.get(
            f"{API_BASE}/analysis/daily", params=params, timeout=TIMEOUT
        )

        # Also get live price
        price_resp = await client.get(
            f"{API_BASE}/stock/{symbol}", timeout=TIMEOUT
        )

    # Find stock in analysis
    stock = None
    if resp.status_code == 200:
        for item in resp.json().get("analysis", []):
            if item.get("symbol", "").upper() == symbol:
                stock = item
                break

    if not stock:
        # Try just showing live price info
        if price_resp.status_code == 200:
            p = price_resp.json()
            return [TextContent(type="text", text=(
                f"## {symbol}\n\n"
                f"LTP: {_fmt_num(p.get('ltp'))} BDT | Change: {_fmt_pct(p.get('change_pct'))}\n"
                f"Volume: {_fmt_num(p.get('volume'), 0)}\n\n"
                f"No daily analysis available for this stock. It may not meet the screening criteria "
                f"(A-category, minimum turnover)."
            ))]
        return [TextContent(type="text", text=f"Stock {symbol} not found in analysis or live prices.")]

    # Build detailed output
    out = f"## {symbol} — {_action_emoji(stock.get('action', ''))} {stock.get('action', 'UNKNOWN')}\n\n"

    out += f"**Price:** {_fmt_num(stock.get('ltp'))} BDT\n\n"

    out += "### Entry/Exit Plan\n"
    out += f"- Entry range: **{_fmt_num(stock.get('entry_low'))}** – **{_fmt_num(stock.get('entry_high'))}** BDT\n"
    out += f"- Stop-loss: **{_fmt_num(stock.get('sl'))}** BDT\n"
    out += f"- Target 1: **{_fmt_num(stock.get('t1'))}** BDT\n"
    out += f"- Target 2: **{_fmt_num(stock.get('t2'))}** BDT\n"
    out += f"- Risk: {_fmt_pct(stock.get('risk_pct'))} | Reward: {_fmt_pct(stock.get('reward_pct'))}\n\n"

    out += "### Technical Indicators\n"
    out += f"- RSI(14): {_fmt_num(stock.get('rsi'))}\n"
    out += f"- StochRSI: {_fmt_num(stock.get('stoch_rsi'))}\n"
    out += f"- MACD: {_fmt_num(stock.get('macd_line'))} / Signal: {_fmt_num(stock.get('macd_signal'))} — **{stock.get('macd_status', '-')}**\n"
    out += f"- Bollinger %B: {_fmt_num(stock.get('bb_pct'))}\n"
    out += f"- ATR: {_fmt_num(stock.get('atr'))} ({_fmt_pct(stock.get('atr_pct'))} volatility)\n"
    out += f"- Support: {_fmt_num(stock.get('support'))} | Resistance: {_fmt_num(stock.get('resistance'))}\n"
    out += f"- 50d Trend: {_fmt_pct(stock.get('trend_50d'))}\n"
    out += f"- Volume ratio: {_fmt_num(stock.get('vol_ratio'))}x avg\n"
    out += f"- Max drawdown: {_fmt_pct(stock.get('max_dd'))}\n\n"

    if stock.get("reasoning"):
        out += f"### Reasoning\n{stock['reasoning']}\n\n"

    if stock.get("wait_days"):
        out += f"**Wait estimate:** {stock['wait_days']}\n\n"

    # Parse scenarios
    scenarios = stock.get("scenarios_json")
    if scenarios:
        if isinstance(scenarios, str):
            try:
                scenarios = json.loads(scenarios)
            except json.JSONDecodeError:
                scenarios = None
        if scenarios:
            out += "### Scenarios\n"
            for sc in scenarios:
                out += f"- **{sc.get('label', '?')}**: {sc.get('description', '')}\n"
            out += "\n"

    return [TextContent(type="text", text=out)]


async def handle_market_summary() -> list[TextContent]:
    """Get market overview."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{API_BASE}/market/summary", timeout=TIMEOUT)

        if resp.status_code != 200:
            return [TextContent(type="text", text=f"API error: {resp.text}")]

        data = resp.json()

    out = "## DSE Market Summary\n\n"
    out += f"**DSEX Index:** {_fmt_num(data.get('dsex_index'))} ({_fmt_pct(data.get('dsex_change_pct'))})\n"
    out += f"**Status:** {data.get('market_status', 'Unknown')}\n\n"
    out += f"- Total volume: {_fmt_num(data.get('total_volume'), 0)}\n"
    out += f"- Total value: {_fmt_num(data.get('total_value'))} mn BDT\n"
    out += f"- Total trades: {_fmt_num(data.get('total_trade'), 0)}\n"
    out += f"- Advances: {data.get('advances', 0)} | Declines: {data.get('declines', 0)} | Unchanged: {data.get('unchanged', 0)}\n"

    # Also get analysis summary if available
    try:
        summary_resp = await httpx.AsyncClient().get(
            f"{API_BASE}/analysis/summary", timeout=TIMEOUT
        )
        if summary_resp.status_code == 200:
            s = summary_resp.json()
            if s.get("total", 0) > 0:
                out += f"\n### Today's Analysis ({s['total']} stocks)\n"
                for action, cnt in s.get("by_action", {}).items():
                    out += f"- {_action_emoji(action)} {action}: {cnt}\n"
    except Exception:
        pass

    return [TextContent(type="text", text=out)]


async def handle_screener(args: dict) -> list[TextContent]:
    """Screen stocks using analysis + signal data."""
    date = args.get("date")

    async with httpx.AsyncClient() as client:
        params = {}
        if date:
            params["date"] = date
        if args.get("action"):
            params["action"] = args["action"]

        resp = await client.get(
            f"{API_BASE}/analysis/daily", params=params, timeout=TIMEOUT
        )

        if resp.status_code != 200:
            return [TextContent(type="text", text=f"API error: {resp.text}")]

        data = resp.json()

    analysis = data.get("analysis", [])
    if not analysis:
        return [TextContent(type="text", text="No analysis data available for screening.")]

    # Apply filters
    filtered = analysis
    if args.get("rsi_max") is not None:
        filtered = [s for s in filtered if (s.get("rsi") or 100) <= args["rsi_max"]]
    if args.get("rsi_min") is not None:
        filtered = [s for s in filtered if (s.get("rsi") or 0) >= args["rsi_min"]]
    if args.get("macd_status"):
        target = args["macd_status"].lower()
        filtered = [s for s in filtered if target in (s.get("macd_status") or "").lower()]
    if args.get("price_min") is not None:
        filtered = [s for s in filtered if (s.get("ltp") or 0) >= args["price_min"]]
    if args.get("price_max") is not None:
        filtered = [s for s in filtered if (s.get("ltp") or 0) <= args["price_max"]]

    # Sort
    sort_key = args.get("sort_by", "score")
    if sort_key == "rsi":
        filtered.sort(key=lambda x: x.get("rsi") or 999)
    elif sort_key == "volume":
        filtered.sort(key=lambda x: x.get("avg_vol") or 0, reverse=True)
    elif sort_key == "change_pct":
        filtered.sort(key=lambda x: abs(x.get("reward_pct") or 0), reverse=True)
    else:
        filtered.sort(key=lambda x: x.get("score") or 0, reverse=True)

    limit = min(args.get("limit", 20), 50)
    filtered = filtered[:limit]

    if not filtered:
        return [TextContent(type="text", text="No stocks match your screening criteria.")]

    out = f"## Screener Results ({len(filtered)} stocks)\n\n"
    out += "| Symbol | Action | LTP | RSI | MACD | Entry | SL | T1 |\n"
    out += "|--------|--------|-----|-----|------|-------|----|----|\n"
    for s in filtered:
        out += (
            f"| {s.get('symbol', '?')} "
            f"| {s.get('action', '?')} "
            f"| {_fmt_num(s.get('ltp'))} "
            f"| {_fmt_num(s.get('rsi'))} "
            f"| {s.get('macd_status', '-')[:20]} "
            f"| {_fmt_num(s.get('entry_low'))}–{_fmt_num(s.get('entry_high'))} "
            f"| {_fmt_num(s.get('sl'))} "
            f"| {_fmt_num(s.get('t1'))} |\n"
        )

    return [TextContent(type="text", text=out)]


async def handle_analysis_history(args: dict) -> list[TextContent]:
    """Show how a stock's analysis changed over past days."""
    symbol = args.get("symbol", "").upper().strip()
    if not symbol:
        return [TextContent(type="text", text="Please provide a stock symbol.")]

    days = min(args.get("days", 7), 30)

    # Get available dates
    async with httpx.AsyncClient() as client:
        dates_resp = await client.get(f"{API_BASE}/analysis/dates", timeout=TIMEOUT)

        if dates_resp.status_code != 200:
            return [TextContent(type="text", text="Could not fetch available analysis dates.")]

        available = dates_resp.json().get("dates", [])

    if not available:
        return [TextContent(type="text", text="No historical analysis data available yet.")]

    # Fetch last N dates
    recent_dates = sorted(available, reverse=True)[:days]
    history = []

    async with httpx.AsyncClient() as client:
        for d in recent_dates:
            resp = await client.get(
                f"{API_BASE}/analysis/daily", params={"date": d}, timeout=TIMEOUT
            )
            if resp.status_code != 200:
                continue
            for item in resp.json().get("analysis", []):
                if item.get("symbol", "").upper() == symbol:
                    history.append({"date": d, **item})
                    break

    if not history:
        return [TextContent(type="text", text=f"No analysis history found for {symbol} in the last {days} days.")]

    out = f"## {symbol} — Analysis History ({len(history)} days)\n\n"
    out += "| Date | Action | LTP | RSI | MACD Status | Entry Range |\n"
    out += "|------|--------|-----|-----|-------------|-------------|\n"

    for h in history:
        out += (
            f"| {h['date']} "
            f"| {_action_emoji(h.get('action', ''))} {h.get('action', '?')} "
            f"| {_fmt_num(h.get('ltp'))} "
            f"| {_fmt_num(h.get('rsi'))} "
            f"| {h.get('macd_status', '-')} "
            f"| {_fmt_num(h.get('entry_low'))}–{_fmt_num(h.get('entry_high'))} |\n"
        )

    if history:
        latest = history[0]
        out += f"\n**Latest ({latest['date']}):** {latest.get('reasoning', 'No reasoning available.')}\n"

    return [TextContent(type="text", text=out)]


# ── entry point ──────────────────────────────────────────────────────

def main():
    import asyncio
    asyncio.run(run_server())


async def run_server():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    main()
