"""SMC chart data — OHLCV + FVG zones + BOS/ChoCh events for DSE stocks."""

import pandas as pd
from data.repository import read_historical_for_symbol


def find_swings(h, l, n=3):
    swings = []
    for i in range(n, len(h) - n):
        if float(h.iloc[i]) == float(h.iloc[max(0, i-n):i+n+1].max()):
            swings.append({"idx": i, "type": "high", "price": float(h.iloc[i])})
        if float(l.iloc[i]) == float(l.iloc[max(0, i-n):i+n+1].min()):
            swings.append({"idx": i, "type": "low", "price": float(l.iloc[i])})
    return swings


def detect_structure(swings):
    events = []
    trend = None
    last_sh = None
    last_sl = None
    for sw in swings:
        if sw["type"] == "high":
            if last_sh is not None:
                if sw["price"] > last_sh["price"]:
                    if trend == "up":
                        events.append({"idx": sw["idx"], "type": "bullish_BOS",
                                       "price": sw["price"], "from_idx": last_sh["idx"],
                                       "from_price": last_sh["price"]})
                    elif trend == "down":
                        events.append({"idx": sw["idx"], "type": "bullish_ChoCh",
                                       "price": sw["price"], "from_idx": last_sh["idx"],
                                       "from_price": last_sh["price"]})
                        trend = "up"
                    else:
                        trend = "up"
                elif sw["price"] < last_sh["price"] and trend is None:
                    trend = "down"
            last_sh = sw
        elif sw["type"] == "low":
            if last_sl is not None:
                if sw["price"] < last_sl["price"]:
                    if trend == "down":
                        events.append({"idx": sw["idx"], "type": "bearish_BOS",
                                       "price": sw["price"], "from_idx": last_sl["idx"],
                                       "from_price": last_sl["price"]})
                    elif trend == "up":
                        events.append({"idx": sw["idx"], "type": "bearish_ChoCh",
                                       "price": sw["price"], "from_idx": last_sl["idx"],
                                       "from_price": last_sl["price"]})
                        trend = "down"
                    else:
                        trend = "down"
                elif sw["price"] > last_sl["price"] and trend is None:
                    trend = "up"
            last_sl = sw
    return events


def detect_fvgs(h, l):
    fvgs = []
    for i in range(2, len(h)):
        if float(h.iloc[i-2]) < float(l.iloc[i]):
            fvgs.append({
                "idx": i - 1, "start_idx": i - 2, "type": "bullish",
                "top": float(l.iloc[i]), "bottom": float(h.iloc[i-2]),
                "size_pct": (float(l.iloc[i]) - float(h.iloc[i-2])) / float(h.iloc[i-2]) * 100,
            })
        if float(l.iloc[i-2]) > float(h.iloc[i]):
            fvgs.append({
                "idx": i - 1, "start_idx": i - 2, "type": "bearish",
                "top": float(l.iloc[i-2]), "bottom": float(h.iloc[i]),
                "size_pct": (float(l.iloc[i-2]) - float(h.iloc[i])) / float(h.iloc[i]) * 100,
            })
    return fvgs


def get_smc_chart(symbol: str, days: int = 180):
    """Returns OHLCV candles + volume + FVG zones + BOS/ChoCh events."""
    df = read_historical_for_symbol(symbol, min_rows=int(days * 1.5))
    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Limit to requested period
    cutoff = df["date"].max() - pd.Timedelta(days=days)
    df = df[df["date"] >= cutoff].reset_index(drop=True)
    if len(df) < 30:
        return None

    o, h, l, c = df["open"], df["high"], df["low"], df["close"]
    v = df["volume"] if "volume" in df.columns else pd.Series([0] * len(df))

    candles = []
    volumes = []
    for i, row in df.iterrows():
        time_str = row["date"].strftime("%Y-%m-%d")
        candles.append({
            "time": time_str,
            "open": round(float(row["open"]), 2),
            "high": round(float(row["high"]), 2),
            "low": round(float(row["low"]), 2),
            "close": round(float(row["close"]), 2),
        })
        vol_val = float(row["volume"]) if "volume" in df.columns and pd.notna(row.get("volume")) else 0
        volumes.append({
            "time": time_str,
            "value": vol_val,
            "color": "#26a69a" if row["close"] >= row["open"] else "#ef5350",
        })

    swings = find_swings(h, l, n=3)
    events = detect_structure(swings)
    fvgs = detect_fvgs(h, l)

    # Filter to recent + meaningful (last ~80 bars, size > 0.3%)
    cutoff_idx = max(0, len(df) - 80)
    recent_fvgs = [f for f in fvgs if f["size_pct"] > 0.3 and f["idx"] >= cutoff_idx]
    recent_events = [e for e in events if e["idx"] >= cutoff_idx]

    def idx_to_time(idx):
        if 0 <= idx < len(df):
            return df.iloc[idx]["date"].strftime("%Y-%m-%d")
        return None

    fvg_zones = []
    for f in recent_fvgs:
        start_time = idx_to_time(f["start_idx"])
        end_idx = min(f["idx"] + 30, len(df) - 1)
        end_time = idx_to_time(end_idx)
        if start_time and end_time:
            fvg_zones.append({
                "type": f["type"],
                "top": round(f["top"], 2),
                "bottom": round(f["bottom"], 2),
                "start_time": start_time,
                "end_time": end_time,
            })

    structure_events = []
    for e in recent_events:
        time = idx_to_time(e["idx"])
        from_time = idx_to_time(e["from_idx"])
        if time and from_time:
            structure_events.append({
                "type": e["type"],
                "price": round(e["price"], 2),
                "from_price": round(e["from_price"], 2),
                "time": time,
                "from_time": from_time,
            })

    return {
        "symbol": symbol.upper(),
        "candles": candles,
        "volumes": volumes,
        "fvgs": fvg_zones,
        "structure": structure_events,
        "current_price": round(float(c.iloc[-1]), 2),
    }
