"""
market_structure.py — Shared price-action market-structure toolkit.

Replicates the underlying CONCEPTS behind a family of popular retail price-action indicators
(fair value gaps, liquidity sweeps, equal highs/lows, retest-and-break continuation) using plain
OHLCV mathematics. These are standard, publicly-documented ICT/SMC price-action techniques, not
proprietary to any vendor — pivots, 3-candle imbalances, and ATR-filtered breakouts are generic
math, not a specific provider's IP. Built once here so every sector script that already has OHLCV
data on hand (futures, TQQQ) can plug in without any new API calls.
"""

import pandas as pd
import numpy as np


def calculate_atr_series(df, period=14):
    """True Range rolling average — same formula used elsewhere in the ecosystem (cross_asset.py, tqqq.py)."""
    high_low = df["high"] - df["low"]
    high_cp = (df["high"] - df["close"].shift()).abs()
    low_cp = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_cp, low_cp], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def find_swing_points(df, lookback=3):
    """
    A bar's high/low is a confirmed swing point if it's the extreme within lookback bars on
    each side. Returns two boolean Series aligned to df's index.
    """
    highs, lows = df["high"], df["low"]
    swing_high = pd.Series(False, index=df.index)
    swing_low = pd.Series(False, index=df.index)
    n = len(df)
    for i in range(lookback, n - lookback):
        window_h = highs.iloc[i - lookback: i + lookback + 1]
        window_l = lows.iloc[i - lookback: i + lookback + 1]
        if highs.iloc[i] == window_h.max():
            swing_high.iloc[i] = True
        if lows.iloc[i] == window_l.min():
            swing_low.iloc[i] = True
    return swing_high, swing_low


def detect_fvgs(df, atr_series=None, atr_mult=0.25, max_lookback=60):
    """
    Fair Value Gap: a 3-candle imbalance where candle 3 doesn't overlap candle 1's range —
    bullish when candle 3's low sits above candle 1's high, bearish the mirror case. Filtered by
    an ATR multiple so noise-sized gaps on quiet bars don't count. Tracks whether each gap has
    since been "filled" (price traded back through the zone) — an unfilled gap is the part that
    matters, since price is statistically drawn back to imbalanced zones.
    """
    df = df.tail(max_lookback).reset_index(drop=True)
    if atr_series is None:
        atr_series = calculate_atr_series(df)
    else:
        atr_series = atr_series.tail(max_lookback).reset_index(drop=True)

    fvgs = []
    for i in range(2, len(df)):
        c0, c2 = df.iloc[i - 2], df.iloc[i]
        atr_val = atr_series.iloc[i] if pd.notna(atr_series.iloc[i]) else 0.0
        min_gap = atr_val * atr_mult
        if c2["low"] > c0["high"] and (c2["low"] - c0["high"]) >= min_gap:
            fvgs.append({"type": "bullish", "top": float(c2["low"]), "bottom": float(c0["high"]), "bar_index": i, "filled": False})
        elif c2["high"] < c0["low"] and (c0["low"] - c2["high"]) >= min_gap:
            fvgs.append({"type": "bearish", "top": float(c0["low"]), "bottom": float(c2["high"]), "bar_index": i, "filled": False})

    for fvg in fvgs:
        for j in range(fvg["bar_index"] + 1, len(df)):
            if df["low"].iloc[j] <= fvg["top"] and df["high"].iloc[j] >= fvg["bottom"]:
                fvg["filled"] = True
                break
    return fvgs


def detect_liquidity_sweep(df, lookback=20):
    """
    Classic stop-hunt pattern: the most recent bar pokes beyond a prior swing high/low (sweeping
    the resting stop-loss liquidity sitting just past it) and then closes back inside that level —
    the rejection is the tell that it was a sweep, not a genuine breakout.
    """
    if len(df) < lookback + 2:
        return None
    recent = df.iloc[-(lookback + 1):-1]
    last = df.iloc[-1]
    prior_high, prior_low = recent["high"].max(), recent["low"].min()

    if last["high"] > prior_high and last["close"] < prior_high:
        return {"type": "bearish_sweep", "swept_level": float(prior_high), "close": float(last["close"])}
    if last["low"] < prior_low and last["close"] > prior_low:
        return {"type": "bullish_sweep", "swept_level": float(prior_low), "close": float(last["close"])}
    return None


def detect_equal_highs_lows(df, lookback=50, tolerance_pct=0.0015, swing_lookback=3):
    """
    Clusters swing highs/lows that sit within tolerance_pct of each other — repeated touches at
    nearly the same level mark a liquidity pool (a lot of resting stops/orders), which is exactly
    the kind of level a sweep targets next.
    """
    window = df.tail(lookback).reset_index(drop=True)
    if len(window) < swing_lookback * 2 + 1:
        return [], []
    sh, sl = find_swing_points(window, lookback=swing_lookback)
    high_vals = window.loc[sh, "high"].tolist()
    low_vals = window.loc[sl, "low"].tolist()

    def cluster(vals):
        clusters = []
        for v in sorted(vals):
            for c in clusters:
                if c["level"] > 0 and abs(v - c["level"]) / c["level"] <= tolerance_pct:
                    c["members"].append(v)
                    c["level"] = sum(c["members"]) / len(c["members"])
                    break
            else:
                clusters.append({"level": v, "members": [v]})
        return [c for c in clusters if len(c["members"]) >= 2]

    return cluster(high_vals), cluster(low_vals)


def calculate_supertrend(symbol_or_df, interval: str = "1day", period: int = 10,
                         multiplier: float = 3.0, api_key: str = None):
    """
    Supertrend indicator — trend direction ("BULLISH"/"BEARISH") and active band level.

    Two paths:
      • symbol_or_df is a str  → TD native SuperTrendEndpoint (authoritative, zero manual math)
      • symbol_or_df is a df   → local ATR-banded fallback (for callers that only have OHLCV)

    Callers with a symbol should pass it as a string; the TD path saves CPU and avoids the
    edge-case rounding differences in the manual loop. The DataFrame path is preserved so
    analyze_market_structure() and any caller without API access continues to work unchanged.
    """
    # ── REST path (previously used TDClient SDK which spawned WebSocket threads on every call,
    #    exhausting PythonAnywhere's thread limit. Plain requests.get() is identical data, zero threads.)
    if isinstance(symbol_or_df, str):
        try:
            import os, requests as _req
            key = api_key or os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")
            # Fetch 2 bars: supertrend level for latest bar, plus current close to derive direction
            st_res = _req.get(
                "https://api.twelvedata.com/supertrend",
                params={"symbol": symbol_or_df, "interval": interval,
                        "period": period, "multiplier": int(multiplier),
                        "outputsize": 2, "apikey": key},
                timeout=10
            ).json()
            values = st_res.get("values", [])
            if values:
                level = round(float(values[0].get("supertrend", 0.0)), 2)
                # Direction: fetch latest close and compare to supertrend band
                price_res = _req.get(
                    "https://api.twelvedata.com/price",
                    params={"symbol": symbol_or_df, "apikey": key},
                    timeout=8
                ).json()
                close = float(price_res.get("price", 0.0))
                if close > 0 and level > 0:
                    trend_dir = "BULLISH" if close > level else "BEARISH"
                else:
                    trend_dir = "NEUTRAL"
                return {"trend": trend_dir, "level": level}
        except Exception as e:
            import logging
            logging.getLogger("market_structure").error(f"SuperTrend REST failed for {symbol_or_df}: {e}")
        return {"trend": "NEUTRAL", "level": 0.0}

    # ── DataFrame fallback path (callers that pass OHLCV directly)
    df = symbol_or_df
    if df is None or len(df) < period + 2:
        return {"trend": "NEUTRAL", "level": 0.0}
    try:
        atr = calculate_atr_series(df, period)
        hl2 = (df["high"] + df["low"]) / 2
        upper_band = hl2 + multiplier * atr
        lower_band = hl2 - multiplier * atr

        trend = pd.Series(1, index=df.index)
        final_upper = upper_band.copy()
        final_lower = lower_band.copy()

        for i in range(period, len(df)):
            close = df["close"].iloc[i]
            if close > final_upper.iloc[i - 1]:
                trend.iloc[i] = 1
            elif close < final_lower.iloc[i - 1]:
                trend.iloc[i] = -1
            else:
                trend.iloc[i] = trend.iloc[i - 1]
                if trend.iloc[i] == 1 and lower_band.iloc[i] < final_lower.iloc[i - 1]:
                    final_lower.iloc[i] = final_lower.iloc[i - 1]
                if trend.iloc[i] == -1 and upper_band.iloc[i] > final_upper.iloc[i - 1]:
                    final_upper.iloc[i] = final_upper.iloc[i - 1]

        last_trend = int(trend.iloc[-1])
        level = float(final_lower.iloc[-1]) if last_trend == 1 else float(final_upper.iloc[-1])
        return {"trend": "BULLISH" if last_trend == 1 else "BEARISH", "level": round(level, 2)}
    except Exception:
        return {"trend": "NEUTRAL", "level": 0.0}


def analyze_market_structure(df, atr_series=None):
    """
    Composite classifier — synthesizes the primitives above into a single, actionable setup label
    instead of requiring callers to juggle four separate function outputs. Mirrors the spirit of
    the "3-Step Institutional Trap" / "AMD FVG" / "Retest & Break" family: liquidity sweep +
    rejection is the highest-conviction setup, an active retest of an unfilled FVG is next, and a
    nearby equal-highs/lows pool is flagged as context (likely next target, not yet a trigger).
    """
    if df is None or len(df) < 25:
        return {"setup": "INSUFFICIENT_DATA", "bias": "NEUTRAL", "detail": "Not enough bars for structure analysis."}

    atr_series = atr_series if atr_series is not None else calculate_atr_series(df)
    sweep = detect_liquidity_sweep(df)
    fvgs = detect_fvgs(df, atr_series)
    eqh, eql = detect_equal_highs_lows(df)
    spot = float(df["close"].iloc[-1])

    if sweep:
        bias = "BULLISH" if sweep["type"] == "bullish_sweep" else "BEARISH"
        return {
            "setup": "LIQUIDITY SWEEP REVERSAL", "bias": bias,
            "detail": f"Swept {sweep['swept_level']:,.2f} and rejected back to {sweep['close']:,.2f} — classic stop-hunt signature.",
            "level": sweep["swept_level"],
        }

    unfilled = [f for f in fvgs if not f["filled"]]
    active_retest = [f for f in unfilled if f["bottom"] <= spot <= f["top"]]
    if active_retest:
        fvg = active_retest[-1]
        bias = "BULLISH" if fvg["type"] == "bullish" else "BEARISH"
        return {
            "setup": f"{fvg['type'].upper()} FVG RETEST", "bias": bias,
            "detail": f"Price is back inside an unfilled {fvg['type']} gap (${fvg['bottom']:,.2f}-${fvg['top']:,.2f}) — statistically drawn to fill.",
            "level": (fvg["top"] + fvg["bottom"]) / 2,
        }

    if unfilled:
        nearest = min(unfilled, key=lambda f: min(abs(spot - f["top"]), abs(spot - f["bottom"])))
        bias = "BULLISH" if nearest["type"] == "bullish" else "BEARISH"
        return {
            "setup": f"UNFILLED {nearest['type'].upper()} FVG NEARBY", "bias": bias,
            "detail": f"Nearest unfilled gap at ${nearest['bottom']:,.2f}-${nearest['top']:,.2f} — a likely magnet if price drifts that way.",
            "level": (nearest["top"] + nearest["bottom"]) / 2,
        }

    near_pool = None
    for cluster in eqh + eql:
        if abs(spot - cluster["level"]) / spot <= 0.01:
            near_pool = cluster
            break
    if near_pool:
        is_high = near_pool in eqh
        return {
            "setup": "EQUAL HIGHS/LOWS LIQUIDITY POOL NEARBY", "bias": "BEARISH" if is_high else "BULLISH",
            "detail": f"Price approaching a {'resistance' if is_high else 'support'} pool from {len(near_pool['members'])} equal touches at ${near_pool['level']:,.2f} — a likely sweep target.",
            "level": near_pool["level"],
        }

    return {"setup": "NO STRUCTURE SETUP", "bias": "NEUTRAL", "detail": "No active sweep, FVG retest, or liquidity pool nearby."}
