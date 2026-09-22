"""
xdca.py — Income ETF Near-Bottom DCA Scanner
Cashflow ZZZ Machine | Rockefeller Ecosystem

Monitors XSPI, XQQI, MLPI, KQQQ for near-bottom DCA entry zones.
10-minute RTH loop (13:00–21:00 UTC = 9:30 AM–4:00 PM ET).

Zone system:
  A — WATCH             Silent. DB state only.
  B — ACCUMULATE        Silent. DB state only.
  C — BUYING OPPORTUNITY  Discord alert → #dividend-ccetfs
  D — BUYING OPPORTUNITY  Discord alert + Pushover (peak fear confirmation)

Underlying proxies (drawdown signal):
  XSPI → SPY  |  XQQI → QQQ  |  KQQQ → QQQ  |  MLPI → XLE

NAV erosion hardening rationale:
  Zone C/D require BOTH RSI oversold AND elevated VIXY z-score.
  This prevents DCA into orderly distribution (calm drift-down).
  High VIXY → covered call premiums elevated → XSPI/XQQI/KQQQ distributions
  temporarily maximized → depressed price + elevated yield = best risk/reward entry.
  Near-52w-low bonus softens the VIXY requirement by 0.3σ (structural support level).

Option 2 (future hardening): seed quarterly buffer reset dates for XSPI and XQQI into DB
  (keys: xdca_xspi_buffer_reset_date, xdca_xqqi_buffer_reset_date) to compute exact
  Innovator Power Buffer utilization instead of 20-day rolling high proxy.
  See xdca_design_notes.md for implementation plan.

API budget per 10-min RTH tick:
  4 ETF price calls + 4 ETF RSI calls + 3 underlying price calls = ~11 credits/tick
  Underlying 20d time series: cached 30 min → ~1 credit/30 min per underlying
  VIXY time series: cached daily → 0 credits after first fetch
  Total: ~11-12 credits/tick vs 144/min limit — extremely lean.
"""

import os
import sys
import json
import time
import random
import logging
import requests
import numpy as np
from datetime import datetime, timezone
from dotenv import load_dotenv
from database import EcosystemDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("XDCA_Scanner")
logging.getLogger("dotenv.main").setLevel(logging.ERROR)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

TD_API_KEY       = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_DIVIDEND = os.getenv("WEBHOOK_DIVIDEND_CCETFS")
PUSHOVER_TOKEN   = os.getenv("PUSHOVER_API_TOKEN")
PUSHOVER_USER    = os.getenv("PUSHOVER_USER_KEY")

# ─────────────────────────────────────────────────────────────────────────────
# DCA TICKER UNIVERSE
# est_yield_pct: approximate annualized yield based on current market conditions.
# Varies with IV (higher VIX → higher CC premiums → temporarily higher yield).
# Update if fund company publishes a materially different target distribution.
# ─────────────────────────────────────────────────────────────────────────────
DCA_TICKERS = {
    "XSPI": {
        "name":          "Innovator S&P 500 Power Buffer+Income",
        "underlying":    "SPY",
        "est_yield_pct": 12.0,   # Innovator Power Buffer CC overlay; updates with IV regime
        "monthly":       True,
    },
    "XQQI": {
        "name":          "Innovator Nasdaq 100 Power Buffer+Income",
        "underlying":    "QQQ",
        "est_yield_pct": 14.0,
        "monthly":       True,
    },
    "MLPI": {
        "name":          "ETRACS Alerian MLP+CC ETN",
        "underlying":    "XLE",   # energy sector proxy — same trigger as scheduler mlpi_entry
        "est_yield_pct": 15.0,
        "monthly":       True,
    },
    "KQQQ": {
        "name":          "Kurv Tech Titans CC ETF",
        "underlying":    "QQQ",
        "est_yield_pct": 15.0,
        "monthly":       True,
    },
    "XBCI": {
        "name":          "Innovator Bitcoin Buffer+Income",
        "underlying":    "IBIT",  # BlackRock Bitcoin ETF — best liquid BTC proxy on TD
        "est_yield_pct": 18.0,    # BTC IV is structurally higher than equity IV → higher CC premium
        "monthly":       True,
    },
}

# Unique underlying tickers to fetch
UNDERLYINGS = list(dict.fromkeys(v["underlying"] for v in DCA_TICKERS.values()))  # [SPY, QQQ, XLE]

# ─────────────────────────────────────────────────────────────────────────────
# ZONE THRESHOLDS
# Checked in D → C → B → A order; first match wins.
# drawdown_pct_min: underlying % below its 20-day rolling high.
# vixy_z_min: VIXY 20-day z-score threshold. None = not required for this zone.
# near_52w_low_pct: if ETF is within this % of its 52w low, VIXY requirement softens 0.3σ.
# cooldown_hours: minimum gap between successive alerts for same sym+zone.
# ─────────────────────────────────────────────────────────────────────────────
ZONE_CONFIG = {
    "D": {
        "label":              "BUYING OPPORTUNITY — PEAK FEAR",
        "rsi_max":            28.0,
        "drawdown_pct_min":   8.0,
        "vixy_z_min":         1.5,
        "near_52w_low_pct":   3.0,
        "color":              0xB71C1C,
        "pushover":           True,
        "discord":            True,
        "cooldown_hours":     4,
    },
    "C": {
        "label":              "BUYING OPPORTUNITY",
        "rsi_max":            35.0,
        "drawdown_pct_min":   5.0,
        "vixy_z_min":         0.8,
        "near_52w_low_pct":   6.0,
        "color":              0xFF4500,
        "pushover":           False,
        "discord":            True,
        "cooldown_hours":     6,
    },
    "B": {
        "label":              "ACCUMULATE",
        "rsi_max":            42.0,
        "drawdown_pct_min":   3.5,
        "vixy_z_min":         0.3,
        "near_52w_low_pct":   None,
        "color":              0xFFA500,
        "pushover":           False,
        "discord":            False,
        "cooldown_hours":     0,
    },
    "A": {
        "label":              "WATCH",
        "rsi_max":            48.0,
        "drawdown_pct_min":   2.0,
        "vixy_z_min":         None,
        "near_52w_low_pct":   None,
        "color":              0xFFD700,
        "pushover":           False,
        "discord":            False,
        "cooldown_hours":     0,
    },
}

LOOP_SLEEP_RTH  = 600   # 10 minutes during Regular Trading Hours
LOOP_SLEEP_OFF  = 1800  # 30 minutes off-hours


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _td_get(endpoint, params):
    """Single Twelve Data REST call. Returns JSON dict or None on error."""
    params["apikey"] = TD_API_KEY
    try:
        r = requests.get(f"https://api.twelvedata.com/{endpoint}", params=params, timeout=12)
        data = r.json()
        if data.get("status") == "error" or ("code" in data and data.get("code") != 200):
            logger.warning(f"[TD] {endpoint}/{params.get('symbol','?')}: {data.get('message','error')}")
            return None
        return data
    except Exception as e:
        logger.error(f"[TD] {endpoint} failed: {e}")
        return None


def _send_discord(payload):
    if not WEBHOOK_DIVIDEND:
        return
    try:
        r = requests.post(WEBHOOK_DIVIDEND, json=payload, timeout=10)
        if r.status_code not in (200, 204):
            logger.warning(f"[Discord] HTTP {r.status_code}")
    except Exception as e:
        logger.error(f"[Discord] {e}")


def _send_pushover(title, message):
    if not PUSHOVER_TOKEN or not PUSHOVER_USER:
        return
    try:
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token":   PUSHOVER_TOKEN,
            "user":    PUSHOVER_USER,
            "title":   title,
            "message": message,
            "priority": 1,
        }, timeout=10)
    except Exception as e:
        logger.error(f"[Pushover] {e}")


def _can_alert(sym, zone_key):
    """True if cooldown has elapsed for this sym+zone pair."""
    hours = ZONE_CONFIG[zone_key]["cooldown_hours"]
    if hours == 0:
        return False
    last_ts = float(db.get_state(f"xdca_{sym}_last_{zone_key}_alert") or 0)
    return (time.time() - last_ts) > (hours * 3600)


def _mark_alerted(sym, zone_key):
    db.update_state(f"xdca_{sym}_last_{zone_key}_alert", str(time.time()))


# ─────────────────────────────────────────────────────────────────────────────
# DATA FETCHES (each with DB caching to minimise TD credits)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_vixy_z(force=False):
    """
    VIXY 20-day z-score. Cached once per calendar day — daily closes don't change
    intraday, so re-fetching every 10 min wastes credits with no signal benefit.
    Returns float or None.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    if not force and db.get_state("xdca_vixy_z_date") == today:
        cached = db.get_state("xdca_vixy_z")
        if cached is not None:
            return float(cached)

    data = _td_get("time_series", {"symbol": "VIXY", "interval": "1day", "outputsize": 20})
    if not data or "values" not in data:
        return None
    closes = np.array([float(v["close"]) for v in data["values"]], dtype=float)
    if len(closes) < 10:
        return None
    mean, std = closes.mean(), closes.std()
    z = float((closes[0] - mean) / std) if std > 0 else 0.0
    db.update_state("xdca_vixy_z",      str(round(z, 3)))
    db.update_state("xdca_vixy_z_date", today)
    logger.info(f"[VIXY] z={z:+.2f}σ cached")
    return z


def fetch_52w_low(sym):
    """
    52-week low for an ETF computed from 252 daily bars (1 year).
    Cached once per calendar day — 1 TD credit per ticker per day (4 = 4 credits/day total).
    The /statistics endpoint requires a higher TD plan tier; time_series works on Grow plan.
    """
    today   = datetime.now().strftime("%Y-%m-%d")
    v_key   = f"xdca_{sym}_52w_low"
    d_key   = f"xdca_{sym}_52w_low_date"
    if db.get_state(d_key) == today:
        v = db.get_state(v_key)
        return float(v) if v else None

    data = _td_get("time_series", {"symbol": sym, "interval": "1day", "outputsize": 252})
    if not data or "values" not in data:
        return None

    lows = [float(b["low"]) for b in data["values"] if "low" in b]
    low  = min(lows) if lows else None

    if low:
        db.update_state(v_key, str(low))
        db.update_state(d_key, today)
        logger.info(f"[{sym}] 52w low cached: ${low:.2f}")
    return low


def fetch_underlying_20d_high(sym):
    """
    Rolling 20-session high for an underlying (SPY / QQQ / XLE).
    Cached 30 minutes to prevent redundant credits while still tracking intraday swings.
    Returns (20d_high, current_price) or (None, None).
    """
    h_key  = f"xdca_ul_{sym}_20d_high"
    p_key  = f"xdca_ul_{sym}_price"
    ts_key = f"xdca_ul_{sym}_ts"

    last_ts = float(db.get_state(ts_key) or 0)
    if (time.time() - last_ts) < 1800:
        h = db.get_state(h_key)
        p = db.get_state(p_key)
        if h and p:
            return float(h), float(p)

    data = _td_get("time_series", {"symbol": sym, "interval": "1day", "outputsize": 20})
    if not data or "values" not in data:
        return None, None

    bars   = data["values"]
    highs  = [float(b["high"]) for b in bars if "high" in b]
    closes = [float(b["close"]) for b in bars if "close" in b]
    if not highs or not closes:
        return None, None

    high_20d     = max(highs)
    current_price = closes[0]

    db.update_state(h_key,  str(high_20d))
    db.update_state(p_key,  str(current_price))
    db.update_state(ts_key, str(time.time()))
    return high_20d, current_price


def fetch_etf_data(sym):
    """Fetch current price and RSI14 for a DCA ticker. Returns (price, rsi) or (None, None)."""
    p_data = _td_get("price", {"symbol": sym})
    if not p_data or "price" not in p_data:
        return None, None
    price = float(p_data["price"])

    r_data = _td_get("rsi", {"symbol": sym, "interval": "1day", "time_period": 14, "outputsize": 1})
    rsi = None
    if r_data and r_data.get("values"):
        rsi = float(r_data["values"][0]["rsi"])

    return price, rsi


# ─────────────────────────────────────────────────────────────────────────────
# ZONE ASSESSMENT
# ─────────────────────────────────────────────────────────────────────────────

def assess_zone(sym, price, rsi, ul_20d_high, ul_price, vixy_z):
    """
    Classify a ticker into the highest applicable zone (D→C→B→A) or None.

    Returns dict:
      zone_key        — "A"/"B"/"C"/"D" or None
      drawdown_pct    — underlying % below 20d high (always computed)
      pct_above_52w_low — ETF % above its own 52w low (None if 52w low unavailable)
      near_52w_low    — True if within the zone's near_52w_low_pct threshold
    """
    result = {"zone_key": None, "drawdown_pct": 0.0, "pct_above_52w_low": None, "near_52w_low": False}

    if price is None or rsi is None:
        return result

    # Underlying drawdown from 20-day high
    if ul_20d_high and ul_20d_high > 0 and ul_price:
        result["drawdown_pct"] = ((ul_20d_high - ul_price) / ul_20d_high) * 100
    drawdown_pct = result["drawdown_pct"]

    # 52w proximity (cached — no extra TD credit on this call)
    low_52w = fetch_52w_low(sym)
    if low_52w and low_52w > 0:
        result["pct_above_52w_low"] = ((price - low_52w) / low_52w) * 100

    for zone_key in ("D", "C", "B", "A"):
        cfg = ZONE_CONFIG[zone_key]

        rsi_ok   = rsi <= cfg["rsi_max"]
        draw_ok  = drawdown_pct >= cfg["drawdown_pct_min"]

        # VIXY gate — check with possible 52w-low softening
        vixy_required = cfg["vixy_z_min"]
        near_low      = False
        if vixy_required is not None and cfg.get("near_52w_low_pct") and result["pct_above_52w_low"] is not None:
            if result["pct_above_52w_low"] <= cfg["near_52w_low_pct"]:
                near_low      = True
                vixy_required = vixy_required - 0.3   # soften by 0.3σ near structural low

        vixy_ok = (cfg["vixy_z_min"] is None) or (vixy_z is not None and vixy_z >= vixy_required)

        if rsi_ok and draw_ok and vixy_ok:
            result["zone_key"]    = zone_key
            result["near_52w_low"] = near_low
            return result

    return result


# ─────────────────────────────────────────────────────────────────────────────
# DISCORD EMBED BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_alert_embed(sym, price, rsi, zone_key, zone_result, vixy_z, ul_price):
    info     = DCA_TICKERS[sym]
    zone_cfg = ZONE_CONFIG[zone_key]
    ul       = info["underlying"]

    est_annual  = price * (info["est_yield_pct"] / 100)
    est_monthly = est_annual / 12

    drawdown_pct      = zone_result["drawdown_pct"]
    pct_above_52w_low = zone_result["pct_above_52w_low"]
    near_52w_low      = zone_result["near_52w_low"]

    vixy_line = f"VIXY fear: `{vixy_z:+.2f}σ`\n" if vixy_z is not None else "VIXY fear: unavailable\n"

    low_line = ""
    if pct_above_52w_low is not None:
        badge = " ← 52w low zone" if near_52w_low else ""
        low_line = f"┣ Above 52w low: `+{pct_above_52w_low:.1f}%`{badge}\n"

    # Forward yield commentary — at Zone C/D high-VIX conditions, CC ETF distributions
    # are temporarily elevated because options premiums expand with volatility.
    if vixy_z and vixy_z >= 1.0:
        yield_note = "IV elevated → CC premiums maximized → distribution yield temporarily higher"
    elif vixy_z and vixy_z >= 0.5:
        yield_note = "Rising IV → options premium expanding → distribution support building"
    else:
        yield_note = "Monitor IV for premium expansion before adding"

    body = (
        f"┣ Price: `${price:.2f}` | RSI: `{rsi:.1f}`\n"
        f"┣ {ul} drawdown from 20d high: `{drawdown_pct:.1f}%`\n"
        f"┣ {vixy_line}"
        f"┣ Est. yield at price: `~{info['est_yield_pct']:.0f}%` annualized\n"
        f"┣ Est. monthly income/share: `~${est_monthly:.3f}`\n"
        f"{low_line}"
        f"┣ Zone: `{zone_cfg['label']}`\n"
        f"┗ {yield_note}"
    )

    return {
        "embeds": [{
            "title":       f"{sym} — {zone_cfg['label']}",
            "description": body,
            "color":       zone_cfg["color"],
            "footer":      {
                "text": (
                    f"{info['name']} | Tier 2 DCA Scanner | "
                    f"{datetime.now().strftime('%H:%M UTC')} | "
                    f"Monthly distributions → margin paydown"
                )
            },
        }]
    }


# ─────────────────────────────────────────────────────────────────────────────
# EOD DAILY STATUS SUMMARY
# Fires once after market close (20:30 UTC) — shows all 4 tickers' zone at close.
# ─────────────────────────────────────────────────────────────────────────────

def dispatch_eod_summary(vixy_z):
    """Send a compact EOD status card for all 4 DCA tickers to #dividend-ccetfs."""
    today = datetime.now().strftime("%Y-%m-%d")
    key   = f"xdca_eod_summary_{today}"
    if db.get_state(key):
        return  # already fired today

    lines = []
    any_signal = False
    for sym in DCA_TICKERS:
        zone   = db.get_state(f"xdca_{sym}_zone") or "—"
        price  = db.get_state(f"xdca_{sym}_price") or "—"
        rsi    = db.get_state(f"xdca_{sym}_rsi") or "—"
        draw   = db.get_state(f"xdca_{sym}_drawdown_pct") or "—"

        if zone in ("C", "D"):
            zone_display = f"🔴 {zone} — {ZONE_CONFIG[zone]['label']}"
            any_signal = True
        elif zone == "B":
            zone_display = "⚠️ B — ACCUMULATE"
            any_signal = True
        elif zone == "A":
            zone_display = "🟡 A — WATCH"
            any_signal = True
        else:
            zone_display = "🟢 No signal"

        ul  = DCA_TICKERS[sym]["underlying"]
        lines.append(f"**{sym}** — ${price} | RSI {rsi} | {ul} -{draw}% | {zone_display}")

    if not any_signal:
        db.update_state(key, "snoozed")
        logger.info("[EOD] All tickers at no-signal — EOD summary snoozed")
        return

    vixy_str = f"{vixy_z:+.2f}σ" if vixy_z is not None else "n/a"
    body = "\n".join(lines) + f"\n\nVIXY fear: `{vixy_str}` | Monthly distributions → CLM/CRF margin paydown"

    payload = {
        "embeds": [{
            "title":       "Tier 2 DCA — EOD Status",
            "description": body,
            "color":       0x2ecc71,
            "footer":      {"text": f"XSPI · XQQI · MLPI · KQQQ | {today} | Not financial advice. Educational purposes only."},
        }]
    }
    _send_discord(payload)
    db.update_state(key, "fired")
    logger.info("[EOD] Daily DCA status summary dispatched")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SCAN
# ─────────────────────────────────────────────────────────────────────────────

def run_scan():
    """One complete scan across all 4 DCA tickers."""
    vixy_z = fetch_vixy_z()

    # Fetch all underlying 20d highs first (cached 30min — one time series call per underlying)
    underlying_cache = {}
    for ul_sym in UNDERLYINGS:
        h, p = fetch_underlying_20d_high(ul_sym)
        underlying_cache[ul_sym] = {"high_20d": h, "price": p}

    for sym, info in DCA_TICKERS.items():
        ul   = info["underlying"]
        ul_h = underlying_cache[ul]["high_20d"]
        ul_p = underlying_cache[ul]["price"]

        price, rsi = fetch_etf_data(sym)
        if price is None:
            logger.warning(f"[{sym}] Price fetch failed — skipping")
            continue

        zone_result = assess_zone(sym, price, rsi, ul_h, ul_p, vixy_z)
        zone_key    = zone_result["zone_key"]
        drawdown    = zone_result["drawdown_pct"]

        # Persist current state to DB (for EOD summary + cross-script reads)
        db.update_state(f"xdca_{sym}_zone",         zone_key or "NONE")
        db.update_state(f"xdca_{sym}_price",         f"{price:.2f}")
        db.update_state(f"xdca_{sym}_rsi",           f"{rsi:.1f}" if rsi else "")
        db.update_state(f"xdca_{sym}_drawdown_pct",  f"{drawdown:.2f}")

        rsi_str = f"{rsi:.1f}" if rsi else "n/a"
        logger.info(f"[{sym}] ${price:.2f} | RSI {rsi_str} | {ul} -{drawdown:.1f}% | zone={zone_key} | vixy_z={vixy_z}")

        if zone_key not in ("C", "D"):
            continue

        if not _can_alert(sym, zone_key):
            logger.info(f"[{sym}] Zone {zone_key} — cooldown active, skipping alert")
            continue

        embed = build_alert_embed(sym, price, rsi, zone_key, zone_result, vixy_z, ul_p)
        _send_discord(embed)

        if ZONE_CONFIG[zone_key]["pushover"]:
            _send_pushover(
                f"{sym} {ZONE_CONFIG[zone_key]['label']}",
                (
                    f"${price:.2f} | RSI {rsi_str}\n"
                    f"{ul} -{drawdown:.1f}% from 20d high\n"
                    f"VIXY {vixy_z:+.2f}σ\n"
                    f"Add to Tier 2 position → monthly dist → margin paydown"
                )
            )

        _mark_alerted(sym, zone_key)
        logger.info(f"[{sym}] Zone {zone_key} alert dispatched")


def run_xdca():
    """Main always-on loop."""
    logger.info("XDCA Scanner — Tier 2 Income ETF Near-Bottom DCA Monitor")

    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "test":
            logger.info("[TEST] Forcing single scan (ignoring cooldowns)")
            # Clear daily VIXY cache so test always fetches fresh
            fetch_vixy_z(force=True)
            run_scan()
            return
        if cmd == "eod":
            dispatch_eod_summary(fetch_vixy_z())
            return

    # Startup jitter: 5–25s offset to desync from monitor.py (13s jitter) and
    # market_analysis.py (18:00 UTC boundary). Prevents simultaneous TD credit bursts.
    _jitter = random.randint(5, 25)
    logger.info(f"[Jitter] {_jitter}s startup offset")
    time.sleep(_jitter)

    while True:
        now_utc = datetime.now(timezone.utc)
        h, m    = now_utc.hour, now_utc.minute
        rth     = 13 <= h < 21

        # TD rate-limit cooldown gate — shared with monitor.py via DB
        _td_cooldown_until = float(db.get_state("td_cooldown_until") or 0.0)
        _td_cooling        = time.time() < _td_cooldown_until

        try:
            if rth and not _td_cooling:
                run_scan()

                # EOD summary: fire once at 20:30–20:59 UTC (after close, before loop exits RTH)
                if h == 20 and m >= 30:
                    dispatch_eod_summary(fetch_vixy_z())

            elif _td_cooling:
                _remaining = int(_td_cooldown_until - time.time())
                logger.info(f"[TD Cooldown] Skipping scan — {_remaining}s remaining")
            else:
                logger.debug("[Off-hours] No scan — market closed")

        except Exception as e:
            logger.error(f"[Loop] Scan error: {e}", exc_info=True)

        sleep_secs = LOOP_SLEEP_RTH if rth else LOOP_SLEEP_OFF
        time.sleep(sleep_secs)


if __name__ == "__main__":
    run_xdca()
