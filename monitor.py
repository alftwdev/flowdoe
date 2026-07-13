"""
monitor.py — Cornerstone Protection Engine
Cashflow ZZZ Machine | Rockefeller Ecosystem

Original architecture preserved in full.
Upgrades in this revision (per engineering session):
  • Dark pool / off-exchange activity detector (price drop on below-avg public volume)
  • CEF premium compression detector (fast intra-session premium collapse)
  • Macro cross-correlation engine (CLM/CRF vs SPY on same session)
  • Seasonal caution flag (March / September historically weak months)
  • 13F large-holder drift watcher (SEC Schedule 13D/G scrape added to filing types)
  • Pulse Report output format (Title / ┣ Data / ┗ Final) for mobile-first Discord readers
  • 3-notification rule enforced per sector via DB-backed counter + cooldown
  • TQQQ options sniper signals routed to WEBHOOK_TRADE_SIGNALS (not cornerstone)
  • All original: SEC EDGAR N-2 watcher, RO risk score, whale flow, VIXY crisis amp,
    ex-div window suppression, RO season flag, 0800 HST daily pulse, Pushover, email
  • Cashflow snapshot block removed per operator instruction
"""

import os
import requests
import time
import sys
import smtplib
import logging
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("Monitor_Engine")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

FRED_API_KEY = os.getenv("FRED_API_KEY")

try:
    from essentials_tools import (
        send_essentials_embed, send_essentials_embed_with_chart,
        generate_line_comparison_chart, get_institutional_conviction,
    )
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# ─────────────────────────────────────────────────────────────────────────────
# WEBHOOK REGISTRY
# All webhooks loaded from .env — each routes to its dedicated Discord channel.
# TQQQ/options signals go to WEBHOOK_TRADE_SIGNALS, never to cornerstone.
# ─────────────────────────────────────────────────────────────────────────────
WEBHOOK_CORNERSTONE    = os.getenv("WEBHOOK_CORNERSTONE_RO")
WEBHOOK_TRADE_SIGNALS  = os.getenv("WEBHOOK_TRADE_SIGNALS")
WEBHOOK_MARKET         = os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_ANNOUNCEMENTS  = os.getenv("WEBHOOK_ANNOUNCEMENTS")
WEBHOOK_DIVIDEND       = os.getenv("WEBHOOK_DIVIDEND_CCETFS")
WEBHOOK_FUTURES        = os.getenv("WEBHOOK_FUTURES_TRADING")
WEBHOOK_CRYPTO         = os.getenv("WEBHOOK_CRYPTO")
WEBHOOK_FED            = os.getenv("WEBHOOK_FED")
WEBHOOK_FOREX          = os.getenv("WEBHOOK_FOREX")  # retained in .env; channel deprecated but key preserved

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

# ─────────────────────────────────────────────────────────────────────────────
# ASSET CONFIG
# ─────────────────────────────────────────────────────────────────────────────
PRIORITY_ASSETS = {
    "CLM": {"nav_ticker": "XCLMX", "default_nav": 6.45},
    "CRF": {"nav_ticker": "XCRFX", "default_nav": 6.30}
}

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS & THRESHOLDS
# ─────────────────────────────────────────────────────────────────────────────

# Ex-dividend heuristic: CLM/CRF ex-div falls mid-month (historically days 15–19).
# A price dip in this window is a scheduled cash-payout event, not dilution/RO risk.
EX_DIV_WINDOW_DAYS = range(15, 20)

# RO Filing Season: historically N-2 filings cluster mid-Feb through mid-Apr.
# Real filing history verified against SEC CIKs across 2016-2025.
RO_FILING_SEASON = (2, 15, 4, 15)  # (start_month, start_day, end_month, end_day)

# VIXY Z-score threshold for crisis-amplification overlay.
CRISIS_VIXY_Z_THRESHOLD = 1.5

# Seasonal weakness months — March and September historically produce the largest
# drawdowns across QQQ/SPY. Caution flag raised during these months for TQQQ sniper
# routing and general risk posture.
SEASONAL_CAUTION_MONTHS = [3, 9]

# Dark pool / off-exchange detection thresholds:
# Price drop significant but public volume BELOW average → suggests off-exchange activity.
DARK_POOL_PRICE_DROP_PCT   = -1.5   # session price change threshold (%)
DARK_POOL_VOLUME_RATIO_MAX = 0.75   # public vol must be < 75% of 20D avg to flag

# CEF premium compression: fast intra-session collapse of premium/discount spread
# that is NOT explained by NAV movement alone → institutional exit off-exchange.
PREMIUM_COMPRESSION_THRESHOLD = -3.0  # % change in premium within one session

# Todd Akin 30%+ premium = RO Watch threshold — historically this is when Cornerstone
# announces the Rights Offering. N-2 filing on EDGAR follows the premium expansion.
PREMIUM_RO_WATCH_THRESHOLD = 30.0

# 3-notification rule: max 3 alerts per sector per rolling 24h window.
# Minor changes are noted in DB but not broadcast. Next MAJOR update re-opens.
ALERT_MAX_PER_SECTOR    = 3
ALERT_COOLDOWN_HOURS    = 24
MINOR_CHANGE_THRESHOLD  = 0.5  # price/score delta below this = minor, do not broadcast

# RO composite score weights — N-2 SEC filing is the single highest-conviction signal.
# EDGAR sources stack: multiple filings in the same cycle = multi-source conviction.
RO_SCORE_WEIGHTS = {
    # EDGAR filing signals (stacking — each detected form adds independently)
    "sec_n2":              60,   # N-2 registration — RO confirmed, act immediately
    "sec_n2a":             50,   # N-2/A amendment — final RO terms/pricing
    "sec_ncsr":             8,   # N-CSR semi-annual — distribution sustainability language
    "sec_def14a":           8,   # DEF 14A proxy — board vote on distribution policy
    "13f_holder_exit":     12,   # SC 13D/G large holder change
    # Premium / spread signals
    "z_danger":            25,
    "z_caution":           12,
    "premium_extreme":     10,
    "premium_compression": 15,
    "premium_30pct_watch": 20,
    # Flow / institutional signals
    "whale_distribution":  15,
    "dark_pool":           18,
    # Macro / systemic signals
    "credit_stress":       10,
    "macro_underperform":  10,
    "crisis_amplification":12,
    "ro_season":            8,
    # Suppressors
    "ex_div_relief":      -10,
}

# EDGAR forms watched and their conviction weights.
# Multiple forms detected simultaneously = conviction stacking.
EDGAR_FORMS_TO_WATCH = {
    "N-2":     "sec_n2",       # RO registration
    "N-2/A":   "sec_n2a",      # RO amendment — final terms
    "SC 13D":  "13f_holder_exit",
    "SC 13G":  "13f_holder_exit",
    "N-CSR":   "sec_ncsr",     # Semi-annual — distribution language
    "DEF 14A": "sec_def14a",   # Proxy — board distribution vote
}

# ─────────────────────────────────────────────────────────────────────────────
# FRED — LIVE HY CREDIT SPREAD (replaces hardcoded 4.5% benchmark)
# BAMLH0A0HYM2: ICE BofA US High Yield Option-Adjusted Spread (daily, %).
# Cached once per calendar day — FRED updates after US market close (~5 PM ET).
# Values: < 3% = compressed/tight | 3–4.5% = normal | > 4.5% = stress | > 7% = crisis
# ─────────────────────────────────────────────────────────────────────────────

def fetch_hy_spread_live() -> float:
    """
    Fetches live HY OAS from FRED. Cached to DB daily to avoid redundant FRED calls
    on each 5-min monitor loop tick. Returns last known value on failure (never 0.0).
    """
    cache_key      = "fred_hy_spread_value"
    cache_date_key = "fred_hy_spread_date"
    today_str      = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cached_date    = db.get_state(cache_date_key)
    if cached_date == today_str:
        cached = db.get_state(cache_key)
        if cached:
            return float(cached)
    if not FRED_API_KEY:
        fallback = float(db.get_state(cache_key) or 4.5)
        logger.warning("FRED_API_KEY not set — using last known HY spread or default 4.5%")
        return fallback
    try:
        url = (
            "https://api.stlouisfed.org/fred/series/observations"
            f"?series_id=BAMLH0A0HYM2&api_key={FRED_API_KEY}"
            "&file_type=json&sort_order=desc&limit=1"
        )
        res = requests.get(url, timeout=12)
        res.raise_for_status()
        val = float(res.json()["observations"][0]["value"])
        if val > 0:
            db.update_state(cache_key, val)
            db.update_state(cache_date_key, today_str)
            logger.info(f"FRED HY spread updated: {val:.2f}%")
            return round(val, 2)
    except Exception as e:
        logger.warning(f"FRED HY spread fetch failed: {e}")
    # Fall back to last cached value; if none, use 4.5 (old hardcoded default)
    return float(db.get_state(cache_key) or 4.5)


# ─────────────────────────────────────────────────────────────────────────────
# NOTIFICATION RATE LIMITER (3-rule)
# ─────────────────────────────────────────────────────────────────────────────

def get_alert_count(sector: str) -> int:
    """Return number of alerts fired for this sector in the last 24h."""
    count_key  = f"alert_count_{sector}"
    reset_key  = f"alert_reset_{sector}"
    now        = datetime.utcnow()
    reset_str  = db.get_state(reset_key, "")
    if reset_str:
        try:
            reset_dt = datetime.fromisoformat(reset_str)
            if (now - reset_dt).total_seconds() > ALERT_COOLDOWN_HOURS * 3600:
                db.update_state(count_key, 0)
                db.update_state(reset_key, now.isoformat())
                return 0
        except Exception:
            pass
    else:
        db.update_state(reset_key, now.isoformat())
    return int(db.get_state(count_key, 0))

def increment_alert_count(sector: str):
    count_key = f"alert_count_{sector}"
    current   = int(db.get_state(count_key, 0))
    db.update_state(count_key, current + 1)

def can_broadcast(sector: str, is_major: bool = True) -> bool:
    """
    Returns True only if:
      • The change is major (is_major=True), AND
      • Fewer than ALERT_MAX_PER_SECTOR alerts have been sent this 24h window.
    Minor changes are noted in logs but never broadcast.
    """
    if not is_major:
        logger.info(f"[{sector}] Minor change noted — not broadcasting (3-rule).")
        return False
    count = get_alert_count(sector)
    if count >= ALERT_MAX_PER_SECTOR:
        logger.info(f"[{sector}] Alert cap ({ALERT_MAX_PER_SECTOR}/24h) reached — suppressing.")
        return False
    return True

# ─────────────────────────────────────────────────────────────────────────────
# SEC EDGAR — N-2 + 13D/G FILING WATCHER
# CIKs verified live 2026-06-23 against SEC company search.
# ─────────────────────────────────────────────────────────────────────────────

def check_sec_edgar(session, ticker):
    """
    Scrapes SEC EDGAR for all forms in EDGAR_FORMS_TO_WATCH.
    Returns a pipe-delimited string of detected signals. Callers check for
    'N-2', 'N-2/A', '13D', '13G', 'N-CSR', 'DEF 14A' substrings.

    Multiple detections = multiple conviction sources — the string will contain
    'MULTI-SOURCE' when ≥2 EDGAR signals fire simultaneously. This is the
    highest-confidence RO pre-signal available outside of a press release.

    EDGAR is always the primary, free, authoritative source. It runs alongside
    (not instead of) market data signals — both must agree for highest conviction.
    """
    cik_map = {"CLM": "0000814083", "CRF": "0000033934"}
    cik = cik_map.get(ticker)
    if not cik:
        return "No N2/RO detected"

    headers = {'User-Agent': 'RockefellerSystem/1.0 (admin@rockefeller.local)'}
    try:
        url  = f"https://data.sec.gov/submissions/CIK{cik}.json"
        res  = session.get(url, headers=headers, timeout=20)
        if res.status_code != 200:
            return "No N2/RO detected"

        data         = res.json()
        filings      = data.get("filings", {}).get("recent", {})
        recent_forms = filings.get("form", [])
        recent_dates = filings.get("filingDate", [])
        flags        = []
        seen_forms   = set()  # deduplicate — N-CSR filed twice/year, only flag once

        # Recency windows — stale filings from completed RO cycles must not retrigger.
        # N-2/N-2/A: 90-day window (an active RO registration clears within ~60 days)
        # SC 13D/G:   180-day window (holder position changes matter longer-term)
        # N-CSR/DEF 14A: informational only, no recency gate (routine annual filings)
        N2_RECENCY_DAYS   = 90
        HOLDER_RECENCY_DAYS = 180
        today_dt = datetime.utcnow().date()

        def filing_age_days(date_str):
            try:
                return (today_dt - datetime.strptime(date_str, "%Y-%m-%d").date()).days
            except Exception:
                return 0  # unknown date — treat as recent to avoid suppressing real alerts

        scan_depth = min(30, len(recent_forms))
        for i in range(scan_depth):
            form = recent_forms[i]
            date = recent_dates[i] if i < len(recent_dates) else "unknown"
            age  = filing_age_days(date)

            if form == "N-2" and "N-2" not in seen_forms:
                if age <= N2_RECENCY_DAYS:
                    flags.append(f"⚠️ N-2 RO REGISTRATION ({date})")
                    seen_forms.add("N-2")
            elif form == "N-2/A" and "N-2/A" not in seen_forms:
                if age <= N2_RECENCY_DAYS:
                    flags.append(f"⚠️ N-2/A RO AMENDMENT ({date})")
                    seen_forms.add("N-2/A")
            elif "SC 13D" in form and "SC 13D" not in seen_forms:
                if age <= HOLDER_RECENCY_DAYS:
                    flags.append(f"⚠️ 13D LARGE HOLDER CHANGE ({date})")
                    seen_forms.add("SC 13D")
            elif "SC 13G" in form and "SC 13G" not in seen_forms:
                if age <= HOLDER_RECENCY_DAYS:
                    flags.append(f"⚠️ 13G INSTITUTIONAL HOLDER CHANGE ({date})")
                    seen_forms.add("SC 13G")
            elif form == "N-CSR" and "N-CSR" not in seen_forms:
                flags.append(f"📋 N-CSR ({date})")
                seen_forms.add("N-CSR")
            elif form == "DEF 14A" and "DEF 14A" not in seen_forms:
                flags.append(f"📋 DEF 14A ({date})")
                seen_forms.add("DEF 14A")

        if not flags:
            return "No N2/RO detected"

        # Conviction stacking: ≥2 EDGAR sources firing = higher confidence signal
        conviction = "🔴 MULTI-SOURCE EDGAR CONVICTION" if len(flags) >= 2 else "single source"
        return f"[{conviction}] " + " | ".join(flags)

    except Exception as e:
        # DNS failures and timeouts are transient — EDGAR is public but PythonAnywhere
        # occasionally has resolution hiccups. Warning not error: already handled gracefully.
        logger.warning(f"[SEC] EDGAR fetch unavailable ({ticker}): {type(e).__name__}")
        return "No N2/RO detected"

# ─────────────────────────────────────────────────────────────────────────────
# TWELVE DATA — LIVE METRICS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_live_metrics(session, symbol, retries=3):
    """
    Three attempts with escalating backoff (2s, 5s) before giving up.
    Twelve Data intermittently times out during peak market hours — a single
    failure with no retry was causing false "Data feed offline" reports.
    Timeout raised to 20s to handle slower responses during high-load windows.
    """
    last_err = None
    backoff  = [0, 2, 5]
    for attempt in range(retries):
        try:
            if backoff[attempt]:
                time.sleep(backoff[attempt])
            p_res  = session.get(
                f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}",
                timeout=20).json()
            price  = float(p_res.get('price', 0.0))
            if price == 0.0:
                raise ValueError(f"price came back 0.0: {p_res}")

            rsi    = 50.0
            r_res = session.get(
                f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day"
                f"&time_period=14&apikey={TD_API_KEY}", timeout=20).json()
            rsi   = float(r_res.get('values', [{'rsi': 50.0}])[0]['rsi'])

            nav_ticker = PRIORITY_ASSETS[symbol]["nav_ticker"]
            nav_res    = session.get(
                f"https://api.twelvedata.com/price?symbol={nav_ticker}&apikey={TD_API_KEY}",
                timeout=20).json()
            nav        = float(nav_res.get('price', PRIORITY_ASSETS[symbol]["default_nav"]))

            return price, rsi, nav
        except Exception as e:
            last_err = e
    logger.error(f"[Data Fetch Error] {symbol} failed after {retries} attempts: {last_err}")
    return 0.0, 50.0, PRIORITY_ASSETS[symbol]["default_nav"]

def fetch_time_series(session, symbol, outputsize=21):
    """Returns list of daily close dicts from Twelve Data, newest first."""
    try:
        res = session.get(
            "https://api.twelvedata.com/time_series",
            params={"symbol": symbol, "interval": "1day",
                    "outputsize": outputsize, "apikey": TD_API_KEY},
            timeout=20).json()
        return res.get("values", [])
    except Exception as e:
        logger.error(f"[Time Series Fetch Error] {symbol}: {e}")
        return []

def fetch_obv_mfi(session, symbol):
    """
    OBV — multi-session cumulative volume pressure. Declining OBV while price holds =
    sustained distribution (stronger signal than any single dark-pool session).
    MFI — volume-weighted RSI. Divergence from price = early accumulation/distribution read.
    Returns dict or None on failure.
    """
    try:
        obv_res = session.get(
            "https://api.twelvedata.com/obv",
            params={"symbol": symbol, "interval": "1day", "outputsize": "6", "apikey": TD_API_KEY},
            timeout=20
        ).json()
        obv_vals = [float(v.get("obv", 0)) for v in obv_res.get("values", [])]
        obv_now  = obv_vals[0] if obv_vals else 0.0
        obv_prev = obv_vals[-1] if len(obv_vals) > 1 else obv_now
        obv_trend = "rising" if obv_now > obv_prev else "falling"
        obv_pct   = ((obv_now - obv_prev) / abs(obv_prev) * 100) if obv_prev != 0 else 0.0

        mfi_res = session.get(
            "https://api.twelvedata.com/mfi",
            params={"symbol": symbol, "interval": "1day", "time_period": 14, "apikey": TD_API_KEY},
            timeout=20
        ).json()
        mfi = float(mfi_res.get("values", [{"mfi": 50.0}])[0].get("mfi", 50.0))

        return {"obv_now": obv_now, "obv_pct": obv_pct, "obv_trend": obv_trend, "mfi": mfi}
    except Exception as e:
        logger.warning(f"OBV/MFI fetch failed for {symbol}: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# SEASONAL CAUTION FLAG
# ─────────────────────────────────────────────────────────────────────────────

def is_seasonal_caution_month(today=None) -> bool:
    today = today or datetime.now(pytz.timezone('Pacific/Honolulu'))
    return today.month in SEASONAL_CAUTION_MONTHS

# ─────────────────────────────────────────────────────────────────────────────
# EX-DIVIDEND & RO SEASON GUARDS
# ─────────────────────────────────────────────────────────────────────────────

def is_near_ex_dividend_window(today=None) -> bool:
    today = today or datetime.now(pytz.timezone('Pacific/Honolulu'))
    return today.day in EX_DIV_WINDOW_DAYS

def is_ro_filing_season(today=None) -> bool:
    today = today or datetime.now(pytz.timezone('Pacific/Honolulu'))
    start_m, start_d, end_m, end_d = RO_FILING_SEASON
    start = today.replace(month=start_m, day=start_d)
    end   = today.replace(month=end_m,   day=end_d)
    return start <= today <= end

# ─────────────────────────────────────────────────────────────────────────────
# CRISIS AMPLIFICATION — VIXY Z-SCORE
# ─────────────────────────────────────────────────────────────────────────────

def check_crisis_amplification_risk(session):
    """
    Self-normalizing VIXY z-score vs its own 20D mean. Real VIX index not available
    at this Twelve Data plan tier; VIXY is the closest proxy.
    Returns (is_crisis_day, vixy_price, vixy_z).
    """
    try:
        values = fetch_time_series(session, "VIXY", outputsize=20)
        if len(values) < 10:
            return False, 0.0, 0.0
        closes  = [float(v["close"]) for v in values]
        current = closes[0]
        mean    = sum(closes) / len(closes)
        std     = (sum((c - mean) ** 2 for c in closes) / len(closes)) ** 0.5
        z       = (current - mean) / std if std > 0 else 0.0
        return z >= CRISIS_VIXY_Z_THRESHOLD, current, z
    except Exception as e:
        logger.error(f"[Crisis Amplification Check Error] {e}")
        return False, 0.0, 0.0

# ─────────────────────────────────────────────────────────────────────────────
# WHALE FLOW — DIRECTION-AWARE
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_rvol_native(symbol: str):
    """
    Fetch RVOL via plain REST — previously used TDClient SDK which spawned a WebSocket
    thread on every call, exhausting PythonAnywhere's thread limit over time.
    Returns the float rvol ratio, or None on failure (caller falls back to manual calc).
    """
    try:
        import requests as _req
        r = _req.get(
            "https://api.twelvedata.com/rvol",
            params={"symbol": symbol, "interval": "1day", "outputsize": 1, "apikey": TD_API_KEY},
            timeout=10
        ).json()
        values = r.get("values", [])
        if values:
            return float(values[0].get("rvol", 1.0))
    except Exception as e:
        logger.debug(f"[RVOL REST] {symbol}: {e} — falling back to manual")
    return None


def detect_whale_flow_direction(session, symbol):
    """
    Distinguishes accumulation from distribution. Generic volume spike alone is not
    actionable — direction of capital flow is what matters for RO front-running.
    RVOL sourced from TD native RVOLEndpoint (authoritative); price change from a
    2-bar time_series fetch. Falls back to manual 20-day ratio if TD endpoint fails.
    Returns (tag_string, relative_volume_ratio).
    """
    try:
        # Price change requires at least 2 bars regardless of which RVOL path we take
        values = fetch_time_series(session, symbol, outputsize=2)
        if len(values) < 2:
            return "NORMAL", 1.0
        price_chg = (float(values[0]["close"]) - float(values[1]["close"])) / float(values[1]["close"]) * 100

        # Try TD native RVOL first — falls back to manual 20D ratio on failure
        rvol = _fetch_rvol_native(symbol)
        if rvol is None:
            # Manual fallback: fetch 21 bars for baseline, use today as numerator
            extended = fetch_time_series(session, symbol, outputsize=21)
            if len(extended) < 11:
                return "NORMAL", 1.0
            today_vol    = float(extended[0]["volume"])
            baseline_vol = sum(float(v["volume"]) for v in extended[1:21]) / max(len(extended[1:21]), 1)
            rvol         = today_vol / baseline_vol if baseline_vol > 0 else 1.0

        if rvol >= 1.8 and price_chg <= -0.5:
            return "🔴 DISTRIBUTION (Whale Sell-Off)", rvol
        if rvol >= 1.8 and price_chg >= 0.5:
            return "🟢 ACCUMULATION (Whale Buy-In)", rvol
        return "NORMAL", rvol
    except Exception as e:
        logger.error(f"[Whale Flow Error] {symbol}: {e}")
        return "NORMAL", 1.0

# ─────────────────────────────────────────────────────────────────────────────
# NEW: DARK POOL / OFF-EXCHANGE DETECTOR
# Catches the Feb/March 2026 pattern: price dropped on below-average public volume,
# suggesting institutional exit routed through dark pools or off-exchange venues.
# ─────────────────────────────────────────────────────────────────────────────

def detect_dark_pool_activity(session, symbol):
    """
    Dark pool signature: meaningful price decline + public volume well below 20D average.
    When institutions sell in size through dark pools, the lit exchange shows thin volume
    while price still falls — the opposite of a normal retail selloff.
    Returns (is_dark_pool, price_chg_pct, vol_ratio, description).
    """
    try:
        values = fetch_time_series(session, symbol, outputsize=21)
        if len(values) < 11:
            return False, 0.0, 1.0, "Insufficient data"

        today_vol    = float(values[0]["volume"])
        baseline_vol = sum(float(v["volume"]) for v in values[1:21]) / max(len(values[1:21]), 1)
        vol_ratio    = today_vol / baseline_vol if baseline_vol > 0 else 1.0
        price_chg    = (float(values[0]["close"]) - float(values[1]["close"])) / float(values[1]["close"]) * 100

        is_dark_pool = (
            price_chg  <= DARK_POOL_PRICE_DROP_PCT and
            vol_ratio  <= DARK_POOL_VOLUME_RATIO_MAX
        )
        desc = (
            f"{'🕵️ ' if is_dark_pool else ''}{price_chg:+.1f}% / {vol_ratio:.2f}x vol — "
            f"{'OFF-EXCHANGE EXIT SIGNAL' if is_dark_pool else 'normal'}"
        )
        return is_dark_pool, price_chg, vol_ratio, desc
    except Exception as e:
        logger.error(f"[Dark Pool Detector Error] {symbol}: {e}")
        return False, 0.0, 1.0, "Error"

# ─────────────────────────────────────────────────────────────────────────────
# NEW: CEF PREMIUM COMPRESSION DETECTOR
# Fast intra-session collapse of the premium/discount spread without a matching
# NAV move = institutional exit. This is distinct from dark pool (which is price-based);
# premium compression is spread-based and CEF-specific.
# ─────────────────────────────────────────────────────────────────────────────

def detect_premium_compression(current_premium: float, ticker: str) -> tuple:
    """
    Compares today's premium to yesterday's cached value.
    A compression > PREMIUM_COMPRESSION_THRESHOLD in one session is a red flag.
    Returns (is_compressed, delta_pct, description).
    """
    try:
        prev_key  = f"{ticker}_premium_prev"
        prev_prem = float(db.get_state(prev_key, current_premium))
        delta     = current_premium - prev_prem

        # Store today's value for tomorrow's comparison
        db.update_state(prev_key, current_premium)

        is_compressed = delta <= PREMIUM_COMPRESSION_THRESHOLD
        desc = (
            f"{'🔴 ' if is_compressed else ''}Δ {delta:+.2f}% — "
            f"{'FAST COMPRESSION' if is_compressed else 'stable'}"
        )
        return is_compressed, delta, desc
    except Exception as e:
        logger.error(f"[Premium Compression Error] {ticker}: {e}")
        return False, 0.0, "Error"

# ─────────────────────────────────────────────────────────────────────────────
# NEW: RO COMPLETION DIP DETECTOR
# After a confirmed N-2 event, watch for the post-RO price collapse back toward NAV.
# That dip is the re-entry signal Todd Akin describes: "buy back when the company
# says it's done." We can't scrape their press releases, but the signature is
# recognizable: premium collapses from 20%+ back below 10% AND price is ≥10% off
# its 60D high — that pattern reliably marks the post-RO bottom.
# ─────────────────────────────────────────────────────────────────────────────

def detect_ro_completion_dip(session, ticker, current_price, current_premium) -> bool:
    """
    Returns True (and dispatches a rebuy alert) when all three conditions are met:
      1. N-2 was previously detected for this ticker (DB key set)
      2. Premium has collapsed from >20% to <10% (post-RO dilution repricing)
      3. Price is ≥10% below the 60D high (dip confirmed, not just sideways)
    Fires once per RO cycle — cleared when conditions reset.
    """
    try:
        n2_key        = f"cornerstone_n2_detected_{ticker}"
        fired_key     = f"cornerstone_ro_dip_fired_{ticker}"
        prev_n2       = db.get_state(n2_key, "")
        already_fired = db.get_state(fired_key, "")

        if not prev_n2 or already_fired:
            return False

        # Condition 2: premium collapsed back below 10%
        if current_premium >= 10.0:
            return False

        # Condition 3: price ≥10% below 60D high
        values = fetch_time_series(session, ticker, outputsize=60)
        if len(values) < 10:
            return False
        high_60d = max(float(v["close"]) for v in values)
        pct_below_high = ((high_60d - current_price) / high_60d) * 100
        if pct_below_high < 10.0:
            return False

        # All conditions met — dispatch rebuy alert and mark as fired
        db.update_state(fired_key, datetime.now().strftime("%Y-%m-%d"))
        dip_msg = (
            f"**{ticker} — 🟢 POST-RO DIP: REBUY ZONE**\n"
            f"┣ Price: ${current_price:.2f} ({pct_below_high:.1f}% below 60D high)\n"
            f"┣ Premium to NAV: {current_premium:.2f}% (was >20% during RO)\n"
            f"┣ RO Cycle: N-2 was previously detected — price has repriced toward NAV\n"
            f"┣ Signal: Premium collapse + price off high = classic post-RO dip pattern\n"
            f"┣ ⚠️ Verify: Confirm Cornerstone announced 'RO complete' before acting\n"
            f"┣ Action: Rebuy position + resume CS DRIP (call broker to confirm DRIP status)\n"
            f"┗ Note: Keep ≥3 shares at all times to preserve NAV DRIP eligibility"
        )
        if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
            send_essentials_embed(
                WEBHOOK_CORNERSTONE,
                f"🟢 {ticker} — Post-RO Rebuy Zone Detected",
                dip_msg, 0x2ecc71
            )
        logger.info(f"[RO Completion Dip] {ticker} — rebuy alert dispatched.")
        return True

    except Exception as e:
        logger.error(f"[RO Completion Dip Error] {ticker}: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# NEW: MACRO CROSS-CORRELATION ENGINE
# CLM/CRF dropping harder than SPY on the same session = CEF-specific risk.
# CLM/CRF dropping less than SPY = macro drag only, no action needed.
# ─────────────────────────────────────────────────────────────────────────────

def check_macro_correlation(session, clm_chg: float, crf_chg: float) -> tuple:
    """
    Fetches SPY session change and compares to CLM/CRF performance.
    Returns (underperforming, spy_chg, interpretation).
    """
    try:
        values    = fetch_time_series(session, "SPY", outputsize=2)
        if len(values) < 2:
            return False, 0.0, "SPY data unavailable"
        spy_chg   = (float(values[0]["close"]) - float(values[1]["close"])) / float(values[1]["close"]) * 100
        avg_cef   = (clm_chg + crf_chg) / 2

        underperforming = (spy_chg < -0.5) and (avg_cef < spy_chg - 1.0)
        if underperforming:
            interp = (
                f"CLM/CRF avg {avg_cef:+.2f}% vs SPY {spy_chg:+.2f}% — "
                f"⚠️ CEF-SPECIFIC UNDERPERFORMANCE (not just macro drag)"
            )
        elif spy_chg < -1.0:
            interp = f"SPY {spy_chg:+.2f}% — macro selloff; CLM/CRF tracking market, no CEF-specific risk"
        else:
            interp = f"SPY {spy_chg:+.2f}% — no macro event"

        return underperforming, spy_chg, interp
    except Exception as e:
        logger.error(f"[Macro Correlation Error] {e}")
        return False, 0.0, "Error"

# ─────────────────────────────────────────────────────────────────────────────
# ACCUMULATION READINESS — "DON'T CATCH A FALLING KNIFE" GUARD
# Built to prevent the Feb/March 2026 pattern: broad market selloff drove
# CLM/CRF prices down; adding margin too early amplified losses. The RO score
# would have been LOW (no N-2, no dark pool trigger) so the STABLE report had
# no "wait" signal. This function adds the missing macro regime layer.
#
# Three independent checks:
#   1. Consecutive down days for this ticker (momentum direction)
#   2. SPY vs 200 SMA (bull/bear macro regime) — passed in via shared cache
#   3. VIXY z-score (fear level — already computed by check_crisis_amplification_risk)
#
# This does NOT affect the RO risk score — it's a separate capital-safety signal.
# ─────────────────────────────────────────────────────────────────────────────

def check_accumulation_readiness(session, ticker: str, vixy_z: float,
                                  spy_vals_200: list = None,
                                  premium: float = None) -> dict:
    """
    Returns a dict: {ready, status, detail, down_streak}.
    Uses pre-fetched spy_vals_200 list (from shared cache) to avoid re-querying SPY.
    Falls back to fetching SPY internally if cache is empty.
    """
    try:
        # ── PREMIUM GATE — evaluated before all other conditions.
        # Buying CLM/CRF at >15% premium to NAV compounds downside even when every
        # other macro signal looks clean. Feb 2, 2026: CLM at 15.6% premium —
        # all other conditions were neutral, but the elevated premium amplified every
        # subsequent point of price decline. This gate closes that gap at zero cost.
        if premium is not None and premium > 15.0:
            return {
                "ready":       False,
                "status":      "WAIT — Elevated Premium (>15% to NAV)",
                "detail":      (
                    f"Premium: {premium:.1f}% to NAV — buying at this spread compounds downside. "
                    f"Target entry: premium < 15% (ideally near NAV during ex-div dip or post-RO). "
                    f"Historical: CLM/CRF at 15%+ premium = elevated RO announcement risk window."
                ),
                "down_streak": 0,
            }

        # 1. Consecutive down days for this ticker (last 10 closes)
        values = fetch_time_series(session, ticker, outputsize=10)
        down_streak = 0
        for i in range(len(values) - 1):
            if float(values[i]["close"]) < float(values[i+1]["close"]):
                down_streak += 1
            else:
                break

        # 2. SPY vs 200 SMA (use shared cache when available)
        spy_above_200 = None
        if spy_vals_200 is None:
            spy_vals_200 = fetch_time_series(session, "SPY", outputsize=200)
        if len(spy_vals_200) >= 50:
            spy_now    = float(spy_vals_200[0]["close"])
            sma200     = sum(float(v["close"]) for v in spy_vals_200) / len(spy_vals_200)
            spy_above_200 = spy_now > sma200

        in_bear      = spy_above_200 == False
        high_fear    = vixy_z >= 1.5
        extreme_fear = vixy_z >= 2.0
        regime_str   = (
            "SPY above 200 SMA (bull)"  if spy_above_200 == True  else
            "SPY below 200 SMA (bear)"  if spy_above_200 == False else
            "SPY regime unavailable"
        )

        # Tiers — worst to best
        if extreme_fear and down_streak >= 3 and in_bear:
            return {
                "ready":       False,
                "status":      "WAIT — Falling Knife (All 3 bearish signals)",
                "detail":      (
                    f"{down_streak}-day down streak | {regime_str} | "
                    f"VIXY z {vixy_z:+.1f}σ — "
                    f"wait for 3 consecutive green closes before adding margin"
                ),
                "down_streak": down_streak,
            }
        elif down_streak >= 5:
            return {
                "ready":       False,
                "status":      f"WAIT — {down_streak}-Day Downtrend",
                "detail":      (
                    f"{down_streak} consecutive closes lower | {regime_str} — "
                    f"momentum still bearish. Wait for 2+ consecutive green closes."
                ),
                "down_streak": down_streak,
            }
        elif in_bear and high_fear:
            return {
                "ready":       False,
                "status":      "CAUTION — Bear Regime + Elevated Fear",
                "detail":      (
                    f"{regime_str} | VIXY z {vixy_z:+.1f}σ | "
                    f"{down_streak} down day(s) — "
                    f"reduce margin exposure, do not add new positions"
                ),
                "down_streak": down_streak,
            }
        elif down_streak >= 3:
            return {
                "ready":       False,
                "status":      f"CAUTION — {down_streak}-Day Slide",
                "detail":      (
                    f"{down_streak} consecutive down days | {regime_str} — "
                    f"monitor for stabilization. In March/Sept: wait for 3 green days per plan"
                ),
                "down_streak": down_streak,
            }
        else:
            bull_bear   = "Bull" if spy_above_200 else "Bear"
            vixy_calm   = "calm" if vixy_z < 0.75 else ("elevated" if vixy_z < 1.5 else "spike ⚠️")
            return {
                "ready":       True,
                "status":      "OPEN",
                "detail":      f"{bull_bear} regime | VIXY z {vixy_z:+.1f}σ ({vixy_calm}) | {down_streak}d streak — deploy",
                "down_streak": down_streak,
            }

    except Exception as e:
        logger.error(f"[Accumulation Readiness] {ticker}: {e}")
        return {
            "ready":       True,
            "status":      "UNKNOWN — check manually",
            "detail":      "Readiness check failed — verify macro regime before adding margin",
            "down_streak": 0,
        }

# ─────────────────────────────────────────────────────────────────────────────
# RO COMPOSITE RISK SCORE
# ─────────────────────────────────────────────────────────────────────────────

def calculate_ro_risk_score(
    sec_shield, z_premium, premium, whale_tag, credit_spread,
    ex_div_near, ro_season=False, crisis_day=False,
    dark_pool=False, premium_compressed=False,
    macro_underperform=False, holder_exit=False,
    premium_30pct_watch=False
):
    """
    Composite Rights-Offering risk score (0–100).
    New signals (dark_pool, premium_compressed, macro_underperform, holder_exit)
    added alongside all original signals — weights defined in RO_SCORE_WEIGHTS.
    """
    score = 0
    # EDGAR signals stack independently — each detected form adds conviction
    if "N-2 RO REGISTRATION" in sec_shield:
        score += RO_SCORE_WEIGHTS["sec_n2"]
    if "N-2/A" in sec_shield:
        score += RO_SCORE_WEIGHTS["sec_n2a"]
    # N-CSR and DEF 14A are routine filings (semi-annual report, annual proxy) —
    # always present in EDGAR, they don't signal RO risk on their own.
    # Only add weight when already elevated by a real signal (N-2 or z_danger).
    if "N-CSR" in sec_shield and score > 0:
        score += RO_SCORE_WEIGHTS["sec_ncsr"]
    if "DEF 14A" in sec_shield and score > 0:
        score += RO_SCORE_WEIGHTS["sec_def14a"]
    if z_premium >= 2.0:
        score += RO_SCORE_WEIGHTS["z_danger"]
    elif z_premium >= 1.5:
        score += RO_SCORE_WEIGHTS["z_caution"]
    if premium > 25.0:
        score += RO_SCORE_WEIGHTS["premium_extreme"]
    if "DISTRIBUTION" in whale_tag:
        score += RO_SCORE_WEIGHTS["whale_distribution"]
    if credit_spread > 4.5:
        score += RO_SCORE_WEIGHTS["credit_stress"]
    if ro_season:
        score += RO_SCORE_WEIGHTS["ro_season"]
    if crisis_day:
        score += RO_SCORE_WEIGHTS["crisis_amplification"]
    if dark_pool:
        score += RO_SCORE_WEIGHTS["dark_pool"]
    if premium_compressed:
        score += RO_SCORE_WEIGHTS["premium_compression"]
    if macro_underperform:
        score += RO_SCORE_WEIGHTS["macro_underperform"]
    if holder_exit:
        score += RO_SCORE_WEIGHTS["13f_holder_exit"]
    if premium_30pct_watch:
        score += RO_SCORE_WEIGHTS["premium_30pct_watch"]
    if ex_div_near and score > 0:
        score += RO_SCORE_WEIGHTS["ex_div_relief"]   # negative weight — schedules dip, not dilution

    score = max(0, min(100, score))
    tier  = "CRITICAL" if score >= 50 else ("ELEVATED" if score >= 25 else "LOW")
    return score, tier

# ─────────────────────────────────────────────────────────────────────────────
# PULSE REPORT FORMATTER
# Mobile-first Discord format: Title / ┣ Data / ┗ Final
# ─────────────────────────────────────────────────────────────────────────────

def _parse_sec_shield(sec_shield: str) -> dict:
    """
    Parse the full EDGAR shield string into structured components for display.
    Returns a dict with keys: ro_active, holder_change, has_routine_only, sec_line, holder_line, edgar_line.
    Routine filings (N-CSR, DEF 14A) are back-pocket only — they do not appear as
    conviction signals in the output when no N-2 or 13D/G is present.
    """
    ro_active     = "N-2 RO REGISTRATION" in sec_shield or "N-2/A" in sec_shield
    holder_change = "13D" in sec_shield or "13G" in sec_shield
    has_routine   = "N-CSR" in sec_shield or "DEF 14A" in sec_shield

    if ro_active:
        form_bit   = "N-2/A amendment" if "N-2/A" in sec_shield else "N-2 registration"
        sec_line   = f"⚠️ {form_bit} — RO ACTIVE"
        edgar_line = f"⚠️ {form_bit} — RO ACTIVE"
    else:
        sec_line   = "No N-2/RO (safe)"
        # ⚡ EDGAR: only show conviction when actionable signal present (13D/G or N-2).
        # Routine N-CSR/DEF 14A filings are stored in DB but never surfaced as red alerts.
        edgar_line = "None"

    if holder_change:
        holder_line = "⚠️ Large holder change detected — monitor"
        edgar_line  = "⚠️ Large holder change (13D/G)"
    else:
        holder_line = "No large-holder changes (safe)"

    return {
        "sec_line":    sec_line,
        "holder_line": holder_line,
        "edgar_line":  edgar_line,
        "ro_active":   ro_active,
        "holder_change": holder_change,
    }


def format_pulse_report(ticker, price, nav, rsi, premium, z_premium,
                         sec_shield, ro_score, ro_tier, whale_status,
                         dark_pool_desc, premium_compression_desc,
                         macro_interp, ex_div_near, ro_season, crisis_day,
                         vixy_z, status, recommendation, verdict,
                         income_note, s_net, alpha_drip, seasonal_caution,
                         y_dist=0.0) -> str:
    """
    Cornerstone Pulse — mobile-first labeled format.

    Fixed lines (always present):
      SEC Filing, Premium to NAV, Holder (13D/G), ⚡ EDGAR, Whale Flow,
      Z-Score, RSI (1D), Div. Yield + RO Risk

    Conditional lines (only when triggered):
      OBV divergence, VIXY spike, RO Season, Seasonal Caution, Ex-Div

    Verdict always last.
    Removed from output (back-pocket / DB only):
      N-CSR, DEF 14A individual lines, Margin Deploy advisory.
    """
    prem_tag = "(neutral)" if 10 <= premium <= 20 else ("(EXTENDED)" if premium > 25 else ("(HIGH)" if premium > 15 else "(DISCOUNT)"))
    rsi_tag  = "(neutral)" if 40 <= rsi <= 60 else ("(OVERBOUGHT)" if rsi > 70 else ("(OVERSOLD)" if rsi < 30 else "(neutral)"))
    z_tag    = "(safe)" if z_premium < 1.0 else ("(caution)" if z_premium < 2.0 else "(DANGER)")

    sec  = _parse_sec_shield(sec_shield)

    # Whale flow: only surface when distribution is detected
    whale_tag = f"⚠️ {whale_status}" if "DISTRIBUTION" in whale_status.upper() else "NORMAL"

    # Conditional lines — inserted before verdict only when triggered
    vixy_line    = f"┣ VIXY: {vixy_z:+.1f}σ spike — reduce size / close puts→calls\n" if crisis_day else ""
    ro_season_line = "┣ RO Season: Active (Feb–Apr window)\n" if ro_season else ""
    seasonal_line  = "┣ Seasonal Caution: Active (March/Sept weakness)\n" if seasonal_caution else ""
    ex_div_line    = "┣ Ex-Div: Scheduled dip (not RO-related)\n" if ex_div_near else ""

    return (
        f"{ticker} — {status}\n"
        f"┣ SEC Filing: {sec['sec_line']}\n"
        f"┣ Premium to NAV: {premium:.2f}% {prem_tag}\n"
        f"┣ Holder (13D/G): {sec['holder_line']}\n"
        f"┣ ⚡ EDGAR: {sec['edgar_line']}\n"
        f"┣ Whale Flow: {whale_tag}\n"
        f"┣ Z-Score: {z_premium:+.1f}σ {z_tag}\n"
        f"┣ RSI (1D): {rsi:.1f} {rsi_tag}\n"
        f"{vixy_line}"
        f"{ro_season_line}"
        f"{seasonal_line}"
        f"{ex_div_line}"
        f"┗ Div. Yield: {y_dist:.1f}% | RO Risk: {ro_score}/100 ({ro_tier})\n"
    )

# ─────────────────────────────────────────────────────────────────────────────
# PER-TICKER REPORT ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def get_ticker_report(session, ticker, spy_chg_cache: dict):
    """
    Full analysis for one CLM/CRF ticker. spy_chg_cache is a shared dict so SPY
    is only fetched once per monitor loop regardless of how many tickers run.
    Returns (formatted_report_string, ro_tier, ro_score).
    """
    price, rsi, nav = fetch_live_metrics(session, ticker)
    if price == 0.0:
        return f"**{ticker}**\n┗ ⚠️ Data feed offline.\n", "LOW", 0

    # ── Whale flow (original)
    whale_status, whale_rvol = detect_whale_flow_direction(session, ticker)

    # ── Distribution math (original)
    annual_div = 1.4580 if ticker == "CLM" else 1.4112  # 2026 distribution profiles
    y_dist     = (annual_div / price) * 100 if price > 0 else 0
    y_nav      = (annual_div / nav)   * 100 if nav   > 0 else 0
    margin_rate     = 7.25
    leverage_ratio  = 1.0
    s_net      = y_dist - (margin_rate * leverage_ratio)
    premium    = ((price - nav) / nav) * 100 if nav > 0 else 0
    alpha_drip = (premium / 100) * y_nav    if nav > 0 else 0

    # ── Premium Z-score (original)
    mu_rho    = float(db.get_state(f"{ticker}_premium_mu",    15.0))
    sigma_rho = float(db.get_state(f"{ticker}_premium_sigma",  4.0))
    z_premium = (premium - mu_rho) / sigma_rho if sigma_rho > 0 else 0

    # ── SEC EDGAR (original + 13D/G added)
    sec_shield = check_sec_edgar(session, ticker)

    # ── Macro/seasonal context (original)
    credit_spread = fetch_hy_spread_live()  # FRED BAMLH0A0HYM2 — live, cached daily
    ex_div_near   = is_near_ex_dividend_window()
    ro_season     = is_ro_filing_season()
    crisis_day, vixy_price, vixy_z = check_crisis_amplification_risk(session)
    seasonal_caution = is_seasonal_caution_month()

    # ── NEW: Dark pool detection
    is_dark_pool, price_chg, vol_ratio, dark_pool_desc = detect_dark_pool_activity(session, ticker)

    # ── NEW: CEF premium compression
    is_compressed, prem_delta, prem_compress_desc = detect_premium_compression(premium, ticker)

    # ── NEW: Macro cross-correlation (SPY fetched once, shared via cache)
    # Also fetch 200 days for the accumulation readiness check (amortised — one fetch serves both).
    if "spy_vals_200" not in spy_chg_cache:
        spy_vals_200 = fetch_time_series(session, "SPY", outputsize=200)
        spy_chg_cache["spy_vals_200"] = spy_vals_200
    else:
        spy_vals_200 = spy_chg_cache["spy_vals_200"]

    if "spy_chg" not in spy_chg_cache:
        if len(spy_vals_200) >= 2:
            spy_chg_cache["spy_chg"] = (
                (float(spy_vals_200[0]["close"]) - float(spy_vals_200[1]["close"])) /
                float(spy_vals_200[1]["close"]) * 100
            )
        else:
            spy_chg_cache["spy_chg"] = 0.0

    spy_chg         = spy_chg_cache["spy_chg"]
    avg_cef_chg     = price_chg  # single ticker; caller averages across both if needed
    macro_underperf = (spy_chg < -0.5) and (avg_cef_chg < spy_chg - 1.0)
    macro_interp    = (
        f"{'⚠️ ' if macro_underperf else ''}{avg_cef_chg:+.1f}% vs SPY {spy_chg:+.1f}% — "
        f"{'CEF underperforming' if macro_underperf else 'tracking market'}"
    ) if spy_chg != 0.0 else "SPY unavailable"

    # ── NEW: 13F / large holder exit signal from SEC scrape
    holder_exit = "13D" in sec_shield or "13G" in sec_shield

    # ── NEW: Track N-2 detection across cycles (used by RO completion dip detector)
    n2_key = f"cornerstone_n2_detected_{ticker}"
    if "N-2 RO REGISTRATION" in sec_shield or "N-2/A" in sec_shield:
        if not db.get_state(n2_key, ""):
            db.update_state(n2_key, datetime.now().strftime("%Y-%m-%d"))
            # Reset the dip-fired flag when a new RO cycle starts
            db.update_state(f"cornerstone_ro_dip_fired_{ticker}", "")
    else:
        # N-2 no longer in recent filings — clear the cycle tracker
        db.update_state(n2_key, "")

    # ── NEW: RO completion dip detector (fires rebuy alert automatically)
    detect_ro_completion_dip(session, ticker, price, premium)

    # ── NEW: 30% premium RO Watch gate (Todd Akin threshold — RO "usually" announced here)
    # Debounced: fires once when premium crosses 30%, resets when it drops back below 25%.
    watch_key    = f"cornerstone_30pct_watch_active_{ticker}"
    was_watching = db.get_state(watch_key, "")
    premium_30pct_watch = False
    if premium >= PREMIUM_RO_WATCH_THRESHOLD:
        premium_30pct_watch = True
        if not was_watching:
            db.update_state(watch_key, "active")
            watch_alert = (
                f"**{ticker} — ⚠️ RO WATCH: 30%+ Premium Threshold Reached**\n"
                f"┣ Premium to NAV: {premium:.2f}% (threshold: {PREMIUM_RO_WATCH_THRESHOLD:.0f}%)\n"
                f"┣ Historical pattern: Cornerstone typically announces RO when premium hits 30%+\n"
                f"┣ N-2 Filing: Not yet detected on EDGAR — but this is the early signal\n"
                f"┣ Action: Monitor Cornerstone press releases + Seeking Alpha CLM/CRF comments\n"
                f"┣ Prepare: If N-2 drops, sell to minimum 3 shares (to preserve CS DRIP status)\n"
                f"┗ Do NOT sell yet — wait for N-2 confirmation before acting"
            )
            if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
                send_essentials_embed(
                    WEBHOOK_CORNERSTONE,
                    f"⚠️ {ticker} — 30% Premium RO Watch Active",
                    watch_alert, 0xf39c12
                )
            logger.info(f"[30% RO Watch] {ticker} — premium {premium:.2f}% crossed threshold, watch alert dispatched.")
    elif premium < 25.0 and was_watching:
        # Premium retreated below 25% — reset the watch so it can fire again next cycle
        db.update_state(watch_key, "")

    # ── RO composite risk score (upgraded with new signals)
    ro_score, ro_tier = calculate_ro_risk_score(
        sec_shield, z_premium, premium, whale_status, credit_spread,
        ex_div_near, ro_season=ro_season, crisis_day=crisis_day,
        dark_pool=is_dark_pool, premium_compressed=is_compressed,
        macro_underperform=macro_underperf, holder_exit=holder_exit,
        premium_30pct_watch=premium_30pct_watch
    )

    # ── Ledger prediction logging (original — only on ELEVATED/CRITICAL)
    if ro_tier in ("ELEVATED", "CRITICAL") or "N-2 RO REGISTRATION" in sec_shield or "N-2/A" in sec_shield:
        try:
            from analytics import HighFidelityAnalyticsEngine
            prediction_id = f"{ticker}_{datetime.now().strftime('%Y%m%d')}"
            HighFidelityAnalyticsEngine().log_ledger_prediction(
                "cornerstone", prediction_id, "DOWN", price, ticker=ticker,
                context=f"RO score {ro_score} ({ro_tier})"
            )
        except Exception as e:
            logger.error(f"Cornerstone ledger logging failed: {e}")

    # ── Status / recommendation logic (original tiers preserved, new signals feed score)
    if "N-2 RO REGISTRATION" in sec_shield or "N-2/A" in sec_shield:
        status       = "🚨 CRITICAL: N-2 DETECTED"
        income_note  = "Distribution/Caution phase"
        verdict      = "🔴 SELL to ≥3 shares — NAV dilution imminent. ≥3 shares preserves DRIP permanently."
        recommendation = "Halt DRIP; sell to 3-share floor; monitor for RO completion."
    elif ro_tier == "CRITICAL":
        status       = "🚨 CRITICAL: RO RISK ELEVATED"
        income_note  = "Distribution/Caution phase"
        verdict      = "🔴 RO risk composite critical — halt DRIP, watch for N-2 on EDGAR."
        recommendation = "Halt DRIP; consider selling before RO announcement."
    elif is_dark_pool:
        status       = "🕵️ WARNING: DARK POOL ACTIVITY"
        income_note  = "Distribution/Caution phase"
        verdict      = "⚠️ Off-exchange exit suspected — monitor EDGAR, do not sell yet."
        recommendation = "Monitor closely. Do NOT sell yet — confirm with SEC module."
    elif is_compressed:
        status       = "⚠️ WARNING: PREMIUM COMPRESSION"
        income_note  = "Distribution/Caution phase"
        verdict      = f"⚠️ Premium compressed {prem_delta:+.2f}% intra-session — pause DRIP, watch for N-2."
        recommendation = "Pause new DRIP reinvestment; watch for RO filing."
    elif ro_tier == "ELEVATED" or z_premium >= 1.5 or premium > 25.0:
        status       = "⚠️ HIGH PREMIUM"
        income_note  = "Distribution/Caution phase"
        verdict      = "⚠️ Premium extended — pause new buys, target entry < 15% or post-ex-div dip."
        recommendation = "Pause reinvestment; build cash position."
    else:
        status       = "✅ STABLE"
        income_note  = "Accumulation phase"
        verdict      = "✅ DRIP active — accumulate on premium dips < 15% or ex-div window."
        recommendation = "Reinvest distributions at NAV."

    report_text = format_pulse_report(
        ticker=ticker, price=price, nav=nav, rsi=rsi, premium=premium,
        z_premium=z_premium, sec_shield=sec_shield, ro_score=ro_score,
        ro_tier=ro_tier, whale_status=whale_status,
        dark_pool_desc=dark_pool_desc, premium_compression_desc=prem_compress_desc,
        macro_interp=macro_interp, ex_div_near=ex_div_near, ro_season=ro_season,
        crisis_day=crisis_day, vixy_z=vixy_z, status=status,
        recommendation=recommendation, verdict=verdict,
        income_note=income_note, s_net=s_net, alpha_drip=alpha_drip,
        seasonal_caution=seasonal_caution, y_dist=y_dist
    )

    # ── OBV + MFI: back-pocket volume pressure signals.
    # Only appended to the report when a divergence fires (price up but volume exiting);
    # otherwise logged to DB for context without cluttering the daily output.
    obv_mfi = fetch_obv_mfi(session, ticker)
    if obv_mfi:
        mfi       = obv_mfi["mfi"]
        obv_trend = obv_mfi["obv_trend"]
        obv_pct   = obv_mfi["obv_pct"]
        price_up  = price_chg >= 0
        divergence = price_up and (obv_trend == "falling") and (mfi < 45)
        db.update_state(f"{ticker}_obv_trend", obv_trend)
        db.update_state(f"{ticker}_mfi", str(round(mfi, 1)))
        if divergence:
            # Divergence = genuine early-warning signal — surface it
            mfi_tag = "🔴 OVERBOUGHT" if mfi > 70 else ("🟢 OVERSOLD" if mfi < 30 else "🟡 NEUTRAL")
            obv_line = f"┣ OBV: {obv_trend} ({obv_pct:+.1f}%/5D) | MFI: {mfi:.1f} {mfi_tag} ⚠️ DIVERGENCE\n"
            report_text = report_text.rstrip("\n") + "\n" + obv_line

    # ── Accumulation gate — back-pocket only; stored in DB, not appended to output.
    acc = check_accumulation_readiness(session, ticker, vixy_z, spy_vals_200, premium=premium)
    db.update_state(f"{ticker}_acc_status", acc["status"])
    db.update_state(f"{ticker}_acc_detail", acc["detail"])

    # ── Persist key metrics for cross-script reads (market_analysis.py morning brief)
    db.update_state(f"{ticker.lower()}_last_z_premium", round(z_premium, 3))
    db.update_state(f"{ticker.lower()}_last_premium",   round(premium, 3))
    db.update_state(f"{ticker.lower()}_last_ro_tier",   ro_tier)

    return report_text, ro_tier, ro_score

# ─────────────────────────────────────────────────────────────────────────────
# CHART BUILDER (original — unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def build_cornerstone_chart():
    """
    Fetches live Finviz daily charts for CLM and CRF, stitches them vertically
    into one image (CLM on top, CRF below) for the Discord embed attachment.
    Falls back to None if either fetch fails — dispatch continues without a chart.
    """
    try:
        from PIL import Image
        import io
        charts = []
        headers = {"User-Agent": "Mozilla/5.0"}
        for ticker in PRIORITY_ASSETS:
            url = f"https://finviz.com/chart.ashx?t={ticker}&ty=c&ta=1&p=d&s=l&theme=dark"
            res = requests.get(url, headers=headers, timeout=15)
            res.raise_for_status()
            img = Image.open(io.BytesIO(res.content)).convert("RGB")
            charts.append(img)

        if not charts:
            return None

        # Stack vertically — both charts same width (Finviz returns consistent dimensions)
        total_height = sum(c.height for c in charts)
        combined = Image.new("RGB", (charts[0].width, total_height), (30, 30, 30))
        y_offset = 0
        for c in charts:
            combined.paste(c, (0, y_offset))
            y_offset += c.height

        buf = io.BytesIO()
        combined.save(buf, format="PNG")
        return buf.getvalue()

    except ImportError:
        # Pillow not installed — return individual CLM chart as fallback
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            url = f"https://finviz.com/chart.ashx?t=CLM&ty=c&ta=1&p=d&s=l&theme=dark"
            res = requests.get(url, headers=headers, timeout=15)
            res.raise_for_status()
            return res.content
        except Exception as e:
            logger.error(f"Finviz chart fallback failed: {e}")
            return None
    except Exception as e:
        logger.error(f"Finviz chart generation failed: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# COMPUTE BOTH FUND REPORTS (shared SPY cache)
# ─────────────────────────────────────────────────────────────────────────────

TIER_RANK = {"LOW": 0, "ELEVATED": 1, "CRITICAL": 2}

def compute_cornerstone_reports():
    """
    Single source of truth — called by both the 0800 HST daily pulse and the
    continuous 5-min escalation loop so they never drift.
    Returns (full_report_string, worst_tier_string).
    """
    reports     = []
    worst_tier  = "LOW"
    spy_cache   = {}  # shared across both tickers — SPY fetched only once

    with requests.Session() as session:
        for ticker in PRIORITY_ASSETS:
            text, tier, score = get_ticker_report(session, ticker, spy_cache)
            reports.append(text)
            if TIER_RANK.get(tier, 0) > TIER_RANK.get(worst_tier, 0):
                worst_tier = tier

    full_report = "\n\n".join(reports)

    credit_spread = fetch_hy_spread_live()  # FRED BAMLH0A0HYM2 — live, cached daily
    if credit_spread > 4.5:
        full_report += (
            f"\n\n🚨 **SYSTEMIC MACRO OVERRIDE:** High Yield Credit Spreads elevated "
            f"({credit_spread:.2f}% — FRED live). CEFs face elevated NAV decay risk in this regime."
        )
        if TIER_RANK["ELEVATED"] > TIER_RANK.get(worst_tier, 0):
            worst_tier = "ELEVATED"

    return full_report, worst_tier

# ─────────────────────────────────────────────────────────────────────────────
# ALERT DISPATCHER — Discord + Pushover + Personal Email + Work Email
# (original four-channel dispatch, unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def dispatch_cornerstone_alert(title, full_report, color, attach_chart=True):
    """
    Fires the same report to all four channels simultaneously:
      1. Discord #cornerstone webhook
      2. Pushover push notification
      3. Personal email
      4. Work email
    """
    chart_bytes = build_cornerstone_chart() if attach_chart else None

    # 1. Discord
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        if chart_bytes:
            send_essentials_embed_with_chart(WEBHOOK_CORNERSTONE, title, full_report, chart_bytes, color)
        else:
            send_essentials_embed(WEBHOOK_CORNERSTONE, title, full_report, color)

    clean_report = full_report.replace("**", "").replace("`", "")

    # 2. Pushover
    pushover_token = os.getenv("PUSHOVER_API_TOKEN")
    pushover_user  = os.getenv("PUSHOVER_USER_KEY")
    if pushover_token and pushover_user:
        try:
            data  = {"token": pushover_token, "user": pushover_user,
                     "title": title, "message": clean_report, "priority": 0}
            files = {"attachment": ("cornerstone_chart.png", chart_bytes, "image/png")} if chart_bytes else None
            requests.post("https://api.pushover.net/1/messages.json",
                          data=data, files=files, timeout=20)
            logger.info("Pushover notification dispatched.")
        except Exception as e:
            logger.error(f"Pushover dispatch failed: {e}")

    # 3 & 4. Email — personal and work
    sender     = os.getenv("SENDER_EMAIL")
    pwd        = os.getenv("EMAIL_APP_PASSWORD")
    work_email = os.getenv("WORK_EMAIL")
    if sender and pwd:
        try:
            msg            = EmailMessage()
            msg.set_content(clean_report)
            msg['Subject'] = title
            msg['From']    = sender
            msg['To']      = f"{sender}, {work_email}" if work_email else sender
            if chart_bytes:
                msg.add_attachment(chart_bytes, maintype="image", subtype="png",
                                   filename="cornerstone_chart.png")
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(sender, pwd)
                smtp.send_message(msg)
            logger.info("Email dispatched — personal + work.")
        except Exception as e:
            logger.error(f"Email dispatch failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# SEASONAL CAUTION ROUTER
# Routes seasonal warnings to #market-analysis webhook, not #cornerstone.
# Fires once per month entry (not every loop tick).
# ─────────────────────────────────────────────────────────────────────────────

def check_and_dispatch_seasonal_caution():
    """
    If entering March or September, dispatch a one-time seasonal caution report
    to #market-analysis. TQQQ put insurance renewal reminder routed to
    #trade-signals. Rate-limited to once per calendar month.
    """
    now       = datetime.now(pytz.timezone('Pacific/Honolulu'))
    month_key = f"seasonal_caution_fired_{now.year}_{now.month}"
    if db.get_state(month_key, ""):
        return
    if not is_seasonal_caution_month(now):
        return

    month_name = "March" if now.month == 3 else "September"
    caution_msg = (
        f"**⚠️ Seasonal Caution — {month_name} {now.year}**\n"
        f"┣ Month: Historically weak for equities (SPY/QQQ)\n"
        f"┣ Action: Reduce new margin draws by 50%\n"
        f"┣ TQQQ Calls: Reduce position size 50% — wait for 3-day confirmation\n"
        f"┣ TQQQ Puts: Increase insurance size 50%\n"
        f"┣ CLM/CRF: Watch for DCA opportunity on dips (timed DCA month)\n"
        f"┗ Reminder: March/Sept = MLPI, MAIN, TDAQ dividends still flowing — margin paydown continues"
    )

    if WEBHOOK_MARKET and HAS_ESSENTIALS:
        send_essentials_embed(WEBHOOK_MARKET, f"⚠️ Seasonal Caution Active — {month_name}", caution_msg, 0xf39c12)

    # TQQQ put renewal reminder → trade signals channel
    tqqq_msg = (
        f"**🛡️ TQQQ Put Insurance — {month_name} Renewal Reminder**\n"
        f"┣ Seasonal caution month active\n"
        f"┣ Put size: Consider 1.5x normal allocation this month\n"
        f"┣ Strike: 10% OTM from current TQQQ price\n"
        f"┣ DTE: 30 days — roll at 14 DTE\n"
        f"┗ Reminder: 30 DTE puts = homeowners insurance — never skip a month"
    )
    if WEBHOOK_TRADE_SIGNALS and HAS_ESSENTIALS:
        send_essentials_embed(WEBHOOK_TRADE_SIGNALS, f"🛡️ TQQQ Put Renewal — {month_name}", tqqq_msg, 0xe67e22)

    db.update_state(month_key, "fired")
    logger.info(f"Seasonal caution dispatched for {month_name} {now.year}.")

# ─────────────────────────────────────────────────────────────────────────────
# 0800 HST DAILY PULSE (original — gate preserved, ledger sweep preserved)
# ─────────────────────────────────────────────────────────────────────────────

def send_daily_pulse(is_test=False):
    """
    Fires the scheduled 0800 HST morning Cornerstone report.
    Deduplicated via DB date-gate so it never fires twice in one calendar day.
    Sweeps and grades any pending ledger predictions older than 5 trading days.
    """
    tz_h = pytz.timezone('Pacific/Honolulu')
    if not is_test:
        current_date = datetime.now(tz_h).strftime("%Y-%m-%d")
        last_pulse   = db.get_state("last_monitor_pulse_date", "")
        if last_pulse == current_date:
            logger.info("Daily pulse already dispatched today — skipping duplicate.")
            return
        db.update_state("last_monitor_pulse_date", current_date)

    # Ledger sweep — grade predictions that have aged ≥5 trading days
    try:
        from analytics import HighFidelityAnalyticsEngine
        graded = HighFidelityAnalyticsEngine().sweep_and_grade_pending("cornerstone", min_age_days=5)
        if graded:
            logger.info(f"Cornerstone ledger: graded {graded} pending call(s).")
    except Exception as e:
        logger.error(f"Cornerstone ledger sweep failed: {e}")

    full_report, worst_tier = compute_cornerstone_reports()

    # SPY GEX macro context — negative gamma = dealers amplify volatility = elevated CEF premium risk.
    # P/C OI removed from this channel: CLM/CRF have no options market so put/call ratios have no
    # CLM/CRF-specific meaning. P/C is handled by tqqq.py's cycle scorer where it belongs.
    # Note: calculate_gex_profile() returns UNKNOWN at current Twelve Data tier (no real OI data).
    # This block only activates once Tradier OI is wired — safe to leave in place.
    try:
        from analytics import HighFidelityAnalyticsEngine
        gex = HighFidelityAnalyticsEngine().calculate_gex_profile("SPY")
        if gex.get("market_state", "UNKNOWN") != "UNKNOWN":
            flip = gex.get("flip_strike", 0.0)
            gex_total = gex.get("gex_total")
            is_neg = "NEGATIVE" in gex.get("market_state", "")
            gex_note = (
                "dealers amplify moves — volatility risk elevated for CLM/CRF premium" if is_neg else
                "dealers suppress moves — stable CEF premium environment"
            )
            gex_line = (
                f"┣ SPY GEX: {gex['market_state']} | Flip ${flip:,.0f}"
                + (f" | Net {gex_total:+.1f}B" if gex_total is not None else "")
                + f" — {gex_note}\n"
            )
            full_report = gex_line + full_report
    except Exception:
        pass

    title = "☕️ Cornerstone Flowstate" + (" 🧪 TEST" if is_test else "")
    color = 0xe74c3c if worst_tier == "CRITICAL" else (0xf1c40f if worst_tier == "ELEVATED" else 0x2ecc71)
    dispatch_cornerstone_alert(title, full_report, color)
    db.update_state("cornerstone_alert_tier_rank", TIER_RANK.get(worst_tier, 0))

# ─────────────────────────────────────────────────────────────────────────────
# CONTINUOUS ESCALATION LOOP (every 5 min, tier-transition debounced)
# ─────────────────────────────────────────────────────────────────────────────

def check_and_escalate_if_critical():
    """
    Runs every loop tick (5 min). Fires an immediate multi-channel red-siren alert
    the moment any fund crosses into ELEVATED or CRITICAL — capital protection cannot
    wait for the 0800 gate. Debounced on tier transitions: a sustained critical state
    does not re-spam; only worsening (ELEVATED → CRITICAL) re-fires.
    3-notification rule enforced via can_broadcast().
    """
    full_report, worst_tier = compute_cornerstone_reports()
    current_rank = TIER_RANK.get(worst_tier, 0)
    prev_rank    = int(db.get_state("cornerstone_alert_tier_rank", 0))

    if current_rank > prev_rank and current_rank > 0:
        if can_broadcast("cornerstone", is_major=True):
            logger.warning(f"🚨 Escalation: {worst_tier} (was rank {prev_rank}) — firing immediate alert.")
            title = "🚨🚨 CORNERSTONE — IMMEDIATE ACTION REQUIRED 🚨🚨"
            dispatch_cornerstone_alert(title, full_report, 0xe74c3c)
            increment_alert_count("cornerstone")

    db.update_state("cornerstone_alert_tier_rank", current_rank)
    return full_report, worst_tier

# ─────────────────────────────────────────────────────────────────────────────
# MAIN MONITOR LOOP
# ─────────────────────────────────────────────────────────────────────────────

def _make_ws_callback():
    """
    Returns a WebSocket callback that fires an immediate escalation check when CLM,
    CRF, or VIXY price moves. The check runs in a thread so the WS receive loop is
    never blocked by the REST calls inside compute_cornerstone_reports().

    Debounce: at most one WS-triggered escalation per 60 seconds to prevent
    callback storms during volatile intraday sessions.
    """
    _last_ws_check = [0.0]  # mutable container for closure state

    def _ws_price_callback(symbol: str, price: float, event: dict):
        if symbol not in ("CLM", "CRF", "VIXY"):
            return
        now = time.monotonic()
        if now - _last_ws_check[0] < 300.0:
            return
        _last_ws_check[0] = now
        logger.info(f"[WS] {symbol} price update ${price:.4f} — triggering immediate escalation check")
        t = threading.Thread(target=_ws_escalation_check, daemon=True)
        t.start()

    return _ws_price_callback


def _ws_escalation_check():
    """Runs check_and_escalate_if_critical() from the WebSocket callback thread."""
    try:
        check_and_escalate_if_critical()
    except Exception as e:
        logger.error(f"[WS Escalation] Error: {e}")


def run_monitor():
    import threading
    tz_h = pytz.timezone('Pacific/Honolulu')

    # CLI test/force mode — fires once and exits
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        send_daily_pulse(is_test=True)
        return

    logger.info("⏳ [Engine Loop] Cornerstone monitor active. DB state tracking enabled.")
    # WS removed: the callback had a 300s debounce — identical to the REST polling
    # interval. Multiple monitor.py process restarts were each opening a new SDK
    # WebSocket connection (module-level singleton doesn't persist across processes),
    # creating N concurrent connection storms that hammered TD and burned CPU.
    # REST polling every 5 min is the protection engine — WS adds no unique value here.

    while True:
        now_utc_h = datetime.now(timezone.utc).hour
        rth = 13 <= now_utc_h < 21   # Regular Trading Hours (13:00–21:00 UTC)

        try:
            if rth:
                # ── Full scan during market hours
                # Price/NAV/RSI fetches, WS-triggered escalation, seasonal check
                check_and_escalate_if_critical()
                check_and_dispatch_seasonal_caution()
            else:
                # ── Off-hours: SEC/EDGAR only — N-2 and SC 13D/G filings drop 24/7
                # Skip the expensive Twelve Data REST calls (no prices to act on)
                try:
                    _sec_session = requests.Session()
                    for _ticker in ("CLM", "CRF"):
                        result = check_sec_edgar(_sec_session, _ticker)
                        if result and any(sig in result for sig in ("N-2", "13D", "13G")):
                            logger.warning(f"[Off-hours SEC] {_ticker}: {result}")
                            dispatch_cornerstone_alert(
                                f"⚠️ Off-hours EDGAR filing detected — {_ticker}",
                                f"┣ Ticker: {_ticker}\n┗ Signal: {result}",
                                color=0xe74c3c,
                                attach_chart=False,
                            )
                except Exception as e:
                    logger.warning(f"[Off-hours SEC] check_sec_edgar error: {e}")

            # ── 0800 HST daily pulse gate (always active — fires once per calendar day)
            now          = datetime.now(tz_h)
            current_date = now.strftime("%Y-%m-%d")
            last_pulse   = db.get_state("last_monitor_pulse_date", "")

            if now.hour >= 8 and last_pulse != current_date:
                logger.info("Triggering 0800 HST daily pulse...")
                send_daily_pulse()
                db.update_state("last_monitor_pulse_date", current_date)

        except Exception as e:
            logger.critical(f"FATAL LOOP EXCEPTION: {e}")

        # RTH: 300s (matches WS callback debounce — no point checking faster than WS fires)
        # Off-hours: 900s (SEC filing check every 15 min — EDGAR accepts filings 24/7)
        sleep_secs = 300 if rth else 900
        time.sleep(sleep_secs)

if __name__ == "__main__":
    run_monitor()
