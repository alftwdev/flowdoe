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

# Suppress dotenv parser warnings for blank/comment lines in .env (line 40 spacer)
logging.getLogger("dotenv.main").setLevel(logging.ERROR)

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
    "CLM": {"nav_ticker": "XCLMX", "default_nav": 6.73},   # updated Aug 16 2026 — NAV per N-2 EDGAR filing Aug 14
    "CRF": {"nav_ticker": "XCRFX", "default_nav": 6.18}    # updated Jul 23 2026; actual NAV ~$6.18
}

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS & THRESHOLDS
# ─────────────────────────────────────────────────────────────────────────────

# Ex-dividend heuristic: CLM/CRF ex-div falls mid-month (historically days 15–19).
# A price dip in this window is a scheduled cash-payout event, not dilution/RO risk.
EX_DIV_WINDOW_DAYS = range(12, 17)   # CLM/CRF historically go ex-div on days 12–16 (was 15–19, missed the 14th)

# RO Filing Season: historically N-2 filings cluster mid-Feb through mid-Apr.
# Real filing history verified against SEC CIKs across 2016-2025.
RO_FILING_SEASON = (2, 15, 4, 15)  # (start_month, start_day, end_month, end_day)

# VIXY Z-score threshold for crisis-amplification overlay.
CRISIS_VIXY_Z_THRESHOLD = 1.5

# Seasonal weakness months — March and September historically produce the largest
# drawdowns across QQQ/SPY. Caution flag raised during these months for TQQQ sniper
# routing and general risk posture.
SEASONAL_CAUTION_MONTHS = [3, 9]

# NAV Determination Month — Cornerstone Board locks 2026 distribution rate at Oct 31 NAV.
# During October: raise alert sensitivity, tighten premium compression threshold to -1.5%.
NAV_DETERMINATION_MONTH = 10  # October

# CEF Institutional Exit — high lit-market volume + SPY flat = not a macro event.
# Distinct from dark pool (dark pool = LOW lit volume). This is HIGH lit volume while
# SPY doesn't explain the move — the Feb 2026 crash pattern.
CEF_INST_EXIT_VOL_RATIO_MIN = 2.0   # vol > 2× 20D avg
CEF_INST_EXIT_SPY_MAX_CHG   = 0.75  # abs(SPY 1d chg) must be < 0.75% to qualify as CEF-specific

# Distribution yield floor — price below which income buyers absorb all selling.
# At 19% yield (Cornerstone's target payout band), any lower price = structural support.
# Fair value = annual_distribution / 0.19. If current price > fair value + 10% = overvalued signal.
DIST_YIELD_TARGET_PCT = 19.0        # 19% = structural support floor yield
DIST_YIELD_OVERVALUED_GAP = 0.10    # price > fair_value × 1.10 = overvalued at new rate

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
margin_rate             = 7.25  # E*TRADE benchmark margin rate (%)

# Tier 2 active positions as of Jul 2026: MLPI (~15%) + MAIN (~8%) only.
# TDAQ and KQQQ are in CLAUDE.md as future candidates, not currently held.
TIER2_ACTIVE_BLENDED    = 11.5  # blended yield of active Tier 2 positions

# RO composite score weights — N-2 SEC filing is the single highest-conviction signal.
# EDGAR sources stack: multiple filings in the same cycle = multi-source conviction.
RO_SCORE_WEIGHTS = {
    # EDGAR filing signals (stacking — each detected form adds independently)
    "sec_n2":              60,   # N-2 registration — RO confirmed, act immediately
    "sec_n2a":             50,   # N-2/A amendment — final RO terms/pricing
    "sec_ncsr":             8,   # N-CSR semi-annual — distribution sustainability language
    "sec_def14a":           8,   # DEF 14A proxy — board vote on distribution policy
    "sec_n14":             20,   # N-14 merger/acquisition — fund structure at risk
    "sec_corresp":          0,   # SEC comment letter — informational only, no score
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
    # Cross-script signals (lightweight, cached-data reads — no new API calls)
    "yield_steepen":       5,   # T10-T2 spread steepened > 20bps in a session (rate pressure on CEF)
    "long_rate_pressure":  8,   # 30-yr Treasury ≥ 5.0% — income buyer rotation risk, CEF premium headwind
    "hy_rapid_widen":      8,   # HY spread widens > 40bps in 5 trading days — credit deterioration signal
    "sentiment_fear":      5,   # SentiSense market mood ≤ 25 (extreme fear = CEF premium risk)
    # Distribution reset cycle signals
    "nav_determination":  12,   # October = NAV lock month; heightened sensitivity window
    "cef_inst_exit":      20,   # High vol + flat SPY = institutional distribution cycle exit
    "dist_overvalued":    10,   # Price > fair-value floor by >10% at new annual distribution rate
    # Pre-N-2 early warning signals (elevation duration — community-validated)
    "premium_streak":     10,   # 10+ consecutive days above 20% premium — board has motive
    # Suppressors
    "ex_div_relief":      -10,
}

# EDGAR forms watched and their conviction weights.
# Multiple forms detected simultaneously = conviction stacking.
EDGAR_FORMS_TO_WATCH = {
    "N-2":     "sec_n2",          # RO registration
    "N-2/A":   "sec_n2a",         # RO amendment — final terms
    "SC 13D":  "13f_holder_exit",
    "SC 13G":  "13f_holder_exit",
    "N-CSR":   "sec_ncsr",        # Semi-annual — distribution language
    "DEF 14A": "sec_def14a",      # Proxy — board distribution vote
    "N-14":    "sec_n14",         # Merger/acquisition registration — fund structure change
    "CORRESP": "sec_corresp",     # SEC comment letter — regulatory scrutiny on active filing
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
# CEFConnect — OFFICIAL NAV (replaces XCLMX/XCRFX proxy for premium display)
# CEFConnect publishes the fund manager's official end-of-day NAV via their
# public API. This is the same value shown on the Cornerstone website and is
# the authoritative figure for premium/discount calculations.
# 0 Twelve Data credits (external HTTP call). Cached daily — NAV doesn't change
# intraday. Falls back to the last cached value then to XCLMX/XCRFX proxy on failure.
# ─────────────────────────────────────────────────────────────────────────────

CEFCONNECT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.cefconnect.com/",
}

def fetch_nav_cefconnect(session, ticker: str) -> tuple:
    """
    Fetches the most recently confirmed NAV from CEFConnect's pricing history API.
    CEFConnect uses XCLMX/XCRFX as its underlying NAV source — the distinction
    from Twelve Data's live XCLMX fetch is that CEFConnect publishes the official
    end-of-day confirmed figure rather than an intraday snapshot that can drift.

    Endpoint: /api/v3/pricinghistory/{ticker}/5D
    Returns the most recent row from PriceHistory:
      NAVData — official confirmed NAV (what Cornerstone website shows)
      DiscountData — premium/discount at that NAV date
      Data — market price at that date (NOT used for premium — we use live price)

    Cached once per calendar day (0 Twelve Data credits, no rate impact).
    Returns (nav: float, confirmed_premium_at_nav_date: float | None, source: str).
    """
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cache_key = f"cefconnect_nav_{ticker}_{today_str}"
    cached    = db.get_state(cache_key)
    if isinstance(cached, dict) and cached.get("nav", 0) > 0:
        return float(cached["nav"]), cached.get("confirmed_premium"), "CEFConnect (cached)"

    url = f"https://www.cefconnect.com/api/v3/pricinghistory/{ticker}/5D"
    try:
        r = session.get(url, headers=CEFCONNECT_HEADERS, timeout=12)
        if r.status_code != 200:
            raise ValueError(f"HTTP {r.status_code}")
        rows = r.json().get("Data", {}).get("PriceHistory", [])
        if not rows:
            raise ValueError("empty PriceHistory")

        # Most recent confirmed row (last entry in the list)
        latest = rows[-1]
        nav    = float(latest.get("NAVData") or 0)
        prem   = latest.get("DiscountData")
        prem   = float(prem) if prem is not None else None
        nav_date = latest.get("DataDateDisplay", "")

        if nav > 0:
            payload = {"nav": nav, "confirmed_premium": prem, "nav_date": nav_date, "date": today_str}
            db.update_state(cache_key, payload)
            logger.info(f"[NAV] CEFConnect {ticker}: NAV=${nav:.4f} (as of {nav_date})" +
                        (f" | confirmed premium={prem:.2f}%" if prem is not None else ""))
            return nav, prem, "CEFConnect"

    except Exception as e:
        logger.warning(f"[NAV] CEFConnect unavailable for {ticker}: {e} — falling back to proxy")

    return 0.0, None, "proxy"


# ─────────────────────────────────────────────────────────────────────────────
# BOX SPREAD POSITION READER (DB-only, zero API calls)
# Box positions are written by scheduler.py --mode box_position --action open.
# monitor.py reads them to surface DTE countdowns, balloon warnings, and RO
# dodge context inside the cornerstone pulse. CLM/CRF content never leaves
# #cornerstone; the box efficiency snippet for #dividend-ccetfs is income-only.
# ─────────────────────────────────────────────────────────────────────────────

def read_active_box_positions() -> list:
    """
    Returns list of open box spread position dicts from DB.
    Each dict includes 'dte_current' computed from today's date.
    Returns [] if no positions or DB read fails.
    """
    try:
        count = int(db.get_state("box_pos_count") or 0)
        today = datetime.utcnow().date()
        positions = []
        for i in range(1, count + 1):
            bp = db.get_state(f"box_pos_{i}")
            if not isinstance(bp, dict) or bp.get("status") != "OPEN":
                continue
            try:
                exp_dt = datetime.strptime(bp["expiration"], "%Y-%m-%d").date()
                bp["dte_current"] = (exp_dt - today).days
                positions.append(bp)
            except Exception:
                pass
        return positions
    except Exception as e:
        logger.warning(f"[Box Positions] DB read failed: {e}")
        return []


def _format_box_pulse_lines(positions: list) -> str:
    """
    Formats active box spread positions as embed lines for the cornerstone daily pulse.
    Shows rate vs margin savings, DTE countdown, balloon warning at ≤60 DTE, and
    a RO dodge reminder if ro_dodge_active flags are set for CLM or CRF.
    Returns empty string when no boxes are open.
    """
    if not positions:
        return ""
    lines = ["┣ 📦 **Box Spread Borrowing:**"]
    for bp in positions:
        dte       = bp.get("dte_current", 0)
        rate      = bp.get("implied_rate_pct", 0.0)
        k1        = int(bp.get("k1", 0))
        k2        = int(bp.get("k2", 0))
        exp       = bp.get("expiration", "?")
        contracts = int(bp.get("contracts", 1))
        width     = int(bp.get("width", 100))
        balloon   = width * contracts * 100
        loan      = bp.get("loan_amount", balloon)
        savings   = round(margin_rate - rate, 2)
        ann_int   = bp.get("annual_interest_usd", round(loan * rate / 100, 2))

        roll_tag = " 🚨 ROLL NOW" if dte <= 30 else (" ⚠️ ROLL SOON" if dte <= 60 else "")
        lines.append(
            f"┣   K{k1}/{k2} exp {exp} | "
            f"Rate `{rate:.2f}%` vs margin `{margin_rate:.2f}%` (saves `{savings:.2f}%`) | "
            f"DTE `{dte}d`{roll_tag}"
        )
        if dte <= 60:
            lines.append(
                f"┣   ⚠️ Balloon `${balloon:,}` due {exp} — "
                f"run box_spread_scan for roll rate. Interest cost: `${ann_int:.0f}/yr`."
            )

    # RO dodge reminder: if we sold CLM/CRF and are waiting for re-entry,
    # the balloon is still owed. Sale proceeds should reduce E*TRADE margin
    # temporarily — NOT assumed to cover the balloon.
    dodging = [t for t in ("CLM", "CRF") if db.get_state(f"ro_dodge_active_{t}")]
    if dodging:
        lines.append(
            f"┣   🔴 RO DODGE ACTIVE ({'/'.join(dodging)}) — "
            f"box balloon(s) owed at expiry regardless of share sale. "
            f"Deploy proceeds → E*TRADE margin paydown until re-entry signal fires."
        )
    return "\n".join(lines) + "\n"


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
            elif form == "N-14" and "N-14" not in seen_forms:
                if age <= N2_RECENCY_DAYS:       # same 90-day window as N-2 registrations
                    flags.append(f"🚨 N-14 MERGER/ACQUISITION REGISTRATION ({date}) — fund structure change")
                    seen_forms.add("N-14")
            elif form == "CORRESP" and "CORRESP" not in seen_forms:
                if age <= HOLDER_RECENCY_DAYS:   # 180-day window — SEC review cycles run long
                    flags.append(f"📋 CORRESP SEC COMMENT LETTER ({date}) — regulatory scrutiny on active filing")
                    seen_forms.add("CORRESP")

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

def _td_sleep_rate_limit(attempt: int, response: dict) -> bool:
    """
    Returns True and sleeps to the next minute boundary if the TD response is a 429.
    Returns False for all other errors so normal backoff applies.
    The 429 means "wait for the next minute" — retrying in 2-5s just burns retries
    inside the same rate-limit window and guarantees failure.
    Only sleeps on attempts 0 and 1 (leaves attempt 2 as the final hard fail).
    """
    if response.get('code') != 429:
        return False
    if attempt >= 2:
        return False  # last attempt — let caller log the failure
    secs_remaining = 62 - datetime.now().second  # +2s buffer past the minute flip
    logger.warning(
        f"[Rate Limit] 429 on attempt {attempt + 1} — sleeping {secs_remaining}s "
        f"to clear the TD minute window ({response.get('message', '')[:80]})"
    )
    # Set loop-level cooldown so the NEXT loop tick also skips TD calls.
    # This prevents a second batch of requests firing immediately after the sleep
    # if market_analysis.py is still consuming credits on the same minute window.
    try:
        db.update_state("td_cooldown_until", round(time.time() + secs_remaining + 5, 1))
    except Exception:
        pass
    time.sleep(secs_remaining)
    return True


def fetch_live_metrics(session, symbol, retries=3):
    """
    Three attempts with 429-aware backoff before giving up.
    On a rate-limit response the sleep advances to the next minute boundary
    (instead of retrying in 2-5s inside the same window and failing again).
    On transient errors the original 2s/5s backoff still applies.
    Timeout raised to 20s to handle slower responses during high-load windows.
    """
    last_err = None
    backoff  = [0, 2, 5]
    for attempt in range(retries):
        try:
            if backoff[attempt]:
                time.sleep(backoff[attempt])
            p_res = session.get(
                f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}",
                timeout=20).json()
            if _td_sleep_rate_limit(attempt, p_res):
                continue
            price = float(p_res.get('price', 0.0))
            if price == 0.0:
                raise ValueError(f"price came back 0.0: {p_res}")

            rsi   = 50.0
            r_res = session.get(
                f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day"
                f"&time_period=14&apikey={TD_API_KEY}", timeout=20).json()
            if not _td_sleep_rate_limit(attempt, r_res):
                rsi = float(r_res.get('values', [{'rsi': 50.0}])[0]['rsi'])

            nav_ticker = PRIORITY_ASSETS[symbol]["nav_ticker"]
            nav_res    = session.get(
                f"https://api.twelvedata.com/price?symbol={nav_ticker}&apikey={TD_API_KEY}",
                timeout=20).json()
            nav = float(nav_res.get('price', PRIORITY_ASSETS[symbol]["default_nav"]))

            return price, rsi, nav
        except Exception as e:
            last_err = e
    logger.error(f"[Data Fetch Error] {symbol} failed after {retries} attempts: {last_err}")
    return 0.0, 50.0, PRIORITY_ASSETS[symbol]["default_nav"]


def fetch_time_series(session, symbol, outputsize=21):
    """
    Returns list of daily close dicts from Twelve Data, newest first.
    Retries up to 3 times: 429s wait for the next minute boundary;
    timeouts and transient errors use 5s/15s progressive backoff.
    Returns [] on all failures (caller must handle empty gracefully).
    """
    _backoff = [0, 5, 15]
    for _attempt in range(3):
        try:
            if _backoff[_attempt]:
                time.sleep(_backoff[_attempt])
            res = session.get(
                "https://api.twelvedata.com/time_series",
                params={"symbol": symbol, "interval": "1day",
                        "outputsize": outputsize, "apikey": TD_API_KEY},
                timeout=25).json()
            if res.get('code') == 429:
                secs = 62 - datetime.now().second
                logger.warning(f"[Rate Limit] time_series {symbol} attempt {_attempt+1} — sleeping {secs}s")
                time.sleep(secs)
                continue  # retry after minute boundary
            vals = res.get("values", [])
            if vals:
                return vals
            # Empty values on non-429 = market closed or bad symbol — don't retry
            logger.warning(f"[Time Series] {symbol}: empty values (market closed or bad symbol)")
            return []
        except Exception as e:
            logger.warning(f"[Time Series] {symbol} attempt {_attempt+1} failed: {e}")
            if _attempt == 2:
                logger.error(f"[Time Series Fetch Error] {symbol}: gave up after 3 attempts — {e}")
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

def is_nav_determination_month(today=None) -> bool:
    """
    October = NAV lock month. Cornerstone Board votes on next year's distribution
    rate using the Oct 31 NAV. During this month, tighten all alert thresholds —
    any premium compression or vol divergence carries higher weight.
    """
    today = today or datetime.now(pytz.timezone('Pacific/Honolulu'))
    return today.month == NAV_DETERMINATION_MONTH

def detect_cef_institutional_exit(session, ticker: str, spy_chg: float) -> tuple:
    """
    Identifies the Feb 2026 crash pattern: HIGH lit-market volume + SPY flat/green.
    This is NOT a dark pool signal (that requires low public vol).
    This is the opposite: institutions exiting publicly, not hiding the trade,
    because the selling pressure overwhelms absorption capacity.

    Signature: vol > 2× 20D avg AND abs(SPY 1D change) < 0.75%.
    Returns (is_inst_exit, vol_ratio, price_chg, description).
    Uses data already fetched by detect_dark_pool_activity — no extra API calls.
    """
    try:
        values = fetch_time_series(session, ticker, outputsize=21)
        if len(values) < 11:
            return False, 0.0, 0.0, "Insufficient data"

        today_vol    = float(values[0]["volume"])
        baseline_vol = sum(float(v["volume"]) for v in values[1:21]) / max(len(values[1:21]), 1)
        vol_ratio    = today_vol / baseline_vol if baseline_vol > 0 else 1.0
        price_chg    = (float(values[0]["close"]) - float(values[1]["close"])) / float(values[1]["close"]) * 100

        # Institutional exit: high volume + price falling + SPY not explaining the move
        is_inst_exit = (
            vol_ratio  >= CEF_INST_EXIT_VOL_RATIO_MIN and
            price_chg  <= -1.0 and                        # CEF is down at least 1%
            abs(spy_chg) < CEF_INST_EXIT_SPY_MAX_CHG      # SPY is not the cause
        )
        desc = (
            f"{'🔴 ' if is_inst_exit else ''}{vol_ratio:.1f}x avg vol / "
            f"{price_chg:+.1f}% vs SPY {spy_chg:+.1f}% — "
            f"{'INST. EXIT: HIGH VOL, SPY FLAT' if is_inst_exit else 'normal'}"
        )
        return is_inst_exit, vol_ratio, price_chg, desc
    except Exception as e:
        logger.error(f"[CEF Inst Exit Detector Error] {ticker}: {e}")
        return False, 0.0, 0.0, "Error"

def check_distribution_yield_floor(price: float, ticker: str) -> tuple:
    """
    Computes the fair-value floor using the known annual distribution and the
    19% yield target (Cornerstone's managed distribution band).

    Fair value = annual_distribution / 0.19
    If current price > fair_value × 1.10 → price is overvalued at the new rate
    and a correction toward fair value is structurally likely.

    Also computes the current implied yield at the current price — this is the
    number income buyers use to decide whether CLM/CRF is cheap or expensive.

    Returns (is_overvalued, fair_value, implied_yield_pct, description).
    Zero API calls — uses known distribution constants from get_ticker_report().
    """
    try:
        annual_div = 1.4268 if ticker == "CLM" else 1.3824   # 2026 reset: $0.1189 × 12 CLM, $0.1152 × 12 CRF
        fair_value = round(annual_div / (DIST_YIELD_TARGET_PCT / 100), 2)
        implied_yield = (annual_div / price * 100) if price > 0 else 0.0

        is_overvalued = price > fair_value * (1 + DIST_YIELD_OVERVALUED_GAP)
        gap_pct = ((price - fair_value) / fair_value * 100) if fair_value > 0 else 0.0

        if is_overvalued:
            desc = (
                f"⚠️ Price ${price:.2f} is {gap_pct:+.1f}% above fair value "
                f"${fair_value:.2f} at {DIST_YIELD_TARGET_PCT:.0f}% yield — "
                f"distribution reset mispricing, correction likely"
            )
        elif implied_yield >= DIST_YIELD_TARGET_PCT:
            desc = f"✅ Yield {implied_yield:.1f}% ≥ {DIST_YIELD_TARGET_PCT:.0f}% target — price at or below fair value (accumulate zone)"
        else:
            desc = f"Yield {implied_yield:.1f}% | Fair value ${fair_value:.2f} | Gap {gap_pct:+.1f}%"

        return is_overvalued, fair_value, round(implied_yield, 1), desc
    except Exception as e:
        logger.error(f"[Dist Yield Floor Error] {ticker}: {e}")
        return False, 0.0, 0.0, "Error"

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

def detect_dark_pool_activity(session, symbol, monthly_dist: float = 0.0):
    """
    Dark pool signature: meaningful price decline + public volume well below 20D average.
    When institutions sell in size through dark pools, the lit exchange shows thin volume
    while price still falls — the opposite of a normal retail selloff.

    Ex-div suppression: if today is in the ex-div window (days 12–16) AND the absolute
    price drop is within 2× the monthly distribution, the drop is a scheduled ex-div dip —
    not dark pool activity. Signal is suppressed and labeled clearly.

    Returns (is_dark_pool, price_chg_pct, vol_ratio, description).
    """
    try:
        values = fetch_time_series(session, symbol, outputsize=21)
        if len(values) < 11:
            return False, 0.0, 1.0, "Insufficient data"

        today_vol    = float(values[0]["volume"])
        baseline_vol = sum(float(v["volume"]) for v in values[1:21]) / max(len(values[1:21]), 1)
        vol_ratio    = today_vol / baseline_vol if baseline_vol > 0 else 1.0
        prev_close   = float(values[1]["close"])
        today_close  = float(values[0]["close"])
        price_chg    = (today_close - prev_close) / prev_close * 100
        abs_drop     = prev_close - today_close   # positive when price fell

        # Ex-div gate: suppress dark pool flag when drop matches the scheduled distribution
        in_exdiv_window = is_near_ex_dividend_window()
        exdiv_drop_max  = monthly_dist * 2.0     # allow up to 2× dist for market noise on ex-div day
        is_exdiv_drop   = (
            in_exdiv_window
            and monthly_dist > 0
            and abs_drop <= exdiv_drop_max
            and price_chg <= 0   # must be a drop, not a gain
        )

        is_dark_pool = (
            not is_exdiv_drop
            and price_chg  <= DARK_POOL_PRICE_DROP_PCT
            and vol_ratio  <= DARK_POOL_VOLUME_RATIO_MAX
        )

        if is_exdiv_drop:
            desc = (
                f"EX-DIV DIP (scheduled) {price_chg:+.1f}% / {vol_ratio:.2f}x vol — "
                f"drop ${abs_drop:.4f} ≈ ${monthly_dist:.4f} dist. Not dark pool."
            )
        else:
            desc = (
                f"{'🕵️ ' if is_dark_pool else ''}{price_chg:+.1f}% / {vol_ratio:.2f}x vol — "
                f"{'OFF-EXCHANGE EXIT SIGNAL' if is_dark_pool else 'CLEAR'}"
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
        _today_reentry = datetime.now().strftime("%Y-%m-%d")
        db.update_state(fired_key, _today_reentry)
        db.update_state(f"ro_dodge_active_{ticker}", "")  # clear the dodge flag — re-entry confirmed
        db.update_state(f"ro_reentry_signal_{ticker}", _today_reentry)
        # Record RO completion date for interval tracking (pre-N-2 streak context)
        db.update_state(f"cornerstone_last_ro_completed_{ticker}", _today_reentry)

        # Journal the re-entry signal
        try:
            _dodge_exec = db.get_state(f"ro_dodge_executed_{ticker}") or {}
            _sell_px = _dodge_exec.get("sell_price", 0.0) if isinstance(_dodge_exec, dict) else 0.0
            _pnl_note = f"Sold at ${_sell_px:.2f}, rebuy at ${current_price:.2f}" if _sell_px > 0 else "Sell price not logged"
            db.log_journal_entry(
                strategy="CLM_CRF",
                event_type="RO_REENTRY_SIGNAL",
                ticker=ticker,
                action="REBUY",
                conviction=5,
                thesis=(
                    f"{ticker} post-RO re-entry: Path A (premium collapse + price off high). "
                    f"Premium {current_premium:.1f}% (<10%), price ${current_price:.2f} "
                    f"({pct_below_high:.1f}% below 60D high of ${high_60d:.2f}). "
                    f"{_pnl_note}. Resume DRIP at NAV."
                ),
                confluences={"current_price": current_price, "current_premium": current_premium,
                             "pct_below_60d_high": round(pct_below_high, 2),
                             "high_60d": high_60d, "path": "A_premium_collapse"},
                conflicts={},
                entry_price=current_price,
            )
        except Exception:
            pass

        # Box spread context — include in the re-entry embed so the operator knows
        # to redeploy the margin that was freed during the dodge.
        _box_positions = read_active_box_positions()
        _box_lines = ""
        if _box_positions:
            _balloon_info = " | ".join([
                f"K{int(bp.get('k1', 0))}/{int(bp.get('k2', 0))} exp {bp.get('expiration', '?')} "
                f"(DTE:{bp.get('dte_current', '?')})"
                for bp in _box_positions
            ])
            _box_lines = (
                f"┣ 📦 Box Spread(s) Active: {_balloon_info}\n"
                f"┣   Boxes roll on their own schedule — redeploy freed margin into this rebuy\n"
            )

        dip_msg = (
            f"**{ticker} — 🟢 POST-RO DIP: REBUY ZONE**\n"
            f"┣ Price: `${current_price:.2f}` ({pct_below_high:.1f}% below 60D high)\n"
            f"┣ Premium to NAV: `{current_premium:.2f}%` (was >20% during RO)\n"
            f"┣ RO Cycle: N-2 was previously detected — price has repriced toward NAV\n"
            f"┣ Signal: Premium collapse + price off high = classic post-RO dip pattern\n"
            f"{_box_lines}"
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


def check_yield_floor_reentry(ticker: str, current_price: float, current_premium: float) -> bool:
    """
    Second re-entry path: fires when price falls to or below the distribution yield floor
    (fair value = annual_dist / 0.19) AND an N-2 cycle was detected ≥45 days ago.
    This catches the bottom even when the 60D-high condition hasn't triggered yet —
    the structural income buyer floor creates a natural support ceiling from below.
    Fires once per RO cycle. Complements detect_ro_completion_dip, not a duplicate.
    """
    try:
        n2_key    = f"cornerstone_n2_detected_{ticker}"
        fired_key = f"cornerstone_floor_reentry_fired_{ticker}"
        prev_n2   = db.get_state(n2_key, "")
        already   = db.get_state(fired_key, "")
        if not prev_n2 or already:
            return False

        # Require at least 45 days since N-2 detection (give RO time to complete)
        try:
            n2_date = datetime.strptime(prev_n2, "%Y-%m-%d").date()
            age_days = (datetime.utcnow().date() - n2_date).days
        except Exception:
            age_days = 0
        if age_days < 45:
            return False

        # Fair value floor: annual_dist / 0.19
        annual_div = 1.4268 if ticker == "CLM" else 1.3824
        fair_value = round(annual_div / 0.19, 2)
        if current_price > fair_value:
            return False

        # All conditions met
        _today_floor = datetime.now().strftime("%Y-%m-%d")
        db.update_state(fired_key, _today_floor)
        db.update_state(f"ro_dodge_active_{ticker}", "")  # clear dodge flag
        db.update_state(f"ro_reentry_signal_{ticker}", _today_floor)
        # Record RO completion date for interval tracking (pre-N-2 streak context)
        db.update_state(f"cornerstone_last_ro_completed_{ticker}", _today_floor)

        # Journal the re-entry signal
        try:
            _dodge_exec = db.get_state(f"ro_dodge_executed_{ticker}") or {}
            _sell_px = _dodge_exec.get("sell_price", 0.0) if isinstance(_dodge_exec, dict) else 0.0
            _pnl_note = f"Sold at ${_sell_px:.2f}, rebuy at ${current_price:.2f}" if _sell_px > 0 else "Sell price not logged"
            db.log_journal_entry(
                strategy="CLM_CRF",
                event_type="RO_REENTRY_SIGNAL",
                ticker=ticker,
                action="REBUY",
                conviction=5,
                thesis=(
                    f"{ticker} post-RO re-entry: Path B (yield floor). "
                    f"Price ${current_price:.2f} <= FV ${fair_value:.2f} "
                    f"({age_days}d since N-2, ≥45 required). "
                    f"Implied yield {round(annual_div / current_price * 100, 1) if current_price > 0 else 0:.1f}%. "
                    f"{_pnl_note}. Resume DRIP at NAV."
                ),
                confluences={"current_price": current_price, "fair_value": fair_value,
                             "days_since_n2": age_days, "path": "B_yield_floor"},
                conflicts={},
                entry_price=current_price,
            )
        except Exception:
            pass

        _box_positions = read_active_box_positions()
        _box_lines = ""
        if _box_positions:
            _box_lines = (
                f"┣ 📦 Box Spread(s) Active — redeploy freed margin into this accumulation zone\n"
            )

        floor_msg = (
            f"**{ticker} — 🟢 YIELD FLOOR RE-ENTRY: Accumulation Zone**\n"
            f"┣ Price: `${current_price:.2f}` at or below fair value `${fair_value}`\n"
            f"┣ Implied Yield: `{(annual_div / current_price * 100):.1f}%` ≥ 19% structural floor\n"
            f"┣ Premium to NAV: `{current_premium:.2f}%` (post-RO repricing)\n"
            f"┣ N-2 Cycle: {age_days}d elapsed — RO likely completed, income buyers absorbing\n"
            f"{_box_lines}"
            f"┣ ⚠️ Verify RO is complete before adding position\n"
            f"┣ Action: Rebuy below fair value = structural alpha — resume DRIP at NAV\n"
            f"┗ Note: This is the accumulation zone the Feb 2026 pattern eventually reached"
        )
        if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
            send_essentials_embed(
                WEBHOOK_CORNERSTONE,
                f"🟢 {ticker} — Yield Floor Accumulation Zone",
                floor_msg, 0x27ae60
            )
        logger.info(f"[Yield Floor Re-entry] {ticker} at ${current_price:.2f} ≤ FV ${fair_value} — alert dispatched.")
        return True

    except Exception as e:
        logger.error(f"[Yield Floor Re-entry Error] {ticker}: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# POST-RO RE-ENTRY SCORER
# Runs only when ro_dodge_active_{ticker} is set (sell-off signal already fired).
# Counts confluence toward re-entry on a 0-100 weighted score.
# Gate: >= 60/100 fires the re-entry alert to #cornerstone + Pushover.
# Score is written to DB every tick so the daily pulse can show progress.
# ─────────────────────────────────────────────────────────────────────────────

def calculate_reentry_score(
    ticker: str,
    price: float,
    nav: float,
    premium: float,
    z_premium: float,
    rvol: float,
    vixy_z: float,
    credit_spread: float,
    spy_above_sma200: bool,
) -> dict:
    """
    Post-RO re-entry confluence scorer. Returns score dict with breakdown.
    Only meaningful when ro_dodge_active_{ticker} is set in DB.
    """
    annual_div = 1.4268 if ticker == "CLM" else 1.3824
    nav_fallback = 6.73 if ticker == "CLM" else 6.18  # CLM updated Aug 16 2026 per N-2 EDGAR filing
    _nav = nav if nav > 0 else nav_fallback

    fair_value = round(annual_div / 0.19, 2)   # FV at 19% yield target
    # Re-entry zone: NAV (bottom of post-RO dip) to NAV + 1.5% (max DRIP efficiency)
    zone_low  = round(_nav * 0.99, 2)
    zone_high = round(_nav * 1.015, 2)

    score = 0
    breakdown = []

    # Price at or below fair value floor (+30) — structural income buyer support
    if price <= fair_value:
        score += 30
        breakdown.append(f"Price {price:.2f} <= FV {fair_value:.2f} (+30)")
    else:
        gap_pct = round((price - fair_value) / fair_value * 100, 1)
        breakdown.append(f"Price {price:.2f} vs FV {fair_value:.2f} — {gap_pct}% above floor (0)")

    # Premium < 10% — dilution fully priced in (+20)
    if premium < 10.0:
        score += 20
        breakdown.append(f"Premium {premium:.1f}% < 10% (+20)")
    elif premium < 15.0:
        score += 8
        breakdown.append(f"Premium {premium:.1f}% — partial compression (+8)")
    else:
        breakdown.append(f"Premium {premium:.1f}% — still elevated (0)")

    # Premium z-score < 0 — below historical average (+15)
    if z_premium < 0.0:
        score += 15
        breakdown.append(f"Z-score {z_premium:+.2f}s below avg (+15)")
    elif z_premium < 1.0:
        score += 5
        breakdown.append(f"Z-score {z_premium:+.2f}s near avg (+5)")
    else:
        breakdown.append(f"Z-score {z_premium:+.2f}s elevated (0)")

    # RVOL normalizing (<= 1.0x) — selling pressure exhausted (+15)
    if rvol > 0:
        if rvol <= 1.0:
            score += 15
            breakdown.append(f"RVOL {rvol:.2f}x — selling exhausted (+15)")
        elif rvol <= 1.3:
            score += 7
            breakdown.append(f"RVOL {rvol:.2f}x — normalizing (+7)")
        else:
            breakdown.append(f"RVOL {rvol:.2f}x — still elevated (0)")

    # 45+ days since N-2 detected — RO process likely complete (+10)
    try:
        _n2_date_str = db.get_state(f"cornerstone_n2_detected_{ticker}", "")
        if _n2_date_str:
            _n2_date = datetime.strptime(_n2_date_str, "%Y-%m-%d").date()
            _age_days = (datetime.utcnow().date() - _n2_date).days
            if _age_days >= 45:
                score += 10
                breakdown.append(f"{_age_days}d since N-2 — RO process complete (+10)")
            else:
                breakdown.append(f"{_age_days}d since N-2 — wait for 45d (+0, {45 - _age_days}d remaining)")
        else:
            breakdown.append("N-2 date unknown (0)")
    except Exception:
        _age_days = 0

    # VIXY z < 1.0 — macro calm (+10)
    if vixy_z < 1.0:
        score += 10
        breakdown.append(f"VIXY z {vixy_z:+.2f}s — macro calm (+10)")
    else:
        breakdown.append(f"VIXY z {vixy_z:+.2f}s — vol elevated (0)")

    # HY spread < 4.5% — credit not stressed (+5)
    if credit_spread < 4.5:
        score += 5
        breakdown.append(f"HY spread {credit_spread:.2f}% < 4.5% — credit OK (+5)")
    else:
        breakdown.append(f"HY spread {credit_spread:.2f}% — credit stressed (0)")

    # SPY above SMA200 — macro tailwind (+5)
    if spy_above_sma200:
        score += 5
        breakdown.append("SPY above SMA200 — bull regime (+5)")
    else:
        breakdown.append("SPY below SMA200 — bear regime (0)")

    # Gate check — score AND hard 45-day prerequisite
    # The 45-day wait ensures the RO subscription period is complete before re-entry.
    # A high score on Day 1 does NOT mean the offering is done — it means the metrics
    # look good before dilution is resolved. Hard gate prevents premature re-buy.
    try:
        _n2_str_gate = db.get_state(f"cornerstone_n2_detected_{ticker}", "")
        _age_days_gate = (
            (datetime.utcnow().date() - datetime.strptime(_n2_str_gate, "%Y-%m-%d").date()).days
            if _n2_str_gate else 0
        )
    except Exception:
        _age_days_gate = 0

    gate_met = score >= 60 and _age_days_gate >= 45
    days_to_gate = max(0, 45 - _age_days_gate)

    implied_yield = round(annual_div / price * 100, 1) if price > 0 else 0.0

    return {
        "score":         score,
        "gate_met":      gate_met,
        "breakdown":     breakdown,
        "fair_value":    fair_value,
        "zone_low":      zone_low,
        "zone_high":     zone_high,
        "implied_yield": implied_yield,
        "nav_used":      _nav,
        "age_days":      _age_days_gate,
        "days_to_gate":  days_to_gate,
    }


def format_reentry_block(ticker: str, r: dict, short: bool = False) -> str:
    """
    Formats the post-RO re-entry tracker block.
    short=True  → 3-line summary for daily pulse (no confluence breakdown)
    short=False → full breakdown for Pushover and direct re-entry alert
    """
    score       = r["score"]
    gate_met    = r["gate_met"]
    fv          = r["fair_value"]
    zl          = r["zone_low"]
    zh          = r["zone_high"]
    iy          = r["implied_yield"]
    age_days    = r.get("age_days", 0)
    days_to_gate = r.get("days_to_gate", max(0, 45 - age_days))

    if gate_met:
        status_line = f"RE-ENTRY SIGNAL ACTIVE — {score}/100"
    elif days_to_gate > 0:
        # 45-day hard gate not yet met — don't call it "signal active" ever
        status_line = f"Watching — `{score}/100` | Gate unlocks in {days_to_gate}d"
    else:
        # 45 days passed but score below 60
        remaining = 60 - score
        status_line = f"Tracking — `{score}/100` | {remaining}pts to signal (60 needed)"

    if short:
        # ── 3-line pulse version — status, zone, countdown only ──────────────
        lines = [
            f"POST-RO RE-ENTRY TRACKER — {ticker}",
            f"┣ {status_line}",
            f"┣ Zone: `${zl:.2f} – ${zh:.2f}` | FV `${fv:.2f}` | Yield `{iy:.1f}%`",
        ]
        if gate_met:
            lines.append("┗ All gates met — Pushover fired. Confirm fill and resume DRIP.")
        elif days_to_gate > 0:
            lines.append(f"┗ RO subscription period: {age_days}/45d elapsed — continue monitoring")
        else:
            unmet = [b for b in r["breakdown"] if "(0)" in b]
            top_unmet = unmet[0].split("(")[0].strip() if unmet else "score below threshold"
            lines.append(f"┗ Closest gap: {top_unmet}")
        return "\n".join(lines)

    # ── Full breakdown version for Pushover / direct alert ───────────────────
    # RO subscription price formula (empirically confirmed from SEC filings):
    #   CLM 2025: max(112% × NAV, 80% × market price) — typically 112% NAV wins
    #   CRF 2025/2026: 104% × NAV at expiration
    # Open-market buyers who wait until price ≤ sub price beat RO participants:
    # no lock-up period, margin-eligible, no subscription paperwork.
    _nav_for_sub = r.get("nav_used", 6.73 if ticker == "CLM" else 6.18)
    if ticker == "CLM":
        _ro_sub_px = round(_nav_for_sub * 1.12, 2)  # CLM: 112% of NAV
    else:
        _ro_sub_px = round(_nav_for_sub * 1.04, 2)  # CRF: 104% of NAV

    lines = [
        f"POST-RO RE-ENTRY TRACKER — {ticker}",
        f"┣ {status_line}",
        f"┣ Re-entry zone:  `${zl:.2f} – ${zh:.2f}`  (NAV to +1.5% premium — max DRIP efficiency)",
        f"┣ RO sub price:   ~`${_ro_sub_px:.2f}` (NAV×1.04) — open-market entry at/below this beats RO participants",
        f"┣ Fair value floor: `${fv:.2f}`  |  Implied yield at current price: `{iy:.1f}%`",
        "┣ Confluence breakdown:",
    ]
    for item in r["breakdown"]:
        lines.append(f"┣   {item}")

    if gate_met:
        lines.append("┗ All gates met — rebuy zone confirmed. Resume DRIP at NAV after fill.")
    elif days_to_gate > 0:
        lines.append(
            f"┗ 45-day gate: {age_days}d elapsed, {days_to_gate}d remaining — "
            f"RO subscription window must close before re-entry"
        )
    else:
        remaining = 60 - score
        unmet = [b for b in r["breakdown"] if "(0)" in b]
        if unmet:
            lines.append(f"┗ {remaining}pts needed — closest: {unmet[0].split('(')[0].strip()}")
        else:
            lines.append(f"┗ {remaining}pts needed to unlock re-entry signal")

    return "\n".join(lines)


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
        # ── HARD BLOCK: RO dodge active — position sold, do NOT accumulate.
        # Accumulation gate is meaningless while we're waiting for the post-RO re-entry
        # signal. Buying now would undo the dodge. Gate stays BLOCKED until ro_dodge_active
        # is cleared by a re-entry signal firing.
        if db.get_state(f"ro_dodge_active_{ticker}", ""):
            _n2_date = db.get_state(f"cornerstone_n2_detected_{ticker}", "?")
            _dodge_ex = db.get_state(f"ro_dodge_executed_{ticker}") or {}
            _sell_px  = _dodge_ex.get("sell_price", 0.0) if isinstance(_dodge_ex, dict) else 0.0
            _sell_str = f" at ${_sell_px:.2f}" if _sell_px > 0 else ""
            return {
                "ready":       False,
                "status":      "BLOCKED — RO active",
                "detail":      (
                    f"N-2 filed {_n2_date}, position sold{_sell_str}. "
                    f"Awaiting re-entry signal (premium collapse or yield floor). Do NOT add margin."
                ),
                "down_streak": 0,
            }

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
                    f"Premium {premium:.1f}% to NAV — need < 15.0% ({premium - 15.0:.1f}pts to threshold). "
                    f"Target entry: ex-div dip or post-RO compression."
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
    premium_30pct_watch=False,
    yield_steepen=False, sentiment_fear=False,
    nav_determination=False, cef_inst_exit=False, dist_overvalued=False,
    long_rate_pressure=False, hy_rapid_widen=False,
    dark_pool_cluster_count=1,
    premium_streak_days=0, ro_interval_elevated=False,
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
    if "N-14 MERGER" in sec_shield:
        score += RO_SCORE_WEIGHTS["sec_n14"]
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
        # Tiered: 2-of-3 session clustering = full +18pts; single session = +8pts only.
        # Multi-session clustering validated by 17-year empirical study (2025): institutions
        # distributing deliberately show repeated low-lit-vol prints across 48–72h.
        score += RO_SCORE_WEIGHTS["dark_pool"] if dark_pool_cluster_count >= 2 else 8
    if premium_compressed:
        score += RO_SCORE_WEIGHTS["premium_compression"]
    if macro_underperform:
        score += RO_SCORE_WEIGHTS["macro_underperform"]
    if holder_exit:
        score += RO_SCORE_WEIGHTS["13f_holder_exit"]
    if premium_30pct_watch:
        score += RO_SCORE_WEIGHTS["premium_30pct_watch"]
    if yield_steepen:
        score += RO_SCORE_WEIGHTS["yield_steepen"]
    if long_rate_pressure:
        score += RO_SCORE_WEIGHTS["long_rate_pressure"]
    if hy_rapid_widen:
        score += RO_SCORE_WEIGHTS["hy_rapid_widen"]
    if sentiment_fear:
        score += RO_SCORE_WEIGHTS["sentiment_fear"]
    if nav_determination:
        score += RO_SCORE_WEIGHTS["nav_determination"]
    if cef_inst_exit:
        score += RO_SCORE_WEIGHTS["cef_inst_exit"]
    if dist_overvalued:
        score += RO_SCORE_WEIGHTS["dist_overvalued"]
    # Premium elevation streak: board has clear motive when premium stays elevated for weeks
    if premium_streak_days >= 10:
        score += RO_SCORE_WEIGHTS["premium_streak"]
    elif premium_streak_days >= 5:
        score += RO_SCORE_WEIGHTS["premium_streak"] // 2  # half weight at 5 days
    # Interval elevated: 10+ months since last RO completion + premium elevated = "overdue" cycle
    if ro_interval_elevated:
        score += 8  # separate from premium_streak — different dimension of risk
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
                         y_dist=0.0,
                         nav_determination=False, cef_inst_exit_desc="",
                         dist_fair_value=0.0, implied_yield=0.0,
                         is_dist_overvalued=False,
                         nav_src="CEFConnect",
                         premium_streak_days=0, premium_velocity_3d=0.0,
                         months_since_last_ro=None, ro_interval_elevated=False) -> str:
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
    prem_tag = ("(neutral)" if 10 <= premium <= 20
                else ("(EXTENDED)" if premium > 25
                else ("(HIGH)" if premium > 15
                else "(DISCOUNT)")))
    rsi_tag  = "(neutral)" if 40 <= rsi <= 60 else ("(OVERBOUGHT)" if rsi > 70 else ("(OVERSOLD)" if rsi < 30 else "(neutral)"))
    z_tag    = "(safe)" if z_premium < 1.0 else ("(caution)" if z_premium < 2.0 else "(DANGER)")

    sec  = _parse_sec_shield(sec_shield)

    # EDGAR/SEC line — no emoji, semicolon separator
    if sec["ro_active"] and sec["holder_change"]:
        edgar_sec_line = "┣ EDGAR/SEC: ⚠️ N-2 RO ACTIVE; Large holder change (13D/G)\n"
    elif sec["ro_active"]:
        edgar_sec_line = "┣ EDGAR/SEC: ⚠️ N-2/RO registration — ACTION REQUIRED\n"
    elif sec["holder_change"]:
        edgar_sec_line = "┣ EDGAR/SEC: ⚠️ Large holder change (13D/G) — monitor\n"
    else:
        edgar_sec_line = "┣ EDGAR/SEC: No N-2; No 13D/G (safe)\n"

    whale_tag = f"⚠️ {whale_status}" if "DISTRIBUTION" in whale_status.upper() else "NORMAL"
    # EX-DIV DIP is a scheduled, expected price drop — not a dark pool signal.
    # Any description containing "EX-DIV", "NOT DARK POOL", "CLEAR", or "NORMAL"
    # is safe and should render without the ⚠️ warning marker.
    _dp_upper = (dark_pool_desc or "").upper()
    dp_safe   = (
        not dark_pool_desc
        or "CLEAR"        in _dp_upper
        or "NORMAL"       in _dp_upper
        or "EX-DIV"       in _dp_upper
        or "NOT DARK POOL" in _dp_upper
    )
    if dp_safe and dark_pool_desc and "CLEAR" not in _dp_upper and "NORMAL" not in _dp_upper:
        # EX-DIV or similar — show the description cleanly, no ⚠️
        dp_tag = dark_pool_desc.split("—")[0].strip()  # e.g. "EX-DIV DIP (scheduled)"
    else:
        dp_tag = "CLEAR" if dp_safe else f"⚠️ {dark_pool_desc}"

    # NAV source label (compact)
    nav_label = "CEFConnect" if "cefconnect" in nav_src.lower() else "proxy"

    # Pre-N-2 "Trim Zone" signal (community-validated insight from Todd Akin's Discord):
    # When premium hits 20%+, start lightening BEFORE the N-2 drops. Once premium
    # reaches 30%, the 30% watch alert fires a separate one-time Discord embed.
    # This intermediate warning (20-29.9%) appears in the daily pulse only — no
    # separate Discord embed, just a visible line to prompt review.
    # Suppressed when RO dodge is already active (N-2 already filed — too late to trim).
    _ro_dodge_now = db.get_state(f"ro_dodge_active_{ticker}", "")
    trim_zone_line = ""
    if not sec["ro_active"] and not _ro_dodge_now:
        if 20.0 <= premium < 30.0:
            trim_zone_line = (
                f"┣ ⚠️ TRIM ZONE (`{premium:.1f}%`) — Premium entering exit range. "
                f"Consider 30–50% reduction. 30% = full watch trigger.\n"
            )
        # 30%+ is handled by the separate 30pct_watch Discord embed (already fires in get_ticker_report)

    # Pre-N-2 streak and velocity lines (appear only when premium is elevated and no active RO)
    # These are early-warning context lines, not hard alerts — they live in the pulse quietly
    # until something actionable is building.
    prem_streak_line = ""
    prem_velocity_line = ""
    ro_interval_line = ""
    if not sec["ro_active"] and not _ro_dodge_now:
        if premium_streak_days >= 5:
            _streak_emoji = "🔴" if premium_streak_days >= 10 else "⚠️"
            prem_streak_line = (
                f"┣ {_streak_emoji} Prem 20%+ Streak: `{premium_streak_days}d` — "
                f"{'RO risk rising — board has sustained motive' if premium_streak_days >= 10 else 'watch — approaching board motive threshold (10d)'}\n"
            )
        if abs(premium_velocity_3d) >= 2.0:
            _vel_dir = "↑ expanding" if premium_velocity_3d > 0 else "↓ compressing"
            _vel_emoji = "⚠️" if premium_velocity_3d > 0 else "✅"
            prem_velocity_line = (
                f"┣ {_vel_emoji} Prem Velocity: `{premium_velocity_3d:+.1f}%` / 3d ({_vel_dir})\n"
            )
        if ro_interval_elevated and months_since_last_ro is not None:
            ro_interval_line = (
                f"┣ 🔴 RO Interval: `{months_since_last_ro:.1f}mo` since last completion — "
                f"overdue window (10+ mo) + premium elevated = heightened cycle risk\n"
            )
        elif months_since_last_ro is not None and months_since_last_ro >= 7.0 and premium >= 15.0:
            ro_interval_line = (
                f"┣ Months since last RO: `{months_since_last_ro:.1f}mo` — "
                f"approaching typical interval (10–18 mo)\n"
            )

    # Conditional lines — only appear when triggered, inserted before Div. Yield
    vixy_line      = f"┣ VIXY: `{vixy_z:+.1f}σ` spike — reduce size / close puts→calls\n" if crisis_day else ""
    ro_season_line = "┣ RO Season: Active (Feb–Apr window)\n" if ro_season else ""
    seasonal_line  = "┣ Seasonal Caution: Active (March/Sept weakness)\n" if seasonal_caution else ""
    nav_det_line   = "┣ ⚠️ NAV Lock Month (Oct) — heightened sensitivity\n" if nav_determination else ""
    inst_exit_line = f"┣ 🔴 Inst. Exit: {cef_inst_exit_desc}\n" if cef_inst_exit_desc and "INST. EXIT" in cef_inst_exit_desc else ""
    dist_line      = (
        f"┣ ⚠️ Dist. Yield: `{implied_yield:.1f}%` — OVERVALUED vs 19% target (reduce exposure)\n"
        if is_dist_overvalued else ""
    )

    return (
        f"**{ticker}** — {status}\n"
        f"┣ Price: `${price:.2f}`\n"
        f"┣ NAV: `${nav:.2f}`\n"
        f"┣ Prem: `{premium:.2f}%` {prem_tag}\n"
        f"{trim_zone_line}"
        f"{prem_streak_line}"
        f"{prem_velocity_line}"
        f"{ro_interval_line}"
        f"{edgar_sec_line}"
        f"┣ Whale Flow: {whale_tag}\n"
        f"┣ Dark Pool: {dp_tag}\n"
        f"┣ Z-Score: `{z_premium:+.1f}σ` {z_tag}\n"
        f"┣ RSI: `{rsi:.1f}` {rsi_tag}\n"
        f"{vixy_line}"
        f"{ro_season_line}"
        f"{seasonal_line}"
        f"{nav_det_line}"
        f"{inst_exit_line}"
        f"{dist_line}"
        f"┣ Div. Yield: `{y_dist:.1f}%`\n"
        f"┗ Verdict = RO Risk: `{ro_score}/100` ({ro_tier})\n"
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
    _stale_price = False
    if price == 0.0:
        # Before declaring offline, try the last known-good price stored in DB.
        # TD intermittently times out during peak hours — stale data beats a blank embed.
        _cached_price = db.get_state(f"{ticker.lower()}_last_price")
        if _cached_price:
            try:
                price = float(_cached_price)
                _stale_price = True
                logger.warning(f"[{ticker}] Live price unavailable — using cached ${price:.2f} (stale)")
            except (TypeError, ValueError):
                pass
    if price == 0.0:
        return f"**{ticker}**\n┗ ⚠️ Data feed offline.\n", "LOW", 0

    # Persist last known-good price for stale-data fallback on next TD outage.
    if not _stale_price and price > 0:
        db.update_state(f"{ticker.lower()}_last_price", round(price, 4))

    # ── Official NAV via CEFConnect (replaces XCLMX/XCRFX proxy for premium accuracy)
    # CEFConnect publishes the fund manager's official end-of-day NAV — same source
    # as the Cornerstone website. Cached daily (0 Twelve Data credits, no rate impact).
    # XCLMX/XCRFX remains in fetch_live_metrics() as a last-resort fallback only.
    _nav_official, _prem_official, _nav_src = fetch_nav_cefconnect(session, ticker)
    if _nav_official > 0:
        nav = _nav_official  # override proxy with official NAV
    else:
        # CEFConnect unreachable — try last DB-cached official NAV before settling for proxy
        _default_nav = PRIORITY_ASSETS[ticker]["default_nav"]
        if nav == _default_nav:
            _cached_nav = db.get_state(f"{ticker.lower()}_last_nav")
            if _cached_nav:
                try:
                    nav = float(_cached_nav)
                    _nav_src = "cached"
                except (TypeError, ValueError):
                    pass
        _nav_src = "proxy" if nav == PRIORITY_ASSETS[ticker]["default_nav"] else "cached"

    if nav > 0:
        db.update_state(f"{ticker.lower()}_last_nav", round(nav, 4))

    # ── Whale flow (original)
    whale_status, whale_rvol = detect_whale_flow_direction(session, ticker)

    # ── Distribution math (original)
    annual_div = 1.4268 if ticker == "CLM" else 1.3824  # 2026 reset: $0.1189×12 CLM, $0.1152×12 CRF
    y_dist     = (annual_div / price) * 100 if price > 0 else 0
    y_nav      = (annual_div / nav)   * 100 if nav   > 0 else 0
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

    # ── NEW: Dark pool detection (multi-session clustering)
    # Single-session detection fires near-randomly. 2025 research (17-year study, TradeAlgo/TradeEcho)
    # finds the high-conviction signal requires clustering across 2+ consecutive sessions:
    # institutions distributing in size repeatedly over 48–72h, not routine low-volume days.
    # Full +18pt score requires 2 of last 3 sessions flagged; single session gets +8pts only.
    _monthly_dist = (1.4268 if ticker == "CLM" else 1.3824) / 12   # $0.1189 CLM / $0.1152 CRF
    is_dark_pool, price_chg, vol_ratio, dark_pool_desc = detect_dark_pool_activity(session, ticker, monthly_dist=_monthly_dist)
    dark_pool_cluster_count = 0
    if is_dark_pool:
        _dp_key = f"dark_pool_session_hist_{ticker}"
        _dp_hist = list(db.get_state(_dp_key) or [])
        _dp_hist.append(1)
        _dp_hist = _dp_hist[-3:]  # rolling 3-session window
        db.update_state(_dp_key, _dp_hist)
        dark_pool_cluster_count = sum(_dp_hist)
    else:
        # Still slide the window forward with a 0 on non-dark-pool sessions
        _dp_key = f"dark_pool_session_hist_{ticker}"
        _dp_hist = list(db.get_state(_dp_key) or [])
        _dp_hist.append(0)
        _dp_hist = _dp_hist[-3:]
        db.update_state(_dp_key, _dp_hist)
        dark_pool_cluster_count = sum(_dp_hist)

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

    # ── NEW: NAV determination month gate (October — tightened sensitivity)
    nav_determination = is_nav_determination_month()

    # ── NEW: CEF institutional exit (high lit vol + flat SPY)
    # spy_chg already computed above — reuse, no extra API call
    is_cef_inst_exit, inst_vol_ratio, _inst_price_chg, cef_inst_exit_desc = \
        detect_cef_institutional_exit(session, ticker, spy_chg)

    # ── NEW: Distribution yield floor (fair value vs current price)
    is_dist_overvalued, dist_fair_value, implied_yield, dist_yield_desc = \
        check_distribution_yield_floor(price, ticker)

    # Log accumulation signal when price is at or below fair value — graded T+14.
    if not is_dist_overvalued and implied_yield >= 19.0 and price > 0:
        try:
            db.log_prediction(
                signal_type="clm_floor",
                ticker=ticker,
                predicted_direction="BULLISH",
                entry_price=price,
                target_days=14,
                notes=f"yield={implied_yield:.1f}% fv=${dist_fair_value}",
            )
        except Exception:
            pass

    # ── NEW: 13F / large holder exit signal from SEC scrape
    holder_exit = "13D" in sec_shield or "13G" in sec_shield

    # ── Track N-2 detection across cycles (anchor for re-entry logic)
    n2_key = f"cornerstone_n2_detected_{ticker}"
    if "N-2 RO REGISTRATION" in sec_shield or "N-2/A" in sec_shield:
        if not db.get_state(n2_key, ""):
            db.update_state(n2_key, datetime.now().strftime("%Y-%m-%d"))
            db.update_state(f"ro_dodge_active_{ticker}", datetime.now().strftime("%Y-%m-%d"))
            # Reset per-cycle fired flags so re-entry detectors are live
            db.update_state(f"cornerstone_ro_dip_fired_{ticker}", "")
            db.update_state(f"cornerstone_floor_reentry_fired_{ticker}", "")
            logger.info(f"[N-2 Cycle] {ticker} — N-2 anchor set {datetime.now().strftime('%Y-%m-%d')}, dodge active.")
    else:
        # N-2 scrolled out of recent filings list.
        # Only clear the cycle anchor once re-entry has been confirmed via one of the two
        # re-entry paths. Until then, preserve the key so the re-entry logic keeps working.
        _path_a_done = db.get_state(f"cornerstone_ro_dip_fired_{ticker}", "")
        _path_b_done = db.get_state(f"cornerstone_floor_reentry_fired_{ticker}", "")
        if _path_a_done or _path_b_done:
            db.update_state(n2_key, "")
            db.update_state(f"ro_dodge_active_{ticker}", "")
            db.update_state(f"cornerstone_n2_initial_alerted_{ticker}", "")  # reset for next cycle
            logger.info(f"[N-2 Cycle] {ticker} — re-entry confirmed, cycle closed.")
        # else: preserve n2_key until re-entry fires

    # ── Re-entry detectors (both run every tick, fire once per RO cycle via DB dedup)
    detect_ro_completion_dip(session, ticker, price, premium)
    check_yield_floor_reentry(ticker, price, premium)

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

    # ── PRE-N-2 EARLY WARNING SIGNALS ────────────────────────────────────────
    # Gap 1: Premium elevation streak — how many consecutive ticks has premium
    # been ≥ 20%? Duration matters: a board watching their own premium for weeks
    # is more likely to file than one seeing a single elevated day.
    # Stored as dict {count, start_date} so we can show days since streak began.
    _streak_key  = f"premium_above20_streak_{ticker}"
    _streak_data = db.get_state(_streak_key) or {}
    if not isinstance(_streak_data, dict):
        _streak_data = {}
    if premium >= 20.0:
        _streak_data["count"]      = _streak_data.get("count", 0) + 1
        _streak_data["start_date"] = _streak_data.get("start_date", datetime.now().strftime("%Y-%m-%d"))
    else:
        _streak_data = {}  # reset on any close below 20%
    db.update_state(_streak_key, _streak_data)
    premium_streak_days = _streak_data.get("count", 0)

    # Gap 2: Premium velocity — rolling 3-session change (fast expansion warning).
    # Uses the same session-over-session cache as premium_compression but looks at
    # the 3-tick moving window to smooth out intraday noise.
    _vel_key  = f"premium_velocity_hist_{ticker}"
    _vel_hist = db.get_state(_vel_key) or []
    if not isinstance(_vel_hist, list):
        _vel_hist = []
    _vel_hist.append(round(premium, 3))
    _vel_hist = _vel_hist[-3:]  # keep last 3 sessions
    db.update_state(_vel_key, _vel_hist)
    premium_velocity_3d = 0.0
    if len(_vel_hist) == 3:
        premium_velocity_3d = round(_vel_hist[-1] - _vel_hist[0], 2)

    # Gap 3: Months since last RO completion — gives time-in-cycle context.
    # CLM/CRF historically file every 10–18 months when premium is elevated.
    # "Overdue" (≥ 10 months since last completion + elevated premium) raises risk.
    _last_ro_str      = db.get_state(f"cornerstone_last_ro_completed_{ticker}", "")
    months_since_last_ro = None
    ro_interval_elevated = False
    if _last_ro_str:
        try:
            _last_ro_date        = datetime.strptime(_last_ro_str, "%Y-%m-%d").date()
            _days_since          = (datetime.utcnow().date() - _last_ro_date).days
            months_since_last_ro = round(_days_since / 30.44, 1)
            # "Overdue" = 10+ months since last completion AND premium currently elevated
            ro_interval_elevated = months_since_last_ro >= 10.0 and premium >= 20.0
        except Exception:
            pass

    # Write pre-N-2 signals to DB for cross-script reads (market_analysis.py morning brief)
    db.update_state(f"{ticker.lower()}_premium_streak_days",   premium_streak_days)
    db.update_state(f"{ticker.lower()}_premium_velocity_3d",   premium_velocity_3d)
    db.update_state(f"{ticker.lower()}_months_since_last_ro",  months_since_last_ro)
    db.update_state(f"{ticker.lower()}_ro_interval_elevated",  ro_interval_elevated)

    # ── Cross-script signal flags (DB reads — zero new API calls) ─────────────
    # Yield curve steepening: read spread written daily by cross_asset.py.
    # Rapid steepening (> 20bps) pressures yield-sensitive CEF premiums.
    yield_steepen = False
    try:
        today_str = datetime.now().strftime("%Y-%m-%d")
        if db.get_state("fred_yield_spread_date") == today_str:
            cur_spread  = db.get_state("fred_yield_spread")
            prev_spread = db.get_state("fred_yield_spread_prev")
            if cur_spread is not None and prev_spread is not None:
                yield_steepen = (float(cur_spread) - float(prev_spread)) > 0.20
    except Exception:
        pass

    # SentiSense market mood ≤ 25 = extreme fear → CEF premium compression risk
    sentiment_fear = False
    try:
        ss_mood = db.get_state("ss_market_mood")
        if isinstance(ss_mood, dict):
            sentiment_fear = int(ss_mood.get("score", 50)) <= 25
    except Exception:
        pass

    # 30-year Treasury ≥ 5.0% = income buyer rotation risk (CEF premium headwind).
    # Reads from DB key written by cross_asset.py (fetch_yield_curve now returns t30).
    long_rate_pressure = False
    try:
        _yc = db.get_state("fred_yield_curve_data")
        if isinstance(_yc, dict):
            _t30 = float(_yc.get("t30", 0.0))
            long_rate_pressure = _t30 >= 5.0
            if _t30 > 0:
                db.update_state("fred_t30_latest", round(_t30, 3))
    except Exception:
        pass

    # HY spread rapid widening: > 40bps vs 5-day-ago cached value = credit stress building.
    hy_rapid_widen = False
    try:
        _hy_prev5 = db.get_state("hy_spread_5d_ago")
        _hy_cur   = credit_spread  # already fetched above
        if _hy_prev5 is not None and _hy_cur > 0:
            hy_rapid_widen = (_hy_cur - float(_hy_prev5)) > 0.40
        # Rolling 5-day update: store today's value under a date key, rotate weekly
        _today_key = f"hy_spread_hist_{datetime.now().strftime('%a')}"  # Mon/Tue/.../Sun
        db.update_state(_today_key, round(_hy_cur, 3))
        # Use Monday's stored value as the 5-day anchor (runs fresh each week)
        _mon_val = db.get_state("hy_spread_hist_Mon")
        if _mon_val is not None:
            db.update_state("hy_spread_5d_ago", float(_mon_val))
    except Exception:
        pass

    # ── RO composite risk score (upgraded with new signals)
    ro_score, ro_tier = calculate_ro_risk_score(
        sec_shield, z_premium, premium, whale_status, credit_spread,
        ex_div_near, ro_season=ro_season, crisis_day=crisis_day,
        dark_pool=is_dark_pool, premium_compressed=is_compressed,
        macro_underperform=macro_underperf, holder_exit=holder_exit,
        premium_30pct_watch=premium_30pct_watch,
        yield_steepen=yield_steepen, sentiment_fear=sentiment_fear,
        nav_determination=nav_determination,
        cef_inst_exit=is_cef_inst_exit,
        dist_overvalued=is_dist_overvalued,
        long_rate_pressure=long_rate_pressure,
        hy_rapid_widen=hy_rapid_widen,
        dark_pool_cluster_count=dark_pool_cluster_count,
        premium_streak_days=premium_streak_days,
        ro_interval_elevated=ro_interval_elevated,
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
        # Set RO dodge flag so box pulse lines show the balloon reminder.
        # Guard: once a re-entry path has fired and cleared the flag, do NOT re-set it
        # on the next tick. N-2 stays in EDGAR permanently after filing, so without this
        # guard the flag would be restored every 5 minutes, making re-entry impossible.
        _path_a = db.get_state(f"cornerstone_ro_dip_fired_{ticker}", "")
        _path_b = db.get_state(f"cornerstone_floor_reentry_fired_{ticker}", "")
        if not _path_a and not _path_b:
            db.update_state(f"ro_dodge_active_{ticker}", datetime.now().strftime("%Y-%m-%d"))
        # Inject box context into verdict if boxes are active
        _active_boxes = read_active_box_positions()
        if _active_boxes:
            _bx = " | ".join([
                f"K{int(b.get('k1', 0))}/{int(b.get('k2', 0))} exp {b.get('expiration', '?')} "
                f"(DTE:{b.get('dte_current', '?')}, balloon:${int(b.get('width', 100)) * int(b.get('contracts', 1)) * 100:,})"
                for b in _active_boxes
            ])
            verdict += (
                f"\n┣ 📦 BOX ACTIVE: {_bx}"
                f"\n┗ Box balloon(s) owed at expiry — NOT retired by this sale. "
                f"Deploy proceeds → E*TRADE margin paydown until re-entry signal."
            )
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
    elif z_premium >= 1.5 or premium > 25.0:
        # True premium elevation — z-score OR absolute threshold confirms expensive
        status       = "⚠️ HIGH PREMIUM"
        income_note  = "Distribution/Caution phase"
        verdict      = "⚠️ Premium extended — pause new buys, target entry < 15% or post-ex-div dip."
        recommendation = "Pause reinvestment; build cash position."
    elif ro_tier == "ELEVATED":
        # ELEVATED from non-premium signals (volume anomaly, RO season, dark pool sub-threshold, etc.)
        # z-score is safe/negative — premium is NOT the risk, something else is. Label accurately.
        status       = "⚠️ RISK ELEVATED"
        income_note  = "Caution — monitor closely"
        verdict      = "⚠️ Risk composite elevated (non-premium) — DRIP may continue, reduce new buys."
        recommendation = "Monitor EDGAR; reduce new position sizing until risk resolves."
    else:
        status       = "✅ STABLE"
        income_note  = "Accumulation phase"
        verdict      = "✅ DRIP active — accumulate on premium dips < 15% or ex-div window."
        recommendation = "Reinvest distributions at NAV."

    # If we fell back to a cached price, mark the status so it's visible in the embed
    if _stale_price:
        status = f"{status} ⚠️ (price stale — TD unavailable)"

    report_text = format_pulse_report(
        ticker=ticker, price=price, nav=nav, rsi=rsi, premium=premium,
        z_premium=z_premium, sec_shield=sec_shield, ro_score=ro_score,
        ro_tier=ro_tier, whale_status=whale_status,
        dark_pool_desc=dark_pool_desc, premium_compression_desc=prem_compress_desc,
        macro_interp=macro_interp, ex_div_near=ex_div_near, ro_season=ro_season,
        crisis_day=crisis_day, vixy_z=vixy_z, status=status,
        recommendation=recommendation, verdict=verdict,
        income_note=income_note, s_net=s_net, alpha_drip=alpha_drip,
        seasonal_caution=seasonal_caution, y_dist=y_dist,
        nav_determination=nav_determination,
        cef_inst_exit_desc=cef_inst_exit_desc,
        dist_fair_value=dist_fair_value,
        implied_yield=implied_yield,
        is_dist_overvalued=is_dist_overvalued,
        nav_src=_nav_src,
        premium_streak_days=premium_streak_days,
        premium_velocity_3d=premium_velocity_3d,
        months_since_last_ro=months_since_last_ro,
        ro_interval_elevated=ro_interval_elevated,
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

    # ── Post-RO re-entry scorer — only runs when a dodge is active for this ticker.
    # Appends a confluence tracker block to the report and writes score to DB.
    # Gate >= 60/100 also fires a dedicated Pushover alert (once per RO cycle).
    _ro_dodge_active = db.get_state(f"ro_dodge_active_{ticker}", "")
    if _ro_dodge_active:
        try:
            _spy_above = False
            if spy_vals_200 and len(spy_vals_200) >= 200:
                _spy_above = float(spy_vals_200[0]["close"]) > (sum(float(v["close"]) for v in spy_vals_200) / len(spy_vals_200))
            _reentry = calculate_reentry_score(
                ticker=ticker, price=price, nav=nav, premium=premium,
                z_premium=z_premium, rvol=whale_rvol if whale_rvol else 0.0,
                vixy_z=vixy_z, credit_spread=credit_spread,
                spy_above_sma200=_spy_above,
            )
            # Persist score for daily pulse display and cross-script reads
            db.update_state(f"{ticker}_reentry_score", _reentry["score"])
            db.update_state(f"{ticker}_reentry_zone", {
                "low": _reentry["zone_low"], "high": _reentry["zone_high"],
                "fair_value": _reentry["fair_value"],
            })
            # Append short 3-line tracker block to the daily pulse
            # Full breakdown is reserved for the Pushover alert below
            _reentry_block = format_reentry_block(ticker, _reentry, short=True)
            report_text = report_text.rstrip("\n") + f"\n\n{_reentry_block}\n"
            # Fire Pushover once when gate is first met in this RO cycle
            if _reentry["gate_met"]:
                _gate_key = f"reentry_gate_fired_{ticker}_{datetime.now().strftime('%Y-%m')}"
                if not db.get_state(_gate_key):
                    db.update_state(_gate_key, True)
                    _p_tok = os.getenv("PUSHOVER_API_TOKEN")
                    _p_usr = os.getenv("PUSHOVER_USER_KEY")
                    if _p_tok and _p_usr:
                        _breakdown_lines = "\n".join(f"  {b}" for b in _reentry["breakdown"])
                        _dodge_exec = db.get_state(f"ro_dodge_executed_{ticker}") or {}
                        _sell_px = _dodge_exec.get("sell_price", 0.0) if isinstance(_dodge_exec, dict) else 0.0
                        _pnl_str = (
                            f"Sold ${_sell_px:.2f} → rebuy ~${price:.2f} "
                            f"({'PROFIT' if _sell_px > price else 'LOSS'} ${abs(_sell_px - price):.2f}/sh)"
                            if _sell_px > 0 else "Sell price not logged"
                        )
                        requests.post(
                            "https://api.pushover.net/1/messages.json",
                            data={
                                "token": _p_tok, "user": _p_usr,
                                "title": f"🟢 {ticker} — RE-ENTRY GATE MET ({_reentry['score']}/100)",
                                "message": (
                                    f"Re-entry zone: ${_reentry['zone_low']:.2f}–${_reentry['zone_high']:.2f}\n"
                                    f"Fair value: ${_reentry['fair_value']:.2f} | "
                                    f"Yield: {_reentry['implied_yield']:.1f}%\n"
                                    f"NAV ref: ${_reentry['nav_used']:.2f}\n"
                                    f"\nSignal confluence:\n{_breakdown_lines}\n"
                                    f"\nRound-trip: {_pnl_str}\n"
                                    f"\nAction: Rebuy position. Resume DRIP at NAV.\n"
                                    f"Keep ≥3 shares always."
                                ),
                                "priority": 1,
                                "sound": "cashregister",
                            },
                            timeout=10,
                        )
                        logger.info(f"[Re-entry Gate] {ticker} score {_reentry['score']}/100 — Pushover fired.")
        except Exception as _re_err:
            logger.warning(f"[Re-entry Scorer] {ticker}: {_re_err}")

    # ── Persist key metrics for cross-script reads (market_analysis.py morning brief)
    # Only write z_premium when NAV is valid — nav=0 produces a nonsense z-score
    if nav > 0:
        db.update_state(f"{ticker.lower()}_last_z_premium", round(z_premium, 3))
    db.update_state(f"{ticker.lower()}_last_premium",   round(premium, 3))
    db.update_state(f"{ticker.lower()}_last_ro_tier",   ro_tier)
    db.update_state(f"{ticker.lower()}_last_ro_score",  ro_score)  # numeric score for cross-script reads

    # ── Log daily premium for z-score calibration (replaces retired CEFConnect API)
    # INSERT OR IGNORE — only the first call per calendar day writes; subsequent
    # 5-min loop ticks skip silently. Builds rolling 252-day empirical baseline.
    try:
        if nav > 0 and price > 0:
            db.store_cef_premium(ticker, nav, price, premium)
    except Exception:
        pass

    # ── Strategy journal — log session observation for CLM/CRF
    try:
        _signals_fired = []
        _conflicts     = []

        if is_dark_pool:           _signals_fired.append("dark_pool")
        if is_compressed:          _signals_fired.append("premium_compression")
        if macro_underperf:        _signals_fired.append("macro_underperform")
        if long_rate_pressure:     _signals_fired.append("long_rate_pressure")
        if hy_rapid_widen:         _signals_fired.append("hy_rapid_widen")
        if ro_season:              _signals_fired.append("ro_season")
        if crisis_day:             _signals_fired.append("vixy_crisis")
        if nav_determination:      _signals_fired.append("nav_determination_month")
        if is_cef_inst_exit:       _signals_fired.append("cef_inst_exit")
        if is_dist_overvalued:     _signals_fired.append("dist_overvalued")
        if holder_exit:            _signals_fired.append("13f_holder_exit")

        # dist_fair_value already computed above via check_distribution_yield_floor()
        _fv = dist_fair_value if dist_fair_value > 0 else (7.51 if ticker == "CLM" else 7.28)
        if price <= _fv:           _signals_fired.append("at_fair_value_floor")
        if price <= _fv * 0.95:   _signals_fired.append("BELOW_FAIR_VALUE")

        if ex_div_near:
            _conflicts.append("ex_div_scheduled_dip_suppressor")
        if spy_chg < -1.0 and price_chg < spy_chg * 0.8:
            _conflicts.append("broad_market_selling_present")
        if z_premium < -1.0:
            _conflicts.append("premium_below_avg_z_negative")

        # Classify the session move
        _macro_class = (
            "CEF_SPECIFIC" if price_chg <= -1.5 and abs(spy_chg) < 0.8 else
            "MACRO"        if spy_chg < -1.0 and price_chg < -1.0 else
            "STABLE"
        )

        # Map status → action label
        _action_map = {
            "🚨 CRITICAL: N-2 DETECTED":         "DODGE",
            "🚨 CRITICAL: RO RISK ELEVATED":      "WATCH_EDGAR",
            "⚠️ WARNING: DARK POOL ACTIVITY":     "MONITOR",
            "⚠️ WARNING: PREMIUM COMPRESSION":    "PAUSE_DRIP",
            "⚠️ HIGH PREMIUM":                    "PAUSE_NEW_BUYS",
            "⚠️ RISK ELEVATED":                   "REDUCE_SIZE",
            "✅ STABLE":                           "ACCUMULATE" if z_premium <= 0.5 else "HOLD",
        }
        _status_clean = status.split(" ⚠️")[0]  # strip stale-price suffix
        _action = _action_map.get(_status_clean, "HOLD")

        # Conviction: score-based for notable events, otherwise low baseline
        _conviction = min(5, max(1, ro_score // 20)) if ro_score >= 20 else (
                      4 if price_chg <= -3.0 else (2 if price_chg <= -1.5 else 1))

        _thesis = (
            f"{ticker} {price_chg:+.1f}% — classified {_macro_class} (SPY {spy_chg:+.1f}%). "
            f"RO: {ro_score}/100 ({ro_tier}). Premium z={z_premium:+.2f}σ ({premium:.1f}%). "
            f"Action: {_action}."
        )

        _confluences = {
            "price":           round(price,      4),
            "nav":             round(nav,         4),
            "premium_pct":     round(premium,     2),
            "premium_z":       round(z_premium,   3),
            "price_chg_pct":   round(price_chg,   2),
            "spy_chg_pct":     round(spy_chg,     2),
            "rsi":             round(rsi,          1),
            "vixy_z":          round(vixy_z,      3),
            "hy_spread":       round(credit_spread, 3),
            "t30":             round(db.get_state("fred_t30_latest") or 0.0, 3),
            "ro_score":        ro_score,
            "ro_tier":         ro_tier,
            "signals_fired":   _signals_fired,
            "macro_classified":_macro_class,
            "fair_value":      _fv,
            "status":          _status_clean,
        }

        # Determine event_type — PRICE_DROP and RO flags bypass daily dedup
        # FLOOR_BREACH is deduplicated (daily) even though it's significant — prevents
        # hundreds of entries per day while price holds below the floor during the loop.
        if "N-2" in sec_shield or ro_tier == "CRITICAL":
            _event = "RO_CRITICAL"
        elif ro_tier == "ELEVATED":
            _event = "RO_ELEVATED"
        elif price_chg <= -1.5 and _macro_class == "CEF_SPECIFIC":
            _event = "PRICE_DROP"
        elif price <= _fv * 0.95:
            _event = "FLOOR_BREACH"   # daily-deduplicated — see _JOURNAL_NOTABLE in database.py
        else:
            _event = "DAILY_OBSERVATION"

        db.log_journal_entry(
            strategy="CLM_CRF",
            event_type=_event,
            ticker=ticker,
            action=_action,
            conviction=_conviction,
            thesis=_thesis,
            confluences=_confluences,
            conflicts={"items": _conflicts},
            entry_price=price,
        )
    except Exception as _je:
        logger.debug(f"Journal entry skipped for {ticker}: {_je}")

    return report_text, ro_tier, ro_score

# ─────────────────────────────────────────────────────────────────────────────
# CHART BUILDER (original — unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def build_cornerstone_chart():
    """
    Builds a dark-theme CLM + CRF candlestick chart from Twelve Data OHLCV (90 days).
    Replaces Finviz (which silently ignores the &theme=dark parameter on free tier).
    Layout: CLM on top, CRF below — stacked vertically, consistent 900×700px total.
    Includes: candlesticks, SMA 20 (orange) + SMA 50 (purple), volume subplot.
    Falls back to None on any failure — dispatch continues without a chart.
    """
    try:
        import io
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        from matplotlib.gridspec import GridSpec
        import numpy as np

        BG       = "#0d1117"
        PANEL    = "#161b22"
        GREEN    = "#2ecc71"
        RED      = "#e74c3c"
        SMA20_C  = "#f39c12"
        SMA50_C  = "#9b59b6"
        GRID_C   = "#21262d"
        TEXT_C   = "#c9d1d9"

        def _sma(closes, n):
            out = [None] * len(closes)
            for i in range(n - 1, len(closes)):
                out[i] = sum(closes[i - n + 1 : i + 1]) / n
            return out

        def _draw_panel(ax_price, ax_vol, rows, ticker):
            """Draw one ticker panel (price + volume) on provided axes."""
            rows = list(reversed(rows))          # oldest first
            dates   = list(range(len(rows)))
            opens   = [float(r.get("open",  r.get("close", 0))) for r in rows]
            highs   = [float(r.get("high",  r.get("close", 0))) for r in rows]
            lows    = [float(r.get("low",   r.get("close", 0))) for r in rows]
            closes  = [float(r.get("close", 0)) for r in rows]
            vols    = [float(r.get("volume", 0)) for r in rows]

            # Candle body + wick
            w = 0.6
            for i, (o, h, l, c) in enumerate(zip(opens, highs, lows, closes)):
                color = GREEN if c >= o else RED
                ax_price.plot([i, i], [l, h], color=color, linewidth=0.8, zorder=2)
                ax_price.add_patch(mpatches.FancyBboxPatch(
                    (i - w / 2, min(o, c)), w, max(abs(c - o), 0.001),
                    boxstyle="square,pad=0", linewidth=0, facecolor=color, zorder=3
                ))

            # SMA lines
            sma20 = _sma(closes, 20)
            sma50 = _sma(closes, 50)
            xs20 = [i for i, v in enumerate(sma20) if v is not None]
            xs50 = [i for i, v in enumerate(sma50) if v is not None]
            if xs20: ax_price.plot(xs20, [sma20[i] for i in xs20], color=SMA20_C, linewidth=1.2, label="SMA 20", zorder=4)
            if xs50: ax_price.plot(xs50, [sma50[i] for i in xs50], color=SMA50_C, linewidth=1.2, label="SMA 50", zorder=4)

            # Volume bars
            vol_colors = [GREEN if closes[i] >= opens[i] else RED for i in range(len(rows))]
            ax_vol.bar(dates, vols, color=vol_colors, alpha=0.6, width=0.8, zorder=2)

            # Styling
            last_close = closes[-1] if closes else 0
            for ax in (ax_price, ax_vol):
                ax.set_facecolor(PANEL)
                ax.tick_params(colors=TEXT_C, labelsize=7)
                for spine in ax.spines.values():
                    spine.set_edgecolor(GRID_C)
                ax.grid(True, color=GRID_C, linewidth=0.5, zorder=1)

            # Ticker label + last price
            ax_price.set_title(
                f"{ticker}  ${last_close:.2f}", color=TEXT_C,
                fontsize=10, fontweight="bold", loc="left", pad=4
            )
            ax_price.legend(
                fontsize=7, loc="upper right",
                facecolor=PANEL, edgecolor=GRID_C, labelcolor=TEXT_C
            )

            # X-axis: show ~6 date labels (month-day)
            n = len(rows)
            step = max(1, n // 6)
            ax_price.set_xticks([])
            tick_pos  = list(range(0, n, step))
            tick_lbls = [rows[i]["datetime"][:10] for i in tick_pos]
            ax_vol.set_xticks(tick_pos)
            ax_vol.set_xticklabels(tick_lbls, rotation=30, ha="right", fontsize=6, color=TEXT_C)
            ax_vol.yaxis.set_visible(False)
            ax_price.set_xlim(-1, n)
            ax_vol.set_xlim(-1, n)

        # ── Fetch data ─────────────────────────────────────────────────────────
        with requests.Session() as sess:
            data = {}
            for tkr in PRIORITY_ASSETS:
                rows = fetch_time_series(sess, tkr, outputsize=90)
                if rows:
                    data[tkr] = rows

        if not data:
            logger.warning("build_cornerstone_chart: no OHLCV data returned — skipping chart.")
            return None

        n_tickers = len(data)
        # constrained_layout=True avoids tight_layout warning with GridSpec + sharex
        fig = plt.figure(
            figsize=(9, 3.5 * n_tickers), facecolor=BG,
            constrained_layout=True,
        )
        gs = GridSpec(
            n_tickers * 2, 1,
            height_ratios=[4 if i % 2 == 0 else 1 for i in range(n_tickers * 2)],
            hspace=0.06, figure=fig,
        )
        fig.patch.set_facecolor(BG)

        for idx, (tkr, rows) in enumerate(data.items()):
            ax_p = fig.add_subplot(gs[idx * 2])
            ax_v = fig.add_subplot(gs[idx * 2 + 1], sharex=ax_p)
            _draw_panel(ax_p, ax_v, rows, tkr)

        buf = io.BytesIO()
        fig.savefig(buf, format="PNG", facecolor=BG, dpi=110)
        plt.close(fig)
        return buf.getvalue()

    except Exception as e:
        logger.error(f"build_cornerstone_chart failed: {e}")
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
            # Append accumulation readiness to each ticker's block (daily pulse only —
            # computed every loop tick in get_ticker_report and stored in DB, but
            # never surfaced to Discord until now).
            _acc_status = db.get_state(f"{ticker}_acc_status") or ""
            _acc_detail = db.get_state(f"{ticker}_acc_detail") or ""
            # Acc. Gate removed from embed — status stored in DB for cross-script reads,
            # not surfaced in the Discord notification (reduces noise on every pulse).
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

    # ── Margin carry spread (Strategy 1 health check) ─────────────────────────
    # Active Tier 2: MLPI (~15%) + MAIN (~8%) only as of Jul 2026.
    # TDAQ / KQQQ are CLAUDE.md candidates, not currently held.
    _TIER2_BLENDED = TIER2_ACTIVE_BLENDED  # 11.5% blended
    try:
        from analytics import HighFidelityAnalyticsEngine as _AE
        _snap = _AE().fetch_fred_macro_snapshot()
        _fedfunds = float(_snap.get("fedfunds") or margin_rate)
        _est_margin = max(margin_rate, _fedfunds + 1.25)
    except Exception:
        _est_margin = margin_rate
    carry_spread = round(_TIER2_BLENDED - _est_margin, 2)
    # Persist for cross-script reads (market_analysis, personal scorecard, Pushover breach alert).
    # NOT appended to full_report — carry spread is a ledger/log signal, not a Discord line item.
    # Breach alert (< 5%) fires via send_daily_pulse() Pushover path below.
    try:
        db.update_state("carry_spread_data", {
            "date": __import__("datetime").date.today().isoformat(),
            "spread": carry_spread,
            "margin_rate": _est_margin,
            "tier2_yield": _TIER2_BLENDED,
        })
    except Exception:
        pass

    # ── Box spread positions block (Strategy 4 health check) ──────────────────
    # DB-only read — zero API calls. Surfaces DTE countdown, balloon warnings,
    # and RO dodge reminder. Content stays in #cornerstone only.
    _box_positions = read_active_box_positions()
    _box_lines = _format_box_pulse_lines(_box_positions)
    if _box_lines:
        # Persist for income channel snippet (box efficiency metrics only, no CLM/CRF data)
        try:
            _best_box = db.get_state("box_spread_best_rate") or {}
            db.update_state("box_context_daily", {
                "date": __import__("datetime").date.today().isoformat(),
                "active_count": len(_box_positions),
                "positions": [
                    {
                        "rate": bp.get("implied_rate_pct"),
                        "dte": bp.get("dte_current"),
                        "balloon": bp.get("width", 100) * bp.get("contracts", 1) * 100,
                        "loan": bp.get("loan_amount"),
                        "annual_interest": bp.get("annual_interest_usd"),
                    }
                    for bp in _box_positions
                ],
                "best_available_rate": _best_box.get("rate_pct"),
                "margin_savings_pct": round(margin_rate - float(_best_box.get("rate_pct") or margin_rate), 2),
            })
        except Exception:
            pass
        full_report += f"\n\n{_box_lines.rstrip()}"

    # ── Cross-Signal Confluence block (DB-only, zero API calls) ───────────────
    # Surfaces market_analysis_bias, TQQQ cycle scores, VIX term structure,
    # intraday ORB bias, and HY spread as routine context lines — not just alerts.
    # This gives the cornerstone operator regime context alongside the CLM/CRF readings.
    try:
        _xlines = []

        # Market Analysis bias (market_analysis.py writes a dict: {label, score, date})
        _ma_raw = db.get_state("market_analysis_bias") or {}
        _ma_label = (_ma_raw.get("label") or "") if isinstance(_ma_raw, dict) else str(_ma_raw)
        if _ma_label:
            _ma_score = _ma_raw.get("score", "") if isinstance(_ma_raw, dict) else ""
            _bias_icon = "📈" if "BULL" in _ma_label.upper() else ("📉" if "BEAR" in _ma_label.upper() else "➖")
            _score_str = f" ({_ma_score:+d})" if isinstance(_ma_score, int) else ""
            _xlines.append(f"┣ Market Bias: {_bias_icon} `{_ma_label}`{_score_str} (market_analysis.py)")

        # TQQQ cycle scores (tqqq.py writes bottom_score / top_score after each eval)
        _bot = db.get_state("tqqq_bottom_score")
        _top = db.get_state("tqqq_top_score")
        if _bot is not None or _top is not None:
            _bot_v = int(_bot) if _bot is not None else "—"
            _top_v = int(_top) if _top is not None else "—"
            _call_gate = "🟢 CALL gate open" if (_bot is not None and int(_bot) >= 55) else "⬜ no entry"
            _put_gate  = "🔴 PUT gate open"  if (_top is not None and int(_top) >= 55) else "⬜ no entry"
            _xlines.append(f"┣ TQQQ Cycle: Bottom `{_bot_v}/100` ({_call_gate}) | Top `{_top_v}/100` ({_put_gate})")

        # VIX term structure (tqqq.py writes vix_term_slope = VIXY/VXZ ratio)
        _vts = db.get_state("vix_term_slope")
        if _vts is not None:
            try:
                _vts_f = float(_vts)
                _vts_lbl = "⚠️ backwardation (vol stress)" if _vts_f >= 3.0 else ("✅ contango (calm)" if _vts_f <= -0.5 else "➖ neutral")
                _xlines.append(f"┣ VIX Term: `{_vts_f:.3f}` — {_vts_lbl}")
            except (TypeError, ValueError):
                pass

        # Intraday ORB bias (scheduler.py writes orb_intraday_bias_{date})
        _today_key = f"orb_intraday_bias_{__import__('datetime').date.today().isoformat()}"
        _orb = db.get_state(_today_key) or ""
        if _orb:
            _orb_icon = "📈" if "BULL" in _orb.upper() else ("📉" if "BEAR" in _orb.upper() else "➖")
            _xlines.append(f"┣ ORB Bias: {_orb_icon} `{_orb}` (intraday)")

        # HY Credit Spread — routine line item (not only when breached)
        _hy = fetch_hy_spread_live()
        _hy_icon = "🚨" if _hy > 4.5 else ("⚠️" if _hy > 3.8 else "✅")
        _xlines.append(f"┣ HY Spread: {_hy_icon} `{_hy:.2f}%` (FRED live — threshold 4.5%)")

        # Carry spread summary (already written to DB above — surface for quick check)
        _cs_icon = "✅" if carry_spread >= 5.0 else ("⚠️" if carry_spread >= 2.0 else "🚨")
        _xlines.append(f"┗ Carry Spread: {_cs_icon} `{carry_spread:+.1f}%` (Tier 2 yield − margin rate)")

        # ── Write to journal (passive, always) — never append to Discord report.
        # Only surface in Discord when a threshold condition is abnormal.
        if _xlines:
            _journal_text = "Cross-Signal Confluence:\n" + "\n".join(_xlines)
            try:
                db.log_journal_entry(entry_type="confluence_snapshot", notes=_journal_text)
            except Exception:
                logger.debug("Journal write skipped — log_journal_entry not available")

        # ── Discord alert: only when something is out of the ordinary
        _alert_lines = []
        if _bot is not None and int(_bot) >= 55:
            _alert_lines.append(f"┣ 🟢 TQQQ CALL gate open — Bottom `{int(_bot)}/100`")
        if _top is not None and int(_top) >= 55:
            _alert_lines.append(f"┣ 🔴 TQQQ PUT gate open — Top `{int(_top)}/100`")
        if _vts is not None:
            try:
                if float(_vts) >= 3.0:
                    _alert_lines.append(f"┣ ⚠️ VIX backwardation `{float(_vts):.3f}` — vol stress signal")
            except (TypeError, ValueError):
                pass
        if _hy > 4.5:
            _alert_lines.append(f"┣ 🚨 HY Spread `{_hy:.2f}%` — above 4.5% threshold")
        if carry_spread < 2.0:
            _alert_lines.append(f"┣ 🚨 Carry Spread `{carry_spread:+.1f}%` — critically thin")
        if _alert_lines:
            if not _alert_lines[-1].startswith("┗"):
                _alert_lines[-1] = _alert_lines[-1].replace("┣", "┗", 1)
            full_report += "\n\n**📡 Confluence Alert**\n" + "\n".join(_alert_lines)

    except Exception as _xe:
        logger.warning(f"Cross-signal confluence block failed: {_xe}")

    return full_report, worst_tier, carry_spread

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

    full_report, worst_tier, carry_spread = compute_cornerstone_reports()

    # SPY GEX macro context — fires as its own bite-size embed BEFORE the CLM/CRF flowstate.
    # Kept separate so the main flowstate stays clean and readable on mobile.
    # Discord-only (no Pushover/email) — it's context, not an alert.
    # Note: calculate_gex_profile() fires when Tradier OI is available (state != "UNKNOWN").
    # Confirmed active as of Jul 2026 — Tradier chain is providing real OI data.
    try:
        from analytics import HighFidelityAnalyticsEngine
        gex = HighFidelityAnalyticsEngine().calculate_gex_profile("SPY")
        if gex.get("market_state", "UNKNOWN") != "UNKNOWN" and HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
            flip      = gex.get("flip_strike", 0.0)
            gex_total = gex.get("gex_total")
            is_neg    = "NEGATIVE" in gex.get("market_state", "")
            gex_note  = (
                "dealers amplify moves — volatility risk elevated for CLM/CRF premium" if is_neg else
                "dealers suppress moves — stable CEF premium environment"
            )
            gex_snippet = (
                f"┣ State: `{gex['market_state']}`\n"
                f"┣ Flip Level: `${flip:,.0f}`"
                + (f" | Net GEX: `{gex_total:+.1f}B`" if gex_total is not None else "")
                + f"\n┗ {gex_note}"
            )
            gex_color = 0xe74c3c if is_neg else 0x2ecc71
            gex_title = "📊 SPY GEX | Macro Context" + (" 🧪" if is_test else "")
            send_essentials_embed(WEBHOOK_CORNERSTONE, gex_title, gex_snippet, gex_color)
    except Exception:
        pass

    title = "☕️ Cornerstone Flowstate" + (" 🧪 TEST" if is_test else "")
    color = 0xe74c3c if worst_tier == "CRITICAL" else (0xf1c40f if worst_tier == "ELEVATED" else 0x2ecc71)
    dispatch_cornerstone_alert(title, full_report, color)
    db.update_state("cornerstone_alert_tier_rank", TIER_RANK.get(worst_tier, 0))
    if worst_tier in ("ELEVATED", "CRITICAL"):
        db.update_state(f"cornerstone_alert_fired_{datetime.now().strftime('%Y-%m-%d')}", True)

    # ── Income channel box efficiency snippet (once per day, box-active only) ──
    # Dispatches borrowing-efficiency metrics to #dividend-ccetfs as income context.
    # NO CLM/CRF data included — this is pure capital-cost optimization content.
    # Reads box_context_daily written by compute_cornerstone_reports().
    try:
        _bctx = db.get_state("box_context_daily") or {}
        _bctx_date = _bctx.get("date", "")
        _today_str = datetime.now(pytz.timezone("Pacific/Honolulu")).strftime("%Y-%m-%d")
        if (_bctx_date == _today_str and int(_bctx.get("active_count", 0)) > 0
                and HAS_ESSENTIALS and WEBHOOK_DIVIDEND):
            _bpos    = _bctx.get("positions", [])
            _avail   = _bctx.get("best_available_rate")
            _savings = _bctx.get("margin_savings_pct", 0.0)
            _lines   = []
            for _p in _bpos:
                _rate = _p.get("rate") or 0
                _dte  = _p.get("dte") or "?"
                _ann  = _p.get("annual_interest") or 0
                _loan = _p.get("loan") or 0
                _lines.append(
                    f"┣ Active: `${_loan:,.0f}` @ `{_rate:.2f}%` | DTE `{_dte}d` | "
                    f"Interest cost `${_ann:.0f}/yr`"
                )
            _avail_line = (
                f"┣ Best available rate today: `{_avail:.2f}%`\n" if _avail else ""
            )
            _snippet = (
                f"**📦 Box Spread Borrowing — Capital Efficiency**\n"
                + "\n".join(_lines) + "\n"
                + _avail_line
                + f"┣ vs E*TRADE margin `{margin_rate:.2f}%` | "
                  f"Saving `{_savings:.2f}%` on deployed capital\n"
                + f"┗ Roll at 30 DTE — check box_spread_scan for new rate"
            )
            send_essentials_embed(
                WEBHOOK_DIVIDEND,
                "📦 Box Spread Cost-of-Capital",
                _snippet, 0x3498db
            )
            logger.info("Box efficiency snippet dispatched to #dividend-ccetfs.")
    except Exception as _be:
        logger.warning(f"Box income snippet failed: {_be}")

    # ── Carry spread Pushover alert — tier-transition + value-change dedup ────
    # Fires on: (1) tier change (safe→warning, warning→critical, or recovery),
    # or (2) value shifts ≥0.5% since last alert. NOT on every daily pulse —
    # a stable +4.2% spread doesn't need a daily nag. Weekly fallback prevents
    # total silence if spread drifts slowly without crossing a tier boundary.
    _CARRY_TIERS = {
        "SAFE":     carry_spread >= 5.0,
        "WARNING":  2.0 <= carry_spread < 5.0,
        "CRITICAL": carry_spread < 2.0,
    }
    _carry_tier_now  = next(k for k, v in _CARRY_TIERS.items() if v)
    _carry_tier_prev = db.get_state("carry_spread_last_tier") or "SAFE"
    _carry_last_val  = float(db.get_state("carry_spread_last_alerted_val") or 0.0)
    _carry_last_week = db.get_state("carry_spread_last_alerted_week") or ""
    _this_week       = datetime.now().strftime("%Y-W%W")

    _tier_changed  = _carry_tier_now != _carry_tier_prev
    _val_drifted   = abs(carry_spread - _carry_last_val) >= 0.5
    _weekly_due    = _carry_last_week != _this_week   # fallback: at least once/week

    _should_alert  = carry_spread < 5.0 and (_tier_changed or _val_drifted or _weekly_due)

    if _should_alert:
        db.update_state("carry_spread_last_tier",         _carry_tier_now)
        db.update_state("carry_spread_last_alerted_val",  round(carry_spread, 2))
        db.update_state("carry_spread_last_alerted_week", _this_week)
        _p_tok = os.getenv("PUSHOVER_API_TOKEN")
        _p_usr = os.getenv("PUSHOVER_USER_KEY")
        if _p_tok and _p_usr:
            try:
                _carry_data = db.get_state("carry_spread_data") or {}
                _change_note = (
                    f"Tier changed: {_carry_tier_prev} → {_carry_tier_now}" if _tier_changed
                    else (f"Drift: was {_carry_last_val:+.1f}%, now {carry_spread:+.1f}%" if _val_drifted
                          else "Weekly carry check")
                )
                _msg = (
                    f"Carry spread: {carry_spread:+.1f}% ({_change_note})\n"
                    f"Tier 2 yield {_carry_data.get('tier2_yield', TIER2_ACTIVE_BLENDED):.1f}% "
                    f"vs margin {_carry_data.get('margin_rate', margin_rate):.2f}%\n"
                    + ("CRITICAL: spread near zero — pause new margin draws" if carry_spread < 2.0
                       else "WARNING: positive carry shrinking — monitor margin rate")
                )
                requests.post(
                    "https://api.pushover.net/1/messages.json",
                    data={"token": _p_tok, "user": _p_usr,
                          "title": "⚠️ MARGIN CARRY ALERT",
                          "message": _msg,
                          "priority": 1 if carry_spread < 2.0 else 0},
                    timeout=10,
                )
                logger.info(f"Carry spread Pushover alert fired: {carry_spread:+.1f}% ({_change_note})")
            except Exception as _e:
                logger.warning(f"Carry spread Pushover failed: {_e}")
    else:
        logger.info(f"Carry spread {carry_spread:+.1f}% ({_carry_tier_now}) — no alert (no tier/value change)")

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
    full_report, worst_tier, _carry = compute_cornerstone_reports()
    current_rank = TIER_RANK.get(worst_tier, 0)
    prev_rank    = int(db.get_state("cornerstone_alert_tier_rank", 0))

    if current_rank > prev_rank and current_rank > 0:
        if can_broadcast("cornerstone", is_major=True):
            logger.warning(f"🚨 Escalation: {worst_tier} (was rank {prev_rank}) — firing immediate alert.")
            # Soften title when ex-div window is active — avoids false-alarm panic
            if is_near_ex_dividend_window() and worst_tier == "ELEVATED":
                title = "📅 CORNERSTONE — ELEVATED (ex-div window active)"
                color = 0xf39c12   # amber — caution, not emergency
            else:
                title = "🚨🚨 CORNERSTONE — IMMEDIATE ACTION REQUIRED 🚨🚨"
                color = 0xe74c3c
            dispatch_cornerstone_alert(title, full_report, color)
            increment_alert_count("cornerstone")
            # Flag for #announcements accuracy index — cornerstone fired today
            _today_flag = datetime.now().strftime("%Y-%m-%d")
            db.update_state(f"cornerstone_alert_fired_{_today_flag}", True)

    db.update_state("cornerstone_alert_tier_rank", current_rank)
    return full_report, worst_tier

# ─────────────────────────────────────────────────────────────────────────────
# MAIN MONITOR LOOP
# ─────────────────────────────────────────────────────────────────────────────

def _cli_log_dodge():
    """
    python monitor.py log-dodge
    Records that the user has executed the RO dodge (sold to ≥3 shares).
    Fetches current price for each ticker, writes journal entry, confirms DB state.
    Run this on Monday after selling — it gives the system a timestamped sell price
    so future re-entry signals can calculate the round-trip P&L.
    """
    session = requests.Session()
    for _t in ("CLM", "CRF"):
        try:
            _pr = session.get(
                f"https://api.twelvedata.com/price?symbol={_t}&apikey={TD_API_KEY}",
                timeout=15
            ).json()
            _sell_px = float(_pr.get("price", 0) or 0)
        except Exception:
            _sell_px = 0.0

        _today = datetime.now().strftime("%Y-%m-%d")
        db.update_state(f"ro_dodge_executed_{_t}", {"date": _today, "sell_price": _sell_px})
        # Ensure anchor keys are set (in case off-hours path set them correctly)
        if not db.get_state(f"cornerstone_n2_detected_{_t}", ""):
            db.update_state(f"cornerstone_n2_detected_{_t}", "2026-08-14")
        if not db.get_state(f"ro_dodge_active_{_t}", ""):
            db.update_state(f"ro_dodge_active_{_t}", _today)

        try:
            db.log_journal_entry(
                strategy="CLM_CRF",
                event_type="RO_DODGE_EXECUTED",
                ticker=_t,
                action="DODGE_EXECUTED",
                conviction=5,
                thesis=(
                    f"{_t} RO dodge executed {_today} — sold to ≥3 shares at "
                    f"${_sell_px:.2f}. N-2 filed 2026-08-14. Proceeds → margin paydown. "
                    f"Re-entry scorers now active; monitoring for premium collapse + yield floor."
                ),
                confluences={"sell_price": _sell_px, "execution_date": _today,
                             "n2_filed": "2026-08-14", "keep_shares": 3},
                conflicts={},
                entry_price=_sell_px,
            )
        except Exception as _je:
            logger.warning(f"Journal write failed for {_t}: {_je}")

        print(f"[{_t}] Dodge logged — sell price ${_sell_px:.2f}, re-entry tracking active.")

    # Print current re-entry score state
    print("\nCurrent re-entry state:")
    for _t in ("CLM", "CRF"):
        _n2  = db.get_state(f"cornerstone_n2_detected_{_t}", "(not set)")
        _dod = db.get_state(f"ro_dodge_active_{_t}", "(not set)")
        _sc  = db.get_state(f"{_t}_reentry_score", "(pending — run next RTH scan)")
        print(f"  {_t}: N-2={_n2} | dodge_active={_dod} | re-entry_score={_sc}")


def run_monitor():
    tz_h = pytz.timezone('Pacific/Honolulu')

    # CLI modes — fire once and exit
    if len(sys.argv) > 1:
        _cmd = sys.argv[1].lower()
        if _cmd in ["test", "force"]:
            send_daily_pulse(is_test=True)
            return
        if _cmd == "log-dodge":
            _cli_log_dodge()
            return

    logger.info("⏳ [Engine Loop] Cornerstone monitor active. DB state tracking enabled.")
    # WS removed: the callback had a 300s debounce — identical to the REST polling
    # interval. Multiple monitor.py process restarts were each opening a new SDK
    # WebSocket connection (module-level singleton doesn't persist across processes),
    # creating N concurrent connection storms that hammered TD and burned CPU.
    # REST polling every 5 min is the protection engine — WS adds no unique value here.

    # Startup jitter: random 3–18s offset so monitor.py loop ticks don't land
    # exactly on the :00 boundary alongside market_analysis.py (18:00 UTC).
    # Reduces simultaneous TD credit consumption without affecting functionality.
    import random as _rand
    _startup_jitter = _rand.randint(3, 18)
    logger.info(f"[Loop Jitter] Startup offset {_startup_jitter}s — desync from market_analysis.py")
    time.sleep(_startup_jitter)

    while True:
        now_utc   = datetime.now(timezone.utc)
        now_utc_h = now_utc.hour
        rth = 13 <= now_utc_h < 21   # Regular Trading Hours (13:00–21:00 UTC)

        # ── Rate limit cooldown gate ──────────────────────────────────────────
        # When a 429 fires anywhere in the loop, _td_cooldown_until is set to
        # time.time() + 65 (one full minute). Skip all TD REST calls this tick.
        _td_cooldown_until = float(db.get_state("td_cooldown_until") or 0.0)
        _td_cooling = time.time() < _td_cooldown_until
        if _td_cooling:
            _secs_left = int(_td_cooldown_until - time.time())
            logger.info(f"[TD Cooldown] Skipping TD calls — {_secs_left}s remaining on rate limit window")

        try:
            if rth and not _td_cooling:
                # ── Full scan during market hours
                # Price/NAV/RSI fetches, WS-triggered escalation, seasonal check
                check_and_escalate_if_critical()
                check_and_dispatch_seasonal_caution()
            elif rth and _td_cooling:
                logger.info("[TD Cooldown] RTH tick skipped — rate limit cooldown active")
            else:
                # ── Off-hours: SEC/EDGAR only — N-2 and SC 13D/G filings drop 24/7
                # Skip the expensive Twelve Data REST calls (no prices to act on)
                try:
                    _sec_session = requests.Session()
                    for _ticker in ("CLM", "CRF"):
                        result = check_sec_edgar(_sec_session, _ticker)
                        if not result:
                            continue

                        has_n2  = "N-2" in result
                        has_13x = "13D" in result or "13G" in result

                        if has_n2:
                            # ── N-2: fire ONCE per RO cycle, then go silent.
                            # The daily pulse at 0800 HST carries continued RO status
                            # via the re-entry tracker block. Off-hours spam adds no value.
                            _cycle_alert_key = f"cornerstone_n2_initial_alerted_{_ticker}"
                            _already_alerted = db.get_state(_cycle_alert_key, "")

                            _today_str = datetime.now().strftime("%Y-%m-%d")

                            # Set DB anchor regardless (idempotent — safe to call every tick)
                            _n2_anchor = f"cornerstone_n2_detected_{_ticker}"
                            if not db.get_state(_n2_anchor, ""):
                                db.update_state(_n2_anchor, _today_str)
                                db.update_state(f"ro_dodge_active_{_ticker}", _today_str)
                                db.update_state(f"cornerstone_ro_dip_fired_{_ticker}", "")
                                db.update_state(f"cornerstone_floor_reentry_fired_{_ticker}", "")
                                logger.info(f"[Off-hours N-2] {_ticker} — cycle anchor set, dodge active.")
                                try:
                                    db.log_journal_entry(
                                        strategy="CLM_CRF",
                                        event_type="RO_N2_DETECTED",
                                        ticker=_ticker,
                                        action="DODGE",
                                        conviction=5,
                                        thesis=(
                                            f"{_ticker} N-2 RO Registration detected off-hours "
                                            f"({_today_str}). Sell to ≥3 shares at next market open "
                                            f"to preserve DRIP NAV eligibility permanently. "
                                            f"Full filing signal: {result}"
                                        ),
                                        confluences={"detection_time": datetime.now().isoformat(),
                                                     "filing_signal": result},
                                        conflicts={},
                                        entry_price=0.0,
                                    )
                                except Exception:
                                    pass

                            if not _already_alerted:
                                # First alert for this RO cycle — clean format, N-2 only
                                _ann_div   = 1.4268 if _ticker == "CLM" else 1.3824
                                _nav_fb    = 6.73   if _ticker == "CLM" else 6.18  # Aug 16 2026 N-2 filing NAV
                                _fv        = round(_ann_div / 0.19, 2)
                                # Re-entry range: NAV (post-dilution bottom) to FV (income buyer floor)
                                _re_lo     = _nav_fb
                                _re_hi     = _fv

                                # Extract filing date from result if present
                                import re as _re
                                _date_match = _re.search(r"\((\d{4}-\d{2}-\d{2})\)", result)
                                _filing_date = _date_match.group(1) if _date_match else _today_str

                                _n2_msg = (
                                    f"🚨 **Off-hours EDGAR SEC filing detected**\n"
                                    f"┣ Ticker: **{_ticker}**\n"
                                    f"┣ Signal: N-2 RO Registration ({_filing_date})\n"
                                    f"┣ Re-entry range: `${_re_lo:.2f} – ${_re_hi:.2f}` "
                                    f"(NAV floor → {_ann_div/0.19*100:.0f}% yield target)\n"
                                    f"┗ Status: Dodge active — sell to ≥3 shares at market open"
                                )
                                dispatch_cornerstone_alert(
                                    f"🚨 N-2 RO Registration — {_ticker}",
                                    _n2_msg,
                                    color=0xe74c3c,
                                    attach_chart=False,
                                )
                                db.update_state(_cycle_alert_key, _today_str)
                                logger.warning(f"[Off-hours N-2] {_ticker} initial alert fired — silent until re-entry.")
                            else:
                                # Already alerted this cycle — log silently, no Discord
                                logger.info(f"[Off-hours N-2] {_ticker} still active (alerted {_already_alerted}) — skipping repeat.")

                        elif has_13x:
                            # 13D/13G: allow once per day via existing 3-notification cap
                            if can_broadcast("cornerstone", is_major=True):
                                _today_str = datetime.now().strftime("%Y-%m-%d")
                                _13x_type = "13D" if "13D" in result else "13G"
                                _13x_msg = (
                                    f"⚠️ Large holder change detected\n"
                                    f"┣ Ticker: **{_ticker}**\n"
                                    f"┣ Filing: SC {_13x_type} — institutional position change\n"
                                    f"┗ Action: Monitor — SC 13D/G signals significant holder movement"
                                )
                                dispatch_cornerstone_alert(
                                    f"⚠️ {_ticker} — Large Holder Change (SC {_13x_type})",
                                    _13x_msg,
                                    color=0xf39c12,
                                    attach_chart=False,
                                )
                                increment_alert_count("cornerstone")
                                logger.warning(f"[Off-hours 13x] {_ticker}: {_13x_type} alert dispatched.")
                except Exception as e:
                    logger.warning(f"[Off-hours SEC] check_sec_edgar error: {e}")

            # ── 0800 HST daily pulse gate (always active — fires once per calendar day)
            now          = datetime.now(tz_h)
            current_date = now.strftime("%Y-%m-%d")
            last_pulse   = db.get_state("last_monitor_pulse_date", "")

            # 08:10 HST = 18:10 UTC — intentionally 10 min after market_analysis.py's
            # 0800 HST cycle to avoid Twelve Data 429 credit contention (both scripts
            # hit the same TD per-minute limit simultaneously at 18:00 UTC).
            if (now.hour > 8 or (now.hour == 8 and now.minute >= 10)) and last_pulse != current_date:
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
