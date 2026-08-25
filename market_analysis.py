"""
market_analysis.py — Always-on Morning Command Center.

Runs as the 6th PythonAnywhere always-on task. Internal 60-second tick loop
with DB-deduped firing at two daily windows (EOD is via scheduler.py):

  13:10 UTC (03:10 HST) → Full morning synthesis brief
  17:00 UTC (07:00 HST) → Mid-session pulse (intraday context update)
  20:20 UTC (10:20 HST) → EOD brief via scheduler.py --mode eod (not this script)

Synthesizes ALL ecosystem feeds into a single #market-analysis embed:
  • FRED macro (VIX, yield curve, Fed Funds, HY spread)
  • VIXY z-score regime
  • SPY/QQQ premarket quotes (Twelve Data)
  • Fear & Greed (Alternative.me)
  • CLM/CRF premium z-score (from monitor.py DB state)
  • TQQQ cycle scores (from tqqq.py DB state)
  • Wheel open positions (from DB)
  • Bias-flag scoring → BULLISH / NEUTRAL / BEARISH posture label

PythonAnywhere CPU rules: REST only, no SDK threads, all FRED cached daily.
"""

import os
import sys
import time
import logging
import traceback
import requests
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone

from dotenv import load_dotenv
from database import EcosystemDatabase
from analytics import HighFidelityAnalyticsEngine

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("MarketAnalysis")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s [MarketAnalysis] %(levelname)s %(message)s"))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

# ── Config ────────────────────────────────────────────────────────────────────
WEBHOOK_MARKET_ANALYSIS = os.getenv("WEBHOOK_MARKET_ANALYSIS")
TWELVE_DATA_API_KEY     = os.getenv("TWELVE_DATA_API_KEY")

# Fire times (UTC hour, minute) and their DB dedup keys
FIRE_SCHEDULE = [
    (13, 10, "ma_morning",    "morning"),    # 03:10 HST — shifted +70min so scheduler.py morning
                                             # (12:50 UTC) has time to write SPY/QQQ POC/VAH/VAL
                                             # and expected-range DB keys before we read them.
    # ma_headlines removed — headlines merged into morning brief as a single summary line
    (17,  0, "ma_intraday",   "intraday"),   # 07:00 HST mid-session
    # ma_eod disabled — scheduler.py --mode eod (20:20 UTC) produces a richer EOD recap
    # (morning call accuracy, signal grading). market_analysis.py EOD duplicated it.
]
FIRE_WINDOW_MIN = 2   # ± minutes around target time


# ── Helpers ───────────────────────────────────────────────────────────────────

def _in_window(now_h: int, now_m: int, t_h: int, t_m: int) -> bool:
    return abs((now_h * 60 + now_m) - (t_h * 60 + t_m)) <= FIRE_WINDOW_MIN


def _already_fired(db: EcosystemDatabase, key: str, date_str: str) -> bool:
    return bool(db.get_state(f"mktana_fired_{key}_{date_str}"))


def _mark_fired(db: EcosystemDatabase, key: str, date_str: str):
    db.update_state(f"mktana_fired_{key}_{date_str}", True)


def _send_embed(title: str, description: str, color: int):
    if not WEBHOOK_MARKET_ANALYSIS:
        logger.warning("WEBHOOK_MARKET_ANALYSIS not set — skipping Discord dispatch.")
        return
    try:
        payload = {
            "embeds": [{
                "title":       title,
                "description": description,
                "color":       color,
                "footer":      {"text": "Not financial advice — educational/informational use only."},
            }]
        }
        r = requests.post(WEBHOOK_MARKET_ANALYSIS, json=payload, timeout=10)
        if r.status_code not in (200, 204):
            logger.error(f"Discord dispatch failed: HTTP {r.status_code} — {r.text[:200]}")
        else:
            logger.info(f"Dispatched: {title}")
    except Exception as e:
        logger.error(f"Discord dispatch error: {e}")


# ── Data Fetchers ─────────────────────────────────────────────────────────────

def _fetch_fear_and_greed() -> tuple:
    """Returns (value: int, classification: str). Fallback (50, 'Neutral')."""
    try:
        r = requests.get("https://api.alternative.me/fng/", timeout=8).json()
        val   = int(r["data"][0]["value"])
        label = r["data"][0]["value_classification"]
        return val, label
    except Exception as e:
        logger.warning(f"Fear & Greed fetch failed: {e}")
        return 50, "Neutral"


def _fetch_spy_qqq_quote(engine: HighFidelityAnalyticsEngine) -> dict:
    """
    Twelve Data /quote for SPY and QQQ — returns price + percent_change.
    Uses engine's cached quote method to avoid redundant API hits.
    """
    result = {}
    for sym in ["SPY", "QQQ"]:
        try:
            url = f"https://api.twelvedata.com/quote?symbol={sym}&apikey={TWELVE_DATA_API_KEY}"
            r = requests.get(url, timeout=10).json()
            if r.get("status") == "error":
                continue
            result[sym] = {
                "price":          float(r.get("close", 0) or r.get("price", 0) or 0),
                "percent_change": float(r.get("percent_change", 0) or 0),
                "previous_close": float(r.get("previous_close", 0) or 0),
                "open":           float(r.get("open", 0) or 0),
            }
        except Exception as e:
            logger.warning(f"Quote fetch failed for {sym}: {e}")
    return result


def _fetch_futures_context(engine: HighFidelityAnalyticsEngine) -> dict:
    """
    /NQ and /ES proxies via QQQ + SPY futures quotes from Twelve Data.
    Returns direction flags for the bias scorer.
    """
    try:
        # Use existing SPY/QQQ data from engine cache if available
        spy_data = {}
        qqq_data = {}
        for sym, store in [("SPY", spy_data), ("QQQ", qqq_data)]:
            try:
                url = f"https://api.twelvedata.com/quote?symbol={sym}&apikey={TWELVE_DATA_API_KEY}"
                r = requests.get(url, timeout=10).json()
                chg = float(r.get("percent_change", 0) or 0)
                store["chg"] = chg
            except Exception:
                store["chg"] = 0.0
        return {"spy_chg": spy_data.get("chg", 0.0), "qqq_chg": qqq_data.get("chg", 0.0)}
    except Exception:
        return {"spy_chg": 0.0, "qqq_chg": 0.0}


# ── Bias-Flag Scoring ─────────────────────────────────────────────────────────

def _calculate_bias_score(engine: HighFidelityAnalyticsEngine, db: EcosystemDatabase) -> dict:
    """
    12+ signal flags, each weighted. Sum → bias_score → label.
    BULLISH: ≥ +20 | NEUTRAL: -19 to +19 | BEARISH: ≤ -20

    Returns dict with bias_score, label, flags_detail, and raw signal values.
    """
    score   = 0
    details = []
    signals = {}

    # ── 1. FRED Real VIX ──────────────────────────────────────────────────────
    try:
        real_vix = engine.fetch_real_vix()
        if real_vix is None:
            real_vix = float(db.get_state("fred_vix_value") or 20.0)
        signals["real_vix"] = real_vix
        if real_vix < 15:
            score += 20
            details.append(f"VIX {real_vix:.1f} → LOW vol regime (+20)")
        elif real_vix < 20:
            score += 10
            details.append(f"VIX {real_vix:.1f} → calm (+10)")
        elif real_vix > 30:
            score -= 20
            details.append(f"VIX {real_vix:.1f} → PANIC regime (-20)")
        elif real_vix > 22:
            score -= 10
            details.append(f"VIX {real_vix:.1f} → elevated (-10)")
    except Exception as e:
        logger.warning(f"Bias: VIX fetch failed: {e}")
        real_vix = 20.0
        signals["real_vix"] = real_vix

    # ── 2. VIXY z-score (intraday fear regime) ────────────────────────────────
    try:
        vixy_price, vixy_z = engine.fetch_vixy_proxy()
        signals["vixy_z"] = vixy_z
        if vixy_z < -0.5:
            score += 10
            details.append(f"VIXY z {vixy_z:+.2f}σ → suppressed fear (+10)")
        elif vixy_z > 1.5:
            score -= 15
            details.append(f"VIXY z {vixy_z:+.2f}σ → fear spike (-15)")
        elif vixy_z > 0.8:
            score -= 7
            details.append(f"VIXY z {vixy_z:+.2f}σ → rising fear (-7)")
    except Exception as e:
        logger.warning(f"Bias: VIXY failed: {e}")
        vixy_price, vixy_z = 20.0, 0.0
        signals["vixy_z"] = 0.0

    # ── 3. Yield curve (FRED) ─────────────────────────────────────────────────
    try:
        yc = engine.fetch_yield_curve()
        signals["yield_spread"] = yc["spread"] if yc else None
        if yc:
            if not yc["inverted"] and yc["spread"] > 0.5:
                score += 10
                details.append(f"Yield curve +{yc['spread']:.2f}% → normal (+10)")
            elif yc["inverted"] and yc["spread"] < -0.3:
                score -= 10
                details.append(f"Yield curve {yc['spread']:.2f}% → inverted (-10)")
    except Exception as e:
        logger.warning(f"Bias: yield curve failed: {e}")

    # ── 4. HY credit spread (FRED) ────────────────────────────────────────────
    try:
        hy = engine.fetch_hy_spread()
        signals["hy_spread"] = hy
        if hy > 0:
            if hy < 4.0:
                score += 10
                details.append(f"HY spread {hy:.2f}% → healthy credit (+10)")
            elif hy > 6.0:
                score -= 15
                details.append(f"HY spread {hy:.2f}% → credit stress (-15)")
            elif hy > 5.0:
                score -= 7
                details.append(f"HY spread {hy:.2f}% → credit caution (-7)")
    except Exception as e:
        logger.warning(f"Bias: HY spread failed: {e}")

    # ── 5. Fear & Greed ───────────────────────────────────────────────────────
    try:
        fg_val, fg_class = _fetch_fear_and_greed()
        signals["fear_greed"] = fg_val
        if fg_val >= 70:
            score += 10
            details.append(f"F&G {fg_val} ({fg_class}) → greed (+10)")
        elif fg_val <= 30:
            score -= 10
            details.append(f"F&G {fg_val} ({fg_class}) → fear (-10)")
        elif fg_val >= 55:
            score += 5
            details.append(f"F&G {fg_val} → mild greed (+5)")
        elif fg_val <= 45:
            score -= 5
            details.append(f"F&G {fg_val} → mild fear (-5)")
    except Exception as e:
        logger.warning(f"Bias: F&G failed: {e}")
        fg_val, fg_class = 50, "Neutral"
        signals["fear_greed"] = 50

    # ── 6. SPY premarket direction ────────────────────────────────────────────
    try:
        futures = _fetch_futures_context(engine)
        spy_chg = futures["spy_chg"]
        qqq_chg = futures["qqq_chg"]
        signals["spy_chg"] = spy_chg
        signals["qqq_chg"] = qqq_chg
        if spy_chg > 0.5:
            score += 15
            details.append(f"SPY {spy_chg:+.2f}% → strong premarket (+15)")
        elif spy_chg > 0.1:
            score += 7
            details.append(f"SPY {spy_chg:+.2f}% → mild premarket bid (+7)")
        elif spy_chg < -0.8:
            score -= 15
            details.append(f"SPY {spy_chg:+.2f}% → strong premarket sell (-15)")
        elif spy_chg < -0.3:
            score -= 7
            details.append(f"SPY {spy_chg:+.2f}% → mild premarket weakness (-7)")
    except Exception as e:
        logger.warning(f"Bias: SPY/QQQ failed: {e}")
        spy_chg, qqq_chg = 0.0, 0.0
        signals.update({"spy_chg": 0.0, "qqq_chg": 0.0})

    # ── 7. CLM/CRF premium z-score (from monitor.py DB) ──────────────────────
    try:
        clm_z = float(db.get_state("clm_last_z_premium") or 0.0)
        crf_z = float(db.get_state("crf_last_z_premium") or 0.0)
        avg_z = (clm_z + crf_z) / 2
        signals["cef_premium_z"] = round(avg_z, 2)
        if avg_z >= 2.0:
            score -= 10
            details.append(f"CLM/CRF premium z {avg_z:+.1f}σ → RO risk elevated (-10)")
        elif avg_z < 0:
            score += 5
            details.append(f"CLM/CRF premium z {avg_z:+.1f}σ → below mean (+5)")
    except Exception as e:
        logger.warning(f"Bias: CLM/CRF z-score read failed: {e}")

    # ── 8. TQQQ cycle signal (from tqqq.py DB) ───────────────────────────────
    try:
        bottom_score = int(db.get_state("tqqq_bottom_score") or 0)
        top_score    = int(db.get_state("tqqq_top_score") or 0)
        signals["tqqq_bottom"] = bottom_score
        signals["tqqq_top"]    = top_score
        if bottom_score >= 55:
            score += 10
            details.append(f"TQQQ bottom score {bottom_score}/100 → CALL desk unlocked (+10)")
        if top_score >= 55:
            score -= 10
            details.append(f"TQQQ top score {top_score}/100 → PUT desk unlocked (-10)")
    except Exception as e:
        logger.warning(f"Bias: TQQQ cycle read failed: {e}")

    # ── 9. ORB intraday bias (from scheduler.py orb_scan DB key) ─────────────
    try:
        from datetime import date as _ma_date
        _orb_key = f"orb_intraday_bias_{_ma_date.today().isoformat()}"
        orb_bias = db.get_state(_orb_key)
        if orb_bias == "BULLISH":
            score += 8
            details.append("ORB intraday bias: BULLISH (SPY/QQQ broke out above range, +8)")
        elif orb_bias == "BEARISH":
            score -= 8
            details.append("ORB intraday bias: BEARISH (SPY/QQQ broke out below range, -8)")
        signals["orb_bias"] = orb_bias or "NEUTRAL"
    except Exception as e:
        logger.warning(f"Bias: ORB read failed: {e}")

    # ── 10. VIX term structure (from tqqq.py DB) ─────────────────────────────
    try:
        vix_term_slope = float(db.get_state("vix_term_slope") or 0.0)
        signals["vix_term_slope"] = vix_term_slope
        if vix_term_slope >= 3.0:
            score -= 12
            details.append(f"VIX term slope {vix_term_slope:+.2f} → backwardation/fear (-12)")
        elif vix_term_slope >= 1.5:
            score -= 7
            details.append(f"VIX term slope {vix_term_slope:+.2f} → vol front-loaded (-7)")
        elif vix_term_slope <= -2.0:
            score += 8
            details.append(f"VIX term slope {vix_term_slope:+.2f} → deep contango/calm (+8)")
        elif vix_term_slope <= -0.5:
            score += 4
            details.append(f"VIX term slope {vix_term_slope:+.2f} → contango (+4)")
    except Exception as e:
        logger.warning(f"Bias: VIX term slope read failed: {e}")
        vix_term_slope = 0.0
        signals["vix_term_slope"] = 0.0

    # ── 11. SPY 50-day SMA (trend regime) ────────────────────────────────────
    try:
        spy_ts = engine._execute_query("time_series", {
            "symbol": "SPY", "interval": "1day", "outputsize": 60
        })
        if spy_ts and spy_ts.get("values") and len(spy_ts["values"]) >= 50:
            closes = [float(v["close"]) for v in spy_ts["values"][:50]]
            spy_sma50 = round(sum(closes) / 50, 2)
            spy_spot = float(spy_ts["values"][0]["close"])
            signals["spy_sma50"] = spy_sma50
            signals["spy_spot"]  = spy_spot
            pct_vs_sma = round((spy_spot / spy_sma50 - 1) * 100, 2)
            if pct_vs_sma > 2.0:
                score += 10
                details.append(f"SPY {pct_vs_sma:+.1f}% above SMA50 → trend intact (+10)")
            elif pct_vs_sma > 0:
                score += 5
                details.append(f"SPY {pct_vs_sma:+.1f}% above SMA50 → mildly bullish (+5)")
            elif pct_vs_sma < -3.0:
                score -= 12
                details.append(f"SPY {pct_vs_sma:+.1f}% below SMA50 → trend broken (-12)")
            elif pct_vs_sma < 0:
                score -= 5
                details.append(f"SPY {pct_vs_sma:+.1f}% below SMA50 → momentum shifted (-5)")
    except Exception as e:
        logger.warning(f"Bias: SPY SMA50 failed: {e}")

    # ── 12. Market breadth (from tqqq.py DB — % stocks above SMA200) ─────────
    try:
        breadth = db.get_state("tqqq_breadth_cache")
        if breadth is not None:
            breadth = float(breadth)
            signals["breadth"] = breadth
            if breadth >= 70:
                score += 8
                details.append(f"Breadth {breadth:.0f}% above SMA200 → broad participation (+8)")
            elif breadth <= 35:
                score -= 10
                details.append(f"Breadth {breadth:.0f}% above SMA200 → narrow market (-10)")
            elif breadth <= 50:
                score -= 4
                details.append(f"Breadth {breadth:.0f}% above SMA200 → deteriorating (-4)")
    except Exception as e:
        logger.warning(f"Bias: breadth read failed: {e}")

    # ── VIX day-over-day acceleration ────────────────────────────────────────
    try:
        prev_vix = db.get_state("fred_vix_prev")
        if prev_vix and real_vix:
            prev_vix = float(prev_vix)
            vix_chg_pct = round((real_vix - prev_vix) / prev_vix * 100, 1) if prev_vix > 0 else 0.0
            signals["vix_chg_pct"] = vix_chg_pct
            if vix_chg_pct >= 20:
                score -= 10
                details.append(f"VIX +{vix_chg_pct:.0f}% DoD → vol acceleration (-10)")
            elif vix_chg_pct >= 10:
                score -= 5
                details.append(f"VIX +{vix_chg_pct:.0f}% DoD → vol rising (-5)")
            elif vix_chg_pct <= -15:
                score += 8
                details.append(f"VIX {vix_chg_pct:.0f}% DoD → vol collapsing, fear fading (+8)")
        # Update previous VIX for next run
        if real_vix:
            db.update_state("fred_vix_prev", real_vix)
    except Exception as e:
        logger.warning(f"Bias: VIX DoD failed: {e}")

    # ── Label ─────────────────────────────────────────────────────────────────
    if score >= 20:
        label = "BULLISH"
        color = 0x2ecc71
    elif score <= -20:
        label = "BEARISH"
        color = 0xe74c3c
    else:
        label = "NEUTRAL"
        color = 0xf1c40f

    return {
        "bias_score":  score,
        "label":       label,
        "color":       color,
        "details":     details,
        "signals":     signals,
        "fg_val":      fg_val,
        "fg_class":    fg_class,
        "vixy_z":      vixy_z,
        "vixy_price":  vixy_price,
        "real_vix":    real_vix,
        "spy_chg":     spy_chg,
        "qqq_chg":     qqq_chg,
    }


# ── MarketWatch Headlines ─────────────────────────────────────────────────────

_MW_MARKET_KEYWORDS = {
    "stock", "stocks", "market", "markets", "nasdaq", "dow", "s&p", "spy", "qqq",
    "fed", "federal", "powell", "treasury", "yield", "yields", "bond", "bonds",
    "rate", "rates", "inflation", "cpi", "pce", "gdp",
    "sector", "chip", "chips", "tech", "ai", "semiconductor",
    "economy", "economic", "earnings", "revenue",
    "fund", "funds", "hedge", "portfolio", "investor", "investors",
    "oil", "crude", "energy", "commodity", "commodities",
    "rally", "selloff", "surge", "plunge", "gains", "losses", "decline",
    "lower", "higher", "fallen", "index", "indices", "bull", "bear",
    "correction", "crash", "options", "futures", "volatility", "vix",
}

_MW_BULLISH = {
    "rally", "gain", "gains", "rise", "higher", "surge", "beat", "breakout",
    "bullish", "strong", "record", "advance", "soar", "jump", "climbs",
    "climbed", "best", "boom", "outperform", "upgraded", "upgrade", "recover",
    "positive", "optimistic", "rebound",
}

_MW_BEARISH = {
    "selloff", "decline", "falls", "lower", "drop", "pullback", "retreat",
    "risk", "warning", "concern", "plunge", "slump", "tumble", "crash",
    "weakness", "downgrade", "bearish", "worst", "loss", "losses",
    "collapse", "fear", "threat", "trouble", "struggle", "drag", "pressure",
    "miss", "missed", "disappoint", "sell",
}


def _fetch_market_headlines(db: EcosystemDatabase) -> list:
    """
    Fetch MarketWatch bulletins RSS. Cached to DB once per calendar day.
    Returns list of dicts: {title, url, age_h, sentiment} (max 5, market-relevant only).
    Returns [] on stale feed or fetch failure.
    Credit cost: 0 — external RSS, no Twelve Data calls.
    """
    date_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cache_key = f"mw_headlines_{date_str}"
    cached    = db.get_state(cache_key)
    if isinstance(cached, list) and cached:
        return cached

    try:
        r = requests.get(
            "https://feeds.marketwatch.com/marketwatch/bulletins",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=12,
        )
        if r.status_code != 200:
            logger.warning(f"[MarketWatch] bulletins HTTP {r.status_code}")
            return []

        root  = ET.fromstring(r.text)
        items = root.findall(".//item")
        if not items:
            return []

        # Staleness guard — if the newest item is > 24h old the feed is broken
        try:
            newest_pub = parsedate_to_datetime(items[0].findtext("pubDate", ""))
            newest_age_h = (datetime.now(timezone.utc) - newest_pub).total_seconds() / 3600
            if newest_age_h > 24:
                logger.warning(f"[MarketWatch] feed stale ({newest_age_h:.0f}h) — skipping dispatch")
                return []
        except Exception:
            newest_age_h = 0.0

        results = []
        for item in items:
            title = item.findtext("title", "").strip()
            link  = item.findtext("link",  "").strip()
            pub   = item.findtext("pubDate", "").strip()
            if not title:
                continue

            # Relevance: at least one market keyword must appear in the title
            words = set(title.lower().replace("-", " ").replace("'", "").split())
            if not (words & _MW_MARKET_KEYWORDS):
                continue

            try:
                pub_dt = parsedate_to_datetime(pub)
                age_h  = (datetime.now(timezone.utc) - pub_dt).total_seconds() / 3600
            except Exception:
                age_h = 0.0

            b_hits = len(words & _MW_BULLISH)
            d_hits = len(words & _MW_BEARISH)
            sentiment = "bullish" if b_hits > d_hits else ("bearish" if d_hits > b_hits else "neutral")

            results.append({"title": title, "url": link, "age_h": age_h, "sentiment": sentiment})

        results = results[:5]
        if results:
            db.update_state(cache_key, results)
        return results

    except Exception as e:
        logger.warning(f"[MarketWatch] headlines fetch failed: {e}")
        return []


def _build_headlines_report(engine: HighFidelityAnalyticsEngine, db: EcosystemDatabase) -> tuple:
    """
    Second morning embed: MarketWatch bulletins headline digest.
    Fires 3 min after the morning brief (13:13 UTC / 03:13 HST).
    No emojis. Embed sidebar: green=bullish, yellow=mixed, red=bearish.
    """
    try:
        import zoneinfo
        now_hst    = datetime.now(timezone.utc).astimezone(zoneinfo.ZoneInfo("US/Hawaii"))
    except ImportError:
        now_hst = datetime.now(timezone.utc)
    date_label = now_hst.strftime("%a %b %-d")

    headlines = _fetch_market_headlines(db)

    if not headlines:
        return (
            f"MARKET HEADLINES — {date_label} | MarketWatch",
            "No market-relevant headlines in current feed. Feed may be between update cycles.",
            0xf1c40f,
        )

    b_count = sum(1 for h in headlines if h["sentiment"] == "bullish")
    d_count = sum(1 for h in headlines if h["sentiment"] == "bearish")
    n_count = sum(1 for h in headlines if h["sentiment"] == "neutral")

    if d_count > b_count:
        agg_label, color = "BEARISH",  0xe74c3c
    elif b_count > d_count:
        agg_label, color = "BULLISH",  0x2ecc71
    else:
        agg_label, color = "MIXED",    0xf1c40f

    lines = []
    for h in headlines:
        age_str = f"{h['age_h']:.0f}h ago" if h["age_h"] < 24 else f"{h['age_h']/24:.0f}d ago"
        pfx     = "(+)" if h["sentiment"] == "bullish" else ("(-)" if h["sentiment"] == "bearish" else "(~)")
        lines.append(f"{pfx} {h['title']}  `[{age_str}]`")

    freshness_h = headlines[0]["age_h"]
    freshness   = f"{freshness_h:.1f}h ago" if freshness_h < 24 else f"{freshness_h/24:.1f}d ago"

    body = (
        "\n".join(lines)
        + f"\n┣ **Aggregate: {agg_label}** ({b_count} bullish | {d_count} bearish | {n_count} neutral)"
        + f"\n┗ `MarketWatch Bulletins | Freshness: {freshness} | {len(headlines)} of 10 items market-relevant`"
    )

    return f"MARKET HEADLINES — {date_label} | MarketWatch", body, color


# ── Report Builders ───────────────────────────────────────────────────────────

def _wheel_idle_str(db: EcosystemDatabase) -> str:
    """Returns top-IVR opportunity when no wheel positions are open."""
    try:
        today_str = datetime.now().strftime("%Y-%m-%d")
        ws = db.get_state("wheel_candidates_snapshot")
        if ws and isinstance(ws, dict) and ws.get("date") == today_str and ws.get("high_count", 0) > 0:
            tops = ws.get("top_candidates", [])[:2]
            if tops:
                top_str = " | ".join(f"`{t['sym']}` IVR {t.get('ivr', 0):.0f}%" for t in tops)
                return f"Idle — top IVR: {top_str}"
    except Exception:
        pass
    return "Idle — run wheel_signals for setups"


def _build_morning_report(engine: HighFidelityAnalyticsEngine, db: EcosystemDatabase) -> tuple:
    """
    Full morning synthesis brief (0310 HST). Returns (title, description, color).
    """
    now_utc   = datetime.now(timezone.utc)   # used for Q1 tax-char gate (lines below)
    now_label = datetime.now().strftime("%a %b %-d")

    bias = _calculate_bias_score(engine, db)

    # Write daily bias to DB — scheduler.py wheel_signals Module 4 reads this to
    # layer directional posture on top of VIX-adjusted delta parameters.
    try:
        db.update_state("market_analysis_bias", {
            "label": bias["label"],
            "score": bias["bias_score"],
            "date":  datetime.now().strftime("%Y-%m-%d"),
        })
    except Exception:
        pass

    # Log prediction to signal_ledger — graded next trading day by announcements.py.
    # Only log directional calls (skip NEUTRAL/CHOP — no edge to score).
    try:
        if bias["label"] in ("BULLISH", "BEARISH"):
            _q = engine._fetch_twelve_data_quotes(["SPY"])
            spy_price = float((_q.get("SPY") or {}).get("close", 0) or 0)
            if spy_price > 0:
                db.log_prediction(
                    signal_type="market_direction",
                    ticker="SPY",
                    predicted_direction=bias["label"],
                    entry_price=spy_price,
                    target_days=1,
                    notes=f"bias_score={bias['bias_score']}/12+",
                )
    except Exception:
        pass

    sigs = bias["signals"]

    # ── OVERNIGHT MARKET STRUCTURE ────────────────────────────────────────────
    # Populated by scheduler.py --mode morning (12:50 UTC, 20min before this brief fires).
    # Reads SPY/QQQ POC/VAH/VAL + expected daily move ranges from DB — zero extra API calls.
    market_structure_section = ""
    try:
        _spy_poc = db.get_state("SPY_poc")
        _spy_vah = db.get_state("SPY_vah")
        _spy_val = db.get_state("SPY_val")
        _spy_up  = db.get_state("SPY_expected_upper")
        _spy_lo  = db.get_state("SPY_expected_lower")
        _qqq_up  = db.get_state("QQQ_expected_upper")
        _qqq_lo  = db.get_state("QQQ_expected_lower")
        _breadth = db.get_state("tqqq_breadth_cache")
        _gex_state  = db.get_state("SPY_session") or ""   # "RTH" / "OVERNIGHT" / ""
        if _spy_poc and _spy_up and _spy_lo:
            _breadth_str = f" | Breadth: `{float(_breadth):.0%}`" if _breadth else ""
            market_structure_section = (
                "\n**OVERNIGHT MARKET STRUCTURE**\n"
                f"┣ SPY: POC `${float(_spy_poc):,.2f}` | VAH `${float(_spy_vah):,.2f}` | VAL `${float(_spy_val):,.2f}`\n"
                f"┣ SPY range: `${float(_spy_lo):,.2f}` – `${float(_spy_up):,.2f}`"
                + (f" | QQQ range: `${float(_qqq_lo):,.2f}` – `${float(_qqq_up):,.2f}`" if _qqq_up else "")
                + f"{_breadth_str}\n"
                f"┗ Break above VAH = bullish continuation · Break below VAL = bearish extension\n"
            )
    except Exception as e:
        logger.warning(f"Morning: market structure DB read failed: {e}")

    # ── MACRO ENVIRONMENT ─────────────────────────────────────────────────────
    try:
        yc   = engine.fetch_yield_curve()
        snap = engine.fetch_fred_macro_snapshot()
        hy   = engine.fetch_hy_spread()
        real_vix = bias["real_vix"]
        _edm = real_vix / 15.874   # sqrt(252) — daily expected move from Natenberg (1994)
        _vix_regime = ('Calm. Options cheap.' if real_vix < 15 else
                       'Low vol.' if real_vix < 20 else
                       'Elevated. Size down.' if real_vix < 30 else
                       'PANIC. Defensive posture.')
        vix_line = f"`{real_vix:.1f}` ±`{_edm:.2f}%`/day — {_vix_regime}"
        # Yield curve: spread + raw 10Y/2Y rates (replacing Treasury & Macro standalone embed)
        if yc:
            yc_line = (
                f"`{yc['spread']:+.2f}%` {yc['label']} "
                f"(10Y `{yc['t10']:.2f}%` | 2Y `{yc['t2']:.2f}%`)"
            )
        else:
            yc_line = "N/A"
        ff_line  = f"`{snap.get('fedfunds', '?')}%` Fed Funds"
        hy_line  = f"`{hy:.2f}%` {'✅ healthy' if hy < 4.5 else '⚠️ stress' if hy < 6 else '🔴 crisis'}" if hy else "N/A"
        cpi_line = f"`{snap.get('cpi_yoy', '?')}%` CPI YoY" if snap.get("cpi_yoy") else ""
        urate    = snap.get("unrate")
    except Exception as e:
        logger.warning(f"Morning: macro section failed: {e}")
        yc_line = hy_line = ff_line = cpi_line = "N/A"
        vix_line = f"`{bias['real_vix']:.1f}`"
        urate = None

    macro_section = (
        "**MACRO ENVIRONMENT**\n"
        f"┣ VIX: {vix_line}\n"
        f"┣ Yield Curve: {yc_line}\n"
        f"┣ {ff_line} | HY Spread: {hy_line}\n"
    )
    if cpi_line and urate:
        macro_section += f"┗ {cpi_line} | Unemployment: `{urate:.1f}%`\n"
    elif cpi_line:
        macro_section += f"┗ {cpi_line}\n"
    else:
        macro_section = _swap_last_bullet(macro_section)

    # ── EQUITY PULSE ──────────────────────────────────────────────────────────
    spy_chg = bias["spy_chg"]
    qqq_chg = bias["qqq_chg"]
    vixy_z  = bias["vixy_z"]
    fg_val  = bias["fg_val"]
    fg_class = bias["fg_class"]

    def _arrow(v): return "▲" if v >= 0 else "▼"
    vol_label = "Low vol regime" if vixy_z < -0.5 else ("Rising fear" if vixy_z > 1.0 else "Normal vol")

    equity_section = (
        "\n**EQUITY PULSE**\n"
        f"┣ SPY: {_arrow(spy_chg)}{abs(spy_chg):.2f}% session | "
        f"QQQ: {_arrow(qqq_chg)}{abs(qqq_chg):.2f}% session\n"
        f"┣ VIXY z-score: `{vixy_z:+.2f}σ` — {vol_label}\n"
        f"┗ Fear & Greed: `{fg_val}` ({fg_class})\n"
    )

    # ── CROSS-CHANNEL SIGNALS ─────────────────────────────────────────────────
    try:
        clm_z     = float(db.get_state("clm_last_z_premium") or 0.0)
        crf_z     = float(db.get_state("crf_last_z_premium") or 0.0)
        clm_ro    = db.get_state("clm_last_ro_tier") or "LOW"
        crf_ro    = db.get_state("crf_last_ro_tier") or "LOW"
        clm_prem  = float(db.get_state("clm_last_premium") or 0.0)
        crf_prem  = float(db.get_state("crf_last_premium") or 0.0)
        clm_score = int(db.get_state("clm_last_ro_score") or 0)
        crf_score = int(db.get_state("crf_last_ro_score") or 0)
        # When N-2 is active, append note so negative z-score isn't misread as "safe"
        _clm_n2 = db.get_state("ro_dodge_active_CLM", "")
        _crf_n2 = db.get_state("ro_dodge_active_CRF", "")
        _clm_z_label = f"`{clm_z:+.1f}σ` (N-2: z irrelevant)" if _clm_n2 else f"`{clm_z:+.1f}σ`"
        _crf_z_label = f"`{crf_z:+.1f}σ` (N-2: z irrelevant)" if _crf_n2 else f"`{crf_z:+.1f}σ`"
        cef_line = (
            f"CLM z:{_clm_z_label} prem:`{clm_prem:.1f}%` RO:`{clm_score}/100` ({clm_ro}) | "
            f"CRF z:{_crf_z_label} prem:`{crf_prem:.1f}%` RO:`{crf_score}/100` ({crf_ro})"
        )
    except Exception:
        cef_line = "CLM/CRF: data pending monitor.py pulse"

    try:
        bottom_score = int(db.get_state("tqqq_bottom_score") or 0)
        top_score    = int(db.get_state("tqqq_top_score") or 0)
        if bottom_score >= 55:
            call_locked = f"🟢 CALL OPEN ({bottom_score}/100) — fear confirmed, LEAP entry eligible"
        elif bottom_score >= 40:
            call_locked = f"🔒 CALL ({bottom_score}/100) — approaching threshold, watch for fear spike"
        else:
            call_locked = f"🔒 CALL ({bottom_score}/100) — no fear signal, market calm"
        if top_score >= 55:
            put_locked = f"🔴 PUT OPEN ({top_score}/100) — extension confirmed, LEAP put eligible"
        elif top_score >= 40:
            put_locked = f"🔒 PUT ({top_score}/100) — building, watch overbought levels"
        else:
            put_locked = f"🔒 PUT ({top_score}/100) — not extended"
        tqqq_line = f"{call_locked} | {put_locked}"
    except Exception:
        tqqq_line = "TQQQ: awaiting cycle update"

    try:
        open_pos     = db.get_open_wheel_positions()
        pos_count    = len(open_pos)
        nearest_exp  = None
        notional     = 0.0
        if open_pos:
            today = datetime.now().date()
            exps  = []
            for p in open_pos:
                try:
                    d = datetime.strptime(p["expiration"], "%Y-%m-%d").date()
                    exps.append((d - today).days)
                except Exception:
                    pass
                try:
                    notional += float(p.get("strike", 0)) * int(p.get("contracts", 1)) * 100
                except Exception:
                    pass
            if exps:
                nearest_exp = min(exps)
        notional_str = f" | Notional: `${notional:,.0f}`" if notional > 0 else ""
        wheel_line = (
            f"{pos_count} open position{'s' if pos_count != 1 else ''}"
            + (f" | Nearest exp: {nearest_exp}d" if nearest_exp is not None else "")
            + notional_str
        ) if pos_count > 0 else _wheel_idle_str(db)
    except Exception:
        wheel_line = "Wheel: DB read pending"

    # MLPI entry signal — uses price data already fetched for bias scorer (no extra API calls).
    # Fires when energy sector (XLE) drops ≥ 1.5% OR yield curve steepened ≥ 20bps,
    # AND MLPI itself is also down (better entry price). All reads from cached DB values.
    mlpi_entry_line = ""
    try:
        xle_data  = engine._execute_query("price", {"symbol": "XLE"})
        mlpi_data = engine._execute_query("price", {"symbol": "MLPI"})
        xle_chg   = float((xle_data  or {}).get("percent_change", 0.0))
        mlpi_chg  = float((mlpi_data or {}).get("percent_change", 0.0))
        today_str = datetime.now().strftime("%Y-%m-%d")
        yc_spread = db.get_state("fred_yield_spread")
        yc_prev   = db.get_state("fred_yield_spread_prev")
        yc_date   = db.get_state("fred_yield_spread_date")
        rate_spike = (
            yc_date == today_str
            and yc_spread is not None and yc_prev is not None
            and (float(yc_spread) - float(yc_prev)) >= 0.20
        )
        energy_red = xle_chg <= -1.5
        mlpi_down  = mlpi_chg <= -0.5
        if (energy_red or rate_spike) and mlpi_down:
            triggers = []
            if energy_red:  triggers.append(f"XLE {xle_chg:+.1f}%")
            if rate_spike:  triggers.append(f"T10-T2 +{float(yc_spread)-float(yc_prev):.2f}% rate spike")
            mlpi_entry_line = (
                f"┣ 🛢️ MLPI ENTRY WINDOW — {' | '.join(triggers)} | MLPI {mlpi_chg:+.1f}% — "
                f"Accumulation conditions. Cash buy (no new margin).\n"
            )
    except Exception:
        pass

    # EDGAR cross-channel alert — reads keys written by monitor.py every 5-min tick.
    # Zero API calls: pure DB reads. Keys: cornerstone_n2_detected_{T}, ro_dodge_active_{T},
    # cornerstone_30pct_watch_active_{T}. Only surfaces when something is active.
    edgar_alerts = []
    try:
        for _ct in ("CLM", "CRF"):
            _n2    = db.get_state(f"cornerstone_n2_detected_{_ct}") or ""
            _dodge = db.get_state(f"ro_dodge_active_{_ct}") or ""
            _watch = db.get_state(f"cornerstone_30pct_watch_active_{_ct}") or ""
            if _n2 and _dodge:
                edgar_alerts.append(f"{_ct} 🔴 N-2 ACTIVE ({_n2}) — RO dodge on, awaiting re-entry")
            elif _n2:
                edgar_alerts.append(f"{_ct} ⚠️ N-2 DETECTED ({_n2}) — monitor.py managing, see #cornerstone")
            elif _watch == "active":
                edgar_alerts.append(f"{_ct} 👀 30%+ premium watch — pre-N-2 threshold, no filing yet")
    except Exception:
        pass
    edgar_line = "┣ 📋 EDGAR: " + " | ".join(edgar_alerts) + "\n" if edgar_alerts else ""

    # Re-entry tracker — surfaces live score when an RO dodge is active.
    # Zero API calls: reads keys written by monitor.py every loop tick.
    # Shows progress toward the 60/100 gate and days remaining in 45-day hard wait.
    reentry_lines = []
    try:
        for _ct in ("CLM", "CRF"):
            if not db.get_state(f"ro_dodge_active_{_ct}"):
                continue
            _rs = db.get_state(f"{_ct}_reentry_score")
            _rz = db.get_state(f"{_ct}_reentry_zone")
            _n2 = db.get_state(f"cornerstone_n2_detected_{_ct}") or ""
            if _rs is None:
                reentry_lines.append(f"{_ct} RO active — re-entry score pending next monitor.py tick")
                continue
            _score = int(_rs)
            _fv    = _rz["fair_value"] if isinstance(_rz, dict) else 0.0
            _zl    = _rz["low"]        if isinstance(_rz, dict) else 0.0
            _zh    = _rz["high"]       if isinstance(_rz, dict) else 0.0
            try:
                from datetime import date as _dt_date
                _age  = (_dt_date.today() - _dt_date.fromisoformat(_n2)).days if _n2 else 0
                _wait = max(0, 45 - _age)
            except Exception:
                _age, _wait = 0, 45
            _gate_str = (
                "gate MET ✅" if (_score >= 60 and _wait == 0)
                else (f"gate unlocks in {_wait}d" if _wait > 0 else f"{60 - _score}pts to gate")
            )
            reentry_lines.append(
                f"{_ct} RO active: `{_score}/100` | "
                f"zone `${_zl:.2f}–${_zh:.2f}` | FV `${_fv:.2f}` | {_gate_str}"
            )
    except Exception:
        pass
    reentry_line = "┣ 🔄 Re-entry: " + " | ".join(reentry_lines) + "\n" if reentry_lines else ""

    # Bollen (2010) mood forward signal — "Twitter Mood Predicts the Stock Market."
    # Low calmness/high anxiety (F&G ≤ 25) Granger-causes DJIA declines 2–6 days later.
    # High euphoria (≥ 75) predicts mean reversion. Source: CNN F&G via tqqq.py (fg_last_known_score).
    mood_fwd_line = ""
    try:
        _ss_mood = db.get_state("fg_last_known_score")
        if _ss_mood is not None:
            _ssv = float(_ss_mood)
            if _ssv <= 25:
                mood_fwd_line = (
                    f"┣ 🧠 Mood (2–6d lead): Anxiety extreme (`{_ssv:.0f}`) — "
                    f"elevated anxiety leads market pressure by 2–6 days (Bollen 2010). Stay defensive.\n"
                )
            elif _ssv >= 75:
                mood_fwd_line = (
                    f"┣ 🧠 Mood (2–6d lead): Euphoria high (`{_ssv:.0f}`) — "
                    f"mean reversion risk in 2–6 days. Tighten strikes, avoid chasing.\n"
                )
    except Exception:
        pass

    # Accumulation readiness — written by monitor.py every loop tick
    acc_lines = []
    for _tkr in ("CLM", "CRF"):
        _status = db.get_state(f"{_tkr}_acc_status") or ""
        _detail = db.get_state(f"{_tkr}_acc_detail") or ""
        if _status:
            acc_lines.append(f"{_tkr}: {_status}" + (f" ({_detail})" if _detail else ""))
    acc_line = "┣ Accumulation: " + " | ".join(acc_lines) + "\n" if acc_lines else ""

    # Pre-N-2 early warning block — reads signals written by monitor.py each tick.
    # Only surfaces when no active RO dodge (avoid noise during active RO cycle).
    # Three dimensions: streak (duration), velocity (speed), interval (cycle timing).
    pre_n2_parts = []
    try:
        for _ct in ("CLM", "CRF"):
            _ro_active_now = db.get_state(f"ro_dodge_active_{_ct}", "")
            if _ro_active_now:
                continue  # active RO — re-entry block already covers it
            _streak  = int(db.get_state(f"{_ct.lower()}_premium_streak_days", 0) or 0)
            _vel     = db.get_state(f"{_ct.lower()}_premium_velocity_3d")
            _vel     = float(_vel) if _vel is not None else 0.0
            _months  = db.get_state(f"{_ct.lower()}_months_since_last_ro")
            _months  = float(_months) if _months is not None else None
            _overdue = db.get_state(f"{_ct.lower()}_ro_interval_elevated", False)

            parts = []
            if _streak >= 5:
                parts.append(f"streak `{_streak}d`")
            if abs(_vel) >= 2.0:
                parts.append(f"vel `{_vel:+.1f}%/3d`")
            if _overdue:
                parts.append(f"interval `{_months:.1f}mo` ⚠️ overdue")
            elif _months is not None and _months >= 7.0:
                parts.append(f"interval `{_months:.1f}mo`")
            if parts:
                pre_n2_parts.append(f"{_ct}: " + " | ".join(parts))
    except Exception:
        pass
    pre_n2_line = "┣ Pre-N-2 watch: " + " | ".join(pre_n2_parts) + "\n" if pre_n2_parts else ""

    # Ex-div reaction signals — written by scheduler.py exdiv_check (20:35 UTC)
    # Only surfaces OVERSHOOT/UNDERSHOOT; EFFICIENT is noise-suppressed.
    _exdiv_parts = []
    for _sym in ("MLPI", "MAIN", "JEPI", "JEPQ", "SCHD", "O", "ARCC"):
        try:
            _r = db.get_state(f"exdiv_reaction_{_sym}")
            if not _r or not isinstance(_r, dict):
                continue
            _v = _r.get("verdict", "")
            if _v not in ("OVERSHOOT", "UNDERSHOOT"):
                continue
            _ex = _r.get("ex_date", "")
            # Only surface if the reaction is from the last 2 calendar days
            from datetime import date as _ma_d, timedelta as _ma_td
            try:
                _age = (_ma_d.today() - _ma_d.fromisoformat(_ex)).days
                if _age > 2:
                    continue
            except Exception:
                continue
            _emoji = "🟢" if _v == "OVERSHOOT" else "🔴"
            _eff   = int((_r.get("efficiency", 1.0)) * 100)
            _exdiv_parts.append(f"{_sym} {_emoji} {_v} ({_eff}% of dist)")
        except Exception:
            continue
    exdiv_line = "┣ Ex-Div: " + " | ".join(_exdiv_parts) + "\n" if _exdiv_parts else ""

    # Signal ledger confidence — 30-day win rates from signal_ledger table.
    # Zero API calls: reads graded predictions already stored by announcements.py grader.
    # Flags ⚠️ when a desk is below 50% — calibrates conviction without changing strategy.
    # Only surfaces when a desk has ≥3 graded signals (avoids noise from tiny samples).
    ledger_conf_line = ""
    try:
        ledger_rates = engine.get_signal_ledger_winrates(days_back=30)
        _conf_parts = []
        _desk_map = {
            "market_direction": "Direction",
            "tqqq_call":        "LEAP CALL",
            "tqqq_put":         "LEAP PUT",
            "clm_floor":        "CEF floor",
            "wheel_csp":        "Wheel",
        }
        for _key, _label in _desk_map.items():
            _r = ledger_rates.get(_key)
            if not _r or _r.get("total", 0) < 3:
                continue  # skip — too few data points to be meaningful
            _w  = _r["wins"]
            _t  = _r["total"]
            _pct = round(_w / _t * 100) if _t else 0
            _flag = "⚠️" if _pct < 50 else "✅"
            _conf_parts.append(f"{_label} {_pct}% ({_w}/{_t}) {_flag}")
        if _conf_parts:
            ledger_conf_line = "┣ 📈 Signal accuracy (30d): " + " | ".join(_conf_parts) + "\n"
    except Exception:
        pass

    # Q1 tax character note (Jan–Mar only) — shows after-tax yield if 1099-DIV data is seeded.
    # Source: db_tools.py --seed-tax-character (run once after 1099-DIV arrives each January).
    _tax_note = ""
    if now_utc.month in (1, 2, 3):
        _marginal = float(os.getenv("MARGINAL_TAX_RATE", "22")) / 100
        _tax_parts = []
        for _ts, _ad in (("clm", 1.458), ("crf", 1.4112)):
            _tc = db.get_state(f"{_ts}_dist_tax_char") or {}
            if not isinstance(_tc, dict) or "roc_pct" not in _tc:
                continue
            _nav_k = f"{_ts}_last_nav"
            _nav   = float(db.get_state(_nav_k) or (6.73 if _ts == "clm" else 6.18))
            _hl_y  = _ad / _nav * 100
            _at_y  = _hl_y * (
                (_tc["roc_pct"] / 100) * 1.0
                + (_tc["qdi_pct"] / 100) * 0.85
                + (_tc["ord_pct"] / 100) * (1 - _marginal)
            )
            _tax_parts.append(
                f"{_ts.upper()} ROC {_tc['roc_pct']:.0f}% → est. after-tax yield {_at_y:.1f}%"
                f" (headline {_hl_y:.1f}%)"
            )
        if _tax_parts:
            _tax_note = "┣ Tax char (" + str(now_utc.year - 1) + " 1099): " + " | ".join(_tax_parts) + "\n"

    # Top headline summary merged into morning brief (2nd embed removed Aug 2026).
    # _fetch_market_headlines caches to DB — zero extra API calls.
    # reentry_line and pre_n2_line removed — they belong in #cornerstone only.
    # ledger_conf_line removed — too granular for daily; use weekly scorecard.
    headlines_line = ""
    try:
        _hl = _fetch_market_headlines(db)
        if _hl:
            _b   = sum(1 for h in _hl if h["sentiment"] == "bullish")
            _d   = sum(1 for h in _hl if h["sentiment"] == "bearish")
            _agg = "BULLISH" if _b > _d else ("BEARISH" if _d > _b else "MIXED")
            _top = " | ".join(h["title"][:55] for h in _hl[:2])
            headlines_line = f"┣ 📰 Headlines ({_agg}, {_b}B/{_d}D): {_top}\n"
    except Exception:
        pass

    signals_section = (
        "\n**CROSS-CHANNEL CONFLUENCE**\n"
        f"┣ CLM/CRF: {cef_line}\n"
        f"{edgar_line}"
        f"{acc_line}"
        f"{exdiv_line}"
        f"{_tax_note}"
        f"{mood_fwd_line}"
        f"┣ TQQQ: {tqqq_line}\n"
        f"┣ Wheel: {wheel_line}\n"
        f"{mlpi_entry_line}"
        f"{headlines_line}"
    )

    # ── BIAS + DIRECTIVES ─────────────────────────────────────────────────────
    vix_params = engine.get_vix_adjusted_params(bias["real_vix"])
    wheel_directive = (
        f"Δ {vix_params['delta_target']:.2f} | {vix_params['dte_min']}–{vix_params['dte_max']} DTE | "
        f"{vix_params['size_scalar']:.0%} size ({vix_params['tier']} VIX regime)"
    )
    score_sign = f"+{bias['bias_score']}" if bias['bias_score'] >= 0 else str(bias['bias_score'])
    directive_label = {
        "BULLISH":  "Favor longs and wheel setups. Bias toward calls on dips.",
        "BEARISH":  "Defensive posture. No new margin draws. Watch puts.",
        "NEUTRAL":  "Selective entries only. Wait for clearer bias before sizing up.",
    }[bias["label"]]

    directives_section = (
        f"\n**TODAY'S POSTURE: {bias['label']} (Score: {score_sign})**\n"
        f"┣ Bias: {directive_label}\n"
        f"┗ Wheel params: {wheel_directive}\n"
    )

    # Congressional trades removed from morning brief (user preference Aug 2026).
    # Data still available via SentiSense API — can be surfaced on demand.
    ss_section = ""

    description = market_structure_section + macro_section + equity_section + signals_section + ss_section + directives_section
    title = f"MORNING BRIEF — {now_label}"
    return title, description, bias["color"]


def _build_intraday_report(engine: HighFidelityAnalyticsEngine, db: EcosystemDatabase) -> tuple:
    """
    Mid-session pulse — lightweight bias re-score with updated SPY/QQQ + VIXY.
    Fires at 20:20 UTC (10:20 HST, ~3 hours into cash session).
    """
    now_label = datetime.now().strftime("%H:%M HST")
    bias = _calculate_bias_score(engine, db)
    spy_chg  = bias["spy_chg"]
    qqq_chg  = bias["qqq_chg"]
    vixy_z   = bias["vixy_z"]
    fg_val   = bias["fg_val"]
    real_vix = bias["real_vix"]

    def _arrow(v): return "▲" if v >= 0 else "▼"
    score_sign = f"+{bias['bias_score']}" if bias['bias_score'] >= 0 else str(bias['bias_score'])

    desc = (
        f"**Mid-Session Bias: {bias['label']} (Score: {score_sign})**\n"
        f"┣ SPY: {_arrow(spy_chg)}{abs(spy_chg):.2f}% | QQQ: {_arrow(qqq_chg)}{abs(qqq_chg):.2f}%\n"
        f"┣ VIX: `{real_vix:.1f}` | VIXY z: `{vixy_z:+.2f}σ`\n"
        f"┣ Fear & Greed: `{fg_val}` ({bias['fg_class']})\n"
    )
    # Surface any open TQQQ signals
    try:
        bottom = int(db.get_state("tqqq_bottom_score") or 0)
        top    = int(db.get_state("tqqq_top_score") or 0)
        if bottom >= 55:
            desc += f"┣ 🟢 TQQQ CALL desk UNLOCKED — bottom score {bottom}/100\n"
        if top >= 55:
            desc += f"┣ 🟢 TQQQ PUT desk UNLOCKED — top score {top}/100\n"
    except Exception:
        pass
    desc += "┗ Intraday context — full brief at 0310 HST daily."
    return "MID-SESSION PULSE", desc, bias["color"]


def _build_eod_report(engine: HighFidelityAnalyticsEngine, db: EcosystemDatabase) -> tuple:
    """
    EOD brief (23:40 UTC / 13:40 HST — after cash close).
    Summarizes the session and flags anything to act on before tomorrow's open.
    """
    now_label = datetime.now().strftime("%a %b %-d")
    bias = _calculate_bias_score(engine, db)
    spy_chg  = bias["spy_chg"]
    qqq_chg  = bias["qqq_chg"]
    vixy_z   = bias["vixy_z"]
    real_vix = bias["real_vix"]

    def _arrow(v): return "▲" if v >= 0 else "▼"
    score_sign = f"+{bias['bias_score']}" if bias['bias_score'] >= 0 else str(bias['bias_score'])

    desc = (
        f"**Session Close — Bias: {bias['label']} (Score: {score_sign})**\n"
        f"┣ SPY: {_arrow(spy_chg)}{abs(spy_chg):.2f}% | QQQ: {_arrow(qqq_chg)}{abs(qqq_chg):.2f}%\n"
        f"┣ VIX close: `{real_vix:.1f}` | VIXY z: `{vixy_z:+.2f}σ`\n"
        f"┣ Fear & Greed: `{bias['fg_val']}` ({bias['fg_class']})\n"
        f"┗ HY Spread: `{bias['signals'].get('hy_spread', 0.0):.2f}%`\n"
    )

    # Wheel position DTE countdown — appended after fixed lines if any positions open
    try:
        open_pos = db.get_open_wheel_positions()
        if open_pos:
            today = datetime.now().date()
            desc += "\n**Open Wheel Positions**\n"
            for pos in open_pos[:5]:
                try:
                    exp_d = datetime.strptime(pos["expiration"], "%Y-%m-%d").date()
                    dte   = (exp_d - today).days
                    urgency = " 🔴 ROLL/CLOSE SOON" if dte <= 7 else (" 🟡 WATCH" if dte <= 14 else "")
                    desc += (
                        f"┣ **{pos['symbol']}** {pos['position_type']} ${pos['strike']:.0f} "
                        f"exp {pos['expiration']} ({dte}d){urgency}\n"
                    )
                except Exception:
                    pass
            desc = _swap_last_bullet(desc)
    except Exception as e:
        logger.warning(f"EOD: wheel position read failed: {e}")

    return f"EOD BRIEF — {now_label}", desc, bias["color"]


def _swap_last_bullet(text: str) -> str:
    """Replace the last ┣ line prefix with ┗ for proper Discord formatting."""
    lines = text.rstrip("\n").split("\n")
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].startswith("┣"):
            lines[i] = "┗" + lines[i][1:]
            break
    return "\n".join(lines) + "\n"


# ── Main Loop ─────────────────────────────────────────────────────────────────

def run():
    db     = EcosystemDatabase()
    engine = HighFidelityAnalyticsEngine()
    logger.info("Market Analysis online. Loop: 60s.")

    BUILDERS = {
        "morning":  _build_morning_report,
        "intraday": _build_intraday_report,
        "eod":      _build_eod_report,
    }

    while True:
        try:
            now_utc  = datetime.now(timezone.utc)
            weekday  = now_utc.weekday()
            is_wkday = weekday < 5
            date_str = now_utc.strftime("%Y-%m-%d")
            h, m     = now_utc.hour, now_utc.minute

            for (t_h, t_m, db_key, mode) in FIRE_SCHEDULE:
                if not is_wkday:
                    continue
                if not _in_window(h, m, t_h, t_m):
                    continue
                if _already_fired(db, db_key, date_str):
                    continue

                logger.info(f"Firing {mode} brief...")
                try:
                    builder = BUILDERS[mode]
                    title, description, color = builder(engine, db)
                    _send_embed(title, description, color)
                    _mark_fired(db, db_key, date_str)  # only mark after successful dispatch
                except Exception as e:
                    logger.error(
                        f"{mode} brief build/dispatch failed: {e}\n"
                        f"{traceback.format_exc()}"
                    )

        except Exception as e:
            logger.error(f"Loop tick error: {e}")

        # Align to wall-clock minute boundary (prevents slow drift over trading day)
        now_ts = time.time()
        time.sleep(60 - (now_ts % 60))


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Market Analysis stopped by operator.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Market Analysis crashed: {e}")
        sys.exit(1)
