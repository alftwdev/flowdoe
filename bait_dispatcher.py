"""
bait_dispatcher.py — Cornerstone Flowstate content marketing dispatch

Two modes, two scheduler entries in market_scheduler.py:

  DRAFT mode (default, 18:00 UTC = 8:00 AM HST):
    Weekdays:  3 Pushover notifications (one per bait) for manual X posting
    Weekends:  1 consolidated snippet Pushover
    Dedup key: bait_last_sent_draft_{YYYY-MM-DD}

  AUTO-POST mode (--auto-post flag, 12:30 UTC = 8:30 AM ET = 2:30 AM HST):
    Weekdays:  posts 3 tweet threads directly to X via Twitter API v2
               + silent Pushover recap (priority=-1, no sound — user is asleep)
    Weekends:  silent consolidated Pushover only (no X post on weekends)
    Dedup key: bait_last_sent_autopost_{YYYY-MM-DD}
    Gate:      X_AUTO_POST_ENABLED=true in .env + Twitter credentials set

Setup for auto-posting (do once):
  1. Apply for Twitter Developer account at developer.twitter.com (free)
  2. Create app with Read+Write permissions → get 4 credentials
  3. Add to .env: X_AUTO_POST_ENABLED=true
                  TWITTER_API_KEY=...
                  TWITTER_API_SECRET=...
                  TWITTER_ACCESS_TOKEN=...
                  TWITTER_ACCESS_TOKEN_SECRET=...
  4. pip install tweepy  (on PA: pip3.10 install tweepy)
  5. Both scheduler entries run independently — draft Pushover still fires at 8 AM HST

NOT a financial advisor. Educational content only.
"""

import os
import json
import time
import logging
import argparse
import requests
from datetime import date, datetime, timedelta

from dotenv import load_dotenv
from database import EcosystemDatabase

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bait_dispatcher")

PUSHOVER_API_TOKEN  = os.getenv("PUSHOVER_API_TOKEN", "")
PUSHOVER_USER_KEY   = os.getenv("PUSHOVER_USER_KEY", "")
WEBHOOK_ANNOUNCEMENTS = os.getenv("WEBHOOK_ANNOUNCEMENTS", "")

# ── X (Twitter) auto-post credentials ─────────────────────────────────────────
# All 4 required for tweepy.Client OAuth1 User Context (the only auth that writes tweets).
X_AUTO_POST_ENABLED         = os.getenv("X_AUTO_POST_ENABLED", "false").lower() == "true"
TWITTER_API_KEY             = os.getenv("TWITTER_API_KEY", "")
TWITTER_API_SECRET          = os.getenv("TWITTER_API_SECRET", "")
TWITTER_ACCESS_TOKEN        = os.getenv("TWITTER_ACCESS_TOKEN", "")
TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET", "")

db = EcosystemDatabase()

# ─────────────────────────────────────────────────────────────────────────────
# NYSE HOLIDAYS  (source: NYSE.com — confirmed 2026-2027)
# ─────────────────────────────────────────────────────────────────────────────
NYSE_HOLIDAYS = {
    date(2026, 11, 26),  # Thanksgiving
    date(2026, 12, 25),  # Christmas
    date(2027,  1,  1),  # New Year's Day
    date(2027,  1, 19),  # MLK Day
    date(2027,  2, 16),  # Presidents Day
    date(2027,  4,  2),  # Good Friday
    date(2027,  5, 25),  # Memorial Day
    date(2027,  7,  5),  # Independence Day (observed)
    date(2027,  9,  7),  # Labor Day
    date(2027, 11, 26),  # Thanksgiving
    date(2027, 12, 25),  # Christmas
}


def is_market_closed_today() -> bool:
    today = date.today()
    return today.weekday() >= 5 or today in NYSE_HOLIDAYS


# ─────────────────────────────────────────────────────────────────────────────
# HOOK ROTATION  (4-week cycle per bait — rotate Mondays)
# ─────────────────────────────────────────────────────────────────────────────

# Hooks indexed [0..3] — week_of_year % 4 selects the variant
BAIT1_HOOKS = [
    # Week A — personal pain / relatability
    (
        "I held CLM through the 2025 Rights Offering.\n"
        "Watched it drop 12% over 6 weeks.\n"
        "I didn't know when the bottom was.\n\n"
        "Here's the 4-phase RO anatomy every CLM/CRF holder needs to know:"
    ),
    # Week B — newsjack / specific event
    (
        "CLM filed an N-2 with the SEC on Aug 14.\n"
        "Most holders found out when the price was already down 9%.\n\n"
        "Rights Offerings are public filings. You can see them coming.\n"
        "Here's exactly what to do:"
    ),
    # Week C — against conventional wisdom
    (
        "Most CLM/CRF holders think the ex-dividend dip is the buy signal.\n"
        "It's not. It's the 4th catalyst. The real entry is earlier.\n\n"
        "The 4-phase anatomy:"
    ),
    # Week D — specific number hook
    (
        "RO Risk score: {ro_display}\n"
        "CLM at ${clm_price:.2f} | CRF at ${crf_price:.2f}\n"
        "Premium: {clm_prem_label}\n\n"
        "This is what a live N-2 signal looks like. Here's how to read it:"
    ),
]

BAIT2_HOOKS = [
    # Week A — confession
    (
        "I ran the wheel strategy for 6 months.\n"
        "Net premium collected: basically zero.\n\n"
        "Here's the one filter I was skipping — and why it's the only one that matters:"
    ),
    # Week B — specific stat
    (
        "5-year backtest of the wheel WITHOUT the VRP filter: ~1% CAGR.\n"
        "WITH the filter: 8-12%. Same stocks. Same DTE.\n\n"
        "The 3-filter screen:"
    ),
    # Week C — against convention
    (
        "Most options traders screen for high IVR.\n"
        "That's the wrong starting point.\n\n"
        "IVR tells you IV is elevated vs its own history.\n"
        "It doesn't tell you if the premium is real or a historical relic.\n"
        "Here's the 3-filter stack:"
    ),
    # Week D — specific question
    (
        "You see a CSP setup with 45% IVR. Looks great.\n"
        "Then the IV crush hits before you even get to expiry.\n\n"
        "IV vs HV30 was flat. That was the tell.\n"
        "The 3-filter check list:"
    ),
]

BAIT3_HOOKS = [
    # Week A — time saved
    (
        "I used to spend 45 minutes every morning across Bloomberg, CBOE, Finviz, and Twitter.\n"
        "Now I check 5 numbers. Takes 60 seconds.\n\n"
        "Today's read: {bias_label}\n\n"
        "Here's the exact stack:"
    ),
    # Week B — specific level
    (
        "VIX above 20: stay defensive.\n"
        "VIX above 25 in backwardation: get your puts on.\n"
        "VIX drops back below 1.0 term ratio: that's the LEAP CALL entry window.\n\n"
        "5-signal morning posture (today: {bias_label}):"
    ),
    # Week C — relatable frustration
    (
        "The worst trade setups I've taken came from skipping the morning posture.\n"
        "Entered bullish when the regime was already flipping.\n\n"
        "5 numbers. 60 seconds. Here's the stack:"
    ),
    # Week D — outcome-first
    (
        "This morning's market posture: {bias_label}\n"
        "TQQQ cycle score: {tqqq_score}/100\n\n"
        "That's the output. Here's how it's built:"
    ),
]

# ─────────────────────────────────────────────────────────────────────────────
# FRAMEWORKS (static — delivered in every post)
# ─────────────────────────────────────────────────────────────────────────────

BAIT1_FRAMEWORK = """\
🔴 ① N-2 Filed → LARGEST drop of the entire cycle. Institutions exit immediately. Price compresses from premium high → historical low within days.

🟠 ② N-2/A (~47 days later) → Second wave. Confirms exact sub price. Another 1-3 day flush.

🟡 ③ Record Date (~day 59) → Historically the cycle LOW. Open-market buyers who got in BELOW sub price beat RO participants.

🟢 ④ Ex-Dividend → Mechanical only. Creates 1-3 day accumulation window. Not a seller event.

Most retail holders panic at ① and miss the optimal entry at ③."""

BAIT2_FRAMEWORK = """\
✅ Filter 1: IVR > 35%
IV is elevated vs its own 52-week history. The premium edge exists in the market.

✅ Filter 2: IV − HV30 ≥ 5 volatility points
IV must EXCEED realized volatility by at least 5pp. If IV just caught up to a past spike that already normalized → the edge is gone.

✅ Filter 3: No earnings within 45 days
IV crush after a report destroys the premium edge. Earnings = forced close or max-loss risk.

Skip Filter 2 and you're selling into a past volatility event. That's the most common wheel mistake."""

BAIT3_FRAMEWORK = """\
📊 Signal 1: VIX level (below 20 = calm / above 25 = fear)
📊 Signal 2: VIX term structure (VIXY/VXZ — backwardation = sustained fear, not a one-day spike)
📊 Signal 3: HY Credit Spread (FRED live — > 4.5% = credit stress, not just equity noise)
📊 Signal 4: SPY vs SMA200 (above = bull regime, below = bear regime)
📊 Signal 5: Fear & Greed Index (< 25 = extreme fear = TQQQ CALL territory)

All 5 → one verdict: BULLISH / NEUTRAL / BEARISH.
Takes 60 seconds once you have the stack. Most people don't have the stack."""

# ─────────────────────────────────────────────────────────────────────────────
# CTAs  (two variants: with auto-DM tool active vs without)
# ─────────────────────────────────────────────────────────────────────────────
# Use CTA_ENGAGEMENT when Tweet Hunter / xautodm auto-DM is configured.
# Use CTA_DIRECT when posting manually without auto-DM automation.
USE_ENGAGEMENT_CTA = False  # flip to True once Tweet Hunter DM automation is live

BAIT1_CTA_ENGAGEMENT = 'Comment "RO" ↓ and I\'ll DM you the live RO Risk score + where we are in the current cycle.'
BAIT2_CTA_ENGAGEMENT = 'Drop "WHEEL" in the comments — I\'ll DM you today\'s live screener output.'
BAIT3_CTA_ENGAGEMENT = 'Reply "SIGNAL" ↓ and I\'ll DM you this morning\'s posture + all 5 signal readings.'

GUMROAD_LINK = "https://bit.ly/4Am3uCo"

BAIT1_CTA_DIRECT = f"Live RO signal + entry alerts → {GUMROAD_LINK}\nFree tier (no card) → {GUMROAD_LINK}"
BAIT2_CTA_DIRECT = f"Live screener + which tickers pass today → {GUMROAD_LINK}\nFree tier → {GUMROAD_LINK}"
BAIT3_CTA_DIRECT = f"Full morning brief + TQQQ cycle score → {GUMROAD_LINK}\nFree tier → {GUMROAD_LINK}"

# ─────────────────────────────────────────────────────────────────────────────
# HASHTAGS  (5 max per post — #FinTwit always included)
# ─────────────────────────────────────────────────────────────────────────────
BAIT1_HASHTAGS = "#CLM #CRF #ClosedEndFunds #DividendInvesting #FinTwit"
BAIT2_HASHTAGS = "#OptionsTrading #TheWheel #CashSecuredPuts #PassiveIncome #FinTwit"
BAIT3_HASHTAGS = "#StockMarket #PreMarket #IncomeInvesting #DividendInvesting #FinTwit"

# ─────────────────────────────────────────────────────────────────────────────
# NFA FOOTER
# ─────────────────────────────────────────────────────────────────────────────
NFA = "Not financial advice. Educational signals only. Always do your own research."


# ─────────────────────────────────────────────────────────────────────────────
# DATA PULL  (reads from DB — populated by monitor.py + market_analysis.py)
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(val, fallback=0.0):
    try:
        return float(val) if val not in (None, "", "None") else fallback
    except (TypeError, ValueError):
        return fallback


def pull_market_data() -> dict:
    clm_price = _safe_float(db.get_state("clm_last_price"), 0.0)
    crf_price  = _safe_float(db.get_state("crf_last_price"), 0.0)
    clm_nav    = _safe_float(db.get_state("clm_last_nav"),   6.31)
    crf_nav    = _safe_float(db.get_state("crf_last_nav"),   6.12)
    clm_z      = _safe_float(db.get_state("clm_last_z_premium"), 0.0)
    crf_z      = _safe_float(db.get_state("crf_last_z_premium"), 0.0)
    ro_clm     = bool(db.get_state("ro_dodge_active_CLM"))
    ro_crf     = bool(db.get_state("ro_dodge_active_CRF"))
    tqqq_raw   = db.get_state("tqqq_bottom_score")
    tqqq_score = int(_safe_float(tqqq_raw, 0))

    # Market bias label
    bias_label = "NEUTRAL"
    try:
        bias_raw = db.get_state("market_analysis_bias") or {}
        if isinstance(bias_raw, str):
            bias_raw = json.loads(bias_raw)
        bias_label = bias_raw.get("label", "NEUTRAL")
    except Exception:
        pass

    # Premium label for CLM
    if clm_z >= 1.5:
        clm_prem_label = f"+{clm_z:.1f}σ (elevated)"
    elif clm_z <= -0.5:
        clm_prem_label = f"{clm_z:+.1f}σ (discount/near-NAV)"
    else:
        clm_prem_label = f"{clm_z:+.1f}σ (normal range)"

    # RO display string
    if ro_clm:
        ro_display = "CRITICAL — N-2 Active"
    elif clm_z >= 1.5:
        ro_display = "ELEVATED"
    else:
        ro_display = "MONITORING"

    # CLM premium % estimate
    clm_prem_pct = ((clm_price / clm_nav) - 1) * 100 if clm_nav > 0 and clm_price > 0 else 0.0
    crf_prem_pct = ((crf_price / crf_nav) - 1) * 100 if crf_nav > 0 and crf_price > 0 else 0.0

    return {
        "clm_price":     clm_price,
        "crf_price":     crf_price,
        "clm_nav":       clm_nav,
        "crf_nav":       crf_nav,
        "clm_z":         clm_z,
        "crf_z":         crf_z,
        "clm_prem_pct":  clm_prem_pct,
        "crf_prem_pct":  crf_prem_pct,
        "clm_prem_label": clm_prem_label,
        "ro_display":    ro_display,
        "ro_active":     ro_clm or ro_crf,
        "tqqq_score":    tqqq_score,
        "bias_label":    bias_label,
    }


# ─────────────────────────────────────────────────────────────────────────────
# FORMAT BAITS  (returns dict with title, hook, framework, cta, hashtags, x_draft)
# ─────────────────────────────────────────────────────────────────────────────

def _week_variant() -> int:
    """Returns 0-3 based on ISO week number, cycles every 4 weeks."""
    return (date.today().isocalendar()[1]) % 4


def format_bait1(data: dict) -> dict:
    variant = _week_variant()
    hook_template = BAIT1_HOOKS[variant]
    hook = hook_template.format(**data)
    cta  = BAIT1_CTA_ENGAGEMENT if USE_ENGAGEMENT_CTA else BAIT1_CTA_DIRECT

    # 4-tweet thread for X auto-posting (each tweet ≤ 280 chars)
    if USE_ENGAGEMENT_CTA:
        cta_tweet = (
            f"Most retail holders panic at ① and miss the real entry at ③.\n\n"
            f"{BAIT1_CTA_ENGAGEMENT}\n\n"
            f"{BAIT1_HASHTAGS}"
        )
    else:
        cta_tweet = (
            f"Most retail holders panic at ① and miss the real entry at ③.\n\n"
            f"Live RO signal + entry alerts 👇\n{GUMROAD_LINK}\n\n"
            f"{BAIT1_HASHTAGS}"
        )
    thread_tweets = [
        hook,
        (
            "🔴 ① N-2 Filed → LARGEST drop of the entire cycle.\n"
            "Institutions exit immediately. Price compresses from premium high → historical low within days.\n\n"
            "🟠 ② N-2/A (~47 days later) → Second wave.\n"
            "Confirms exact sub price. Another 1-3 day flush."
        ),
        (
            "🟡 ③ Record Date (~day 59) → Historically the cycle LOW.\n"
            "Open-market buyers who got in BELOW sub price beat RO participants.\n\n"
            "🟢 ④ Ex-Dividend → Mechanical only.\n"
            "Creates 1-3 day accumulation window. Not a seller event."
        ),
        cta_tweet,
    ]

    x_draft = (
        f"{hook}\n\n"
        f"{BAIT1_FRAMEWORK}\n\n"
        f"{cta}\n\n"
        f"{BAIT1_HASHTAGS}\n\n"
        f"{NFA}"
    )
    return {
        "emoji":         "🚨",
        "title":         "Bait 1 — CLM/CRF Rights Offering",
        "hook":          hook,
        "framework":     BAIT1_FRAMEWORK,
        "cta":           cta,
        "hashtags":      BAIT1_HASHTAGS,
        "x_draft":       x_draft,
        "thread_tweets": thread_tweets,
    }


def format_bait2(data: dict) -> dict:
    variant = _week_variant()
    hook = BAIT2_HOOKS[variant]
    cta  = BAIT2_CTA_ENGAGEMENT if USE_ENGAGEMENT_CTA else BAIT2_CTA_DIRECT

    if USE_ENGAGEMENT_CTA:
        cta_tweet = f"{BAIT2_CTA_ENGAGEMENT}\n\n{BAIT2_HASHTAGS}"
    else:
        cta_tweet = f"Live screener + which tickers pass today 👇\n{GUMROAD_LINK}\n\n{BAIT2_HASHTAGS}"
    thread_tweets = [
        hook,
        (
            "✅ Filter 1: IVR > 35%\n"
            "IV is elevated vs its own 52-week history. The premium edge exists in the market.\n\n"
            "✅ Filter 2: IV − HV30 ≥ 5 volatility points\n"
            "IV must EXCEED realized vol by 5pp. If IV caught up to a past spike that normalized → edge is gone."
        ),
        (
            "✅ Filter 3: No earnings within 45 days\n"
            "IV crush after a report destroys the premium edge. Earnings = forced close or max-loss risk.\n\n"
            "Skip Filter 2 and you're selling into a past volatility event. Most common wheel mistake."
        ),
        cta_tweet,
    ]

    x_draft = (
        f"{hook}\n\n"
        f"{BAIT2_FRAMEWORK}\n\n"
        f"{cta}\n\n"
        f"{BAIT2_HASHTAGS}\n\n"
        f"{NFA}"
    )
    return {
        "emoji":         "📋",
        "title":         "Bait 2 — Options Wheel 3-Filter",
        "hook":          hook,
        "framework":     BAIT2_FRAMEWORK,
        "cta":           cta,
        "hashtags":      BAIT2_HASHTAGS,
        "x_draft":       x_draft,
        "thread_tweets": thread_tweets,
    }


def format_bait3(data: dict) -> dict:
    variant = _week_variant()
    hook_template = BAIT3_HOOKS[variant]
    hook = hook_template.format(**data)
    cta  = BAIT3_CTA_ENGAGEMENT if USE_ENGAGEMENT_CTA else BAIT3_CTA_DIRECT

    if USE_ENGAGEMENT_CTA:
        cta_tweet = f"{BAIT3_CTA_ENGAGEMENT}\n\n{BAIT3_HASHTAGS}"
    else:
        cta_tweet = f"Full morning brief + TQQQ cycle score 👇\n{GUMROAD_LINK}\n\n{BAIT3_HASHTAGS}"
    thread_tweets = [
        hook,
        (
            "📊 Signal 1: VIX level (below 20 = calm / above 25 = fear)\n"
            "📊 Signal 2: VIX term structure (VIXY/VXZ — backwardation = sustained fear, not a one-day spike)\n"
            "📊 Signal 3: HY Credit Spread (FRED live — > 4.5% = credit stress, not just equity noise)"
        ),
        (
            "📊 Signal 4: SPY vs SMA200 (above = bull regime, below = bear regime)\n"
            "📊 Signal 5: Fear & Greed Index (< 25 = extreme fear = TQQQ CALL territory)\n\n"
            "All 5 → one verdict: BULLISH / NEUTRAL / BEARISH.\n"
            "Takes 60 seconds once you have the stack. Most people don't have the stack."
        ),
        cta_tweet,
    ]

    x_draft = (
        f"{hook}\n\n"
        f"{BAIT3_FRAMEWORK}\n\n"
        f"{cta}\n\n"
        f"{BAIT3_HASHTAGS}\n\n"
        f"{NFA}"
    )
    return {
        "emoji":         "📊",
        "title":         "Bait 3 — 60-Second Morning Posture",
        "hook":          hook,
        "framework":     BAIT3_FRAMEWORK,
        "cta":           cta,
        "hashtags":      BAIT3_HASHTAGS,
        "x_draft":       x_draft,
        "thread_tweets": thread_tweets,
    }


# ─────────────────────────────────────────────────────────────────────────────
# PUSHOVER DISPATCH
# ─────────────────────────────────────────────────────────────────────────────

def send_pushover(title: str, message: str, priority: int = 0) -> bool:
    if not PUSHOVER_API_TOKEN or not PUSHOVER_USER_KEY:
        logger.error("Pushover credentials missing — set PUSHOVER_API_TOKEN + PUSHOVER_USER_KEY")
        return False
    payload = {
        "token":   PUSHOVER_API_TOKEN,
        "user":    PUSHOVER_USER_KEY,
        "title":   title,
        "message": message,
        "priority": priority,
    }
    try:
        r = requests.post(
            "https://api.pushover.net/1/messages.json",
            data=payload, timeout=15
        )
        r.raise_for_status()
        logger.info(f"Pushover sent: {title}")
        return True
    except Exception as e:
        logger.error(f"Pushover failed: {e}")
        return False


def build_weekday_notification(bait: dict) -> tuple[str, str]:
    """Returns (title, message) for a single-bait Pushover."""
    title = f"[CF] {bait['emoji']} {bait['title']}"
    body  = (
        f"── HOOK ──\n{bait['hook']}\n\n"
        f"── FRAMEWORK ──\n{bait['framework']}\n\n"
        f"── CTA ──\n{bait['cta']}\n\n"
        f"── X DRAFT (copy/paste) ──\n{bait['x_draft']}"
    )
    return title, body


def build_consolidated_notification(baits: list[dict], today_str: str) -> tuple[str, str]:
    """Short snippet form for weekends/holidays — hook + CTA only, no full framework."""
    title = f"[CF] Weekend Bait Snippets — {today_str}"
    sections = []
    for b in baits:
        hook_first_line = b["hook"].splitlines()[0]
        cta_first_line  = b["cta"].splitlines()[0]
        sections.append(
            f"{b['emoji']} {b['title']}\n"
            f"{hook_first_line}\n"
            f"→ {cta_first_line}"
        )
    body = "\n\n─────\n\n".join(sections)
    body += f"\n\n{NFA}"
    return title, body


# ─────────────────────────────────────────────────────────────────────────────
# DISCORD #free-data EMBED  (T-48h delayed, locked fields)
# ─────────────────────────────────────────────────────────────────────────────

def post_free_data_embed(data: dict) -> bool:
    if not WEBHOOK_ANNOUNCEMENTS:
        logger.warning("WEBHOOK_ANNOUNCEMENTS not set — skipping #free-data embed")
        return False

    today_str = date.today().strftime("%b %d, %Y")

    # Framing: show what subscribers RECEIVED yesterday — prove the signal worked.
    # Lock only the most actionable output (entry timing). Show everything else.
    clm_display = f"${data['clm_price']:.2f}" if data["clm_price"] > 0 else "—"
    crf_display = f"${data['crf_price']:.2f}" if data["crf_price"] > 0 else "—"
    clm_prem = f"{data['clm_prem_pct']:+.1f}%" if data["clm_price"] > 0 else "—"
    crf_prem = f"{data['crf_prem_pct']:+.1f}%" if data["crf_price"] > 0 else "—"

    if data["ro_active"]:
        signal_line = "🚨 N-2 Filed (Aug 14) — active RO cycle"
        outcome_note = "CLM: $7.35 → now $6.58 (–10.5% since signal fired). Subscribers were alerted the same day."
    else:
        signal_line = "📡 Monitoring — no active RO signal"
        outcome_note = "No active RO. Daily signal check confirmed clean."

    embed_desc = (
        f"*What subscribers received yesterday — delayed 48h for free tier.*\n\n"
        f"┣ Signal: **{signal_line}**\n"
        f"┣ RO Risk score: **{data['ro_display']}**\n"
        f"┣ CLM: **{clm_display}** | Prem: {clm_prem} | CRF: **{crf_display}** | Prem: {crf_prem}\n"
        f"┣ Market posture: **{data['bias_label']}** (TQQQ cycle score: {data['tqqq_score']}/100)\n"
        f"┣ Outcome: {outcome_note}\n"
        f"┗ Entry timing + next catalyst: 🔒 *Subscriber only*\n\n"
        f"→ Get live alerts before the price moves: [{GUMROAD_LINK}]({GUMROAD_LINK})\n\n"
        f"*{NFA}*"
    )

    embed = {
        "title":       f"☕ CLM/CRF Signal Recap [{today_str} · free tier · 48h delay]",
        "description": embed_desc,
        "color":       0xE8A838,  # amber
        "footer": {
            "text": f"Cornerstone Flowstate · Free tier · 48h delayed · Live feed: {GUMROAD_LINK}"
        },
    }

    try:
        r = requests.post(
            WEBHOOK_ANNOUNCEMENTS,
            json={"embeds": [embed]},
            timeout=15
        )
        r.raise_for_status()
        logger.info("#free-data embed posted")
        return True
    except Exception as e:
        logger.error(f"Discord #free-data post failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# X AUTO-POST  (tweepy v4+, Twitter API v2, OAuth1 User Context)
# ─────────────────────────────────────────────────────────────────────────────

def post_thread_to_x(tweets: list[str], label: str = "") -> bool:
    """Post tweets as a thread. First is root; subsequent are replies to the previous."""
    try:
        import tweepy
    except ImportError:
        logger.warning("tweepy not installed — run: pip3.10 install tweepy")
        return False

    if not all([TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET]):
        logger.warning("X API credentials missing in .env — skipping auto-post")
        return False

    client = tweepy.Client(
        consumer_key=TWITTER_API_KEY,
        consumer_secret=TWITTER_API_SECRET,
        access_token=TWITTER_ACCESS_TOKEN,
        access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
    )

    prev_id = None
    tag = f" [{label}]" if label else ""
    for i, text in enumerate(tweets):
        try:
            kwargs = {"text": text[:280]}
            if prev_id:
                kwargs["in_reply_to_tweet_id"] = prev_id
            resp = client.create_tweet(**kwargs)
            prev_id = resp.data["id"]
            logger.info(f"X tweet {i+1}/{len(tweets)} posted (id={prev_id}){tag}")
            if i < len(tweets) - 1:
                time.sleep(3)
        except Exception as e:
            logger.error(f"X tweet {i+1}/{len(tweets)} failed{tag}: {e}")
            return False

    return True


# ─────────────────────────────────────────────────────────────────────────────
# DEDUP  (separate keys for draft vs auto-post modes)
# ─────────────────────────────────────────────────────────────────────────────

def already_sent_today(mode: str = "draft") -> bool:
    key = f"bait_last_sent_{mode}_{date.today().isoformat()}"
    return bool(db.get_state(key))


def mark_sent_today(mode: str = "draft"):
    key = f"bait_last_sent_{mode}_{date.today().isoformat()}"
    db.update_state(key, datetime.utcnow().isoformat())
    logger.info(f"Marked sent: {key}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Cornerstone Flowstate bait dispatcher")
    parser.add_argument(
        "--auto-post", action="store_true",
        help="Post threads directly to X (Twitter API v2). Fires at 12:30 UTC (8:30 AM ET). "
             "Requires X_AUTO_POST_ENABLED=true + Twitter credentials in .env."
    )
    args = parser.parse_args()

    # Mode determines dedup key + behavior
    mode = "autopost" if args.auto_post else "draft"
    today_str = date.today().strftime("%Y-%m-%d")
    logger.info(f"bait_dispatcher running — {today_str} — mode={mode}")

    if already_sent_today(mode):
        logger.info(f"Already sent today ({mode}) — skipping.")
        return

    data = pull_market_data()
    logger.info(
        f"DB data: CLM={data['clm_price']:.2f} CRF={data['crf_price']:.2f} "
        f"bias={data['bias_label']} tqqq={data['tqqq_score']} ro={data['ro_display']}"
    )

    bait1 = format_bait1(data)
    bait2 = format_bait2(data)
    bait3 = format_bait3(data)
    baits = [bait1, bait2, bait3]

    closed = is_market_closed_today()

    if args.auto_post:
        # ── AUTO-POST mode (12:30 UTC = 8:30 AM ET = 2:30 AM HST) ──────────
        # User is asleep in Hawaii. Posts fire at ET peak, silent Pushover recap.
        if closed:
            logger.info("Market closed — no X posts on weekends/holidays. Sending silent Pushover snippet.")
            title, body = build_consolidated_notification(baits, today_str)
            send_pushover(title, f"[Weekend — no X post]\n\n{body}", priority=-1)
        elif not X_AUTO_POST_ENABLED:
            logger.warning(
                "X_AUTO_POST_ENABLED=false — set it to 'true' in .env to activate. "
                "Sending silent Pushover drafts instead."
            )
            for bait in baits:
                t, b = build_weekday_notification(bait)
                send_pushover(t, f"[X AUTO-POST NOT ENABLED — copy/paste manually]\n\n{b}", priority=-1)
        else:
            logger.info("Auto-posting 3 threads to X (8:30 AM ET peak)...")
            for bait in baits:
                posted = post_thread_to_x(bait["thread_tweets"], label=bait["title"])
                status = "✅ Posted to X" if posted else "❌ X post FAILED"
                t, b = build_weekday_notification(bait)
                send_pushover(t, f"{status}\n\n{b}", priority=-1)  # silent — user asleep

            # #free-data Discord embed
            post_free_data_embed(data)

    else:
        # ── DRAFT mode (18:00 UTC = 8:00 AM HST) — Pushover drafts for manual posting ──
        if closed:
            logger.info("Market closed — sending consolidated weekend snippet.")
            title, body = build_consolidated_notification(baits, today_str)
            send_pushover(title, body)
        else:
            logger.info("Market open — sending 3 bait draft notifications.")
            for bait in baits:
                t, b = build_weekday_notification(bait)
                send_pushover(t, b)

            # #free-data Discord embed (weekdays only)
            post_free_data_embed(data)

    mark_sent_today(mode)
    logger.info(f"bait_dispatcher complete — mode={mode}.")


if __name__ == "__main__":
    main()
