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
  X:
  1. Apply for Twitter Developer account at developer.twitter.com (free)
  2. Create app with Read+Write permissions → get 4 credentials
  3. Add to .env: X_AUTO_POST_ENABLED=true
                  TWITTER_API_KEY=...
                  TWITTER_API_SECRET=...
                  TWITTER_ACCESS_TOKEN=...
                  TWITTER_ACCESS_TOKEN_SECRET=...
  4. pip install tweepy  (on PA: pip3.10 install tweepy)

  Threads (fires alongside X in auto-post mode):
  1. See threads_client.py module docstring for full one-time OAuth flow
  2. Add to .env: THREADS_AUTO_POST_ENABLED=true
                  THREADS_ACCESS_TOKEN=<long-lived token>
                  THREADS_USER_ID=<your Threads numeric user ID>
                  THREADS_APP_ID=<Meta app ID>
                  THREADS_APP_SECRET=<Meta app secret>
  3. Seed token expiry once: python3.10 threads_client.py --seed-expiry
  4. Auto-refresh handles all subsequent renewals (60-day tokens, refreshed 7d before expiry)

  Both scheduler entries run independently — draft Pushover still fires at 8 AM HST

NOT a financial advisor. Educational content only.
"""

import os
import re
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
# Read-only bearer token for X search (separate from write credentials).
# Optional — add TWITTER_BEARER_TOKEN to .env to enable trend research.
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")

# ── Threads (Instagram) auto-post credentials ──────────────────────────────────
# Fires alongside X in auto-post mode. See threads_client.py for one-time setup.
THREADS_AUTO_POST_ENABLED = os.getenv("THREADS_AUTO_POST_ENABLED", "false").lower() == "true"

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
# HOOK ROTATION  (7 variants, daily cycle — week × weekday combo prevents repetition for 7 weeks)
# BODY ROTATION  (3 variants — different angles on the same framework)
# ─────────────────────────────────────────────────────────────────────────────

def _daily_variant(n_hooks: int) -> int:
    """Selects hook index using ISO week × weekday so the same weekday never repeats the same hook.
    With 7 hooks: cycle before exact weekday+hook repetition = LCM(7,5) = 35 weekdays = 7 weeks."""
    iso = date.today().isocalendar()
    return (iso[1] * 5 + iso[2]) % n_hooks


def _body_variant() -> int:
    """Selects body tweet variant 0-2 using a different offset from hook so they diverge."""
    iso = date.today().isocalendar()
    return (iso[1] + iso[2]) % 3


# Hooks indexed [0..6]
BAIT1_HOOKS = [
    # 0 — personal pain / relatability
    (
        "I held CLM through the 2025 Rights Offering.\n"
        "Watched it drop 12% over 6 weeks.\n"
        "I didn't know when the bottom was.\n\n"
        "The 4-phase RO anatomy:"
    ),
    # 1 — newsjack / EDGAR is public
    (
        "CLM filed an N-2 with the SEC on Aug 14.\n"
        "Most holders found out when the price was already down 9%.\n\n"
        "Rights Offerings are public filings. You can see them coming.\n\n"
        "The protocol:"
    ),
    # 2 — against conventional wisdom
    (
        "Most CLM/CRF holders think the ex-dividend dip is the buy signal.\n"
        "It's not. It's the 4th catalyst. The real entry is earlier.\n\n"
        "The 4-phase anatomy:"
    ),
    # 3 — specific live data hook
    (
        "RO Risk score: {ro_display}\n"
        "CLM ${clm_price:.2f} | CRF ${crf_price:.2f}\n"
        "Premium: {clm_prem_label}\n\n"
        "This is what a live N-2 signal looks like:"
    ),
    # 4 — income math / distribution reset hook
    (
        "CLM paid $0.1215/month on a $7.35 share.\n"
        "Then the Board locked 2027 distributions based on a lower NAV.\n\n"
        "Most retail holders didn't understand what that meant until the price had already moved.\n\n"
        "The mechanism:"
    ),
    # 5 — discovery story / early warning edge
    (
        "The N-2 filing appeared on EDGAR.gov before market open.\n"
        "By the time the market opened, CLM was already down 4%.\n\n"
        "If you know what an N-2 is, you have a head start on every retail holder who doesn't.\n\n"
        "The protocol:"
    ),
    # 6 — information gap hook
    (
        "A CLM/CRF N-2 filing on EDGAR means one thing: Rights Offering incoming.\n"
        "Most retail holders don't know what an N-2 is.\n"
        "By the time they find out, the price has already moved.\n\n"
        "The 4 phases:"
    ),
]

BAIT2_HOOKS = [
    # 0 — confession / personal failure
    (
        "I ran the wheel strategy for 6 months.\n"
        "Net premium collected: basically zero.\n\n"
        "The one filter I was skipping:"
    ),
    # 1 — specific backtest stat
    (
        "5-year wheel backtest WITHOUT the VRP filter: ~1% CAGR.\n"
        "WITH it: 8-12%. Same stocks. Same DTE.\n\n"
        "3 filters. 60 seconds:"
    ),
    # 2 — against convention
    (
        "Most options traders screen for high IVR.\n"
        "That's the wrong starting point.\n\n"
        "IVR tells you IV is elevated vs its own history.\n"
        "It doesn't tell you if the premium is real or a past-event relic.\n\n"
        "The 3-filter stack:"
    ),
    # 3 — specific scenario failure
    (
        "You see a CSP setup with 45% IVR. Looks great.\n"
        "Then the IV crush hits before you even get to expiry.\n\n"
        "IV vs HV30 was flat. That was the tell.\n\n"
        "The 3-filter checklist:"
    ),
    # 4 — IVR alone is not enough
    (
        "I thought any IVR above 40% was a green light for the wheel.\n"
        "Got burned 3 times before I found the second filter.\n\n"
        "IVR tells you IV is elevated. It doesn't tell you the edge is actually there right now.\n\n"
        "The 3-filter stack:"
    ),
    # 5 — earnings trap (the invisible IV destroyer)
    (
        "Earnings within 45 days is the most invisible IV trap.\n"
        "The setup looks perfect. High IVR. Strong premium.\n"
        "Then the report drops. IV crush. The premium evaporates.\n\n"
        "Filter 3 exists for exactly this:"
    ),
    # 6 — simplicity / most people skip it
    (
        "The difference between a wheel that compounds and one that bleeds:\n"
        "5 minutes of pre-screening.\n\n"
        "Most people skip it because they think high IV alone is the signal.\n\n"
        "The 3-filter stack:"
    ),
]

BAIT3_HOOKS = [
    # 0 — time saved / problem-first
    (
        "I used to spend 45 minutes every morning across Bloomberg, CBOE, Finviz, and Twitter.\n"
        "Now I check 5 numbers. Takes 60 seconds.\n\n"
        "Today's read: {bias_label}\n\n"
        "The stack:"
    ),
    # 1 — specific thresholds + actionable levels
    (
        "VIX above 20: stay defensive.\n"
        "VIX above 25 in backwardation: get your puts on.\n"
        "VIX drops back below 1.0 term ratio: that's the LEAP CALL entry window.\n\n"
        "5-signal morning posture — today: {bias_label}"
    ),
    # 2 — relatable failure (no system cost me real money)
    (
        "The worst setups I've taken came from skipping the morning posture.\n"
        "Entered bullish when the regime was already flipping.\n\n"
        "5 numbers. 60 seconds. Today reads {bias_label}.\n\n"
        "The stack:"
    ),
    # 3 — live data / scarcity
    (
        "Most retail analysis takes 45 minutes and still leaves you guessing.\n\n"
        "5 signals. 60 seconds. {bias_label}.\n"
        "TQQQ cycle score: {tqqq_score}/100.\n\n"
        "The stack:"
    ),
    # 4 — worst trade story / confirmation bias
    (
        "The worst trade I ever placed: 4 of these 5 signals were pointing wrong.\n"
        "I ignored them all. I thought I had a setup.\n\n"
        "I had confirmation bias.\n\n"
        "Today reads {bias_label}. The 5-signal stack:"
    ),
    # 5 — VIX term structure edge (most don't know this signal)
    (
        "When VIX term structure flips into backwardation, something is breaking.\n"
        "Most people don't even know what VIX term structure is.\n\n"
        "It's one of 5 signals in the morning posture. Today: {bias_label}"
    ),
    # 6 — CEF cross-signal / CLM/CRF drop tells you the type of move
    (
        "When CLM/CRF drops and SPY is flat, that's a CEF-specific move.\n"
        "When both drop together, that's macro.\n\n"
        "Knowing the difference changes what you do next.\n\n"
        "Morning posture today: {bias_label}. The 5 signals:"
    ),
]

# ─────────────────────────────────────────────────────────────────────────────
# BODY TWEET VARIANTS  (tweets 2+3 in the thread — 3 angles per bait)
# Rotates independently of hooks so content mix stays fresh even on repeat hooks
# ─────────────────────────────────────────────────────────────────────────────

BAIT1_BODY = [
    # Variant 0 — clinical / factual
    [
        (
            "Phase 1 — N-2 Filed: Largest drop of the entire cycle.\n"
            "Institutions exit immediately. Price compresses from premium high to historical low within days.\n\n"
            "Phase 2 — N-2/A (approx. 47 days later): Second wave.\n"
            "Confirms exact sub price. Another 1-3 day flush."
        ),
        (
            "Phase 3 — Record Date (approx. day 59): Historically the cycle low.\n"
            "Open-market buyers who got in BELOW sub price beat RO participants.\n\n"
            "Phase 4 — Ex-Dividend: Mechanical only.\n"
            "Creates 1-3 day accumulation window. Not a seller event."
        ),
    ],
    # Variant 1 — narrative / story angle
    [
        (
            "Phase 1 hits on the N-2 filing day.\n"
            "Most retail holders don't watch EDGAR. By the time they hear about it, price is already down 5-8%.\n\n"
            "Phase 2 — 47 days later — is the second wave.\n"
            "The exact sub price gets set. Another flush."
        ),
        (
            "Phase 3, the record date, is where 2022 and 2025 both bottomed.\n"
            "Open-market buyers at this price beat RO participants.\n\n"
            "Phase 4 is the dividend ex-date. Mechanical drop.\n"
            "It creates a 1-3 day window. Most people mistake it for the real dip."
        ),
    ],
    # Variant 2 — live proof / numbers from the current cycle
    [
        (
            "2026 cycle in real time:\n"
            "N-2 filed Aug 14. CLM dropped $7.35 to $6.38 by Day 42.\n"
            "Premium: 25% to 2% in 11 days.\n\n"
            "N-2/A is next: approx. Oct 1-2. Another 1-3 day flush as the exact sub price gets locked."
        ),
        (
            "Record date expected: Oct 13-16.\n"
            "Ex-dividend confirmed: Oct 15.\n"
            "Both catalysts land in the same week.\n\n"
            "In 2022 and 2025, the record date was the cycle low.\n"
            "Open-market buyers at current prices already beat RO participants."
        ),
    ],
]

BAIT2_BODY = [
    # Variant 0 — clean filter explanations (clinical)
    [
        (
            "Filter 1: IVR above 35%\n"
            "IV is elevated vs its own 52-week history. The premium edge exists in the market.\n\n"
            "Filter 2: IV minus HV30 at least 5 volatility points\n"
            "IV must EXCEED realized vol by 5pp. If IV caught up to a past spike that normalized, the edge is gone."
        ),
        (
            "Filter 3: No earnings within 45 days\n"
            "IV crush after a report destroys the premium edge. Earnings = forced close or max-loss risk.\n\n"
            "Skip Filter 2 and you're selling into a past volatility event. Most common wheel mistake."
        ),
    ],
    # Variant 1 — consequences of skipping each filter
    [
        (
            "Skip Filter 1 (IVR > 35%) and you're selling in a calm market.\n"
            "The premium looks fine — there's just no edge. You're collecting insurance when no one needs it.\n\n"
            "Skip Filter 2 (IV - HV30 >= 5pp) and you're selling into a past spike that already normalized."
        ),
        (
            "Skip Filter 3 (no earnings within 45 days) and you're holding through a binary event.\n"
            "IV crush after a report can cut premium collected by 60-80% overnight.\n\n"
            "All 3 filters together: the edge is structural. Not luck."
        ),
    ],
    # Variant 2 — quick scan checklist format
    [
        (
            "The 60-second pre-trade checklist:\n"
            "IVR above 35%? If no: skip it.\n"
            "IV minus HV30 above 5 points? If no: skip it.\n"
            "Earnings within 45 days? If yes: skip it.\n\n"
            "All 3 green: delta 0.20, 30-45 DTE."
        ),
        (
            "Why these 3 specifically:\n"
            "Filter 1 = IV is elevated vs history.\n"
            "Filter 2 = the premium edge is real, not a past-event relic.\n"
            "Filter 3 = no binary event to destroy the edge.\n\n"
            "Miss any one of them and the 5-year backtest shows ~1% CAGR."
        ),
    ],
]

BAIT3_BODY = [
    # Variant 0 — plain list (signal names + what they mean)
    [
        (
            "1. VIX level — below 20 = calm / above 25 = fear\n"
            "2. VIX term structure — VIXY/VXZ ratio; backwardation = sustained fear, not a one-day spike\n"
            "3. HY Credit Spread — FRED live; above 4.5% = credit stress bleeding into equity risk"
        ),
        (
            "4. SPY vs SMA200 — above = bull regime / below = bear regime\n"
            "5. Fear and Greed Index — below 25 = extreme fear = TQQQ CALL territory\n\n"
            "All 5 lead to one verdict: BULLISH / NEUTRAL / BEARISH.\n"
            "Takes 60 seconds once you have the stack. Most people don't have the stack."
        ),
    ],
    # Variant 1 — how the signals interact / tell a story together
    [
        (
            "VIX level tells you HOW scared the market is.\n"
            "VIX term structure tells you HOW LONG the fear is expected to last.\n"
            "Backwardation = not a one-day spike. Institutional hedging is entrenched.\n\n"
            "HY credit spread connects equity fear to credit market stress."
        ),
        (
            "SPY vs SMA200 tells you the regime.\n"
            "Fear and Greed below 25 in a bearish regime = highest-conviction LEAP CALL setup.\n\n"
            "All 5 together in 60 seconds gives you a posture.\n"
            "One number instead of 45 minutes across 4 different sites."
        ),
    ],
    # Variant 2 — what changes in your behavior based on each reading
    [
        (
            "How the readings change your posture:\n"
            "VIX above 25 + backwardation: reduce wheel delta to 0.15 or sit out.\n"
            "HY spread above 4.5%: margin risk is elevated — don't add leverage.\n"
            "SPY below SMA200: bear regime — LEAPs favor PUTs over CALLs."
        ),
        (
            "Fear and Greed below 25 in a bearish regime: open TQQQ LEAP CALL, 9-18 month DTE.\n"
            "Fear and Greed above 75 in a bullish regime: LEAP PUT desk activates.\n\n"
            "All 5 signals tell you where you are in the cycle.\n"
            "That changes everything downstream."
        ),
    ],
]

# ─────────────────────────────────────────────────────────────────────────────
# FRAMEWORKS (static — delivered in every post)
# ─────────────────────────────────────────────────────────────────────────────

BAIT1_FRAMEWORK = """\
Phase 1 — N-2 Filed
Largest drop of the entire cycle. Institutions exit immediately. Price compresses from premium high to historical low within days.

Phase 2 — N-2/A (approx. 47 days later)
Second wave. Confirms exact sub price. Another 1-3 day flush.

Phase 3 — Record Date (approx. day 59)
Historically the cycle low. Open-market buyers who got in BELOW sub price beat RO participants.

Phase 4 — Ex-Dividend
Mechanical only. Creates 1-3 day accumulation window. Not a seller event.

Most retail holders panic at Phase 1 and miss the optimal entry at Phase 3."""

BAIT2_FRAMEWORK = """\
Filter 1: IVR above 35%
IV is elevated vs its own 52-week history. The premium edge exists in the market.

Filter 2: IV minus HV30 at least 5 volatility points
IV must EXCEED realized volatility by 5pp. If IV just caught up to a past spike that already normalized, the edge is gone.

Filter 3: No earnings within 45 days
IV crush after a report destroys the premium edge. Earnings = forced close or max-loss risk.

Skip Filter 2 and you're selling into a past volatility event. That's the most common wheel mistake."""

BAIT3_FRAMEWORK = """\
1. VIX level — below 20 = calm / above 25 = fear
2. VIX term structure — VIXY/VXZ ratio; backwardation = sustained fear, not a one-day spike
3. HY Credit Spread — FRED live; above 4.5% = credit stress bleeding into equity risk
4. SPY vs SMA200 — above = bull regime / below = bear regime
5. Fear and Greed Index — below 25 = extreme fear = TQQQ CALL territory

All 5 lead to one verdict: BULLISH / NEUTRAL / BEARISH.
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

BAIT1_CTA_DIRECT = f"Live RO signal + entry alerts → {GUMROAD_LINK}\nFree tier included — no card needed."
BAIT2_CTA_DIRECT = f"Live screener + which tickers pass today → {GUMROAD_LINK}\nFree tier included — no card needed."
BAIT3_CTA_DIRECT = f"Full morning brief + TQQQ cycle score → {GUMROAD_LINK}\nFree tier included — no card needed."

# ─────────────────────────────────────────────────────────────────────────────
# HASHTAGS  (5 max per post — #FinTwit always included)
# ─────────────────────────────────────────────────────────────────────────────
BAIT1_HASHTAGS = "#CLM #CRF #ClosedEndFunds #DividendInvesting #FinTwit"
BAIT2_HASHTAGS = "#OptionsTrading #TheWheel #CashSecuredPuts #PassiveIncome #FinTwit"
BAIT3_HASHTAGS = "#TQQQ #PreMarket #IncomeInvesting #DividendInvesting #FinTwit"

# ─────────────────────────────────────────────────────────────────────────────
# RO / DISTRIBUTION CONSTANTS  (mirrors CLAUDE.md §0-B — update each cycle)
# ─────────────────────────────────────────────────────────────────────────────
CLM_ANNUAL_DIST   = 1.458     # $0.1215/month × 12 — 2026 confirmed
CRF_ANNUAL_DIST   = 1.4112    # $0.1176/month × 12 — 2026 confirmed
CLM_FAIR_VALUE    = 7.67      # annual_dist / 0.19
CRF_FAIR_VALUE    = 7.43
CLM_SUB_PRICE_EST = 6.56      # 104% × CLM NAV ~$6.31 — update after 424B3 sets exact price
CRF_SUB_PRICE_EST = 6.37      # 104% × CRF NAV ~$6.12
N2_FILED_DATE     = "2026-08-14"  # update to actual N-2 date each new cycle

# ─────────────────────────────────────────────────────────────────────────────
# CONTENT ANGLES  (drive hook selection + framing — set by research engine)
# ─────────────────────────────────────────────────────────────────────────────
ANGLE_EDGAR_BREAKING = "EDGAR_BREAKING"   # new N-2/N-2/A/424B3 in last 3 days
ANGLE_PRE_CATALYST   = "PRE_CATALYST"     # ≤7 days to N-2/A or record date
ANGLE_RO_MID_CYCLE   = "RO_MID_CYCLE"    # active RO, phase B/C (Day 15-55)
ANGLE_ENTRY_SIGNAL   = "ENTRY_SIGNAL"     # open-market price ≤ sub price
ANGLE_MACRO_STRESS   = "MACRO_STRESS"     # HY spread >4.5% or VIX backwardation
ANGLE_YIELD_COMPARE  = "YIELD_COMPARE"    # yield >20% at current price
ANGLE_TREND_RESPONSE = "TREND_RESPONSE"   # X community talking about CLM/CRF pain
ANGLE_DEFAULT        = "DEFAULT"          # fallback: date-based rotation

# Angle → preferred hook index per bait (None = fall back to _daily_variant)
ANGLE_HOOK_MAP = {
    ANGLE_EDGAR_BREAKING: {"bait1": 1, "bait2": None, "bait3": None},
    ANGLE_PRE_CATALYST:   {"bait1": 2, "bait2": None, "bait3": None},
    ANGLE_RO_MID_CYCLE:   {"bait1": 5, "bait2": None, "bait3": 6},
    ANGLE_ENTRY_SIGNAL:   {"bait1": 3, "bait2": None, "bait3": 3},
    ANGLE_MACRO_STRESS:   {"bait1": 0, "bait2": None, "bait3": 1},
    ANGLE_YIELD_COMPARE:  {"bait1": 4, "bait2": None, "bait3": 0},
    ANGLE_TREND_RESPONSE: {"bait1": 6, "bait2": None, "bait3": 2},
}

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
        clm_prem_label = f"elevated (+{clm_z:.1f} above avg)"
    elif clm_z <= -0.5:
        clm_prem_label = f"near-NAV ({clm_z:+.1f} below avg)"
    else:
        clm_prem_label = f"normal range ({clm_z:+.1f} vs avg)"

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
# RESEARCH ENGINE  (runs before content generation — drives angle + template selection)
# Sources: EDGAR EFTS, X search, DB state (FRED/SentiSense cached by other scripts)
# All external calls cached to DB per-day; fails gracefully on any source outage.
# ─────────────────────────────────────────────────────────────────────────────

def _check_edgar_today() -> dict | None:
    """
    Searches EDGAR EFTS for new CLM/CRF N-2/N-2/A/424B3 filings in the last 3 days.
    Cached to DB as bait_edgar_check_{date} so EDGAR is never hit more than once/day.
    Returns {ticker, type, date, url} or None.
    """
    today_str = date.today().isoformat()
    cache_key = f"bait_edgar_check_{today_str}"
    cached = db.get_state(cache_key)
    if cached is not None:
        try:
            return json.loads(cached) if cached and cached != "null" else None
        except Exception:
            pass

    since = (date.today() - timedelta(days=3)).isoformat()
    found = None

    for ticker, cik in [("CLM", "0000814083"), ("CRF", "0000033934")]:
        try:
            r = requests.get(
                "https://efts.sec.gov/LATEST/search-index",
                params={
                    "q": f'"{ticker.lower()}"',
                    "forms": "N-2,N-2/A,424B3",
                    "dateRange": "custom",
                    "startdt": since,
                    "enddt": today_str,
                },
                timeout=10,
                headers={"User-Agent": "CornerFlowstate/1.0 alwinalmazan@gmail.com"},
            )
            if r.ok:
                hits = r.json().get("hits", {}).get("hits", [])
                for h in hits:
                    src  = h.get("_source", {})
                    form = src.get("form_type", "")
                    if form in ("N-2", "N-2/A", "424B3"):
                        found = {
                            "ticker": ticker,
                            "type":   form,
                            "date":   src.get("file_date", ""),
                            "url":    (
                                f"https://www.sec.gov/cgi-bin/browse-edgar"
                                f"?action=getcompany&CIK={cik}&type=N-2&dateb=&owner=include&count=5"
                            ),
                        }
                        break
            if found:
                break
            time.sleep(1)
        except Exception as e:
            logger.warning(f"EDGAR check failed for {ticker}: {e}")

    db.update_state(cache_key, json.dumps(found))
    if found:
        logger.info(f"EDGAR: {found['type']} found for {found['ticker']} ({found['date']})")
    return found


def _get_x_trending() -> list[str]:
    """
    Searches recent X posts for CLM/CRF community sentiment themes.
    Uses bearer token (read-only) — add TWITTER_BEARER_TOKEN to .env to enable.
    Themes returned: price_pain, buy_signal, yield_focus, ro_awareness, nav_premium.
    Fails gracefully — empty list if no token or search fails.
    """
    if not TWITTER_BEARER_TOKEN:
        return []
    today_str = date.today().isoformat()
    cache_key = f"bait_x_trends_{today_str}"
    cached = db.get_state(cache_key)
    if cached is not None:
        try:
            return json.loads(cached) or []
        except Exception:
            pass

    themes = []
    try:
        import tweepy
        client  = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN)
        results = client.search_recent_tweets(
            query=(
                '($CLM OR $CRF OR "closed end fund" OR "rights offering" CLM) '
                'lang:en -is:retweet'
            ),
            max_results=10,
            tweet_fields=["public_metrics", "text"],
        )
        if results.data:
            for tweet in results.data:
                t = tweet.text.lower()
                if any(w in t for w in ["drop", "fell", "down", "loss", "bag"]):
                    themes.append("price_pain")
                if any(w in t for w in ["buy", "accumulate", "dip", "add"]):
                    themes.append("buy_signal")
                if any(w in t for w in ["dividend", "yield", "income", "distribution"]):
                    themes.append("yield_focus")
                if any(w in t for w in ["rights", "n-2", "offering", "dilut", "ro"]):
                    themes.append("ro_awareness")
                if any(w in t for w in ["nav", "premium", "discount"]):
                    themes.append("nav_premium")
            themes = list(set(themes))
    except Exception as e:
        logger.info(f"X trending search skipped: {e}")

    db.update_state(cache_key, json.dumps(themes))
    return themes


def _get_ro_cycle_context() -> dict:
    """
    Computes RO cycle timing, phase labels, catalyst ETAs, and per-ticker value math.
    Pure calculation from CLAUDE.md constants — zero API calls.
    """
    today = date.today()
    days_since_n2 = 0
    try:
        n2_date       = date.fromisoformat(N2_FILED_DATE)
        days_since_n2 = (today - n2_date).days
        n2a_est       = n2_date + timedelta(days=47)
        record_est    = n2_date + timedelta(days=59)
        expiry_est    = n2_date + timedelta(days=84)
        days_to_n2a    = (n2a_est - today).days
        days_to_record = (record_est - today).days
        days_to_expiry = (expiry_est - today).days
    except Exception:
        n2a_est = record_est = expiry_est = None
        days_to_n2a = days_to_record = days_to_expiry = None

    if days_since_n2 < 14:
        ro_phase, ro_phase_label = "A", "Post-filing panic (0-14d)"
    elif days_since_n2 < 40:
        ro_phase, ro_phase_label = "B", "Consolidation window (15-40d)"
    elif days_since_n2 < 55:
        ro_phase, ro_phase_label = "C", "Pre-N2/A watch (41-55d)"
    elif days_since_n2 < 70:
        ro_phase, ro_phase_label = "D", "N-2/A + record date approach (56-70d)"
    elif days_since_n2 < 90:
        ro_phase, ro_phase_label = "E", "Subscription window / clearing (71-90d)"
    else:
        ro_phase, ro_phase_label = "F", "Post-RO recovery"

    return {
        "days_since_n2":   days_since_n2,
        "ro_phase":        ro_phase,
        "ro_phase_label":  ro_phase_label,
        "n2a_est":         n2a_est.isoformat()    if n2a_est    else "TBD",
        "record_est":      record_est.isoformat() if record_est else "TBD",
        "expiry_est":      expiry_est.isoformat() if expiry_est else "TBD",
        "days_to_n2a":     days_to_n2a,
        "days_to_record":  days_to_record,
        "days_to_expiry":  days_to_expiry,
        "clm_sub_price":   CLM_SUB_PRICE_EST,
        "crf_sub_price":   CRF_SUB_PRICE_EST,
    }


def _gather_research_context(data: dict) -> dict:
    """
    Orchestrates all pre-post research. Returns enriched context dict.
    The 'angle' key drives hook selection and post framing for the day.
    Call once per run — results cached to DB so repeat calls are free.
    """
    logger.info("Research engine running...")

    edgar  = _check_edgar_today()
    x_trends = _get_x_trending()
    ro_ctx = _get_ro_cycle_context()

    # Macro signals already cached to DB by monitor.py / market_analysis.py
    hy_spread  = _safe_float(db.get_state("hy_spread_cached"),     0.0)
    vix_slope  = _safe_float(db.get_state("vix_term_slope"),       1.0)
    fred_yield = _safe_float(db.get_state("fred_yield_spread"),    0.0)

    # Per-ticker live yield at current price
    clm_yield = (CLM_ANNUAL_DIST / data["clm_price"] * 100) if data["clm_price"] > 0 else 0.0
    crf_yield = (CRF_ANNUAL_DIST / data["crf_price"] * 100) if data["crf_price"] > 0 else 0.0

    # Open-market vs RO sub price comparison
    clm_vs_sub       = (data["clm_price"] - ro_ctx["clm_sub_price"]) if data["clm_price"] > 0 else None
    crf_vs_sub       = (data["crf_price"] - ro_ctx["crf_sub_price"]) if data["crf_price"] > 0 else None
    open_market_wins = (clm_vs_sub is not None and clm_vs_sub < 0)

    research = {
        **data,
        "edgar_event":      edgar,
        "x_trends":         x_trends,
        "ro_ctx":           ro_ctx,
        "hy_spread":        hy_spread,
        "vix_slope":        vix_slope,
        "fred_yield":       fred_yield,
        "clm_yield":        clm_yield,
        "crf_yield":        crf_yield,
        "clm_vs_sub":       clm_vs_sub,
        "crf_vs_sub":       crf_vs_sub,
        "open_market_wins": open_market_wins,
    }

    research["angle"] = _select_angle(research)

    logger.info(
        f"Research complete — angle={research['angle']} edgar={'YES:'+edgar['type'] if edgar else 'None'} "
        f"x_trends={x_trends} ro_phase={ro_ctx['ro_phase']} "
        f"clm_yield={clm_yield:.1f}% open_market_wins={open_market_wins}"
    )
    return research


def _select_angle(research: dict) -> str:
    """Priority-ordered angle selection. Drives hook choice and post framing."""
    ro = research.get("ro_ctx", {})

    # P1 — breaking EDGAR event (last 3 days)
    if research.get("edgar_event"):
        return ANGLE_EDGAR_BREAKING

    # P2 — within 7 days of a major RO catalyst
    if research.get("ro_active"):
        d_n2a    = ro.get("days_to_n2a")
        d_record = ro.get("days_to_record")
        if (d_n2a    is not None and 0 <= d_n2a    <= 7) or \
           (d_record is not None and 0 <= d_record <= 7):
            return ANGLE_PRE_CATALYST

    # P3 — active RO mid-cycle (phases B or C — where we are Day 42)
    if research.get("ro_active") and ro.get("ro_phase") in ("B", "C"):
        return ANGLE_RO_MID_CYCLE

    # P4 — open-market buyer beating RO sub price (active RO)
    if research.get("open_market_wins") and research.get("ro_active"):
        return ANGLE_ENTRY_SIGNAL

    # P5 — macro stress (HY spread elevated or VIX in backwardation)
    if research.get("hy_spread", 0) > 4.5 or research.get("vix_slope", 1.0) < 1.0:
        return ANGLE_MACRO_STRESS

    # P6 — X community expressing CLM/CRF pain = teachable moment
    if "price_pain" in research.get("x_trends", []) or \
       "ro_awareness" in research.get("x_trends", []):
        return ANGLE_TREND_RESPONSE

    # P7 — yield compelling at current price
    if research.get("clm_yield", 0) > 20:
        return ANGLE_YIELD_COMPARE

    return ANGLE_DEFAULT


def _get_hook_idx(angle: str, bait_key: str, n_hooks: int) -> int:
    """Returns angle-preferred hook index, falling back to date rotation."""
    preferred = ANGLE_HOOK_MAP.get(angle, {}).get(bait_key)
    if preferred is not None and preferred < n_hooks:
        return preferred
    return _daily_variant(n_hooks)


def _seo_add_cashtags(tweet: str) -> str:
    """
    Ensures $CLM and $CRF cashtags appear in tweet 1 for X topic indexing.
    X algorithm uses cashtags for finance topic discovery (separate from hashtags).
    Appended to first line only if not already present — never breaks mid-sentence.
    """
    has_clm = "$CLM" in tweet or "CLM" in tweet.split("\n")[0]
    has_crf = "$CRF" in tweet or "CRF" in tweet.split("\n")[0]
    if has_clm and has_crf:
        return tweet
    tags = ""
    if not has_clm:
        tags += " $CLM"
    if not has_crf:
        tags += " $CRF"
    lines = tweet.split("\n", 1)
    lines[0] = lines[0].rstrip() + tags
    return "\n".join(lines)


def _build_research_summary(research: dict) -> str:
    """
    Human-readable research brief for Pushover draft notifications.
    Shows what was found today and WHY this angle/hook was selected.
    Appears once at the top of the first bait notification.
    """
    ro      = research.get("ro_ctx", {})
    edgar   = research.get("edgar_event")
    x_tr    = research.get("x_trends", [])
    angle   = research.get("angle", ANGLE_DEFAULT)

    edgar_line   = (
        f"EDGAR: {edgar['type']} filed {edgar['date']} ({edgar['ticker']}) — BREAKING"
        if edgar else "EDGAR: No new filing in last 3 days"
    )
    trends_line  = (
        f"X search: themes found — {', '.join(x_tr)}"
        if x_tr else "X search: no bearer token / no results"
    )
    macro_line   = (
        f"HY spread {research['hy_spread']:.2f}% | "
        f"VIX slope {research['vix_slope']:.3f} "
        f"({'backwardation' if research['vix_slope'] < 1.0 else 'contango'})"
    ) if research.get("hy_spread") else ""

    days   = ro.get("days_since_n2", 0)
    phase  = ro.get("ro_phase_label", "—")
    d_n2a  = ro.get("days_to_n2a")
    d_rec  = ro.get("days_to_record")
    cats   = []
    if d_n2a  is not None and d_n2a  >= 0: cats.append(f"N-2/A in ~{d_n2a}d ({ro.get('n2a_est','?')})")
    if d_rec  is not None and d_rec  >= 0: cats.append(f"Record date in ~{d_rec}d ({ro.get('record_est','?')})")

    vs_sub = ""
    if research.get("clm_vs_sub") is not None:
        direction = "WINS vs RO sub" if research["open_market_wins"] else "above sub price"
        vs_sub = (
            f"CLM ${research['clm_price']:.2f} vs sub ~${CLM_SUB_PRICE_EST:.2f} "
            f"— open-market buyer {direction} (${abs(research['clm_vs_sub']):.2f})\n"
        )

    yield_line = ""
    if research.get("clm_yield", 0) > 0:
        yield_line = f"Yield at price: CLM {research['clm_yield']:.1f}% | CRF {research['crf_yield']:.1f}%\n"

    return (
        f"── RESEARCH [{date.today().isoformat()}] ──\n"
        f"{edgar_line}\n"
        f"{trends_line}\n"
        f"Angle: {angle}\n"
        f"RO: Day {days} — {phase}"
        + (f" | Upcoming: {' | '.join(cats)}" if cats else "")
        + f"\n{vs_sub}"
        + f"{yield_line}"
        + (f"{macro_line}\n" if macro_line else "")
        + f"Bias: {research.get('bias_label','?')} | TQQQ: {research.get('tqqq_score',0)}/100"
    )


# ─────────────────────────────────────────────────────────────────────────────
# FORMAT BAITS  (returns dict with title, hook, framework, cta, hashtags, x_draft)
# ─────────────────────────────────────────────────────────────────────────────

def format_bait1(data: dict, research: dict | None = None) -> dict:
    angle     = (research or {}).get("angle", ANGLE_DEFAULT)
    hook_idx  = _get_hook_idx(angle, "bait1", len(BAIT1_HOOKS))
    hook      = BAIT1_HOOKS[hook_idx].format(**data)
    hook_seo  = _seo_add_cashtags(hook)   # X: ensures $CLM $CRF on tweet 1
    cta       = BAIT1_CTA_ENGAGEMENT if USE_ENGAGEMENT_CTA else BAIT1_CTA_DIRECT
    body      = BAIT1_BODY[_body_variant()]

    if USE_ENGAGEMENT_CTA:
        cta_tweet = (
            f"Most retail holders panic at Phase 1 and miss the real entry at Phase 3.\n\n"
            f"{BAIT1_CTA_ENGAGEMENT}\n\n"
            f"{BAIT1_HASHTAGS}"
        )
    else:
        cta_tweet = (
            f"Most retail holders panic at Phase 1 and miss the real entry at Phase 3.\n\n"
            f"Live RO signal + entry alerts:\n{GUMROAD_LINK}\n\n"
            f"{BAIT1_HASHTAGS}"
        )
    thread_tweets = [hook_seo, body[0], body[1], cta_tweet]

    x_draft = (
        f"{hook_seo}\n\n"
        f"{BAIT1_FRAMEWORK}\n\n"
        f"{cta}\n\n"
        f"{BAIT1_HASHTAGS}\n\n"
        f"{NFA}"
    )
    return {
        "emoji":         "🚨",
        "title":         f"Bait 1 — CLM/CRF RO [{angle}]",
        "hook":          hook_seo,
        "framework":     BAIT1_FRAMEWORK,
        "cta":           cta,
        "hashtags":      BAIT1_HASHTAGS,
        "x_draft":       x_draft,
        "thread_tweets": thread_tweets,
    }


def format_bait2(data: dict, research: dict | None = None) -> dict:
    angle    = (research or {}).get("angle", ANGLE_DEFAULT)
    hook_idx = _get_hook_idx(angle, "bait2", len(BAIT2_HOOKS))
    hook     = BAIT2_HOOKS[hook_idx]
    cta      = BAIT2_CTA_ENGAGEMENT if USE_ENGAGEMENT_CTA else BAIT2_CTA_DIRECT
    body     = BAIT2_BODY[_body_variant()]

    if USE_ENGAGEMENT_CTA:
        cta_tweet = f"{BAIT2_CTA_ENGAGEMENT}\n\n{BAIT2_HASHTAGS}"
    else:
        cta_tweet = f"Live screener + which tickers pass today:\n{GUMROAD_LINK}\n\n{BAIT2_HASHTAGS}"
    thread_tweets = [hook, body[0], body[1], cta_tweet]

    x_draft = (
        f"{hook}\n\n"
        f"{BAIT2_FRAMEWORK}\n\n"
        f"{cta}\n\n"
        f"{BAIT2_HASHTAGS}\n\n"
        f"{NFA}"
    )
    return {
        "emoji":         "📋",
        "title":         f"Bait 2 — Wheel 3-Filter [{angle}]",
        "hook":          hook,
        "framework":     BAIT2_FRAMEWORK,
        "cta":           cta,
        "hashtags":      BAIT2_HASHTAGS,
        "x_draft":       x_draft,
        "thread_tweets": thread_tweets,
    }


def format_bait3(data: dict, research: dict | None = None) -> dict:
    angle     = (research or {}).get("angle", ANGLE_DEFAULT)
    hook_idx  = _get_hook_idx(angle, "bait3", len(BAIT3_HOOKS))
    hook      = BAIT3_HOOKS[hook_idx].format(**data)
    cta       = BAIT3_CTA_ENGAGEMENT if USE_ENGAGEMENT_CTA else BAIT3_CTA_DIRECT
    body      = BAIT3_BODY[_body_variant()]

    tqqq_score = data.get("tqqq_score", "?")
    if USE_ENGAGEMENT_CTA:
        cta_tweet = f"{BAIT3_CTA_ENGAGEMENT}\n\n{BAIT3_HASHTAGS}"
    else:
        cta_tweet = (
            f"Full 12-signal brief + live TQQQ score ({tqqq_score}/100) — subscribers only.\n"
            f"Subscribers see all 5 readings + today's entry threshold.\n"
            f"Free tier: {GUMROAD_LINK}\n\n"
            f"{BAIT3_HASHTAGS}"
        )
    thread_tweets = [hook, body[0], body[1], cta_tweet]

    x_draft = (
        f"{hook}\n\n"
        f"{BAIT3_FRAMEWORK}\n\n"
        f"{cta}\n\n"
        f"{BAIT3_HASHTAGS}\n\n"
        f"{NFA}"
    )
    return {
        "emoji":         "📊",
        "title":         f"Bait 3 — Morning Posture [{angle}]",
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


def build_weekday_notification(bait: dict, research_summary: str = "") -> tuple[str, str]:
    """Returns (title, message) for a single-bait Pushover.
    research_summary is prepended to the first bait only (pass "" for subsequent baits).
    """
    title = f"[CF] {bait['emoji']} {bait['title']}"
    body  = ""
    if research_summary:
        body += f"{research_summary}\n\n"
    body += (
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

    CLM_FAIR_VALUE = 7.67  # 2026 confirmed (annual_dist / 0.19)
    CRF_FAIR_VALUE = 7.43

    if data["ro_active"]:
        signal_line = "🚨 Active RO — N-2 filed, monitoring daily"
        clm_vs_fv = ((data["clm_price"] / CLM_FAIR_VALUE) - 1) * 100 if data["clm_price"] > 0 else 0
        outcome_note = (
            f"CLM at ${data['clm_price']:.2f} ({clm_vs_fv:+.1f}% vs fair value ${CLM_FAIR_VALUE:.2f}). "
            f"Subscribers tracking live entry zones."
        )
    else:
        signal_line = "📡 Monitoring — no active RO signal"
        prem_str = f"{data['clm_prem_pct']:+.1f}%" if data["clm_price"] > 0 else "—"
        outcome_note = f"CLM premium {prem_str} vs NAV. Daily signal check: clean."

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
# THREADS CONTENT ADAPTER
# ─────────────────────────────────────────────────────────────────────────────

_EMOJI_RE = re.compile(
    "[\U00010000-\U0010ffff"   # supplementary multilingual plane (most emojis)
    "\U0001F300-\U0001F9FF"   # misc symbols, emoticons, transport
    "\U00002600-\U000027BF"   # misc symbols (sun, moon, etc.)
    "\U0000FE00-\U0000FE0F"   # variation selectors
    "\U000024C2-\U0001F251"   # enclosed chars
    "]+",
    flags=re.UNICODE,
)


def _adapt_for_threads(tweets: list[str]) -> list[str]:
    """
    Adapts X thread_tweets for Threads posting:
    - Strips lines that are exclusively hashtags (Meta confirmed hashtags don't boost
      organic reach on Threads — they clutter the CTA without SEO benefit)
    - Strips any remaining emoji characters as a safety net (Threads API is strict
      about certain Unicode code points in some content categories)
    - Keeps all substantive content identical (within Threads 500-char limit)
    """
    adapted = []
    for tweet in tweets:
        lines = tweet.split("\n")
        cleaned = []
        for line in lines:
            words = line.strip().split()
            # Drop lines where every word is a hashtag
            if words and all(w.startswith("#") for w in words):
                continue
            # Strip any stray emoji characters
            line = _EMOJI_RE.sub("", line).strip()
            cleaned.append(line)
        text = "\n".join(cleaned).strip()
        while "  " in text:
            text = text.replace("  ", " ")
        while "\n\n\n" in text:
            text = text.replace("\n\n\n", "\n\n")
        adapted.append(text.strip())
    return adapted


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

    data     = pull_market_data()
    research = _gather_research_context(data)   # research step — before any content generation
    research_summary = _build_research_summary(research)

    logger.info(
        f"DB data: CLM={data['clm_price']:.2f} CRF={data['crf_price']:.2f} "
        f"bias={data['bias_label']} tqqq={data['tqqq_score']} ro={data['ro_display']} "
        f"angle={research['angle']}"
    )

    bait1 = format_bait1(data, research)
    bait2 = format_bait2(data, research)
    bait3 = format_bait3(data, research)
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
            logger.info("Auto-posting 3 threads to X + Threads (8:30 AM ET peak)...")
            try:
                from threads_client import post_thread as _post_threads
            except ImportError:
                _post_threads = None

            for i, bait in enumerate(baits):
                # ── X ──────────────────────────────────────────────────────
                x_posted = post_thread_to_x(bait["thread_tweets"], label=bait["title"])
                x_status = "✅ X" if x_posted else "❌ X FAILED"

                # ── Threads ─────────────────────────────────────────────────
                th_status = "⏸ Threads disabled"
                if THREADS_AUTO_POST_ENABLED and _post_threads is not None:
                    th_posts   = _adapt_for_threads(bait["thread_tweets"])
                    th_posted  = _post_threads(th_posts, db=db, label=bait["title"])
                    th_status  = "✅ Threads" if th_posted else "❌ Threads FAILED"
                elif THREADS_AUTO_POST_ENABLED and _post_threads is None:
                    th_status  = "❌ threads_client.py missing"

                t, b = build_weekday_notification(bait, research_summary if i == 0 else "")
                send_pushover(t, f"{x_status} · {th_status}\n\n{b}", priority=-1)  # silent
                if i < len(baits) - 1:
                    logger.info("Waiting 5 min before next thread (natural cadence)...")
                    time.sleep(300)

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
            for i, bait in enumerate(baits):
                t, b = build_weekday_notification(bait, research_summary if i == 0 else "")
                send_pushover(t, b)

            # #free-data Discord embed (weekdays only)
            post_free_data_embed(data)

    mark_sent_today(mode)
    logger.info(f"bait_dispatcher complete — mode={mode}.")


if __name__ == "__main__":
    main()
