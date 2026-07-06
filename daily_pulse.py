"""
daily_pulse.py — Velocity Banking Daily Snapshot
Cashflow ZZZ Machine | Personal Finance Layer

SimpleFIN accounts + Twelve Data Grow market context → Pushover only (never Discord).
Run once daily via PythonAnywhere Scheduled Tasks — deliberately standalone from run_monitor().

Usage:
  python daily_pulse.py            # normal daily run (deduped by date)
  python daily_pulse.py --force    # override dedup, re-send today
  python daily_pulse.py --claim    # one-time: convert SIMPLEFIN_TOKEN → SIMPLEFIN_ACCESS_URL
"""

import os
import sys
import json
import base64
import logging
import requests
from datetime import datetime, date
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DailyPulse")

SIMPLEFIN_ACCESS_URL = os.getenv("SIMPLEFIN_ACCESS_URL", "")
SIMPLEFIN_TOKEN      = os.getenv("SIMPLEFIN_TOKEN", "")
TD_API_KEY           = os.getenv("TWELVE_DATA_API_KEY", "")
PUSHOVER_API_TOKEN   = os.getenv("PUSHOVER_API_TOKEN", "")
PUSHOVER_USER_KEY    = os.getenv("PUSHOVER_USER_KEY", "")

STATE_FILE = os.path.join(BASE_DIR, ".daily_pulse_state.json")

# Flag words that identify credit card / liability accounts by name
CREDIT_KEYWORDS = ("visa", "mastercard", "card", "credit", "amex", "platinum", "gold")

# Low-balance threshold — NFCU/M1 liquid checking only (not credit cards)
LIQUID_LOW_THRESHOLD = 2000.0

# NAV proxy tickers and defaults
NAV_TICKERS  = {"CLM": "XCLMX", "CRF": "XCRFX"}
NAV_DEFAULTS = {"CLM": 6.45, "CRF": 6.30}

# ─────────────────────────────────────────────────────────────────────────────
# ONE-TIME SETUP — Claim SimpleFIN Access URL
# ─────────────────────────────────────────────────────────────────────────────

def claim_simplefin_access_url():
    if not SIMPLEFIN_TOKEN:
        print("❌  SIMPLEFIN_TOKEN not set in .env")
        sys.exit(1)
    try:
        claim_url  = base64.b64decode(SIMPLEFIN_TOKEN + "=" * (-len(SIMPLEFIN_TOKEN) % 4)).decode().strip()
        res        = requests.post(claim_url, timeout=20)
        res.raise_for_status()
        access_url = res.text.strip()
        print(f"\n✅  Access URL claimed:\n\n  {access_url}\n")
        print("Add as SIMPLEFIN_ACCESS_URL in .env — do not re-run --claim.\n")
    except Exception as e:
        print(f"❌  Claim failed: {e}")
        sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# SIMPLEFIN — Account Fetch + Categorisation
# ─────────────────────────────────────────────────────────────────────────────

def fetch_simplefin_accounts():
    """
    Returns three lists of account dicts: (liquid, credit, brokerage).
    Liquid  = positive-balance checking/savings accounts (cash reserves).
    Credit  = negative-balance credit/card accounts (liabilities owed).
    Brokerage = investment accounts (E*TRADE, M1 invest, etc.).
    """
    if not SIMPLEFIN_ACCESS_URL or SIMPLEFIN_ACCESS_URL.startswith("#"):
        logger.warning("SIMPLEFIN_ACCESS_URL not configured")
        return [], [], []

    try:
        url = SIMPLEFIN_ACCESS_URL.rstrip("/") + "/accounts"
        res = requests.get(url, timeout=20)
        res.raise_for_status()
        raw = res.json().get("accounts", [])
    except Exception as e:
        logger.error(f"SimpleFIN fetch failed: {e}")
        return [], [], []

    liquid, credit, brokerage = [], [], []
    brokerage_orgs = ("e*trade", "etrade", "fidelity", "schwab", "td ameritrade",
                      "vanguard", "robinhood", "webull", "m1 finance")

    for a in raw:
        org   = a.get("org", {}).get("name", "Unknown")
        name  = a.get("name", "Account")
        bal   = float(a.get("balance", 0.0))
        avail = float(a.get("available-balance") or bal)
        entry = {"org": org, "name": name, "balance": bal, "available": avail}

        org_lower  = org.lower()
        name_lower = name.lower()
        is_credit  = (bal < 0) or any(k in name_lower for k in CREDIT_KEYWORDS)
        is_broker  = any(k in org_lower for k in brokerage_orgs)

        if is_credit:
            credit.append(entry)
        elif is_broker:
            brokerage.append(entry)
        else:
            liquid.append(entry)

    logger.info(f"SimpleFIN: {len(liquid)} liquid | {len(credit)} credit | {len(brokerage)} brokerage")
    return liquid, credit, brokerage

# ─────────────────────────────────────────────────────────────────────────────
# TWELVE DATA — CEF Snapshot (price / NAV / RSI / premium)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_cef_snapshot():
    results = {}
    session = requests.Session()
    for ticker, nav_ticker in NAV_TICKERS.items():
        try:
            price = float(session.get(
                f"https://api.twelvedata.com/price?symbol={ticker}&apikey={TD_API_KEY}",
                timeout=12).json().get("price", 0.0))
            nav = float(session.get(
                f"https://api.twelvedata.com/price?symbol={nav_ticker}&apikey={TD_API_KEY}",
                timeout=12).json().get("price", NAV_DEFAULTS[ticker]))
            rsi_res = session.get(
                f"https://api.twelvedata.com/rsi?symbol={ticker}&interval=1day"
                f"&time_period=14&apikey={TD_API_KEY}", timeout=12).json()
            rsi = float(rsi_res.get("values", [{"rsi": 50.0}])[0]["rsi"])
            premium = ((price - nav) / nav * 100) if nav > 0 else 0.0
            results[ticker] = {"price": price, "nav": nav, "rsi": rsi, "premium": premium}
        except Exception as e:
            logger.error(f"CEF fetch failed {ticker}: {e}")
            results[ticker] = {"price": 0.0, "nav": NAV_DEFAULTS[ticker], "rsi": 50.0, "premium": 0.0}
    return results

# ─────────────────────────────────────────────────────────────────────────────
# TWELVE DATA — Market Regime (SPY SMA200 + VIXY z-score)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_market_regime():
    """
    Returns (spy_price, bull_regime, vixy_z, vixy_label).
    Bull regime = SPY above its 200-day SMA.
    VIXY z = fear spike detection vs own 20D mean (calm / elevated / spike).
    """
    session = requests.Session()
    try:
        spy_res = session.get(
            "https://api.twelvedata.com/time_series",
            params={"symbol": "SPY", "interval": "1day", "outputsize": 200, "apikey": TD_API_KEY},
            timeout=15).json()
        spy_vals  = spy_res.get("values", [])
        spy_price = float(spy_vals[0]["close"]) if spy_vals else 0.0
        sma200    = sum(float(v["close"]) for v in spy_vals) / len(spy_vals) if spy_vals else 0.0
        bull      = spy_price > sma200 if sma200 > 0 else None
    except Exception as e:
        logger.error(f"SPY regime fetch failed: {e}")
        spy_price, bull = 0.0, None

    try:
        vix_res  = session.get(
            "https://api.twelvedata.com/time_series",
            params={"symbol": "VIXY", "interval": "1day", "outputsize": 20, "apikey": TD_API_KEY},
            timeout=12).json()
        closes   = [float(v["close"]) for v in vix_res.get("values", [])]
        mean     = sum(closes) / len(closes)
        std      = (sum((c - mean) ** 2 for c in closes) / len(closes)) ** 0.5
        vixy_z   = (closes[0] - mean) / std if std > 0 else 0.0
    except Exception as e:
        logger.error(f"VIXY fetch failed: {e}")
        vixy_z = 0.0

    vixy_label = "calm" if vixy_z < 0.75 else ("elevated ⚠️" if vixy_z < 1.5 else "spike 🚨")
    return spy_price, bull, vixy_z, vixy_label

# ─────────────────────────────────────────────────────────────────────────────
# STATE — Net Worth Delta
# ─────────────────────────────────────────────────────────────────────────────

def load_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        logger.error(f"State save failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# FORMAT — ┣/┗ Pulse Message
# ─────────────────────────────────────────────────────────────────────────────

# Noisy account names from bank feeds → cleaner display labels.
# Keys are substrings (lowercased) found in the raw account name.
ACCOUNT_NAME_MAP = {
    "active duty checking": "Checking",
    "flagship rewards":     "Flagship Visa",
    "visa signature":       "Visa",
    "platinum card":        "Amex Platinum",
    "gold card":            "Amex Gold",
    "individual brokerage": "Brokerage",
}

def _clean_name(org, name):
    """
    Strip trailing (XXXX) account number suffix and apply ACCOUNT_NAME_MAP
    to replace noisy bank-assigned account names with clean display labels.
    """
    import re
    name = re.sub(r"\s*\(?\w{4}\)?\s*$", "", name).strip()
    name_lower = name.lower()
    for fragment, replacement in ACCOUNT_NAME_MAP.items():
        if fragment in name_lower:
            name = replacement
            break
    org_short = org.split()[0] if org and org != "Unknown" else ""
    return f"{org_short} — {name}" if org_short else name


def _delta(current, prev):
    """Signed dollar delta string, or empty string if no prior value."""
    if prev is None:
        return ""
    d = current - prev
    arrow = "↑" if d >= 0 else "↓"
    return f" ({arrow}${abs(d):,.0f})"


def _brokerage_deltas(current, state):
    """
    Returns delta strings for 1D / 3M / 6M / 1Y using dated snapshots
    stored in state['snapshots']. Falls back to 'N/A' until enough history
    accumulates (snapshots are written once per day, so 1Y takes 365 runs).
    """
    today      = date.today()
    snapshots  = state.get("snapshots", {})
    horizons   = {"1D": 1, "3M": 90, "6M": 180, "1Y": 365}
    parts      = []
    for label, days in horizons.items():
        target = (today - __import__("datetime").timedelta(days=days)).isoformat()
        # Find the closest snapshot on or before the target date
        past_dates = sorted(k for k in snapshots if k <= target)
        if past_dates:
            past_val = snapshots[past_dates[-1]]
            d = current - past_val
            arrow = "↑" if d >= 0 else "↓"
            parts.append(f"{label}: {arrow}${abs(d):,.0f}")
        else:
            parts.append(f"{label}: —")
    return " | ".join(parts)


def format_pulse_message(liquid, credit, brokerage, cef, regime, state):
    today     = date.today().strftime("%b %d, %Y")
    spy_price, bull, vixy_z, vixy_label = regime
    lines     = []

    # ── Section 1: Cash Reserves (liquid checking/savings only)
    total_liquid = sum(a["balance"] for a in liquid)
    prev_liquid  = state.get("total_liquid")
    lines.append("CASH RESERVES")
    for a in liquid:
        lines.append(f"┣ {_clean_name(a['org'], a['name'])}: ${a['balance']:,.2f}")
    lines.append(f"┗ Total: ${total_liquid:,.2f}{_delta(total_liquid, prev_liquid)}")

    # ── Section 2: Credit / Liabilities
    total_owed = sum(a["balance"] for a in credit)
    lines.append("")
    lines.append("CREDIT / LIABILITIES")
    if credit:
        for a in credit:
            lines.append(f"┣ {_clean_name(a['org'], a['name'])}: ${a['balance']:,.2f}")
        lines.append(f"┗ Total Owed: ${total_owed:,.2f}")
    else:
        lines.append("┗ No credit balances")

    # ── Section 3: Brokerage (skip zero-balance accounts)
    active_brokers = [a for a in brokerage if a["balance"] != 0]
    total_brokerage = sum(a["balance"] for a in active_brokers if a["balance"] > 0)
    lines.append("")
    lines.append("BROKERAGE")
    for a in active_brokers:
        deltas = _brokerage_deltas(a["balance"], state) if "etrade" in a["org"].lower() or "e*trade" in a["org"].lower() else ""
        lines.append(f"┣ {_clean_name(a['org'], a['name'])}: ${a['balance']:,.2f}")
        if deltas:
            lines.append(f"┃  {deltas}")
    lines.append(f"┗ Total Portfolio: ${total_brokerage:,.2f}{_delta(total_brokerage, state.get('total_brokerage'))}")

    # ── Section 4: Net Worth Snapshot
    net_worth = total_liquid + total_owed + total_brokerage
    lines.append("")
    lines.append("NET WORTH SNAPSHOT")
    lines.append(f"┣ Liquid:    ${total_liquid:,.2f}")
    lines.append(f"┣ Owed:      ${total_owed:,.2f}")
    lines.append(f"┣ Portfolio: ${total_brokerage:,.2f}")
    lines.append(f"┗ Net Worth: ${net_worth:,.2f}{_delta(net_worth, state.get('net_worth'))}")

    # ── Section 5: Market Regime
    lines.append("")
    lines.append("MARKET REGIME")
    regime_str = "Bull — above 200 SMA" if bull else ("Bear — below 200 SMA" if bull is False else "Unknown")
    lines.append(f"┣ SPY: ${spy_price:,.2f} — {regime_str}")
    lines.append(f"┣ VIXY z: {vixy_z:+.1f}σ ({vixy_label})")
    deploy = ("🟢 GO — conditions met" if (bull and vixy_z < 1.5)
              else ("🔴 HOLD — bear regime" if not bull else "⚠️ CAUTION — fear elevated"))
    lines.append(f"┗ Margin Deploy: {deploy}")

    title   = f"💼 Daily Pulse — {today}"
    message = "\n".join(lines)
    return title, message, 0


def _short_name(name):
    """Strip the trailing (XXXX) account number suffix SimpleFIN appends."""
    import re
    return re.sub(r"\s*\(\w+\)\s*$", "", name).strip()

# ─────────────────────────────────────────────────────────────────────────────
# PUSHOVER DISPATCH — personal financial data, never Discord
# ─────────────────────────────────────────────────────────────────────────────

def push_to_pushover(title, message, priority=0):
    if not PUSHOVER_API_TOKEN or not PUSHOVER_USER_KEY:
        logger.error("Pushover credentials missing")
        return False
    payload = {
        "token": PUSHOVER_API_TOKEN, "user": PUSHOVER_USER_KEY,
        "title": title, "message": message, "priority": priority,
    }
    if priority == 1:
        payload["retry"] = 60
        payload["expire"] = 3600
    try:
        requests.post("https://api.pushover.net/1/messages.json", data=payload, timeout=15).raise_for_status()
        logger.info(f"Pushover dispatched (priority {priority}): {title}")
        return True
    except Exception as e:
        logger.error(f"Pushover failed: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_daily_pulse(force=False):
    today_str = date.today().isoformat()
    state     = load_state()

    if not force and state.get("last_run_date") == today_str:
        logger.info("Already sent today — use --force to override")
        return

    liquid, credit, brokerage = fetch_simplefin_accounts()
    regime = fetch_market_regime()

    title, message, _ = format_pulse_message(liquid, credit, brokerage, {}, regime, state)
    success = push_to_pushover(title, message, priority=0)

    if success:
        total_liquid    = sum(a["balance"] for a in liquid)
        active_brokers  = [a for a in brokerage if a["balance"] != 0]
        total_brokerage = sum(a["balance"] for a in active_brokers if a["balance"] > 0)
        net_worth       = total_liquid + sum(a["balance"] for a in credit) + total_brokerage

        # Store dated snapshot for brokerage delta history (1D/3M/6M/1Y)
        snapshots = state.get("snapshots", {})
        snapshots[today_str] = total_brokerage
        # Prune snapshots older than 400 days to keep state file lean
        cutoff = (date.today() - __import__("datetime").timedelta(days=400)).isoformat()
        snapshots = {k: v for k, v in snapshots.items() if k >= cutoff}

        state.update({
            "last_run_date":   today_str,
            "total_liquid":    total_liquid,
            "total_brokerage": total_brokerage,
            "net_worth":       net_worth,
            "snapshots":       snapshots,
        })
        save_state(state)


if __name__ == "__main__":
    if "--claim" in sys.argv:
        claim_simplefin_access_url()
        sys.exit(0)
    run_daily_pulse(force="--force" in sys.argv)
