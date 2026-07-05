"""
daily_pulse.py — Velocity Banking Daily Snapshot
Cashflow ZZZ Machine | Personal Finance Layer

Pulls SimpleFIN account balances (NFCU + AMEX) + CLM/CRF market data from Twelve Data,
formats a concise daily summary, and pushes it to Pushover only — never Discord.

Run once daily via PythonAnywhere Scheduled Tasks, NOT inside run_monitor().
Deliberately standalone so a crash here never affects the 24/7 cornerstone loop.

Usage:
  python daily_pulse.py            # normal daily run (deduped by date)
  python daily_pulse.py --force    # override dedup, re-send today's pulse

Setup (one-time):
  1. Run: python daily_pulse.py --claim
     This converts your SIMPLEFIN_TOKEN into a permanent SIMPLEFIN_ACCESS_URL.
     Copy the printed URL into .env — never re-claim (the token is one-use).
  2. Add to .env:
       SIMPLEFIN_ACCESS_URL=https://...
       SIMPLEFIN_TOKEN=<base64 claim token>   # only needed for --claim step
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

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — all from .env
# ─────────────────────────────────────────────────────────────────────────────
SIMPLEFIN_ACCESS_URL  = os.getenv("SIMPLEFIN_ACCESS_URL", "")
SIMPLEFIN_TOKEN       = os.getenv("SIMPLEFIN_TOKEN", "")
TD_API_KEY            = os.getenv("TWELVE_DATA_API_KEY", "")
PUSHOVER_API_TOKEN    = os.getenv("PUSHOVER_API_TOKEN", "")
PUSHOVER_USER_KEY     = os.getenv("PUSHOVER_USER_KEY", "")

# State file — tracks last run date for deduplication (no DB dependency).
STATE_FILE = os.path.join(BASE_DIR, ".daily_pulse_state.json")

# Low-balance alert threshold — fires an extra Pushover priority-1 if NFCU
# checking drops below this (your 1-month bill buffer target from CLAUDE.md).
NFCU_LOW_BALANCE_THRESHOLD = 2000.0

# CLM/CRF NAV proxies (Mutual Fund tickers on TD Grow)
NAV_TICKERS = {"CLM": "XCLMX", "CRF": "XCRFX"}
NAV_DEFAULTS = {"CLM": 6.45, "CRF": 6.30}

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Claim SimpleFIN Access URL (one-time, --claim flag)
# ─────────────────────────────────────────────────────────────────────────────

def claim_simplefin_access_url():
    """
    Converts the base64 setup token into a permanent Access URL.
    Run once: python daily_pulse.py --claim
    Save the printed URL as SIMPLEFIN_ACCESS_URL in .env — never call this again.
    """
    if not SIMPLEFIN_TOKEN:
        print("❌  SIMPLEFIN_TOKEN not set in .env")
        sys.exit(1)
    try:
        claim_url = base64.b64decode(SIMPLEFIN_TOKEN).decode().strip()
        res = requests.post(claim_url, timeout=20)
        res.raise_for_status()
        access_url = res.text.strip()
        print("\n✅  Access URL claimed successfully.")
        print(f"\nAdd this to your .env as SIMPLEFIN_ACCESS_URL:\n\n  {access_url}\n")
        print("⚠️  Do not re-run --claim — the setup token is one-use only.\n")
    except Exception as e:
        print(f"❌  Claim failed: {e}")
        sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2a — Fetch SimpleFIN Accounts
# ─────────────────────────────────────────────────────────────────────────────

def fetch_simplefin_accounts() -> list[dict]:
    """
    GET {access_url}/accounts — Basic Auth embedded in the Access URL.
    Returns list of account dicts: {name, org, balance, currency, available}.

    SimpleFIN refreshes balances ~once/24h on their backend. Running this
    once daily is sufficient; polling more frequently returns stale data.

    Institution coverage note:
      SimpleFIN's supported-institution list is JavaScript-rendered and cannot
      be verified programmatically. Manually confirm NFCU, AMEX, and E*TRADE
      are listed at bridge.simplefin.org before connecting each account.
      If an institution is not connected, it simply won't appear here.
    """
    if not SIMPLEFIN_ACCESS_URL:
        logger.warning("SIMPLEFIN_ACCESS_URL not set — skipping account fetch")
        return []

    # Access URL format: https://user:pass@bridge.simplefin.org/simplefin
    accounts_url = SIMPLEFIN_ACCESS_URL.rstrip("/") + "/accounts"
    try:
        res = requests.get(accounts_url, timeout=20)
        res.raise_for_status()
        data = res.json()
        accounts = []
        for acct_set in data.get("accounts", []):
            # SimpleFIN response: each entry is an account object
            if isinstance(acct_set, dict):
                org  = acct_set.get("org", {}).get("name", "Unknown")
                name = acct_set.get("name", "Account")
                bal  = float(acct_set.get("balance", 0.0))
                avail = float(acct_set.get("available-balance") or bal)
                curr  = acct_set.get("currency", "USD")
                accounts.append({
                    "org": org, "name": name,
                    "balance": bal, "available": avail, "currency": curr,
                })
        logger.info(f"SimpleFIN: {len(accounts)} account(s) retrieved")
        return accounts
    except Exception as e:
        logger.error(f"SimpleFIN fetch failed: {e}")
        return []

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2b — Fetch CLM/CRF from Twelve Data
# ─────────────────────────────────────────────────────────────────────────────

def fetch_cef_snapshot() -> dict:
    """
    Pulls price, NAV, and premium for CLM and CRF.
    Reuses the same Twelve Data endpoints as monitor.py — no extra credits.
    Returns dict keyed by ticker.
    """
    results = {}
    session = requests.Session()
    for ticker, nav_ticker in NAV_TICKERS.items():
        try:
            p_res = session.get(
                f"https://api.twelvedata.com/price?symbol={ticker}&apikey={TD_API_KEY}",
                timeout=12).json()
            price = float(p_res.get("price", 0.0))

            n_res = session.get(
                f"https://api.twelvedata.com/price?symbol={nav_ticker}&apikey={TD_API_KEY}",
                timeout=12).json()
            nav = float(n_res.get("price", NAV_DEFAULTS[ticker]))

            premium = ((price - nav) / nav * 100) if nav > 0 else 0.0
            results[ticker] = {"price": price, "nav": nav, "premium": premium}
            logger.info(f"{ticker}: ${price:.4f} | NAV ${nav:.4f} | Premium {premium:.1f}%")
        except Exception as e:
            logger.error(f"TD fetch failed for {ticker}: {e}")
            results[ticker] = {"price": 0.0, "nav": NAV_DEFAULTS[ticker], "premium": 0.0}
    return results

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2c — Net Worth Delta (day-over-day)
# ─────────────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def save_state(state: dict):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        logger.error(f"State save failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Format Pushover Message
# ─────────────────────────────────────────────────────────────────────────────

def format_pulse_message(accounts: list[dict], cef: dict, state: dict) -> tuple[str, str, int]:
    """
    Returns (title, message, priority).
    Priority 1 = high (audible alert) for low-balance condition, else 0 = normal.
    """
    today     = date.today().strftime("%b %d, %Y")
    priority  = 0
    lines     = [f"Daily Pulse — {today}\n"]

    # ── Account Balances
    total_cash = 0.0
    nfcu_low   = False
    if accounts:
        lines.append("ACCOUNTS")
        for a in accounts:
            bal_str = f"${a['balance']:,.2f}"
            avail_str = f"${a['available']:,.2f}" if a['available'] != a['balance'] else ""
            avail_tag = f" (avail {avail_str})" if avail_str else ""
            lines.append(f"  {a['org']} — {a['name']}: {bal_str}{avail_tag}")
            # Sum positive balances as cash (exclude credit card debt)
            if a["balance"] > 0:
                total_cash += a["balance"]
            # Low-balance check on any NFCU checking-type account
            if "navy federal" in a["org"].lower() or "nfcu" in a["org"].lower():
                if a["available"] < NFCU_LOW_BALANCE_THRESHOLD:
                    nfcu_low = True
                    priority = 1
    else:
        lines.append("ACCOUNTS")
        lines.append("  ⚠️  No SimpleFIN accounts connected yet")
        lines.append("  Run: python daily_pulse.py --claim to set up")

    # ── CEF Snapshot
    lines.append("")
    lines.append("CORNERSTONE CEFs")
    for ticker, d in cef.items():
        prem_flag = "⚠️ " if d["premium"] > 15 else ""
        lines.append(
            f"  {ticker}: ${d['price']:.4f} | NAV ${d['nav']:.4f} | "
            f"{prem_flag}Premium {d['premium']:+.1f}%"
        )

    # ── Net Worth Delta
    yesterday_total = state.get("total_cash", None)
    if yesterday_total is not None and total_cash > 0:
        delta = total_cash - yesterday_total
        arrow = "↑" if delta >= 0 else "↓"
        lines.append("")
        lines.append(f"NET CASH DELTA vs Yesterday: {arrow} ${abs(delta):,.2f}")

    # ── Low-balance warning
    if nfcu_low:
        lines.append("")
        lines.append(f"⚠️  LOW BALANCE ALERT")
        lines.append(f"  NFCU available balance below ${NFCU_LOW_BALANCE_THRESHOLD:,.0f} buffer target")
        lines.append(f"  Review bill timing — margin paydown may need to pause")

    # ── Premium gate reminder
    for ticker, d in cef.items():
        if d["premium"] > 15:
            lines.append("")
            lines.append(f"⚠️  {ticker} DRIP GATE: Premium {d['premium']:.1f}% > 15% — DRIP paused per accumulation gate")

    title = f"💼 Daily Pulse — {today}"
    if nfcu_low:
        title = f"⚠️ Low Balance — {today}"

    return title, "\n".join(lines), priority

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Dispatch to Pushover
# ─────────────────────────────────────────────────────────────────────────────

def push_to_pushover(title: str, message: str, priority: int = 0):
    """
    Sends to Pushover only — never Discord (personal financial data).
    Priority 1 requires retry + expire params (Pushover API requirement).
    """
    if not PUSHOVER_API_TOKEN or not PUSHOVER_USER_KEY:
        logger.error("Pushover credentials not set — cannot send")
        return False

    payload = {
        "token":   PUSHOVER_API_TOKEN,
        "user":    PUSHOVER_USER_KEY,
        "title":   title,
        "message": message,
        "priority": priority,
    }
    if priority == 1:
        payload["retry"]  = 60   # retry every 60s
        payload["expire"] = 3600 # for up to 1 hour

    try:
        res = requests.post("https://api.pushover.net/1/messages.json", data=payload, timeout=15)
        res.raise_for_status()
        logger.info(f"Pushover dispatched (priority {priority}): {title}")
        return True
    except Exception as e:
        logger.error(f"Pushover dispatch failed: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_daily_pulse(force: bool = False):
    today_str = date.today().isoformat()
    state     = load_state()

    # Deduplication — skip if already ran today (unless --force)
    if not force and state.get("last_run_date") == today_str:
        logger.info("Daily pulse already sent today — use --force to override")
        return

    accounts = fetch_simplefin_accounts()
    cef      = fetch_cef_snapshot()
    title, message, priority = format_pulse_message(accounts, cef, state)

    success = push_to_pushover(title, message, priority)

    if success:
        # Save state for next-day delta calc
        total_cash = sum(a["balance"] for a in accounts if a["balance"] > 0)
        state["last_run_date"] = today_str
        if total_cash > 0:
            state["total_cash"] = total_cash
        save_state(state)


if __name__ == "__main__":
    if "--claim" in sys.argv:
        claim_simplefin_access_url()
        sys.exit(0)

    force = "--force" in sys.argv
    run_daily_pulse(force=force)
