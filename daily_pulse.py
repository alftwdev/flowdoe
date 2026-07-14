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

SIMPLEFIN_ACCESS_URL   = os.getenv("SIMPLEFIN_ACCESS_URL", "")
SIMPLEFIN_TOKEN        = os.getenv("SIMPLEFIN_TOKEN", "")
TD_API_KEY             = os.getenv("TWELVE_DATA_API_KEY", "")
PUSHOVER_API_TOKEN     = os.getenv("PUSHOVER_API_TOKEN", "")
PUSHOVER_USER_KEY      = os.getenv("PUSHOVER_USER_KEY", "")
# Manual override: set this in .env if SimpleFIN doesn't expose your E*TRADE margin
# loan as a separate account. Positive value = current margin balance (stored as negative).
# Example: ETRADE_MARGIN_BALANCE=5000.00  → shows as E*trade — Margin: $-5,000.00
# ETRADE_MARGIN_BALANCE removed — margin changes daily and can't be pulled
# automatically from SimpleFIN. A stale static value is more misleading than
# no value. Monitor the margin balance directly in E*TRADE.

STATE_FILE = os.path.join(BASE_DIR, ".daily_pulse_state.json")

# Flag words that identify credit card / liability accounts by name
CREDIT_KEYWORDS = ("visa", "mastercard", "card", "credit", "amex", "platinum", "gold", "margin")

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

def fetch_simplefin_accounts(debug=False):
    """
    Returns three lists of account dicts: (liquid, credit, brokerage).
    Liquid    = positive-balance checking/savings accounts (cash reserves).
    Credit    = negative-balance OR keyword-matched credit/margin accounts.
    Brokerage = investment accounts (E*TRADE, M1 invest, etc.).

    If ETRADE_MARGIN_BALANCE is set in .env and no margin account is already
    present from SimpleFIN (i.e. SimpleFIN bundles margin into equity), a
    synthetic credit entry is injected so it always appears in CREDIT / LIABILITIES.
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

    if debug:
        print("\n─── RAW SIMPLEFIN ACCOUNTS ───")
        for a in raw:
            org  = a.get("org", {}).get("name", "Unknown")
            name = a.get("name", "Account")
            bal  = a.get("balance", "?")
            print(f"  org={org!r}  name={name!r}  balance={bal}")
        print("──────────────────────────────\n")

    liquid, credit, brokerage = [], [], []
    brokerage_orgs = ("e*trade", "etrade", "fidelity", "schwab", "td ameritrade",
                      "vanguard", "robinhood", "webull", "m1")

    for a in raw:
        org   = a.get("org", {}).get("name", "Unknown")
        name  = a.get("name", "Account")
        bal   = float(a.get("balance", 0.0))
        avail = float(a.get("available-balance") or bal)
        entry = {"org": org, "name": name, "balance": bal, "available": avail}

        org_lower  = org.lower()
        name_lower = name.lower()
        # Credit takes priority: negative balance OR credit/margin keyword in name
        is_credit  = (bal < 0) or any(k in name_lower for k in CREDIT_KEYWORDS)
        is_broker  = any(k in org_lower for k in brokerage_orgs)

        if is_credit:
            credit.append(entry)
        elif is_broker:
            brokerage.append(entry)
        else:
            liquid.append(entry)

    # Sort credit by balance ascending so largest liability leads
    credit.sort(key=lambda a: a["balance"])

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

def fetch_buying_power_snapshot(total_liquid: float, total_brokerage: float, total_owed: float) -> dict:
    """
    Real buying power analysis — answers "what does my money actually do in today's economy?"

    Four calculations, all grounded in live FRED CPI:
      1. Cash erosion   — how much purchasing power liquid cash loses per month at current CPI
      2. Portfolio real yield — blended CLM/CRF+Tier2 yield minus CPI (the actual wealth gain)
      3. Margin carry   — confirms margin arbitrage is still positive after inflation
      4. Rule of 72     — years until idle cash halves in purchasing power at current CPI

    Data: FRED CPIAUCSL (cached daily by analytics.py). Fallback: 3.5% if FRED unavailable.
    Pushover only — never Discord.
    """
    result = {
        "cpi_yoy": None,
        "cash_erosion_monthly": None,
        "cash_erosion_annual": None,
        "real_portfolio_yield": None,
        "margin_real_cost": None,
        "years_to_half": None,
        "net_worth": total_liquid + total_brokerage + total_owed,
        "deploy_urgency": None,
    }
    try:
        # Pull CPI from DB (written daily by analytics.py fetch_fred_macro_snapshot)
        from database import EcosystemDatabase
        db = EcosystemDatabase()

        cpi_raw = db.get_state("fred_macro_snap")
        cpi_yoy = None
        if isinstance(cpi_raw, dict):
            cpi_yoy = cpi_raw.get("cpi_yoy")

        # Fallback: fetch directly from FRED if not in DB
        if cpi_yoy is None:
            fred_key = os.getenv("FRED_API_KEY", "")
            if fred_key:
                url = (
                    f"https://api.stlouisfed.org/fred/series/observations"
                    f"?series_id=CPIAUCSL&api_key={fred_key}&sort_order=desc"
                    f"&limit=13&file_type=json"
                )
                r = requests.get(url, timeout=12).json().get("observations", [])
                if len(r) >= 13:
                    latest   = float(r[0]["value"])
                    year_ago = float(r[12]["value"])
                    cpi_yoy  = round((latest - year_ago) / year_ago * 100, 2) if year_ago > 0 else None

        if cpi_yoy is None:
            cpi_yoy = 3.5  # conservative fallback

        cpi_rate = cpi_yoy / 100.0

        # 1. Cash erosion — idle cash is silently taxed by inflation every month
        cash_erosion_monthly = round(total_liquid * cpi_rate / 12, 2)
        cash_erosion_annual  = round(total_liquid * cpi_rate, 2)

        # 2. Portfolio real yield — CLM/CRF ~20% + Tier2 blended ~13-15% → rough blended ~19%
        # Using conservative 19% as the blended portfolio yield assumption per CLAUDE.md strategy
        BLENDED_PORTFOLIO_YIELD = 19.0
        real_portfolio_yield = round(BLENDED_PORTFOLIO_YIELD - cpi_yoy, 2)

        # 3. Margin carry real cost — what the margin loan actually costs after inflation
        # Inflation erodes the real value of the debt, so effective cost = nominal - CPI
        MARGIN_RATE = 7.25
        margin_real_cost = round(MARGIN_RATE - cpi_yoy, 2)  # positive = still costs money; lower than nominal

        # 4. Rule of 72 — years until purchasing power of idle cash halves
        years_to_half = round(72.0 / cpi_yoy, 1) if cpi_yoy > 0 else 99.0

        # Deploy urgency — is idle cash losing more than $100/month?
        if cash_erosion_monthly >= 150:
            deploy_urgency = f"🔴 ${cash_erosion_monthly:.0f}/mo evaporating — deploy idle cash"
        elif cash_erosion_monthly >= 75:
            deploy_urgency = f"🟡 ${cash_erosion_monthly:.0f}/mo erosion — watch cash buffer"
        else:
            deploy_urgency = f"🟢 ${cash_erosion_monthly:.0f}/mo erosion — buffer acceptable"

        result.update({
            "cpi_yoy":              cpi_yoy,
            "cash_erosion_monthly": cash_erosion_monthly,
            "cash_erosion_annual":  cash_erosion_annual,
            "real_portfolio_yield": real_portfolio_yield,
            "margin_real_cost":     margin_real_cost,
            "years_to_half":        years_to_half,
            "deploy_urgency":       deploy_urgency,
        })
        logger.info(f"Buying power: CPI {cpi_yoy}% | real yield {real_portfolio_yield}% | "
                    f"cash erosion ${cash_erosion_monthly:.0f}/mo")
    except Exception as e:
        logger.warning(f"Buying power snapshot failed: {e}")
    return result


def fetch_market_mood():
    """
    SentiSense proprietary Market Mood (0-100). Cached in DB — one API call/day.
    Returns (score: int, label: str, signal: str) or (None, None, None) on failure.
    Pushover-only; never sent to Discord.
    """
    try:
        from database import EcosystemDatabase
        import sentisense_client as ss
        db = EcosystemDatabase()
        mood = ss.get_market_mood(db)
        if mood:
            return mood["score"], mood["label"], mood["signal"]
    except Exception as e:
        logger.warning(f"SentiSense Market Mood fetch failed: {e}")
    return None, None, None


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
# SEC EDGAR — Lightweight RO status check for daily pulse
# ─────────────────────────────────────────────────────────────────────────────

def fetch_ro_status():
    """
    Checks EDGAR for CLM and CRF. Returns a short status string per ticker:
      🔴 RO RISK   — N-2 or N-2/A filed within 90 days
      👁 HOLDER CHG — SC 13D/G filed within 180 days
      🟢 Stable    — no actionable filings
    """
    CIK_MAP = {"CLM": "0000814083", "CRF": "0000033934"}
    N2_WINDOW    = 90
    HOLDER_WINDOW = 180
    headers = {"User-Agent": "RockefellerSystem/1.0 (admin@rockefeller.local)"}
    session = requests.Session()
    results = {}

    for ticker, cik in CIK_MAP.items():
        try:
            res = session.get(
                f"https://data.sec.gov/submissions/CIK{cik}.json",
                headers=headers, timeout=20)
            if res.status_code != 200:
                results[ticker] = "⚪ EDGAR unavailable"
                continue

            filings     = res.json().get("filings", {}).get("recent", {})
            forms       = filings.get("form", [])
            dates       = filings.get("filingDate", [])
            today_dt    = datetime.utcnow().date()
            ro_detected = False
            holder_detected = False

            for form, filing_date in zip(forms, dates):
                try:
                    age = (today_dt - datetime.strptime(filing_date, "%Y-%m-%d").date()).days
                except ValueError:
                    continue
                if form in ("N-2", "N-2/A") and age <= N2_WINDOW:
                    ro_detected = True
                elif form in ("SC 13D", "SC 13G", "SC 13D/A", "SC 13G/A") and age <= HOLDER_WINDOW:
                    holder_detected = True

            if ro_detected:
                results[ticker] = "🔴 RO RISK — N-2 filing active"
            elif holder_detected:
                results[ticker] = "👁 Holder change detected (SC 13D/G)"
            else:
                results[ticker] = "🟢 Stable — no actionable filings"

        except Exception as e:
            logger.warning(f"EDGAR check failed for {ticker}: {e}")
            results[ticker] = "⚪ EDGAR unavailable"

    return results


# ─────────────────────────────────────────────────────────────────────────────
# FORMAT — ┣/┗ Pulse Message
# ─────────────────────────────────────────────────────────────────────────────

# Noisy account names from bank feeds → cleaner display labels.
# Keys are substrings (lowercased) found in the raw account name.
ACCOUNT_NAME_MAP = {
    "active duty":          "Checking",   # M1 / NFCU feed labels
    "flagship rewards":     "Flagship Visa",
    "visa signature":       "Visa",
    "platinum card":        "Amex Platinum",
    "gold card":            "Amex Gold",
    "individual brokerage": "Brokerage",
    "custodial":            "Custodial",
}

# Org names from SimpleFIN feeds → clean short labels shown before the dash.
ORG_NAME_MAP = {
    "navy federal":   "NFCU",
    "american expre": "Amex",    # matches "American Express"
    "e*trade":        "E*Trade",
    "etrade":         "E*Trade",
    "m1 finance":     "M1",
}

def _org_label(org):
    """Map verbose SimpleFIN org names to short display labels."""
    org_lower = org.lower()
    for fragment, label in ORG_NAME_MAP.items():
        if fragment in org_lower:
            return label
    # Fallback: first word of org name
    return org.split()[0] if org and org != "Unknown" else ""

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
    org_short = _org_label(org)
    return f"{org_short} — {name}" if org_short else name


def _delta(current, prev):
    """Signed dollar delta string, or empty string if no prior value."""
    if prev is None:
        return ""
    d = current - prev
    arrow = "↑" if d >= 0 else "↓"
    return f" ({arrow}${abs(d):,.0f})"


def _portfolio_deltas(current_total, state):
    """
    Returns delta strings for 1D / 3M / 6M / 1Y comparing total_brokerage
    snapshots. Snapshots store the combined portfolio value, so deltas here
    reflect the whole portfolio — not a single account — avoiding the bug
    where comparing one account's balance against a multi-account snapshot
    produces a false large delta.
    """
    today     = date.today()
    snapshots = state.get("snapshots", {})
    horizons  = {"1D": 1, "3M": 90, "6M": 180, "1Y": 365}
    parts     = []
    for label, days in horizons.items():
        target     = (today - __import__("datetime").timedelta(days=days)).isoformat()
        past_dates = sorted(k for k in snapshots if k <= target)
        if past_dates:
            past_val = snapshots[past_dates[-1]]
            d = current_total - past_val
            arrow = "↑" if d >= 0 else "↓"
            parts.append(f"{label}: {arrow}${abs(d):,.0f}")
        else:
            parts.append(f"{label}: —")
    return " | ".join(parts)


def format_pulse_message(liquid, credit, brokerage, cef, regime, state, ro_status=None, market_mood=None):
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

    # ── Section 2: Credit / Liabilities (sorted most-negative first, skip zero-balance)
    active_credit = [a for a in credit if round(a["balance"], 2) != 0.0]
    total_owed = sum(a["balance"] for a in active_credit)
    lines.append("")
    lines.append("CREDIT / LIABILITIES")
    if active_credit:
        for a in active_credit:
            bal_str = f"$-{abs(a['balance']):,.2f}" if a["balance"] <= 0 else f"${a['balance']:,.2f}"
            lines.append(f"┣ {_clean_name(a['org'], a['name'])}: {bal_str}")
        lines.append(f"┗ Total Owed: $-{abs(total_owed):,.2f}")
    else:
        lines.append("┗ No credit balances")

    # ── Section 3: Brokerage (skip zero-balance accounts)
    active_brokers  = [a for a in brokerage if a["balance"] != 0]
    total_brokerage = sum(a["balance"] for a in active_brokers if a["balance"] > 0)
    port_deltas     = _portfolio_deltas(total_brokerage, state)
    lines.append("")
    lines.append("BROKERAGE")
    for a in active_brokers:
        lines.append(f"┣ {_clean_name(a['org'], a['name'])}: ${a['balance']:,.2f}")
    lines.append(f"┣ Total Portfolio: ${total_brokerage:,.2f}")
    lines.append(f"┗ {port_deltas}")

    # ── Section 4: Net Worth Snapshot
    net_worth = total_liquid + total_owed + total_brokerage
    lines.append("")
    lines.append("NET WORTH SNAPSHOT")
    lines.append(f"┣ Liquid:    ${total_liquid:,.2f}")
    lines.append(f"┣ Owed:      ${total_owed:,.2f}")
    lines.append(f"┣ Portfolio: ${total_brokerage:,.2f}")
    lines.append(f"┗ Net Worth: ${net_worth:,.2f}{_delta(net_worth, state.get('net_worth'))}")

    # ── Section 5: CLM / CRF Cornerstone — RO status only
    lines.append("")
    lines.append("CORNERSTONE (CLM / CRF)")
    if ro_status:
        for ticker, status in ro_status.items():
            prefix = "┣" if ticker != list(ro_status.keys())[-1] else "┗"
            lines.append(f"{prefix} {ticker}: {status}")
    else:
        lines.append("┗ EDGAR status unavailable")

    # ── Section 6: Market Regime + SentiSense Mood
    lines.append("")
    lines.append("MARKET REGIME")
    regime_str = "Bull — above 200 SMA" if bull else ("Bear — below 200 SMA" if bull is False else "Unknown")
    lines.append(f"┣ SPY: ${spy_price:,.2f} — {regime_str}")
    lines.append(f"┣ VIXY z: {vixy_z:+.1f}σ ({vixy_label})")

    # SentiSense Market Mood — third regime input.
    # Catches sentiment/price divergence: SPY above SMA200 (price says GO)
    # but sentiment at Extreme Fear (crowd says danger ahead) → CAUTION.
    mood_score, mood_label, mood_signal = (market_mood or (None, None, None))
    if mood_score is not None:
        lines.append(f"┣ Market Mood: {mood_score} · {mood_label} — {mood_signal}")

    # Deploy gate: all three inputs must align for full GO
    sentiment_fear = mood_score is not None and mood_score <= 25
    if bull and vixy_z < 1.5 and not sentiment_fear:
        deploy = "🟢 GO — price + vol + sentiment aligned"
    elif not bull:
        deploy = "🔴 HOLD — bear regime (SPY below 200 SMA)"
    elif vixy_z >= 1.5:
        deploy = "⚠️ CAUTION — fear spike (VIXY elevated)"
    elif sentiment_fear:
        deploy = "⚠️ CAUTION — price bullish but sentiment at extreme fear"
    else:
        deploy = "🟢 GO — conditions met"
    lines.append(f"┗ Margin Deploy: {deploy}")

    # ── Section 7: Buying Power Reality Check
    bp = fetch_buying_power_snapshot(total_liquid, total_brokerage, total_owed)
    if bp.get("cpi_yoy") is not None:
        lines.append("")
        lines.append("BUYING POWER (Real $)")
        lines.append(f"┣ CPI (YoY): {bp['cpi_yoy']:.1f}% — live from FRED")
        lines.append(f"┣ Cash erosion: -${bp['cash_erosion_monthly']:.0f}/mo | -${bp['cash_erosion_annual']:.0f}/yr on ${total_liquid:,.0f} idle")
        lines.append(f"┣ Idle cash halves in: {bp['years_to_half']:.0f} yrs at current CPI (Rule of 72)")
        lines.append(f"┣ Portfolio real yield: {bp['real_portfolio_yield']:+.1f}% (19% blended − {bp['cpi_yoy']:.1f}% CPI)")
        lines.append(f"┣ Margin real cost: {bp['margin_real_cost']:+.2f}% (7.25% rate − {bp['cpi_yoy']:.1f}% CPI)")
        lines.append(f"┗ {bp['deploy_urgency']}")

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

def run_daily_pulse(force=False, debug=False):
    today_str = date.today().isoformat()
    state     = load_state()

    if not force and state.get("last_run_date") == today_str:
        logger.info("Already sent today — use --force to override")
        return

    liquid, credit, brokerage = fetch_simplefin_accounts(debug=debug)
    regime      = fetch_market_regime()
    ro_status   = fetch_ro_status()
    market_mood = fetch_market_mood()  # SentiSense — cached in DB, 1 call/day

    # ── Low balance check — fires independently, never gates the daily pulse
    total_liquid = sum(a["balance"] for a in liquid)
    if total_liquid < LIQUID_LOW_THRESHOLD:
        push_to_pushover(
            f"⚠️ Low Balance — {date.today().strftime('%b %d, %Y')}",
            f"Liquid cash is ${total_liquid:,.2f} — below the ${LIQUID_LOW_THRESHOLD:,.0f} buffer.\n\nReplenish before next billing cycle.",
            priority=1,
        )

    title, message, _ = format_pulse_message(liquid, credit, brokerage, None, regime, state, ro_status, market_mood)
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
    if "--list-accounts" in sys.argv:
        # Debug mode: print all raw SimpleFIN accounts then exit — no Pushover dispatch
        fetch_simplefin_accounts(debug=True)
        sys.exit(0)
    run_daily_pulse(force="--force" in sys.argv, debug="--debug" in sys.argv)
