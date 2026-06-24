import os
import requests
import time
import sys
import smtplib
import logging
from email.message import EmailMessage
from datetime import datetime
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("Monitor_Engine")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

try:
    from essentials_tools import (
        send_essentials_embed, send_essentials_embed_with_chart,
        generate_line_comparison_chart, get_institutional_conviction,
    )
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

PRIORITY_ASSETS = {
    "CLM": {"nav_ticker": "XCLMX", "default_nav": 6.45},
    "CRF": {"nav_ticker": "XCRFX", "default_nav": 6.30}
}

# Cornerstone funds (CLM/CRF) run a managed distribution policy fixed at 21% of NAV, paid monthly,
# with ex-dividend historically falling mid-month (e.g. Feb 17 / Mar 16, 2026). That ex-div drop is
# a SCHEDULED, expected dip from the cash payout leaving the fund — not a dilution/RO signal — and
# must not be misclassified as danger. Heuristic window (no scraped exact calendar exists for this).
EX_DIV_WINDOW_DAYS = range(15, 20)

# Rights Offering risk-score weighting. N-2/SEC filing is the single confirmed, highest-conviction
# signal; everything else is a leading indicator pieced together from historical RO precedent
# (elevated premium Z-score, whale distribution into strength, and macro credit stress all tend to
# precede an RO announcement by days to weeks).
RO_SCORE_WEIGHTS = {
    "sec_n2": 60, "z_danger": 25, "z_caution": 12, "premium_extreme": 10,
    "whale_distribution": 15, "credit_stress": 10, "ex_div_relief": -10,
    "ro_season": 8, "crisis_amplification": 12,
}

# Verified against the real SEC filing history (CIKs above) across 2016-2025: in the years CLM/CRF
# actually filed a Rights Offering (2021, 2022, 2025), the initial N-2 was filed in mid-to-late
# February, with the N-2/A amendment ~6 weeks later in early April — and the post-amendment price
# drop in each of those years (Apr 2021, Apr 2022) lines up within days of the N-2/A. 2023 and 2024
# had zero N-2 activity and zero RO-attributable drops. This doesn't mean an RO is guaranteed every
# February, but the historical base rate of one happening in this window is high enough to raise
# the baseline risk score during it rather than wait for the filing to already be public.
RO_FILING_SEASON = (2, 15, 4, 15)  # (start_month, start_day, end_month, end_day)

# The worst CLM/CRF drawdowns analyzed back to 2020 (COVID, the Aug 5 2024 yen-carry-unwind crash)
# almost all cluster on broad-market VIX-spike days, not idiosyncratic CLM/CRF news — these funds'
# realized beta vs SPY is actually sub-1.0 in calm markets (~0.83-0.86, computed from 252D returns),
# but they crash far harder than SPY's worst days during genuine crisis events (leverage convexity:
# a fixed-dollar leverage amount becomes a larger % of NAV as equity shrinks in a selloff). A VIXY
# volatility-spike flag catches this tail risk that a simple beta multiplier would miss.
CRISIS_VIXY_Z_THRESHOLD = 1.5

def check_sec_edgar(session, ticker):
    """
    Scrapes SEC EDGAR in real-time for N-2/Rights Offering Filings.
    CIKs verified live against SEC's company search (2026-06-23) — the previous hardcoded values
    (0000081074 / 0000084560) 404 on EDGAR; they don't correspond to any real company. This
    detector has never been able to find a real N-2 filing for either fund until this fix.
    Real CIKs: CLM = Cornerstone Strategic Investment Fund (formerly Cornerstone Strategic Value
    Fund / Clemente Global Growth Fund); CRF = Cornerstone Total Return Fund.
    """
    cik_map = {"CLM": "0000814083", "CRF": "0000033934"}
    cik = cik_map.get(ticker)
    if not cik: return "No N2/ RO detected"
    
    # SEC requires strict User-Agent formatting to prevent 403 Forbidden blocks
    headers = {'User-Agent': 'RockefellerSystem/1.0 (admin@rockefeller.local)'}
    try:
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        res = session.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            data = res.json()
            recent_forms = data.get("filings", {}).get("recent", {}).get("form", [])
            for i in range(min(10, len(recent_forms))):
                if "N-2" in recent_forms[i]:
                    return "⚠️ N-2 FILING DETECTED"
        return "No N2/ RO detected"
    except Exception as e:
        logger.error(f"[SEC Fetch Error] {e}")
        return "No N2/ RO detected"

def fetch_live_metrics(session, symbol):
    try:
        p_res = session.get(f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
        price = float(p_res.get('price', 0.0))

        rsi = 50.0
        if price > 0:
            r_res = session.get(f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}", timeout=10).json()
            rsi = float(r_res.get('values', [{'rsi': 50.0}])[0]['rsi'])

        nav_ticker = PRIORITY_ASSETS[symbol]["nav_ticker"]
        nav_res = session.get(f"https://api.twelvedata.com/price?symbol={nav_ticker}&apikey={TD_API_KEY}", timeout=10).json()
        nav = float(nav_res.get('price', PRIORITY_ASSETS[symbol]["default_nav"]))
        
        return price, rsi, nav
    except Exception as e:
        logger.error(f"[Data Fetch Error] {e}")
        return 0.0, 50.0, PRIORITY_ASSETS[symbol]["default_nav"]

def is_near_ex_dividend_window(today=None):
    """Heuristic mid-month ex-div proximity check (see EX_DIV_WINDOW_DAYS note above)."""
    today = today or datetime.now(pytz.timezone('Pacific/Honolulu'))
    return today.day in EX_DIV_WINDOW_DAYS

def is_ro_filing_season(today=None):
    """Mid-Feb through mid-April — see RO_FILING_SEASON note above for the historical evidence."""
    today = today or datetime.now(pytz.timezone('Pacific/Honolulu'))
    start_m, start_d, end_m, end_d = RO_FILING_SEASON
    start = today.replace(month=start_m, day=start_d)
    end = today.replace(month=end_m, day=end_d)
    return start <= today <= end

def check_crisis_amplification_risk(session):
    """
    Real computed VIXY (VIX futures ETF) z-score vs its own 20D mean — same self-normalizing
    approach used elsewhere in the ecosystem (the real VIX index isn't available at this Twelve
    Data plan tier). Returns (is_crisis_day, vixy_price, vixy_z).
    """
    try:
        res = session.get(
            "https://api.twelvedata.com/time_series",
            params={"symbol": "VIXY", "interval": "1day", "outputsize": "20", "apikey": TD_API_KEY},
            timeout=10,
        ).json()
        values = res.get("values", [])
        if len(values) < 10:
            return False, 0.0, 0.0
        closes = [float(v["close"]) for v in values]
        current, mean = closes[0], sum(closes) / len(closes)
        std = (sum((c - mean) ** 2 for c in closes) / len(closes)) ** 0.5
        z = (current - mean) / std if std > 0 else 0.0
        return z >= CRISIS_VIXY_Z_THRESHOLD, current, z
    except Exception as e:
        logger.error(f"[Crisis Amplification Check Error] {e}")
        return False, 0.0, 0.0

def detect_whale_flow_direction(session, symbol):
    """
    Distinguishes whale ACCUMULATION from whale DISTRIBUTION (sell-off) — the generic
    get_institutional_conviction() helper only flags a volume spike, it doesn't say which way
    capital is moving, and "which way" is exactly what matters for RO front-running.
    """
    try:
        url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=21&apikey={TD_API_KEY}"
        res = session.get(url, timeout=10).json()
        values = res.get("values", [])
        if len(values) < 11:
            return "NORMAL", 1.0
        today_vol = float(values[0]["volume"])
        baseline_vol = sum(float(v["volume"]) for v in values[1:21]) / len(values[1:21])
        if baseline_vol == 0:
            return "NORMAL", 1.0
        rvol = today_vol / baseline_vol
        price_chg_pct = (float(values[0]["close"]) - float(values[1]["close"])) / float(values[1]["close"]) * 100
        if rvol >= 1.8 and price_chg_pct <= -0.5:
            return "🔴 DISTRIBUTION (Whale Sell-Off)", rvol
        if rvol >= 1.8 and price_chg_pct >= 0.5:
            return "🟢 ACCUMULATION (Whale Buy-In)", rvol
        return "NORMAL", rvol
    except Exception as e:
        logger.error(f"[Whale Flow Error] {symbol}: {e}")
        return "NORMAL", 1.0

def calculate_ro_risk_score(sec_shield, z_premium, premium, whale_tag, credit_spread, ex_div_near,
                             ro_season=False, crisis_day=False):
    """Composite Rights-Offering risk score (0-100) from every leading indicator we track."""
    score = 0
    if "N-2" in sec_shield:
        score += RO_SCORE_WEIGHTS["sec_n2"]
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
    if ex_div_near and score > 0:
        score += RO_SCORE_WEIGHTS["ex_div_relief"]  # negative — scheduled dip context, not dilution risk
    score = max(0, min(100, score))
    if score >= 50:
        tier = "CRITICAL"
    elif score >= 25:
        tier = "ELEVATED"
    else:
        tier = "LOW"
    return score, tier

def get_ticker_report(session, ticker):
    price, rsi, nav = fetch_live_metrics(session, ticker)
    if price == 0.0:
        return f"{ticker}\n⚠️ *Data Feed Offline.*\n", "LOW", 0

    # Whale Flow Tracking — direction-aware (accumulation vs. distribution/sell-off), not just a
    # generic volume-spike flag, since "which way is the whale moving" is the actionable signal.
    whale_status, whale_rvol = detect_whale_flow_direction(session, ticker)

    # Margin Arbitrage, DRIP Alpha & Z-Score Mathematics
    annual_div = 1.4580 if ticker == "CLM" else 1.4112 # 2026 Distribution Profiles
    y_dist = (annual_div / price) * 100 if price > 0 else 0
    y_nav = (annual_div / nav) * 100 if nav > 0 else 0
    
    margin_rate = 7.25 # Standard benchmark margin cost
    leverage_ratio = 1.0 # Baseline leverage parity
    s_net = y_dist - (margin_rate * leverage_ratio)
    
    premium = ((price - nav) / nav) * 100 if nav > 0 else 0
    alpha_drip = (premium / 100) * y_nav if nav > 0 else 0
    
    # Fetch 1Y rolling premium means from DB (fallbacks provided)
    mu_rho = float(db.get_state(f"{ticker}_premium_mu", 15.0))
    sigma_rho = float(db.get_state(f"{ticker}_premium_sigma", 4.0))
    z_premium = (premium - mu_rho) / sigma_rho if sigma_rho > 0 else 0

    # SEC Scraping Engine
    sec_shield = check_sec_edgar(session, ticker)

    # Macro overlay + scheduled ex-div context feed directly into the RO risk score below.
    credit_spread = float(db.get_state("credit_spread", 0.0))
    ex_div_near = is_near_ex_dividend_window()
    ro_season = is_ro_filing_season()
    crisis_day, vixy_price, vixy_z = check_crisis_amplification_risk(session)
    ro_score, ro_tier = calculate_ro_risk_score(
        sec_shield, z_premium, premium, whale_status, credit_spread, ex_div_near,
        ro_season=ro_season, crisis_day=crisis_day,
    )

    # Ledger: only log a prediction when a real risk claim is actually made (ELEVATED/CRITICAL or
    # an N-2 hit) — "no signal" days aren't logged, so the win rate can't be inflated by counting
    # quiet days as free wins. Graded 5 trading days later by sweep_and_grade_pending() in
    # send_daily_pulse, once the outcome window has actually played out.
    if ro_tier in ("ELEVATED", "CRITICAL") or "N-2" in sec_shield:
        try:
            from analytics import HighFidelityAnalyticsEngine
            prediction_id = f"{ticker}_{datetime.now().strftime('%Y%m%d')}"
            HighFidelityAnalyticsEngine().log_ledger_prediction(
                "cornerstone", prediction_id, "DOWN", price, ticker=ticker,
                context=f"RO score {ro_score} ({ro_tier})"
            )
        except Exception as e:
            logger.error(f"Cornerstone ledger logging failed: {e}")

    # Strategy Logic Flow
    z_tag = "(safe)" if z_premium < 1.0 else ("(caution)" if z_premium < 2.0 else "(DANGER)")
    rsi_tag = "(neutral)" if 40 <= rsi <= 60 else ""
    prem_tag = "(neutral)" if 10 <= premium <= 20 else ""
    ex_div_line = "┣ Ex-Div Window: Active (scheduled distribution dip expected, not RO-related)\n" if ex_div_near else ""
    ro_season_line = "┣ RO Filing Season: Active (mid-Feb to mid-Apr — historically when N-2 filings appear)\n" if ro_season else ""
    crisis_line = f"┣ Market Stress: 🔴 CRISIS DAY (VIXY z {vixy_z:+.2f}σ) — leveraged tail-risk amplification active\n" if crisis_day else ""

    if "N-2" in sec_shield:
        status = "🚨 CRITICAL: N-2 DETECTED"
        income_note = "Distribution/Caution phase"
        verdict = "Active SEC N-2/RO filing detected. Immediate NAV dilution imminent."
        recommendation = "Halt DRIP immediately; prepare protective hedge."
    elif ro_tier == "CRITICAL":
        status = "🚨 CRITICAL: RO RISK ELEVATED"
        income_note = "Distribution/Caution phase"
        verdict = "Composite Rights-Offering risk score breached the critical threshold."
        recommendation = "Halt DRIP; consider selling before a potential RO announcement."
    elif ro_tier == "ELEVATED" or z_premium >= 1.5 or premium > 25.0:
        status = "⚠️ HIGH PREMIUM"
        income_note = "Distribution/Caution phase"
        verdict = "Premium highly extended above historical norms. RO risk elevated."
        recommendation = "Pause reinvestment; build cash position."
    else:
        status = "✅ STABLE"
        income_note = "Accumulation phase"
        verdict = "Premium variance within historical standard deviations. No active dilution signatures."
        recommendation = "Reinvest distributions at NAV"

    report_text = (
        f"{ticker}\n"
        f"Status:  {status}\n"
        f"┣ Premium to NAV: {premium:.2f}% {prem_tag}\n"
        f"┣ Premium Z-Score (1Y): {z_premium:+.1f} {z_tag}\n"
        f"┣ SEC: {sec_shield}\n"
        f"┣ RO Risk Score: {ro_score}/100 ({ro_tier})\n"
        f"{ex_div_line}"
        f"{ro_season_line}"
        f"{crisis_line}"
        f"┣ RSI (1D): {rsi:.1f} {rsi_tag}\n"
        f"┣ Net Arbitrage Spread: +{s_net:.2f}%\n"
        f"┣ DRIP Alpha Capture: +{alpha_drip:.2f}%\n"
        f"┣ Income Note: {income_note}\n"
        f"┣ Whale Flow: {whale_status}\n"
        f"┣ Recommendation: {recommendation}\n"
        f"┗ Strategy Verdict: {verdict}\n"
    )
    return report_text, ro_tier, ro_score

TIER_RANK = {"LOW": 0, "ELEVATED": 1, "CRITICAL": 2}

def build_cornerstone_chart():
    """Price vs. NAV for both funds, rebased to 100 over 60 days — the premium gap IS the strategy
    signal, so visualizing price diverging from NAV is more useful here than a plain candlestick."""
    try:
        from analytics import HighFidelityAnalyticsEngine
        engine = HighFidelityAnalyticsEngine()
        series = {}
        for ticker, cfg in PRIORITY_ASSETS.items():
            price_df = engine.fetch_crypto_ohlc(ticker, outputsize=60)
            nav_df = engine.fetch_crypto_ohlc(cfg["nav_ticker"], outputsize=60)
            if price_df is not None and not price_df.empty:
                series[f"{ticker} Price"] = price_df["close"]
            if nav_df is not None and not nav_df.empty:
                series[f"{ticker} NAV"] = nav_df["close"]
        if not series:
            return None
        return generate_line_comparison_chart(series, "Cornerstone CLM/CRF | Price vs. NAV (Rebased to 100, 60D)")
    except Exception as e:
        logger.error(f"Cornerstone chart generation failed: {e}")
        return None

def compute_cornerstone_reports():
    """Single source of truth for both the scheduled daily pulse and the instant escalation path —
    computed once per call so the continuous monitor loop and the 0800 HST report never drift."""
    reports, worst_tier = [], "LOW"
    with requests.Session() as session:
        for ticker in PRIORITY_ASSETS:
            text, tier, score = get_ticker_report(session, ticker)
            reports.append(text)
            if TIER_RANK.get(tier, 0) > TIER_RANK.get(worst_tier, 0):
                worst_tier = tier
    full_report = "\n\n".join(reports)

    credit_spread = float(db.get_state("credit_spread", 0.0))
    if credit_spread > 4.5:
        full_report += f"\n\n🚨 **SYSTEMIC MACRO OVERRIDE:** High Yield Credit Spreads are elevated ({credit_spread:.2f}%). CEFs face high probability of NAV decay in this regime."
        if TIER_RANK["ELEVATED"] > TIER_RANK.get(worst_tier, 0):
            worst_tier = "ELEVATED"

    return full_report, worst_tier

def dispatch_cornerstone_alert(title, full_report, color, attach_chart=True):
    """Fires the same report across all four channels: Discord, Pushover, personal email, work email."""
    chart_bytes = build_cornerstone_chart() if attach_chart else None

    # 1. Discord Dispatch
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        if chart_bytes:
            send_essentials_embed_with_chart(WEBHOOK_CORNERSTONE, title, full_report, chart_bytes, color)
        else:
            send_essentials_embed(WEBHOOK_CORNERSTONE, title, full_report, color)

    clean_report = full_report.replace("**", "").replace("`", "")

    # 2. Pushover Dispatch (with chart attachment when available — 2.5MB Pushover limit)
    pushover_token = os.getenv("PUSHOVER_API_TOKEN")
    pushover_user = os.getenv("PUSHOVER_USER_KEY")
    if pushover_token and pushover_user:
        try:
            data = {"token": pushover_token, "user": pushover_user, "title": title, "message": clean_report, "priority": 0}
            files = {"attachment": ("cornerstone_chart.png", chart_bytes, "image/png")} if chart_bytes else None
            requests.post("https://api.pushover.net/1/messages.json", data=data, files=files, timeout=10)
            logger.info("Pushover notification executed successfully.")
        except Exception as e:
            logger.error(f"Pushover transmission failed: {e}")

    # 3. Email Dispatch — personal AND work, per explicit requirement
    sender = os.getenv("SENDER_EMAIL")
    pwd = os.getenv("EMAIL_APP_PASSWORD")
    work_email = os.getenv("WORK_EMAIL")
    if sender and pwd:
        try:
            msg = EmailMessage()
            msg.set_content(clean_report)
            msg['Subject'] = title
            msg['From'] = sender
            msg['To'] = f"{sender}, {work_email}" if work_email else sender
            if chart_bytes:
                msg.add_attachment(chart_bytes, maintype="image", subtype="png", filename="cornerstone_chart.png")
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(sender, pwd)
                smtp.send_message(msg)
            logger.info("Email notification executed successfully (personal + work).")
        except Exception as e:
            logger.error(f"Email transmission failed: {e}")

def send_daily_pulse(is_test=False):
    if not is_test:
        tz_h_guard = pytz.timezone('Pacific/Honolulu')
        current_date = datetime.now(tz_h_guard).strftime("%Y-%m-%d")
        last_pulse = db.get_state("last_monitor_pulse_date", "")
        if last_pulse == current_date:
            logger.info("Daily pulse already dispatched today — skipping duplicate call.")
            return
        db.update_state("last_monitor_pulse_date", current_date)

    try:
        from analytics import HighFidelityAnalyticsEngine
        graded = HighFidelityAnalyticsEngine().sweep_and_grade_pending("cornerstone", min_age_days=5)
        if graded:
            logger.info(f"Cornerstone ledger: graded {graded} pending RO-risk call(s).")
    except Exception as e:
        logger.error(f"Cornerstone ledger sweep failed: {e}")

    full_report, worst_tier = compute_cornerstone_reports()
    title = "☕️ Cornerstone Flowstate Update" + (" - 🧪 Test Only" if is_test else "")
    color = 0xe74c3c if worst_tier == "CRITICAL" else (0xf1c40f if worst_tier == "ELEVATED" else 0x2ecc71)
    dispatch_cornerstone_alert(title, full_report, color)
    # Keep the escalation tracker in sync so a quiet 0800 report doesn't leave a stale CRITICAL
    # flag that would block a real future escalation from re-firing.
    db.update_state("cornerstone_alert_tier_rank", TIER_RANK.get(worst_tier, 0))

def check_and_escalate_if_critical():
    """
    Runs every loop tick (every 5 min), independent of the once-daily 0800 HST gate. The moment
    a N-2 filing, premium Z-score breach, or whale sell-off pushes either fund into ELEVATED/CRITICAL
    territory, this fires an immediate red-siren alert across all four channels — capital protection
    can't wait for the next scheduled report. Debounced on tier *transitions* so a sustained critical
    state doesn't re-spam every 5 minutes; any further worsening (ELEVATED -> CRITICAL) re-fires.
    """
    full_report, worst_tier = compute_cornerstone_reports()
    current_rank = TIER_RANK.get(worst_tier, 0)
    prev_rank = int(db.get_state("cornerstone_alert_tier_rank", 0))

    if current_rank > prev_rank and current_rank > 0:
        logger.warning(f"🚨 Cornerstone risk escalation: {worst_tier} (was rank {prev_rank}) — firing immediate alert.")
        title = "🚨🚨 CORNERSTONE RO ALERT — IMMEDIATE ACTION REQUIRED 🚨🚨"
        dispatch_cornerstone_alert(title, full_report, 0xe74c3c)

    db.update_state("cornerstone_alert_tier_rank", current_rank)
    return full_report, worst_tier

def run_monitor():
    tz_h = pytz.timezone('Pacific/Honolulu')
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        send_daily_pulse(is_test=True)
        return

    logger.info("⏳ [Engine Loop] Monitoring active. Database State tracking enabled.")

    while True:
        try:
            # Continuous capital-protection scan — runs every tick regardless of the daily gate.
            check_and_escalate_if_critical()

            now = datetime.now(tz_h)
            current_date = now.strftime("%Y-%m-%d")
            last_pulse = db.get_state("last_monitor_pulse_date", "")

            if now.hour >= 8 and last_pulse != current_date:
                logger.info("Triggering standard 0800 HST Pulse...")
                send_daily_pulse()
                db.update_state("last_monitor_pulse_date", current_date)

        except Exception as e:
            logger.critical(f"FATAL LOOP EXCEPTION CAUGHT: {e}")

        time.sleep(300)

if __name__ == "__main__":
    run_monitor()
