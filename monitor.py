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
    from essentials_tools import send_essentials_embed, get_institutional_conviction
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

PRIORITY_ASSETS = {
    "CLM": {"nav_ticker": "XCLMX", "default_nav": 6.45},
    "CRF": {"nav_ticker": "XCRFX", "default_nav": 6.30}
}

def check_sec_edgar(session, ticker):
    """Scrapes SEC EDGAR in real-time for N-2/Rights Offering Filings."""
    cik_map = {"CLM": "0000081074", "CRF": "0000084560"}
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

def get_ticker_report(session, ticker):
    price, rsi, nav = fetch_live_metrics(session, ticker)
    if price == 0.0: 
        return f"{ticker}\n⚠️ *Data Feed Offline.*\n"

    # Whale Flow Tracking
    whale_status = "NORMAL"
    if HAS_ESSENTIALS:
        try:
            whale_res = get_institutional_conviction(ticker, TD_API_KEY)
            whale_status = whale_res[0] if isinstance(whale_res, tuple) else whale_res
        except Exception:
            pass

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

    # Strategy Logic Flow
    z_tag = "(safe)" if z_premium < 1.0 else ("(caution)" if z_premium < 2.0 else "(DANGER)")
    rsi_tag = "(neutral)" if 40 <= rsi <= 60 else ""
    prem_tag = "(neutral)" if 10 <= premium <= 20 else ""

    if "N-2" in sec_shield:
        status = "🚨 CRITICAL: N-2 DETECTED"
        income_note = "Distribution/Caution phase"
        verdict = "Active SEC N-2/RO filing detected. Immediate NAV dilution imminent."
        recommendation = "Halt DRIP immediately; prepare protective hedge."
    elif z_premium >= 1.5 or premium > 25.0:
        status = "⚠️ HIGH PREMIUM"
        income_note = "Distribution/Caution phase"
        verdict = "Premium highly extended above historical norms. RO risk elevated."
        recommendation = "Pause reinvestment; build cash position."
    else:
        status = "✅ STABLE"
        income_note = "Accumulation phase"
        verdict = "Premium variance within historical standard deviations. No active dilution signatures."
        recommendation = "Reinvest distributions at NAV"

    return (
        f"{ticker}\n"
        f"Status:  {status}\n"
        f"┣ Premium to NAV: {premium:.2f}% {prem_tag}\n"
        f"┣ Premium Z-Score (1Y): {z_premium:+.1f} {z_tag}\n"
        f"┣ SEC: {sec_shield}\n"
        f"┣ RSI (1D): {rsi:.1f} {rsi_tag}\n"
        f"┣ Net Arbitrage Spread: +{s_net:.2f}%\n"
        f"┣ DRIP Alpha Capture: +{alpha_drip:.2f}%\n"
        f"┣ Income Note: {income_note}\n"
        f"┣ Whale Flow: {whale_status}\n"
        f"┣ Recommendation: {recommendation}\n"
        f"┗ Strategy Verdict: {verdict}\n"
    )

def send_daily_pulse(is_test=False):
    reports = []
    # Connection pooling to prevent Twelve Data & SEC API timeouts
    with requests.Session() as session:
        for ticker in PRIORITY_ASSETS:
            reports.append(get_ticker_report(session, ticker))
            
    full_report = "\n\n".join(reports)
    
    # Ecosystem Supplement: CEF Credit Shield Check
    credit_spread = float(db.get_state("credit_spread", 0.0))
    if credit_spread > 4.5:
        full_report += f"\n\n🚨 **SYSTEMIC MACRO OVERRIDE:** High Yield Credit Spreads are elevated ({credit_spread:.2f}%). CEFs face high probability of NAV decay in this regime."

    title = "☕️ Cornerstone Flowstate Update" + (" - 🧪 Test Only" if is_test else "")
    color = 0xe74c3c if "CRITICAL" in full_report or credit_spread > 4.5 else (0xf1c40f if "HIGH PREMIUM" in full_report else 0x2ecc71)
    
    # 1. Discord Dispatch
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, full_report, color)

    clean_report = full_report.replace("**", "").replace("`", "")
    
    # 2. Pushover Dispatch
    pushover_token = os.getenv("PUSHOVER_API_TOKEN")
    pushover_user = os.getenv("PUSHOVER_USER_KEY")
    if pushover_token and pushover_user:
        try:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": pushover_token,
                "user": pushover_user,
                "title": title,
                "message": clean_report,
                "priority": 0
            }, timeout=5)
            logger.info("Pushover notification executed successfully.")
        except Exception as e:
            logger.error(f"Pushover transmission failed: {e}")

    # 3. Email Dispatch
    sender = os.getenv("SENDER_EMAIL")
    pwd = os.getenv("EMAIL_APP_PASSWORD")
    if sender and pwd:
        try:
            msg = EmailMessage()
            msg.set_content(clean_report)
            msg['Subject'] = title
            msg['From'] = sender
            msg['To'] = sender
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(sender, pwd)
                smtp.send_message(msg)
            logger.info("Email notification executed successfully.")
        except Exception as e: 
            logger.error(f"Email transmission failed: {e}")

def run_monitor():
    tz_h = pytz.timezone('Pacific/Honolulu')
    if len(sys.argv) > 1 and sys.argv[cite: 1].lower() in ["test", "force"]:
        send_daily_pulse(is_test=True)
        return

    logger.info("⏳ [Engine Loop] Monitoring active. Database State tracking enabled.")

    while True:
        try:
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
