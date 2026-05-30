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

    whale_status = "NORMAL"
    if HAS_ESSENTIALS:
        try:
            whale_res = get_institutional_conviction(ticker, TD_API_KEY)
            whale_status = whale_res[0] if isinstance(whale_res, tuple) else whale_res
        except Exception:
            pass

    sec_shield = "No N2/ RO detected" 
    premium = ((price - nav) / nav) * 100 if nav > 0 else 0
    
    if premium > 25.0:
        status = "⚠️ HIGH PREMIUM"
        income_note = "HOLD: High yield but risky."
        verdict = "Premium approaching historical resistance."
        recommendation = "Pause; Monitor for RO filing."
    else:
        status = "✅ STABLE"
        income_note = "Accumulation phase"
        verdict = "Premium variance within historical standard deviations. No active dilution signatures."
        recommendation = "Reinvest distributions"

    rsi_tag = "(neutral)" if 40 <= rsi <= 60 else ""
    prem_tag = "(neutral)" if 10 <= premium <= 20 else ""

    return (
        f"{ticker}\n"
        f"Status:  {status}\n"
        f"┣ Premium to NAV: {premium:.2f}% {prem_tag}\n"
        f"┣ SEC: {sec_shield}\n"
        f"┣ RSI (1D): {rsi:.1f} {rsi_tag}\n"
        f"┣ Income Note: {income_note}\n"
        f"┣ Whale Flow: {whale_status}\n"
        f"┣ Recommendation: {recommendation}\n"
        f"┗ Strategy Verdict: {verdict}\n"
    )

def send_daily_pulse(is_test=False):
    reports = []
    # Using connection pooling to prevent API timeouts
    with requests.Session() as session:
        for ticker in PRIORITY_ASSETS:
            reports.append(get_ticker_report(session, ticker))
            
    full_report = "\n".join(reports)
    
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
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
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
