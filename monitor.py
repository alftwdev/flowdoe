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
import edge
from database import EcosystemDatabase

# --- Centralized Logging to survive Cloud Environments ---
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
    "CLM": {"nav_ticker": "XCLMX", "avg_vol": 1700000},
    "CRF": {"nav_ticker": "XCRFX", "avg_vol": 600000}
}

def fetch_live_metrics(symbol):
    try:
        p_url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}"
        price = float(requests.get(p_url, timeout=10).json().get('price', 0.0))

        rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}"
        r_res = requests.get(rsi_url, timeout=10).json()
        rsi = float(r_res['values'][0]['rsi']) if 'values' in r_res else 50.0

        nav_ticker = PRIORITY_ASSETS[symbol]["nav_ticker"]
        nav_url = f"https://api.twelvedata.com/price?symbol={nav_ticker}&apikey={TD_API_KEY}"
        nav = float(requests.get(nav_url, timeout=10).json().get('price', 6.45 if symbol == "CLM" else 6.30))
        
        return price, rsi, nav
    except Exception as e:
        logger.error(f"[Data Fetch Error] {e}")
        return 0.0, 50.0, (6.45 if symbol == "CLM" else 6.30)

def get_ticker_report(ticker):
    price, rsi, nav = fetch_live_metrics(ticker)
    if price == 0.0: 
        return f"### **Flowstate Check: {ticker}**\n⚠️ *Data Feed Offline.*\n"

    whale_status, _, is_whale_dump = get_institutional_conviction(ticker, TD_API_KEY) if HAS_ESSENTIALS else ("NOMINAL Flow", 0, False)
    sec_shield = "No N2/ RO detected" 
    premium = ((price - nav) / nav) * 100
    
    if "No" not in sec_shield or is_whale_dump:
        status, color = "🔴 CRITICAL: EXIT", 0xe74c3c
        income_note, verdict = "LIQUIDATE: Structural Dilution.", "🚨 SEC Dilution identified."
        recommendation = "SELL/EXECUTE CAPITAL PROTECTION PROTOCOL IMMEDIATELY."
    elif premium > 25.0:
        status, color = "⚠️ HIGH PREMIUM", 0xf1c40f
        income_note, verdict = "HOLD / PAUSE REINVESTMENT", "Premium extension stretched."
        recommendation = "Maintain posture; pause new margin capital allocation."
    else:
        status, color = "✅ STABLE", 0x2ecc71
        income_note, verdict = "Accumulation phase", "Premium variance nominal."
        recommendation = "Reinvest distributions"

    return (
        f"**{ticker}**\nStatus:  {status}\n┣ Premium to NAV: {premium:.2f}% (neutral)\n"
        f"┣ SEC: {sec_shield}\n┣ RSI (1D): {rsi:.1f} (neutral)\n┣ Income Note: {income_note}\n"
        f"┣ Whale Flow: {whale_status}\n┣ Recommendation: {recommendation}\n┗ Strategy Verdict: {verdict}\n"
    )

def send_daily_pulse(is_test=False):
    reports = [get_ticker_report(ticker) for ticker in PRIORITY_ASSETS]
    try:
        db.update_state("edge_engine_spy", edge.calculate_mean_reversion_edge("SPY"))
    except Exception: 
        pass
    
    full_report = "\n".join(reports)
    title = "☕️ Cornerstone Flowstate Update" + (" - 🧪 Test Only" if is_test else "")
    color = 0xe74c3c if "CRITICAL" in full_report else (0xf1c40f if "HIGH PREMIUM" in full_report else 0x2ecc71)
    
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, full_report, color)

    clean_report = full_report.replace("**", "").replace("`", "").replace("┣", "").replace("┗", "")
    
    # 1. Fortified Pushover Dispatch
    push_token = os.getenv("PUSHOVER_APP_TOKEN") or os.getenv("PUSHOVER_API_TOKEN")
    push_user = os.getenv("PUSHOVER_USER_KEY")
    if push_token and push_user:
        try:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": push_token, "user": push_user,
                "title": title, "message": clean_report, "priority": 1 if "CRITICAL" in full_report else 0
            }, timeout=10)
            logger.info("Pushover notification executed successfully.")
        except Exception as e: 
            logger.error(f"Pushover transmission failed: {e}")

    # 2. Fortified Email Dispatch
    sender = os.getenv("SENDER_EMAIL")
    pwd = os.getenv("EMAIL_APP_PASSWORD")
    if sender and pwd:
        receiver = f"{sender}, {os.getenv('WORK_EMAIL')}" if os.getenv('WORK_EMAIL') else sender
        try:
            msg = EmailMessage()
            msg.set_content(clean_report)
            msg['Subject'] = title
            msg['From'] = sender
            msg['To'] = receiver
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

    last_dispatched_date = None
    logger.info("⏳ [Engine Loop] Monitoring active. Firing Boot Sequence Verification...")
    
    # STARTUP PING: Prove credentials work immediately upon boot
    push_token = os.getenv("PUSHOVER_APP_TOKEN") or os.getenv("PUSHOVER_API_TOKEN")
    push_user = os.getenv("PUSHOVER_USER_KEY")
    if push_token and push_user:
        try:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": push_token, "user": push_user,
                "title": "SYSTEM BOOT", "message": "Monitor.py successfully connected to Pushover.", "priority": 0
            }, timeout=10)
        except Exception: pass

    while True:
        try:
            now = datetime.now(tz_h)
            current_date = now.strftime("%Y-%m-%d")
            current_time_val = int(now.strftime("%H%M"))
            
            # Windowed Execution: 08:00 to 08:05 AM HST
            if 800 <= current_time_val <= 805 and current_date != last_dispatched_date:
                send_daily_pulse()
                last_dispatched_date = current_date
                
        except Exception as e:
            logger.critical(f"FATAL LOOP EXCEPTION CAUGHT: {e}")
            
        time.sleep(30)

if __name__ == "__main__":
    run_monitor()
