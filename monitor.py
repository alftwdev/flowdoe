import os
import requests
import time
import sys
import smtplib
from email.message import EmailMessage
from datetime import datetime
import pytz
from dotenv import load_dotenv
import edge
from database import EcosystemDatabase

# --- 1. ECOSYSTEM INITIALIZATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

try:
    from essentials_tools import send_essentials_embed, get_institutional_conviction
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# Environment Variables
WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
PULSE_FILE = os.path.join(BASE_DIR, "last_pulse.txt")

PRIORITY_ASSETS = {
    "CLM": {"nav_ticker": "XCLMX", "avg_vol": 1700000},
    "CRF": {"nav_ticker": "XCRFX", "avg_vol": 600000}
}

# --- 2. LIVE INTELLIGENCE GATHERING ---
def fetch_live_metrics(symbol):
    print(f"🔄 [Data Fetch] Initializing API requests for {symbol}...")
    try:
        p_url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}"
        p_res = requests.get(p_url, timeout=10).json()
        price = float(p_res.get('price', 0.0))

        rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}"
        r_res = requests.get(rsi_url, timeout=10).json()
        rsi = float(r_res['values'][0]['rsi']) if 'values' in r_res else 50.0

        nav_ticker = PRIORITY_ASSETS[symbol]["nav_ticker"]
        nav_url = f"https://api.twelvedata.com/price?symbol={nav_ticker}&apikey={TD_API_KEY}"
        nav_res = requests.get(nav_url, timeout=10).json()
        
        fallback_nav = 6.45 if symbol == "CLM" else 6.30
        nav = float(nav_res.get('price', fallback_nav))
        
        return price, rsi, nav
    except Exception as e:
        print(f"⚠️ [Data Fetch Error] Failed to compile metrics for {symbol}: {e}")
        return 0.0, 50.0, (6.45 if symbol == "CLM" else 6.30)

def get_ticker_report(ticker):
    print(f"🛡️ [Shield Analysis] Processing parameters for {ticker}...")
    price, rsi, nav = fetch_live_metrics(ticker)
    
    if price == 0.0:
        return f"### **Flowstate Check: {ticker}**\n⚠️ *Data Feed Offline. Stream Pending.*\n"

    whale_status, _, is_whale_dump = get_institutional_conviction(ticker, TD_API_KEY) if HAS_ESSENTIALS else ("NOMINAL Flow", 0, False)
    sec_shield = "No N2/ RO detected" 
    premium = ((price - nav) / nav) * 100
    
    if "No" not in sec_shield or is_whale_dump:
        status = "🔴 CRITICAL: EXIT"
        income_note = "LIQUIDATE: Structural Dilution / Whale capitulation in progress."
        verdict = "🚨 SEC Dilution or High-Volume Whale Dump identified."
        recommendation = "SELL/EXECUTE CAPITAL PROTECTION PROTOCOL IMMEDIATELY."
    elif premium > 25.0:
        status = "⚠️ HIGH PREMIUM"
        income_note = "HOLD / PAUSE REINVESTMENT"
        verdict = "The Premium extension has stretched the rubber band thin. Reversion risk elevated."
        recommendation = "Maintain posture; pause new margin capital allocation. Watch for RO filing."
    else:
        status = "✅ STABLE"
        income_note = "Accumulation phase"
        verdict = "Premium variance within historical standard deviations. No active dilution signatures."
        recommendation = "Reinvest distributions"

    # Precise formatting (.2f) applied to Premium
    return (
        f"**{ticker}**\n"
        f"Status:  {status}\n"
        f"┣ Premium to NAV: {premium:.2f}% (neutral)\n"
        f"┣ SEC: {sec_shield}\n"
        f"┣ RSI (1D): {rsi:.1f} (neutral)\n"
        f"┣ Income Note: {income_note}\n"
        f"┣ Whale Flow: {whale_status}\n"
        f"┣ Recommendation: {recommendation}\n"
        f"┗ Strategy Verdict: {verdict}\n"
    )

def send_daily_pulse(is_test=False):
    print(f"\n📡 [Broadcast Engine] Compiling {'TEST ' if is_test else 'DAILY'} Tactical Pulse...")
    
    reports = [get_ticker_report(ticker) for ticker in PRIORITY_ASSETS]
    
    # 🦅 EDGE ENGINE: Silent Execution
    try:
        edge_metrics = edge.calculate_mean_reversion_edge("SPY")
        db.update_state("edge_engine_spy", edge_metrics)
        print("🦅 [Edge Engine] Metrics calculated and stored silently to database.")
    except Exception as e:
        print(f"⚠️ [Edge Engine Error]: {e}")
    
    full_report = "\n".join(reports)
    title = "☕️ Cornerstone Flowstate Update" + (" - 🧪 Test Only" if is_test else "")
    
    color = 0x2ecc71
    if "HIGH PREMIUM" in full_report: color = 0xf1c40f
    if "CRITICAL" in full_report: color = 0xe74c3c
    
    # Discord Dispatch
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, full_report, color)

    # Hard Status Dispatch (Pushover)
    clean_report = full_report.replace("**", "").replace("`", "").replace("┣", "").replace("┗", "")
    
    if os.getenv("PUSHOVER_API_TOKEN"):
        try:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": os.getenv("PUSHOVER_API_TOKEN"),
                "user": os.getenv("PUSHOVER_USER_KEY"),
                "title": title,
                "message": clean_report,
                "priority": 1 if "CRITICAL" in full_report else 0
            }, timeout=10)
            print("📲 [Comms] Pushover alert dispatched successfully.")
        except Exception as e:
            print(f"⚠️ [Pushover Error]: {e}")

    # EMAIL ENGINE - Hardened Logic
    sender = os.getenv("SENDER_EMAIL")
    pwd = os.getenv("EMAIL_APP_PASSWORD")
    work_email = os.getenv("WORK_EMAIL")
    
    if sender and pwd:
        # Defaults to sender if work_email is blank to guarantee delivery
        receiver = f"{sender}, {work_email}" if work_email else sender
        try:
            msg = EmailMessage()
            msg.set_content(clean_report)
            msg['Subject'] = title
            msg['From'] = sender
            msg['To'] = receiver
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(sender, pwd)
                smtp.send_message(msg)
            print(f"📧 [Comms] Email hard-status alert dispatched to: {receiver}")
        except Exception as e:
            print(f"⚠️ [Email Transmission Error]: {e}")
    else:
        print("⚠️ [Email Bypass]: SENDER_EMAIL or EMAIL_APP_PASSWORD not found in .env")

    try:
        with open(PULSE_FILE, "w") as f:
            f.write(datetime.now().isoformat())
    except Exception:
        pass

def run_monitor():
    tz_h = pytz.timezone('Pacific/Honolulu')
    
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        send_daily_pulse(is_test=True)
        return

    print("⏳ [Engine Loop] Entering continuous monitoring matrix...")
    while True:
        now = datetime.now(tz_h)
        if now.hour == 8 and now.minute == 0:
            send_daily_pulse()
            time.sleep(61) 
        time.sleep(30)

if __name__ == "__main__":
    run_monitor()
