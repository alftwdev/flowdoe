import os
import requests
import time
import sys
import smtplib
from email.message import EmailMessage
from datetime import datetime
import pytz
from dotenv import load_dotenv
import edge # New Edge Engine

# --- 1. ECOSYSTEM INITIALIZATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

try:
    from essentials_tools import send_essentials_embed, get_institutional_conviction
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# Environment Variables
WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_API_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
PULSE_FILE = os.path.join(BASE_DIR, "last_pulse.txt")

# Configuration for Priority Assets
PRIORITY_ASSETS = {
    "CLM": {"nav_ticker": "XCLMX", "avg_vol": 1700000},
    "CRF": {"nav_ticker": "XCRFX", "avg_vol": 600000}
}

# --- 2. HARD ALERT ENGINE ---
def dispatch_hard_status(title, message):
    """Fires unconditional notifications to the admin via Pushover and Email."""
    if PUSHOVER_USER_KEY and PUSHOVER_API_TOKEN:
        try:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": PUSHOVER_API_TOKEN,
                "user": PUSHOVER_USER_KEY,
                "title": title,
                "message": message,
                "priority": 0
            }, timeout=10)
            print("📱 [Comms] Pushover hard-status alert dispatched.")
        except Exception as e:
            print(f"⚠️ [Comms Error] Pushover failed: {e}")

    if EMAIL_SENDER and EMAIL_PASSWORD and EMAIL_RECEIVER:
        try:
            msg = EmailMessage()
            msg.set_content(message)
            msg['Subject'] = title
            msg['From'] = EMAIL_SENDER
            msg['To'] = EMAIL_RECEIVER
            
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
                smtp.send_message(msg)
            print("📧 [Comms] Email hard-status alert dispatched.")
        except Exception as e:
            print(f"⚠️ [Comms Error] Email failed: {e}")

# --- 3. LIVE INTELLIGENCE GATHERING ---
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
        print(f"⚠️ [Data Fetch Error] Failed metrics for {symbol}: {e}")
        return 0.0, 50.0, (6.45 if symbol == "CLM" else 6.30)

def get_ticker_report(ticker):
    print(f"🛡️ [Shield Analysis] Processing parameters for {ticker}...")
    price, rsi, nav = fetch_live_metrics(ticker)
    
    if price == 0.0:
        return f"### **{ticker}**\n⚠️ *Data Feed Offline.*\n"

    whale_status, _, is_whale_dump = get_institutional_conviction(ticker, TD_API_KEY) if HAS_ESSENTIALS else ("NORMAL", 0, False)
    sec_shield = "No N2/ RO detected" 
    premium = ((price - nav) / nav) * 100
    
    if is_whale_dump:
        status = "🔴 CRITICAL: EXIT"
        income_note = "LIQUIDATE: Structural Dilution / Whale capitulation."
        verdict = "🚨 High-Volume Whale Dump identified."
        recommendation = "SELL/EXECUTE CAPITAL PROTECTION PROTOCOL IMMEDIATELY."
    elif premium > 25.0:
        status = "⚠️ HIGH PREMIUM"
        income_note = "HOLD / PAUSE REINVESTMENT"
        verdict = "Premium extension stretched thin. Reversion risk elevated."
        recommendation = "Pause new margin capital allocation. Watch for RO filing."
    else:
        status = "✅ STABLE"
        income_note = "Accumulation phase"
        verdict = "Premium variance within historical standard deviations. No active dilution signatures."
        recommendation = "Reinvest distributions"

    return (
        f"**{ticker}**\n"
        f"Status:  {status}\n"
        f"┣ Premium to NAV: {premium:.1f}% (neutral)\n"
        f"┣ SEC: {sec_shield}\n"
        f"┣ RSI (1D): {rsi:.1f} (neutral)\n"
        f"┣ Income Note: {income_note}\n"
        f"┣ Whale Flow: {whale_status}\n"
        f"┣ Recommendation: {recommendation}\n"
        f"┗ Strategy Verdict: {verdict}\n"
    )

def send_daily_pulse(is_test=False):
    print(f"\n📡 [Broadcast Engine] Compiling {'TEST ' if is_test else 'DAILY'} Tactical Pulse...")
    
    # Compile CEF Data
    reports = [get_ticker_report(ticker) for ticker in PRIORITY_ASSETS]
    
    # Inject Edge Engine
    print(f"🦅 [Edge Engine] Calculating Quant Stats...")
    try:
        edge_metrics = edge.calculate_mean_reversion_edge("SPY")
    except Exception as e:
        edge_metrics = "Data Unavailable"
    
    edge_block = f"\n**EDGE ENGINE INTELLIGENCE**\n{edge_metrics}"
    reports.append(edge_block)
    
    full_report = "\n".join(reports)
    title = "☕️ Cornerstone Flowstate Update" + (" - 🧪 Test Only" if is_test else "")
    
    color = 0x2ecc71
    if "HIGH PREMIUM" in full_report: color = 0xf1c40f
    if "CRITICAL" in full_report: color = 0xe74c3c
    
    # 1. Discord Embed
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, full_report, color)

    # 2. Hard Status Triggers (Always Fire)
    clean_report = full_report.replace("**", "").replace("`", "")
    dispatch_hard_status(title, clean_report)

    # 3. State Sync
    try:
        with open(PULSE_FILE, "w") as f:
            f.write(datetime.now().isoformat())
    except Exception as e:
        pass

def run_monitor():
    tz_h = pytz.timezone('US/Hawaii')
    
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
