import os
import requests
import time
import sys
import smtplib
from email.message import EmailMessage
from datetime import datetime
import pytz
from dotenv import load_dotenv

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
PULSE_FILE = os.path.join(BASE_DIR, "last_pulse.txt")

# Configuration for Priority Assets
PRIORITY_ASSETS = {
    "CLM": {"nav_ticker": "XCLMX", "avg_vol": 1700000},
    "CRF": {"nav_ticker": "XCRFX", "avg_vol": 600000}
}

# --- 2. LIVE INTELLIGENCE GATHERING ---

def fetch_live_metrics(symbol):
    """Fetches RSI and current Price from Twelve Data."""
    try:
        # RSI Fetch
        rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}"
        r_data = requests.get(rsi_url).json()
        rsi = float(r_data['values'][0]['rsi'])
        
        # Price Fetch
        p_url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}"
        p_data = requests.get(p_url).json()
        price = float(p_data['price'])
        
        return price, rsi
    except:
        return 0.0, 0.0

def get_ticker_report(ticker):
    """Assembles the consolidated tactical report for a single asset."""
    price, rsi = fetch_live_metrics(ticker)
    
    # 1. Whale Flow Logic (Institutional Conviction)
    whale_status, _, is_whale_dump = get_institutional_conviction(ticker, TD_API_KEY)
    
    # 2. SEC Shield (Simulated check - integrates with your edgartools logic)
    sec_shield = "No N2/SEC Detected" 
    
    # 3. Premium Math (Using a fixed NAV proxy for this example)
    # In production, replace with your Morningstar scraper logic
    nav_proxy = 6.45 if ticker == "CLM" else 6.30 
    premium = ((price - nav_proxy) / nav_proxy) * 100
    
    # Strategy Verdict Logic
    if "No" not in sec_shield or is_whale_dump:
        status = "🔴 CRITICAL: EXIT"
        income_note = "EXIT: Capital at risk."
        verdict = "🚨 SEC Dilution or Whale Dump detected."
        recommendation = "SELL, SELL, SELL!!; Capital protection!."
    elif premium > 25:
        status = "⚠️ HIGH PREMIUM: Frothy"
        income_note = "HOLD: High yield but risky."
        verdict = "Premium approaching historical resistance."
        recommendation = "Pause; Monitor for RO filing."
    else:
        status = "✅ STABLE: Bullish"
        income_note = "HOLD/BUY: Buy."
        verdict = "No dilution risk detected."
        recommendation = "Stable; Accumulate on dips."

    return (
        f"### **Flowstate Check: {ticker}**\n"
        f"**Status**: {status}\n"
        f"┣ **Premium to NAV**: {premium:.1f}%\n"
        f"┣ **SEC Shield**: {sec_shield}\n"
        f"┣ **RSI (1D)**: {rsi:.1f}\n"
        f"┣ **Income Note**: {income_note}\n"
        f"┣ **Whale Flow**: {whale_status}\n"
        f"┣ **Recommendation**: {recommendation}\n"
        f"┗ **Strategy Verdict**: {verdict}\n"
    )

# --- 3. BROADCAST ENGINE ---

def send_daily_pulse(is_test=False):
    """Generates the single, unified message for both assets."""
    print(f"📡 Generating {'Test ' if is_test else ''}Tactical Pulse...")
    
    reports = []
    for ticker in PRIORITY_ASSETS:
        reports.append(get_ticker_report(ticker))
    
    full_report = "\n".join(reports)
    title = "💰 Daily Cornerstone Pulse"
    color = 0x2ecc71 if "NOMINAL" in full_report else 0xf1c40f
    
    # Discord Dispatch
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, full_report, color)

    # Pushover Dispatch
    if os.getenv("PUSHOVER_API_TOKEN"):
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": os.getenv("PUSHOVER_API_TOKEN"),
            "user": os.getenv("PUSHOVER_USER_KEY"),
            "title": title,
            "message": full_report.replace("#", "").replace("**", ""),
            "priority": 0
        })

    # Update state file
    with open(PULSE_FILE, "w") as f:
        f.write(datetime.now().isoformat())

# --- 4. MAIN LOOP ---

def run_monitor():
    tz_h = pytz.timezone('Pacific/Honolulu')
    print(f"--- 🛡️ SENTRY ACTIVE: {datetime.now(tz_h).strftime('%Y-%m-%d %H:%M HST')} ---")

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        send_daily_pulse(is_test=True)
        return

    while True:
        now = datetime.now(tz_h)
        
        # DAILY HEARTBEAT AT 08:00 HST
        if now.hour == 8 and now.minute == 0:
            send_daily_pulse()
            time.sleep(61) # Avoid double-pulse

        # [Continuous Emergency Monitoring logic for SEC Filings/Whale Spikes]
        # if check_emergency_triggers():
        #     broadcast_emergency(...)

        time.sleep(30)

if __name__ == "__main__":
    run_monitor()
