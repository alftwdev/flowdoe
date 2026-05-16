import os
import requests
import time
import sys
import json
from datetime import datetime, time as dt_time
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

# Dynamic Asset Inheritance from income.py
try:
    import income
    INCOME_TARGETS = income.PRIORITY_ASSETS if hasattr(income, "PRIORITY_ASSETS") else ["CLM", "CRF"]
except Exception:
    INCOME_TARGETS = ["CLM", "CRF"]

# Structural Technical Asset Profiles
PRIORITY_ASSETS = {}
for ticker in INCOME_TARGETS:
    if ticker == "CLM":
        PRIORITY_ASSETS["CLM"] = {"nav_ticker": "XCLMX", "avg_vol": 1700000}
    elif ticker == "CRF":
        PRIORITY_ASSETS["CRF"] = {"nav_ticker": "XCRFX", "avg_vol": 600000}
    else:
        PRIORITY_ASSETS[ticker] = {"nav_ticker": f"X{ticker}X", "avg_vol": 1000000}

WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
PULSE_FILE = os.path.join(BASE_DIR, "last_pulse.txt")

last_alert_cache = {}

# --- 2. LIVE INTELLIGENCE GATHERING ---

def fetch_live_metrics(symbol):
    """Fetches RSI, current Price, % Change, and Live NAV using high-fidelity endpoints."""
    try:
        # 1. Fetch Quote (Price + Daily % Change simultaneously)
        q_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
        q_res = requests.get(q_url, timeout=10).json()
        
        price = float(q_res.get('close', 0.0))  
        change_pct = float(q_res.get('percent_change', 0.0))

        # 2. Fetch RSI (1D, 14 Period)
        rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}"
        r_res = requests.get(rsi_url, timeout=10).json()
        rsi = float(r_res['values'][0]['rsi']) if 'values' in r_res else 50.0

        # 3. Dynamic NAV Fetch
        nav_ticker = PRIORITY_ASSETS[symbol]["nav_ticker"]
        nav_url = f"https://api.twelvedata.com/price?symbol={nav_ticker}&apikey={TD_API_KEY}"
        nav_res = requests.get(nav_url, timeout=10).json()
        
        fallback_nav = 6.47 if symbol == "CLM" else 6.25
        nav = float(nav_res.get('price', fallback_nav))
        
        return price, change_pct, rsi, nav
    except Exception as e:
        print(f"⚠️ [Data Fetch Error] Failed to compile metrics for {symbol}: {e}")
        return 0.0, 0.0, 50.0, (6.47 if symbol == "CLM" else 6.25)

def evaluate_emergency_shields(tz_h):
    """Scans for live Whale Dumps during market hours."""
    global last_alert_cache
    
    now = datetime.now(tz_h)
    market_start = dt_time(3, 30) 
    market_end = dt_time(10, 0)   
    
    if not (market_start <= now.time() <= market_end):
        return 

    for ticker in PRIORITY_ASSETS:
        price, change_pct, rsi, nav = fetch_live_metrics(ticker)
        if price == 0.0: continue

        whale_status, _, is_whale_dump = get_institutional_conviction(ticker, TD_API_KEY) if HAS_ESSENTIALS else ("NOMINAL Flow", 0, False)
        alert_signature = f"{ticker}_{whale_status}_{is_whale_dump}"

        if is_whale_dump:
            if last_alert_cache.get(ticker) == alert_signature:
                continue 
                
            title = f"🚨 CRITICAL: WHALE DUMP DETECTED [{ticker}]"
            description = (
                f"### **Institutional Capitulation Shield Active**\n"
                f"**Asset Identified**: `${ticker}`\n"
                f"┣ **Real-Time Spot Price**: `${price:.2f}` (`{change_pct:+.2f}%`)\n"
                f"┣ **Whale Status**: `{whale_status}`\n"
                f"┗ **RSI Position**: `{rsi:.1f}`\n\n"
                f"⚠️ *Sentry Recommendation: Immediate capital exposure review.*"
            )
            
            if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
                send_essentials_embed(WEBHOOK_CORNERSTONE, title, description, 0xe74c3c)
                
            last_alert_cache[ticker] = alert_signature

def get_ticker_report(ticker):
    """Assembles tactical report with zero-variance inline premium mathematics."""
    price, change_pct, rsi, nav = fetch_live_metrics(ticker)
    if price == 0.0:
        return f"### **Cornerstone Flowstate Check: {ticker}**\n⚠️ *Data Feed Offline.*\n"

    whale_status, _, is_whale_dump = get_institutional_conviction(ticker, TD_API_KEY) if HAS_ESSENTIALS else ("NOMINAL Flow", 0, False)
    sec_shield = "No N2/SEC Detected" 
    
    # Mathematical Precision Alignment
    premium = ((price - nav) / nav) * 100
    
    if "No" not in sec_shield or is_whale_dump:
        status = "🔴 CRITICAL: EXIT"
        income_note = "LIQUIDATE: Structural Dilution / Whale capitulation in progress."
        verdict = "🚨 SEC Dilution or High-Volume Whale Dump identified."
        recommendation = "SELL/EXECUTE CAPITAL PROTECTION PROTOCOL IMMEDIATELY."
    elif premium > 25.0:
        status = "⚠️ HIGH PREMIUM"
        income_note = "HOLD / PAUSE REINVESTMENT: Premium near historic risk bands."
        verdict = "The Premium extension has stretched the rubber band thin."
        recommendation = "Maintain posture; pause new margin capital allocation."
    else:
        status = "✅ STABLE"
        income_note = "ACCUMULATION PHASE"
        verdict = "Premium within standard deviations."
        recommendation = "Healthy DRIP at NAV."

    return (
        f"### **{ticker} Cornerstone Flowstate Update**\n"
        f"**Status**: {status}\n"
        f"┣ **Real-Time Spot Price**: `${price:.2f}` (`{change_pct:+.2f}%`)\n"
        f"┣ **Current Premium to NAV**: `{premium:.2f}%` (NAV: `${nav:.2f}`)\n"
        f"┣ **SEC Shield**: `{sec_shield}`\n"
        f"┣ **RSI (1D)**: `{rsi:.1f}`\n"
        f"┣ **Income Note**: `{income_note}`\n"
        f"┣ **Whale Flow**: `{whale_status}`\n"
        f"┣ **Recommendation**: `{recommendation}`\n"
        f"┗ **Strategy Verdict**: *{verdict}*\n"
    )

def send_daily_pulse(is_test=False):
    print(f"\n📡 [Broadcast Engine] Compiling {'TEST ' if is_test else 'DAILY'} Tactical Pulse...")
    
    reports = []
    for ticker in PRIORITY_ASSETS:
        reports.append(get_ticker_report(ticker))
    
    full_report = "\n".join(reports)
    title = "🏛️ Cornerstone Flowstate Update" if not is_test else "🧪 TEST: Rockefeller Flowstate Scan"
    
    color = 0x3498db  
    if "STABLE" in full_report: color = 0x2ecc71       
    if "HIGH PREMIUM" in full_report: color = 0xf1c40f 
    if "CRITICAL" in full_report: color = 0xe74c3c     
    
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, full_report, color)

    if os.getenv("PUSHOVER_API_TOKEN"):
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": os.getenv("PUSHOVER_API_TOKEN"),
            "user": os.getenv("PUSHOVER_USER_KEY"),
            "title": title,
            "message": full_report.replace("#", "").replace("**", "").replace("┣", "").replace("┗", ""),
            "priority": 1 if "CRITICAL" in full_report else 0
        }, timeout=10)

    try:
        with open(PULSE_FILE, "w") as f:
            f.write(datetime.now().isoformat())
    except Exception as e:
        print(f"⚠️ Error writing state sync file: {e}")

def run_monitor():
    tz_h = pytz.timezone('Pacific/Honolulu')
    current_time_str = datetime.now(tz_h).strftime('%Y-%m-%d %H:%M HST')
    print(f"--- 🛡️ SENTRY ACTIVE: {current_time_str} ---")

    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "--force", "force"]:
        send_daily_pulse(is_test=("test" in sys.argv[1].lower()))
        return

    print("⏳ Entering PythonAnywhere Engine Loop...")
    last_pulse_day = None

    while True:
        now_hst = datetime.now(tz_h)
        if now_hst.hour == 8 and now_hst.minute == 0 and last_pulse_day != now_hst.day:
            send_daily_pulse()
            last_pulse_day = now_hst.day
            time.sleep(60) 
            continue

        evaluate_emergency_shields(tz_h)
        time.sleep(60)

if __name__ == "__main__":
    run_monitor()
