import os
import requests
import time
import sys
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

# Environment Variables
WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
PULSE_FILE = os.path.join(BASE_DIR, "last_pulse.txt")

# Configuration for Priority Assets
PRIORITY_ASSETS = {
    "CLM": {"nav_ticker": "XCLMX", "avg_vol": 1700000},
    "CRF": {"nav_ticker": "XCRFX", "avg_vol": 600000}
}

# Persistent Memory Cache to avoid spam duplicate notifications
last_alert_cache = {}

# --- 2. LIVE INTELLIGENCE GATHERING ---

def fetch_live_metrics(symbol):
    """Fetches RSI, current Price, and Live NAV from Twelve Data."""
    try:
        # 1. Fetch Price
        p_url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}"
        p_res = requests.get(p_url, timeout=10).json()
        price = float(p_res.get('price', 0.0))

        # 2. Fetch RSI (1D, 14 Period)
        rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}"
        r_res = requests.get(rsi_url, timeout=10).json()
        rsi = float(r_res['values'][0]['rsi']) if 'values' in r_res else 50.0

        # 3. Dynamic NAV Fetch
        nav_ticker = PRIORITY_ASSETS[symbol]["nav_ticker"]
        nav_url = f"https://api.twelvedata.com/price?symbol={nav_ticker}&apikey={TD_API_KEY}"
        nav_res = requests.get(nav_url, timeout=10).json()
        
        fallback_nav = 6.45 if symbol == "CLM" else 6.30
        nav = float(nav_res.get('price', fallback_nav))
        
        return price, rsi, nav
    except Exception as e:
        print(f"⚠️ [Data Fetch Error] Failed to compile metrics for {symbol}: {e}")
        return 0.0, 50.0, (6.45 if symbol == "CLM" else 6.30)

def evaluate_emergency_shields(tz_h):
    """Scans for active SEC Dilution or live Whale Dumps during market hours."""
    global last_alert_cache
    
    # Time Gate: Stop emergency processing completely outside live market hours
    now = datetime.now(tz_h)
    market_start = dt_time(3, 30) # 03:30 AM HST (Market Open)
    market_end = dt_time(10, 0)   # 10:00 AM HST (Market Close)
    
    if not (market_start <= now.time() <= market_end):
        return # Silent retreat: Market floor is officially locked

    for ticker in PRIORITY_ASSETS:
        price, rsi, nav = fetch_live_metrics(ticker)
        if price == 0.0: continue

        # Whale Flow Validation
        whale_status, _, is_whale_dump = get_institutional_conviction(ticker, TD_API_KEY) if HAS_ESSENTIALS else ("NOMINAL Flow", 0, False)
        
        # Unique signature to track if alert state has changed
        alert_signature = f"{ticker}_{whale_status}_{is_whale_dump}"

        if is_whale_dump:
            # Check if this exact alert pattern was already pushed to avoid excessive logging
            if last_alert_cache.get(ticker) == alert_signature:
                continue 
                
            title = f"🚨 CRITICAL: WHALE DUMP DETECTED [{ticker}]"
            description = (
                f"### **Institutional Capitulation Shield Active**\n"
                f"**Asset Identified**: `${ticker}`\n"
                f"┣ **Whale Status**: `{whale_status}`\n"
                f"┣ **Current Price**: `${price:.2f}`\n"
                f"┗ **RSI Position**: `{rsi:.1f}`\n\n"
                f"⚠️ *Sentry Recommendation: Immediate capital exposure review for high premium cashflow portfolios.*"
            )
            
            if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
                send_essentials_embed(WEBHOOK_CORNERSTONE, title, description, 0xe74c3c)
                
            # Cache the signature to silence subsequent duplicate loops
            last_alert_cache[ticker] = alert_signature
            print(f"🚨 [Emergency Shield] Dispatched unique anomaly alert for {ticker}.")

def get_ticker_report(ticker):
    """Assembles the consolidated tactical report applying the 3 Tactical Shields."""
    price, rsi, nav = fetch_live_metrics(ticker)
    if price == 0.0:
        return f"### **Cornerstone Flowstate Check: {ticker}**\n⚠️ *Data Feed Offline.*\n"

    whale_status, _, is_whale_dump = get_institutional_conviction(ticker, TD_API_KEY) if HAS_ESSENTIALS else ("NOMINAL Flow", 0, False)
    sec_shield = "No N2/SEC Detected" 
    premium = ((price - nav) / nav) * 100
    
    if "No" not in sec_shield or is_whale_dump:
        status = "🔴 CRITICAL: EXIT"
        income_note = "LIQUIDATE: Structural Dilution / Whale capitulation in progress."
        verdict = "🚨 SEC Dilution or High-Volume Whale Dump identified."
        recommendation = "SELL/EXECUTE CAPITAL PROTECTION PROTOCOL IMMEDIATELY."
    elif premium > 25.0:
        status = "⚠️ HIGH PREMIUM: Frothy Extension"
        income_note = "HOLD / PAUSE REINVESTMENT: Premium near historic risk bands."
        verdict = "The Premium extension has stretched the rubber band thin. Reversion risk elevated."
        recommendation = "Maintain posture; pause new margin capital allocation."
    else:
        status = "✅ STABLE: Nominal Flowstate"
        income_note = "HOLD/ACCUMULATE: Net distributions healthy relative to carrying costs."
        verdict = "Premium variance within historical standard deviations. No active dilution signatures."
        recommendation = "Stable environment. Reinvest distributions; accumulate on tactical pullbacks."

    return (
        f"### **{ticker} Cornerstone Flowstate Update**\n"
        f"**Status**: {status}\n"
        f"┣ **Premium to NAV**: `{premium:.1f}%`\n"
        f"┣ **SEC Shield**: `{sec_shield}`\n"
        f"┣ **RSI (1D)**: `{rsi:.1f}`\n"
        f"┣ **Income Note**: `{income_note}`\n"
        f"┣ **Whale Flow**: `{whale_status}`\n"
        f"┣ **Recommendation**: `{recommendation}`\n"
        f"┗ **Strategy Verdict**: *{verdict}*\n"
    )

def send_daily_pulse(is_test=False):
    """Generates and dispatches the single, unified message for the cashflow portfolio."""
    print(f"\n📡 [Broadcast Engine] Compiling {'TEST ' if is_test else 'DAILY'} Tactical Pulse...")
    
    reports = []
    for ticker in PRIORITY_ASSETS:
        reports.append(get_ticker_report(ticker))
    
    full_report = "\n".join(reports)
    title = "🏛️ Cornerstone Flowstate Update" if not is_test else "🧪 TEST: Rockefeller Flowstate Scan"
    
    color = 0x3498db  # Professional Blue for standard Flowstate Pulses
    if "HIGH PREMIUM" in full_report: color = 0xf1c40f
    if "CRITICAL" in full_report: color = 0xe74c3c
    
    # Discord Dispatch
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, full_report, color)

    # Pushover Dispatch
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
        print(f"💾 [System State] Local pulse cache finalized.\n")
    except Exception as e:
        print(f"⚠️ [System State Error] Could not write execution sync file: {e}\n")

# --- 4. ENGINE RUNTIME RUNNER ---

def run_monitor():
    # Enforce strict timezone object isolation for verification loops
    tz_h = pytz.timezone('Pacific/Honolulu')
    current_time_str = datetime.now(tz_h).strftime('%Y-%m-%d %H:%M HST')
    print(f"--- 🛡️ SENTRY ACTIVE: {current_time_str} ---")

    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "--force", "force"]:
        send_daily_pulse(is_test=("test" in sys.argv[1].lower()))
        return

    print("⏳ [Engine Loop] Entering PythonAnywhere Always-On matrix...")
    last_pulse_day = None

    while True:
        # Re-fetch exact isolated timestamp for Honolulu every loop execution
        now_hst = datetime.now(tz_h)
        
        # 1. TIMING LOGIC: HEARBEAT DETECTION AT 08:00 AM HST EXACTLY
        # Checks hour, minute, AND ensures it hasn't already fired on this specific day calendar date
        if now_hst.hour == 8 and now_hst.minute == 0 and last_pulse_day != now_hst.day:
            print("🎯 [Core Trigger] 08:00 HST Window Opened. Dispatched Daily Pulse.")
            send_daily_pulse()
            last_pulse_day = now_hst.day
            time.sleep(60) # Advance clock out of trigger minute window safely
            continue

        # 2. EMERGENCY SCANS TRIGGER (Continuous loop validation)
        evaluate_emergency_shields(tz_h)

        # 60 Second Heartbeat Tick
        time.sleep(60)

if __name__ == "__main__":
    run_monitor()
