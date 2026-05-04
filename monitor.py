import datetime
import sys
import os
import requests
import time
import urllib3
import re
from edgar import Company, set_identity
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

# --- 0. CONFIG ---
load_dotenv() 
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_API_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TD_API_KEY") # Ensure this matches your .env

set_identity(f"Alwin Almazan {SENDER_EMAIL}")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_PATH, "sent_filings.txt")

# --- 1. THE VENTURE MONITOR ---

def get_venture_market_status(ticker):
    """Venture Tier: Pulls real-time quote + technical context for whale detection."""
    url = f"https://api.twelvedata.com/quote?symbol={ticker}&apikey={TD_API_KEY}"
    try:
        data = requests.get(url, timeout=10).json()
        if data.get("status") == "error": return None
        
        return {
            "price": float(data['close']),
            "change_pct": float(data['percent_change']),
            "volume": int(data['volume']),
            "avg_volume": int(data['average_volume']),
            "is_dumping": int(data['volume']) > (int(data['average_volume']) * 1.5) and float(data['percent_change']) < -2.0
        }
    except: return None

def get_official_nav(ticker):
    """Morningstar Scraping with a hard-coded safety fallback."""
    url = f"https://www.morningstar.com/cefs/xase/{ticker.lower()}/quote"
    try:
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
        match = re.search(r'"lastActualNav":(\d+\.\d+)', resp.text)
        if match: return float(match.group(1))
    except: pass
    return 6.43 if ticker == "CLM" else 6.23

# --- 2. SENTRY LOGIC ---

def run_sentry():
    print(f"--- 🛡️ SENTRY CHECK: {datetime.datetime.now()} ---")
    
    # Timing Logic
    current_hour_utc = datetime.datetime.now(datetime.timezone.utc).hour
    is_pulse_hour = (current_hour_utc == 18) # 08:00 HST
    is_test = "test" in sys.argv
    
    emergency_triggered = False
    reports = []

    for ticker in ["CLM", "CRF"]:
        # A. SEC Filing (The RO Edge)
        try:
            comp = Company(ticker)
            filings = comp.get_filings(form=["N-2", "424B3"])
            sec_hit = False
            if filings is not None and not filings.empty:
                f_id = filings.latest().accession_number
                if not os.path.exists(LOG_FILE): open(LOG_FILE, 'w').close()
                with open(LOG_FILE, "r") as f:
                    if f_id not in f.read():
                        sec_hit = True
                        with open(LOG_FILE, "a") as f_app: f_app.write(f"{f_id}\n")
        except: sec_hit = False

        # B. Venture Market Data
        mkt = get_venture_market_status(ticker)
        nav = get_official_nav(ticker)
        
        if mkt and nav:
            premium = ((mkt['price'] - nav) / nav) * 100
            
            # TRIGGER DEFINITIONS
            is_whale_dump = mkt['is_dumping']
            is_high_premium = premium > 25.0
            
            status = "✅ STABLE"
            if sec_hit: 
                status, emergency_triggered = "🚨 SELL SIGNAL: N-2 FILED", True
            elif is_whale_dump: 
                status, emergency_triggered = "🚨 WHALE DUMP DETECTED", True
            elif is_high_premium: 
                status, emergency_triggered = "⚠️ HIGH PREMIUM WARNING", True

            # Format Report
            report = (
                f"**{ticker}**: {status}\n"
                f"└ Price: ${mkt['price']:.2f} | NAV: ${nav:.2f} ({premium:.1f}% Prem)\n"
                f"└ Vol: {mkt['volume']:,} (Avg: {mkt['avg_volume']:,})"
            )
            reports.append(report)

    # --- 3. SMART NOTIFICATION DISPATCH ---
    if reports:
        full_msg = "\n\n".join(reports)
        
        # Scenario A: Emergency (Pushover + Discord immediately)
        if emergency_triggered:
            # Discord
            send_essentials_embed(
                webhook_url=WEBHOOK_CORNERSTONE,
                title="🚨 CORNERSTONE EMERGENCY ALERT",
                description=full_msg,
                color=0xe74c3c
            )
            # Pushover
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": PUSHOVER_API_TOKEN, "user": PUSHOVER_USER_KEY,
                "title": "🚨 CLM/CRF SELL ALERT", "message": full_msg.replace("**", ""),
                "priority": 1, "sound": "emergency", "retry": 60, "expire": 3600
            })
            print("🚨 Emergency Alert Dispatched.")

        # Scenario B: Daily Pulse (Every day at 0800 HST)
        elif is_pulse_hour or is_test:
            send_essentials_embed(
                webhook_url=WEBHOOK_CORNERSTONE,
                title="💎 Daily Cornerstone Pulse (0800 HST)",
                description="Monitoring for RO Filings and Institutional Dumps.\n\n" + full_msg,
                color=0x3498db
            )
            # Personal Pushover (No sound, low priority)
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": PUSHOVER_API_TOKEN, "user": PUSHOVER_USER_KEY,
                "title": "🤖 Daily Pulse: CLM/CRF", "message": full_msg.replace("**", ""),
                "priority": 0
            })
            print("📅 Daily Pulse Dispatched.")

if __name__ == "__main__":
    run_sentry()
