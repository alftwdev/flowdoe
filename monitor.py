import datetime
import sys
import os
import requests
import time
import urllib3
import re
import smtplib
from email.message import EmailMessage
from edgar import Company, set_identity
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

# --- 0. LOAD SECURE VAULT ---
load_dotenv() 

# --- 1. IDENTITY & CREDENTIALS ---
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_API_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
WORK_EMAIL = os.getenv("WORK_EMAIL")
WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY") # New Venture Key

# --- SAFETY CHECK ---
if not TD_API_KEY:
    print("❌ ERROR: TWELVE_DATA_API_KEY not found in .env!")
    sys.exit(1)

set_identity(f"Alwin Almazan {SENDER_EMAIL}")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_PATH, "sent_filings.txt")

RECIPIENTS = [SENDER_EMAIL]
if WORK_EMAIL: RECIPIENTS.append(WORK_EMAIL)

# --- 2. TWELVE DATA INTEGRATION ---

def get_venture_data(ticker):
    """Fetches Real-Time Data using Venture Tier with robust error handling"""
    print(f"    [TD] Fetching Real-Time Data for {ticker}...")
    # Using 'quote' endpoint which is 1 credit
    url = f"https://api.twelvedata.com/quote?symbol={ticker}&apikey={TD_API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        
        # Twelve Data error messages usually come in a 'message' key
        if "message" in data and data.get("status") == "error":
            print(f"    [TD] API ERROR: {data['message']}")
            return None
        
        # Ensure we are pulling the most recent price available
        # 'price' is for live, 'close' is for last recorded
        price = data.get('price') or data.get('close')
        
        return {
            "price": float(price),
            "volume": int(data['volume']),
            "prev_close": float(data['previous_close']),
            "avg_volume": int(data['average_volume'])
        }
    except Exception as e:
        print(f"    [TD] SCRIPT ERROR: {e}")
        return None

def get_official_nav(ticker):
    """Upgraded: Uses Morningstar as backup, but you could add TD Fundamentals here later"""
    print(f"    [NAV] Fetching {ticker} NAV Anchor...")
    url = f"https://www.morningstar.com/cefs/xase/{ticker.lower()}/quote"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        match = re.search(r'"lastActualNav":(\d+\.\d+)', response.text)
        if match:
            val = float(match.group(1))
            print(f"    [NAV] SUCCESS: {ticker} = ${val}")
            return val
    except Exception as e:
        print(f"    [NAV] ERROR: {e}")
    return 6.43 if ticker == "CLM" else 6.23

# --- 3. THE SENTRY CHECK ---

def run_sentry_check():
    print(f"\n--- VENTURE SENTRY START: {datetime.datetime.now()} ---")
    is_manual_test = "test" in sys.argv
    current_hour_utc = datetime.datetime.now(datetime.timezone.utc).hour
    is_heartbeat_hour = (17 <= current_hour_utc <= 19)

    # REFRESH NAV ANCHORS (Daily Pulse)
    if is_manual_test or is_heartbeat_hour:
        for t in ["CLM", "CRF"]:
            nav = get_official_nav(t)
            with open(os.path.join(BASE_PATH, f"{t}_anchor.txt"), "w") as f: f.write(str(nav))

    reports = []
    emergency = False
    credit_usage = 0

    for ticker in ["CLM", "CRF"]:
        print(f"\nPROCESS: Analyzing {ticker}...")
        try:
            # A. SEC FILING CHECK (100 Credit logic equivalent if using API)
            # We stay with 'edgar' library for now as it's free, but monitor frequency
            comp = Company(ticker)
            filings = comp.get_filings(form=["N-2", "N-2/A", "424B3"])
            sec_alert = False
            if filings is not None and not filings.empty:
                f_id = filings.latest().accession_number
                if not os.path.exists(LOG_FILE): open(LOG_FILE, 'w').close()
                with open(LOG_FILE, "r") as f:
                    if f_id not in f.read():
                        sec_alert = True
                        with open(LOG_FILE, "a") as f_app: f_app.write(f"{f_id}\n")

            # B. REAL-TIME MARKET DATA (Twelve Data Venture)
            mkt = get_venture_data(ticker)
            credit_usage += 1
            
            # C. NAV & PREMIUM CALC
            anchor_file = os.path.join(BASE_PATH, f"{ticker}_anchor.txt")
            with open(anchor_file, "r") as f: nav = float(f.read().strip())

            if mkt and nav:
                premium = ((mkt['price'] - nav) / nav) * 100
                
                # WHALE ALERT: Volume > 150% of Average (Real-time from TD)
                dump_detected = (mkt['volume'] > mkt['avg_volume'] * 1.5) and (mkt['price'] < mkt['prev_close'] * 0.98)

                status = "✅ STABLE"
                if dump_detected: status, emergency = "🚨 WHALE DUMP DETECTED", True
                if premium > 25: status, emergency = "🚨 HIGH PREMIUM (>25%)", True
                if sec_alert: status, emergency = "🚨 SELL SIGNAL: N-2 FILED", True
                
                reports.append(f"**{ticker}**: {status}\n**Premium**: {premium:.2f}%\n**Price**: ${mkt['price']:.2f} | **NAV**: ${nav:.2f}\n**Vol**: {mkt['volume']:,} (Avg: {mkt['avg_volume']:,})")
                print(f"    [RES] Status: {status}")

        except Exception as e:
            print(f"    [ERR] Failed {ticker}: {e}")

# --- 4. DISPATCH LOGIC ---
    if reports:
        # Determine the Summary Headline
        if emergency:
            # Check the specific cause for the headline
            if any("SEC N-2" in r or "SEC FILED" in r for r in reports):
                summary_headline = "🚨 **CRITICAL: NEW SEC N-2 FILING DETECTED**"
            elif any("WHALE" in r or "VOLATILITY" in r for r in reports):
                summary_headline = "🚨 **VOLATILITY ALERT: HEAVY SELLING**"
            else:
                summary_headline = "🚨 **ALERT: HIGH PREMIUM THRESHOLD BREACHED**"
        else:
            summary_headline = "**STATUS: NO NEW SEC FILINGS DETECTED**"

        # Combine headline with the detailed reports
        full_msg = f"{summary_headline}\n\n" + "\n\n".join(reports)
        
        # DISCORD (Subscriber Feed)
        if WEBHOOK_CORNERSTONE:
            print("    [DISCORD] Broadcasting Heartbeat...")
            send_essentials_embed(
                webhook_url=WEBHOOK_CORNERSTONE,
                title="💎 Venture Heartbeat" if not emergency else "🚨 CORNERSTONE EMERGENCY",
                description=full_msg,
                color=0xe74c3c if emergency else 0x3498db
            )

        # PUSHOVER (Personal Capital Guard)
        # Note: Removing markdown ** for Pushover's plain-text display
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": PUSHOVER_API_TOKEN, 
            "user": PUSHOVER_USER_KEY,
            "title": "🚨 SELL ALERT" if emergency else "🤖 Daily Pulse",
            "message": full_msg.replace("**", ""),
            "priority": 1 if emergency else 0,
            "sound": "emergency" if emergency else "none"
        }, timeout=10)

    print(f"\n--- SENTRY FINISHED (Credits Used: {credit_usage}) ---\n")

if __name__ == "__main__":
    run_sentry_check()
