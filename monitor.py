import datetime
import sys
import os
import requests
import time
import urllib3
import re
import smtplib
import yfinance as yf  
from email.message import EmailMessage
from edgar import Company, set_identity
from dotenv import load_dotenv

# --- 0. LOAD SECURE VAULT ---
load_dotenv() 

# --- 1. IDENTITY & CREDENTIALS ---
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_API_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
WORK_EMAIL = os.getenv("WORK_EMAIL")

# Configuration
set_identity(f"Alwin Almazan {SENDER_EMAIL}")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_PATH, "sent_filings.txt")

# Ensure yfinance doesn't trip over itself on different systems
yf.set_tz_cache_location(os.path.join(BASE_PATH, "yf_cache"))

RECIPIENTS = [SENDER_EMAIL]
if WORK_EMAIL:
    RECIPIENTS.append(WORK_EMAIL)

def send_emergency_email(subject, body):
    """Bypasses cellular data and sends a direct email."""
    print(f"    [EMAIL] Preparing email for {RECIPIENTS}...")
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECIPIENTS)
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(SENDER_EMAIL, EMAIL_APP_PASSWORD)
            smtp.send_message(msg)
            print("    [EMAIL] SUCCESS: Email sent.")
    except Exception as e:
        print(f"    [EMAIL] ERROR: {e}")

def get_official_nav(ticker):
    """Pulls live NAV from Morningstar."""
    print(f"    [NAV] Fetching {ticker} from Morningstar...")
    url = f"https://www.morningstar.com/cefs/xase/{ticker.lower()}/quote"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
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

def get_market_data(ticker):
    """
    YFINANCE 2026 UPDATE: Pulls high-accuracy Price and Volume.
    Uses a minimal retrieval method to avoid data center detection.
    """
    print(f"    [MKT] Fetching Price/Vol for {ticker} from Yahoo Finance...")
    try:
        # We access the ticker directly. yfinance handles its own headers 
        # but we use fast_info for raw speed and to avoid extra API hits.
        yf_ticker = yf.Ticker(ticker)
        info = yf_ticker.fast_info
        
        # Check if we got a valid response
        price = info['last_price']
        if price is None or price == 0:
            raise ValueError(f"Yahoo returned empty data for {ticker}")
            
        return {
            "price": float(price),
            "volume": int(info['last_volume']),
            "prev_close": float(info['previous_close'])
        }
    except Exception as e:
        print(f"    [MKT] ERROR: {e}")
        return None

def run_sentry_check():
    print(f"\n--- SENTRY START: {datetime.datetime.now()} ---")
    is_manual_test = "test" in sys.argv
    current_hour_utc = datetime.datetime.now(datetime.timezone.utc).hour
    is_heartbeat_hour = (17 <= current_hour_utc <= 19) # 8:00 AM HST

    # Daily NAV anchor refresh
    if is_manual_test or is_heartbeat_hour:
        print("ACTION: Refreshing Daily NAV Anchors...")
        for t in ["CLM", "CRF"]:
            nav = get_official_nav(t)
            anchor_path = os.path.join(BASE_PATH, f"{t}_anchor.txt")
            with open(anchor_path, "w") as f: 
                f.write(str(nav))

    reports = []
    emergency = False

    for ticker in ["CLM", "CRF"]:
        print(f"\nPROCESS: Analyzing {ticker}...")
        try:
            # 1. SEC WATCHDOG
            comp = Company(ticker)
            filings = comp.get_filings(form=["N-2", "N-2/A", "424B3"])
            sec_alert = False
            if filings is not None and not filings.empty:
                f_id = filings.latest().accession_number
                if not os.path.exists(LOG_FILE): 
                    open(LOG_FILE, 'w').close()
                with open(LOG_FILE, "r") as f:
                    content = f.read()
                    if f_id not in content:
                        sec_alert = True
                        with open(LOG_FILE, "a") as f_app: 
                            f_app.write(f"{f_id}\n")

            # 2. MARKET SENTINEL
            mkt = get_market_data(ticker)
            anchor_file = os.path.join(BASE_PATH, f"{ticker}_anchor.txt")
            
            if not os.path.exists(anchor_file):
                nav = get_official_nav(ticker)
                with open(anchor_file, "w") as f: f.write(str(nav))
            
            with open(anchor_file, "r") as f:
                nav = float(f.read().strip())

            if mkt and nav:
                # PREMIUM CALCULATION
                premium = ((mkt['price'] - nav) / nav) * 100
                avg_vol = 1700000 if ticker == "CLM" else 600000
                dump_detected = (mkt['volume'] > avg_vol * 1.4) and (mkt['price'] < mkt['prev_close'] * 0.97)

                status = "✅ STABLE"
                if dump_detected: status, emergency = "🚨 VOLATILITY DUMP", True
                if premium > 25: status, emergency = "🚨 HIGH PREMIUM", True
                if sec_alert: status, emergency = "🚨 SELL SIGNAL: SEC N-2", True
                
                reports.append(f"{ticker}: {status}\nPremium: {premium:.2f}%\nPrice: ${mkt['price']:.2f} | NAV: ${nav:.2f}\nVol: {mkt['volume']:,}")
                print(f"    [RES] Status: {status}")

        except Exception as e:
            print(f"    [ERR] Failed {ticker}: {e}")

    # --- 3. DISPATCHER ---
    if reports:
        msg = "\n\n".join(reports)

        # Pushover dispatch
        print("\nACTION: Dispatching Pushover...")
        try:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": PUSHOVER_API_TOKEN, "user": PUSHOVER_USER_KEY,
                "title": "🚨 EMERGENCY: SELL" if emergency else "🤖 Daily Heartbeat",
                "message": msg
            }, timeout=10)
        except Exception as e:
            print(f"    [PUSH] ERROR: {e}")

        # Email dispatch
        if emergency or is_heartbeat_hour or is_manual_test:
            print("ACTION: Dispatching Email...")
            subj = "🚨 PORTFOLIO EMERGENCY" if emergency else "🤖 Daily Heartbeat: All Clear"
            send_emergency_email(subj, msg)

    print(f"\n--- SENTRY FINISHED: {datetime.datetime.now()} ---\n")

if __name__ == "__main__":
    run_sentry_check()
