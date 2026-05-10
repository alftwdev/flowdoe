import datetime
import sys
import os
import requests
import pandas as pd
import smtplib
import urllib3
import pytz
from email.message import EmailMessage
from edgar import Company, set_identity
from dotenv import load_dotenv

# --- PRE-FLIGHT DIAGNOSTICS ---
print("Checking environment...")

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    print("⚠️  Warning: essentials_tools.py not found. Discord disabled.")
    HAS_ESSENTIALS = False

# --- 1. INITIALIZATION ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_PATH, ".env")

if not os.path.exists(ENV_PATH):
    print(f"❌ CRITICAL ERROR: .env file not found at {ENV_PATH}")
    sys.exit(1)

load_dotenv(ENV_PATH)

# Safe retrieval of environment variables
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
WORK_EMAIL = os.getenv("WORK_EMAIL")
WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
PUSHOVER_USER = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
TD_KEY_RAW = os.getenv("TWELVE_DATA_API_KEY")
TWELVE_DATA_KEY = str(TD_KEY_RAW).strip() if TD_KEY_RAW else None

if not TWELVE_DATA_KEY:
    print("❌ CRITICAL ERROR: TWELVE_DATA_API_KEY is missing from .env")
    sys.exit(1)

# SEC Identity - Strict Formatting
if SENDER_EMAIL:
    set_identity(f"Alwin Almazan {SENDER_EMAIL}")
else:
    print("⚠️  Warning: SENDER_EMAIL missing. EDGAR scans might fail.")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
LOG_FILE = os.path.join(BASE_PATH, "sent_filings.txt")

def get_venture_data(ticker):
    """Fetches market data with fallback logic."""
    url = f"https://api.twelvedata.com/quote?symbol={ticker}&apikey={TWELVE_DATA_KEY}"
    fallback_avg = 1700000 if ticker == "CLM" else 950000 
    
    try:
        response = requests.get(url, timeout=12)
        resp = response.json()
        
        if resp.get("status") != "ok":
            print(f"    ❌ API Error for {ticker}: {resp.get('message', 'Unknown Error')}")
            return get_price_only_fallback(ticker, fallback_avg)
            
        vol = int(resp.get('volume', 0))
        if vol == 0:
            ts_url = f"https://api.twelvedata.com/time_series?symbol={ticker}&interval=1min&outputsize=1&apikey={TWELVE_DATA_KEY}"
            ts_res = requests.get(ts_url).json()
            if 'values' in ts_res:
                vol = int(ts_res['values'][0]['volume'])
        
        return {
            "price": float(resp['close']),
            "vol": vol,
            "avg_vol": int(resp.get('average_volume', fallback_avg)),
            "change": float(resp.get('percent_change', 0))
        }
    except Exception as e:
        print(f"    ⚠️ Connection Error for {ticker}: {e}")
    return None

def get_price_only_fallback(ticker, fallback_avg):
    try:
        url = f"https://api.twelvedata.com/price?symbol={ticker}&apikey={TWELVE_DATA_KEY}"
        resp = requests.get(url, timeout=10).json()
        if "price" in resp:
            return {"price": float(resp['price']), "vol": 0, "avg_vol": fallback_avg, "change": 0}
    except: pass
    return None

def run_sentry_check():
    tz_honolulu = pytz.timezone('Pacific/Honolulu')
    now = datetime.datetime.now(tz_honolulu)
    
    is_test = "test" in sys.argv
    is_pulse_time = (now.hour == 8 and now.minute < 15)
    
    print(f"--- 🛡️ SENTRY START: {now.strftime('%Y-%m-%d %H:%M:%S')} (HST) ---")
    
    reports = []
    sec_detected = False
    vol_spike = False
    high_premium = False
    overall_status = "STABLE"

    for ticker in ["CLM", "CRF"]:
        print(f"PROCESS: Scanning EDGAR for {ticker}...")
        try:
            comp = Company(ticker)
            filings = comp.get_filings(form=["N-2", "424B3", "N-2/A"])
            if filings is not None and not filings.empty:
                acc = filings.latest().accession_number
                if not os.path.exists(LOG_FILE): open(LOG_FILE, 'w').close()
                with open(LOG_FILE, "r") as f:
                    if acc not in f.read():
                        sec_detected = True
                        with open(LOG_FILE, "a") as fw: fw.write(f"{acc}\n")
                        print(f"🚨 NEW FILING DETECTED: {acc}")
        except Exception as e: print(f"    ⚠️ SEC Error: {e}")

        print(f"PROCESS: Fetching {ticker} Tape...")
        mkt = get_venture_data(ticker)
        
        nav_path = os.path.join(BASE_PATH, f"{ticker}_anchor.txt")
        try:
            with open(nav_path, "r") as f: nav = float(f.read().strip())
        except: nav = 6.43 if ticker == "CLM" else 6.23

        if mkt:
            price = mkt['price']
            premium = ((price - nav) / nav) * 100
            current_status = "STABLE"
            
            if mkt['vol'] > (mkt['avg_vol'] * 1.4) and mkt['change'] < -2.5:
                vol_spike, current_status, overall_status = True, "🚨 VOLATILITY DUMP", "CRITICAL"
            elif premium > 25:
                high_premium, current_status, overall_status = True, "🚨 HIGH PREMIUM", "WARNING"
            elif premium > 21:
                current_status = "⚠️ CAUTION"
                if overall_status == "STABLE": overall_status = "CAUTION"

            reports.append(f"**{ticker}: {current_status}**\n└ Price: ${price:.2f} | NAV: ${nav:.2f} ({premium:.1f}% Prem)\n└ Vol: {mkt['vol']:,}")
        else:
            reports.append(f"**{ticker}: DATA ERROR**\n└ Failed to retrieve real-time tape.")

    # Decision Engine
    if is_test or sec_detected or vol_spike or is_pulse_time:
        action_line = "🚨 SELL NOW" if (sec_detected or vol_spike) else "✅ No RO/SEC detected"
        color = 0xe74c3c if (sec_detected or vol_spike) else 0x2ecc71
        full_msg = f"**Daily Cornerstone Pulse**\nStatus: {action_line}\n\n" + "\n".join(reports)
        
        if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
            send_essentials_embed(WEBHOOK_CORNERSTONE, "🛠️ Heartbeat" if is_test else "🛡️ Sentry Signal", full_msg, color)

        if PUSHOVER_TOKEN and PUSHOVER_USER:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": PUSHOVER_TOKEN, "user": PUSHOVER_USER, 
                "title": f"Sentry: {overall_status}", "message": full_msg.replace("**", ""), "priority": 1 if color == 0xe74c3c else 0
            })

        if SENDER_EMAIL and EMAIL_APP_PASSWORD:
            try:
                msg = EmailMessage()
                msg.set_content(full_msg.replace("**", ""))
                msg['Subject'] = f"Sentry Update: {overall_status}"; msg['From'] = SENDER_EMAIL; msg['To'] = f"{SENDER_EMAIL}, {WORK_EMAIL}"
                with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                    smtp.login(SENDER_EMAIL, EMAIL_APP_PASSWORD); smtp.send_message(msg)
                print("   ✅ Dispatch Successful.")
            except Exception as e: print(f"    ⚠️ Email failed: {e}")

    print(f"--- 🛡️ SENTRY FINISHED: {datetime.datetime.now(tz_honolulu).strftime('%H:%M:%S')} ---")

if __name__ == "__main__":
    run_sentry_check()
