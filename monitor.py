import datetime
import sys
import os
import requests
import pandas as pd
import smtplib
import urllib3
from email.message import EmailMessage
from edgar import Company, set_identity
from dotenv import load_dotenv

# Try to import essential tools for Discord
try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- 1. INITIALIZATION ---
# Using absolute path for PythonAnywhere reliability
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

SENDER_EMAIL = os.getenv("SENDER_EMAIL")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
WORK_EMAIL = os.getenv("WORK_EMAIL")
WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
PUSHOVER_USER = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
TWELVE_DATA_KEY = os.getenv("TWELVE_DATA_API_KEY")

set_identity(f"Alwin Almazan {SENDER_EMAIL}")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")
LOG_FILE = os.path.join(BASE_PATH, "sent_filings.txt")

def get_venture_data(ticker):
    url = f"https://api.twelvedata.com/quote?symbol={ticker}&apikey={TWELVE_DATA_KEY}"
    fallback_avg = 1700000 if ticker == "CLM" else 950000 
    try:
        resp = requests.get(url, timeout=12).json()
        if resp.get("status") == "ok":
            return {
                "price": float(resp['close']),
                "vol": int(resp.get('volume', 0)),
                "avg_vol": int(resp.get('average_volume', fallback_avg)),
                "change": float(resp.get('percent_change', 0))
            }
        b_resp = requests.get(f"https://api.twelvedata.com/price?symbol={ticker}&apikey={TWELVE_DATA_KEY}", timeout=10).json()
        if "price" in b_resp:
            return {"price": float(b_resp['price']), "vol": 0, "avg_vol": fallback_avg, "change": 0}
    except: pass
    return None

def run_sentry_check():
    now = datetime.datetime.now()
    is_test = "test" in sys.argv
    is_pulse_time = (now.hour == 8 and now.minute < 15)
    
    print(f"--- SENTRY START: {now.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    reports = []
    sec_detected = False
    vol_spike = False
    high_premium = False

    for ticker in ["CLM", "CRF"]:
        print(f"PROCESS: Analyzing {ticker} SEC Filings...")
        try:
            comp = Company(ticker)
            filings = comp.get_filings(form=["N-2", "424B3"])
            if filings is not None and not filings.empty:
                acc = filings.latest().accession_number
                if not os.path.exists(LOG_FILE): open(LOG_FILE, 'w').close()
                with open(LOG_FILE, "r") as f:
                    if acc not in f.read():
                        sec_detected = True
                        with open(LOG_FILE, "a") as fw: fw.write(f"{acc}\n")
        except: pass

        print(f"PROCESS: Fetching {ticker} Venture Market Data...")
        mkt = get_venture_data(ticker)
        nav_path = os.path.join(BASE_PATH, f"{ticker}_anchor.txt")
        try:
            with open(nav_path, "r") as f: nav = float(f.read().strip())
        except: nav = 6.43 if ticker == "CLM" else 6.23

        if mkt:
            price = mkt['price']
            premium = ((price - nav) / nav) * 100
            status = "STABLE"
            if mkt['vol'] > 0 and mkt['vol'] > (mkt['avg_vol'] * 1.4) and mkt['change'] < -2.5:
                vol_spike, status = True, "🚨 VOLATILITY DUMP"
            elif premium > 25:
                high_premium, status = True, "🚨 HIGH PREMIUM"
            elif premium > 21:
                status = "⚠️ CAUTION"

            reports.append(f"**{ticker}: {status}**\n└ Price: ${price:.2f} | NAV: ${nav:.2f} ({premium:.1f}% Prem)\n└ Vol: {mkt['vol']:,}")
            print(f"   [RESULT] {ticker}: {status} (${price})")

    if sec_detected: action_line = "🚨 SELL NOW - RO/SEC FILING DETECTED"
    elif vol_spike: action_line = "🚨 SELL NOW - VOLUME SPIKE DETECTED"
    elif high_premium: action_line = "⚠️ High premium is approaching"
    else: action_line = "✅ No RO/SEC filing detected"

    # CRITICAL LOGIC: Send if Test, SEC, Vol Spike, OR Daily Pulse window
    should_send = is_test or sec_detected or vol_spike or is_pulse_time
    
    if reports and should_send:
        full_msg = f"**Daily Cornerstone Pulse**\nStatus: {action_line}\n\n" + "\n".join(reports)
        
        if HAS_ESSENTIALS:
            print("ACTION: Dispatching to Discord...")
            send_essentials_embed(WEBHOOK_CORNERSTONE, "🛠️ Heartbeat" if is_test else "🛡️ Sentry Pulse", full_msg, 0xe74c3c if (sec_detected or vol_spike) else 0x2ecc71)
        
        print("ACTION: Dispatching to Pushover & Email...")
        requests.post("https://api.pushover.net/1/messages.json", data={"token": PUSHOVER_TOKEN, "user": PUSHOVER_USER, "title": "Sentry Alert", "message": full_msg.replace("**", ""), "priority": 1 if (sec_detected or vol_spike) else 0})
        try:
            msg = EmailMessage()
            msg.set_content(full_msg.replace("**", "")); msg['Subject'] = "Sentry Tactical Update"; msg['From'] = SENDER_EMAIL; msg['To'] = f"{SENDER_EMAIL}, {WORK_EMAIL}"
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(SENDER_EMAIL, EMAIL_APP_PASSWORD); smtp.send_message(msg)
        except: print("   ⚠️ Email dispatch failed.")

    print(f"--- SENTRY FINISHED: {datetime.datetime.now().strftime('%H:%M:%S')} ---\n")

if __name__ == "__main__":
    run_sentry_check()
