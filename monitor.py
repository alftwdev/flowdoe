import os
import requests
import time
import sys
import traceback
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
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
WORK_EMAIL = os.getenv("WORK_EMAIL")
WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
PUSHOVER_USER = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
TD_KEY_RAW = os.getenv("TWELVE_DATA_API_KEY")
TWELVE_DATA_KEY = str(TD_KEY_RAW).strip() if TD_KEY_RAW else None

# Persistence
FILING_LOG = os.path.join(BASE_DIR, "sent_filings.txt")
PULSE_FILE = os.path.join(BASE_DIR, "last_pulse.txt")

PRIORITY_ASSETS = {
    "CLM": {"nav_ticker": "XCLMX"},
    "CRF": {"nav_ticker": "XCRFX"}
}

# --- 2. NOTIFICATION CHANNELS ---

def broadcast_alert(level, title, message):
    """Surgical Dispatch: Discord, Pushover, and Email."""
    print(f"    [BROADCAST] Sending {level} alert: {title}...")
    
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        color = 0xe74c3c if "RED" in message else 0x2ecc71 if "GREEN" in message else 0x3498db
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, message, color)

    if PUSHOVER_USER and PUSHOVER_TOKEN:
        try:
            requests.post("https://api.pushover.net/1/messages.json", data={
                "token": PUSHOVER_TOKEN, "user": PUSHOVER_USER,
                "title": title, "message": message,
                "priority": 1 if "RED" in message else 0
            }, timeout=10)
        except Exception as e: print(f"    [!] Pushover Fail: {e}")

    if "RED" in message and SENDER_EMAIL and WORK_EMAIL:
        try:
            msg = EmailMessage()
            msg.set_content(message)
            msg['Subject'] = f"SENTRY ALERT: {title}"
            msg['From'] = SENDER_EMAIL
            msg['To'] = WORK_EMAIL
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(SENDER_EMAIL, EMAIL_APP_PASSWORD)
                smtp.send_message(msg)
        except Exception as e: print(f"    [!] Email Fail: {e}")

# --- 3. THE INTEL ENGINE ---

def get_posture_report(ticker):
    """Fetches full technical posture for the Pulse report."""
    print(f"    [SENTRY] Analyzing {ticker} posture...")
    try:
        # Techs
        q_url = f"https://api.twelvedata.com/quote?symbol={ticker}&apikey={TWELVE_DATA_KEY}"
        r_url = f"https://api.twelvedata.com/rsi?symbol={ticker}&interval=1day&outputsize=1&apikey={TWELVE_DATA_KEY}"
        
        q_res = requests.get(q_url).json()
        r_res = requests.get(r_url).json()
        
        price = float(q_res.get('close', 0))
        rsi = float(r_res['values'][0]['rsi']) if 'values' in r_res else 0.0
        
        # NAV Math (Using Twelve Data Mutual Fund proxy)
        nav_ticker = PRIORITY_ASSETS[ticker]["nav_ticker"]
        n_res = requests.get(f"https://api.twelvedata.com/price?symbol={nav_ticker}&apikey={TWELVE_DATA_KEY}").json()
        nav_price = float(n_res.get('price', price * 0.82)) # Fallback logic
        
        premium = ((price - nav_price) / nav_price) * 100
        whale_label, _, _ = get_institutional_conviction(ticker, TWELVE_DATA_KEY)
        
        # Posture Logic
        if premium > 18.0 or rsi > 68:
            posture, note, verdict = "🚨 RED (EXIT / AVOID)", "EXIT: Capital at risk.", "SEC Dilution Risk detected."
        else:
            posture, note, verdict = "✅ GREEN (STABLE)", "HOLD/BUY: Nominal.", "No dilution risk detected."

        return (
            f"Rockefeller Posture Report: {ticker}\n"
            f"Current Posture: {posture}\n"
            f"┣ Price: ${price:.2f}\n"
            f"┣ Premium to NAV: {premium:.1f}%\n"
            f"┣ RSI (1D): {rsi:.1f}\n"
            f"┣ Income Note: {note}\n"
            f"┗ Whale Flow: {whale_label}\n\n"
            f"**Strategy Verdict**: {verdict}\n"
            "Team ESSENTIALS | Rockefeller Strategic Intelligence"
        )
    except Exception as e:
        return f"Error analyzing {ticker}: {e}"

def send_daily_pulse(force=False):
    """Triggers the detailed posture report."""
    print(f"\n[HEARTBEAT] Initiating Pulse {'(FORCED)' if force else '(SCHEDULED)'}...")
    for ticker in PRIORITY_ASSETS:
        report = get_posture_report(ticker)
        broadcast_alert("NOMINAL", f"Sentry Pulse: {ticker}", report)
    
    with open(PULSE_FILE, "w") as f:
        f.write(str(time.time()))
    print("[HEARTBEAT] Pulse Sequence Complete.\n")

# --- 4. EXECUTION ---

def run_monitor():
    tz = pytz.timezone('Pacific/Honolulu')
    print(f"--- 🛡️ SENTRY ACTIVE: {datetime.now(tz).strftime('%Y-%m-%d %H:%M HST')} ---")
    
    # IMMEDIATE TEST CHECK
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        send_daily_pulse(force=True)
        print("--- [TEST COMPLETE] Exiting... ---")
        return

    while True:
        try:
            # Heartbeat logic (24h)
            if not os.path.exists(PULSE_FILE) or (time.time() - os.path.getmtime(PULSE_FILE)) > 86400:
                send_daily_pulse()

            # Continuous Checks
            # check_sec_filings()
            # check_whales()
            
            sys.stdout.write(".")
            sys.stdout.flush()
            time.sleep(900)
        except Exception as e:
            print(f"\n[ERROR] {traceback.format_exc()}")
            time.sleep(60)

if __name__ == "__main__":
    run_monitor()
