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

# Environment Variables (Synced with trade_signals.py standards)
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
WORK_EMAIL = os.getenv("WORK_EMAIL")
WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
PUSHOVER_USER = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
TD_KEY_RAW = os.getenv("TWELVE_DATA_API_KEY")
TWELVE_DATA_KEY = str(TD_KEY_RAW).strip() if TD_KEY_RAW else None

# Tracking & Persistence
FILING_LOG = os.path.join(BASE_DIR, "sent_filings.txt")

# Tactical Thresholds (The Three Tactical Shields)
PRIORITY_ASSETS = {
    "CLM": {"avg_vol": 1700000, "cik": "0000706247"}, 
    "CRF": {"avg_vol": 600000, "cik": "0000309341"}
}
PREMIUM_LIMIT = 25.0 

def broadcast_alert(level, subject, body):
    """Multi-channel Tactical Dispatch: Discord, Pushover, Email."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    full_body = f"{body}\n\n*Time: {timestamp} HST*"
    
    # 1. Discord (Team ESSENTIALS Embed Style)
    if WEBHOOK_CORNERSTONE:
        payload = {"content": f"## 🚨 {level}: {subject}\n{full_body}"}
        requests.post(WEBHOOK_CORNERSTONE, json=payload, timeout=10)
    
    # 2. Pushover (Mobile Priority)
    if PUSHOVER_USER and PUSHOVER_TOKEN:
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": PUSHOVER_TOKEN, "user": PUSHOVER_USER, "message": full_body, "title": subject, "priority": 1 if level == "CRITICAL" else 0
        }, timeout=10)

    # 3. Email (Redundant Record)
    if all([SENDER_EMAIL, EMAIL_APP_PASSWORD, WORK_EMAIL]):
        msg = EmailMessage()
        msg.set_content(full_body)
        msg['Subject'] = f"[{level}] {subject}"
        msg['From'] = SENDER_EMAIL
        msg['To'] = WORK_EMAIL
        try:
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(SENDER_EMAIL, EMAIL_APP_PASSWORD)
                smtp.send_message(msg)
        except: pass

# --- SHIELD 1: THE N-2 SEC SHIELD ---
def check_sec_filings():
    """Scans SEC EDGAR for N-2, N-2/A, or 424B3 for priority CIKs."""
    try:
        from edgar import Company, get_filings
        for ticker, info in PRIORITY_ASSETS.items():
            filings = get_filings(form=["N-2", "N-2/A", "424B3"]).filter(cik=info['cik'])
            if not filings: continue
            
            latest = filings[0]
            acc_num = latest.accession_number
            
            # Check against persistence file
            if not os.path.exists(FILING_LOG): open(FILING_LOG, 'w').close()
            with open(FILING_LOG, 'r') as f:
                seen = f.read().splitlines()
            
            if acc_num not in seen:
                msg = (f"🚨 **SEC SELL SIGNAL: ${ticker}**\n"
                       f"New Filing: `{latest.form}` detected.\n"
                       f"Accession: `{acc_num}`\n"
                       f"Action: Immediate Posture Review - Potential Rights Offering.")
                broadcast_alert("CRITICAL", f"SEC SHIELD: {ticker}", msg)
                with open(FILING_LOG, 'a') as f: f.write(f"{acc_num}\n")
    except Exception as e:
        # Silently fail on SEC scrape to avoid notification fatigue
        pass

# --- SHIELD 2: THE WHALE DUMP MONITOR ---
def check_whale_activity(symbol, current_vol, current_price, prev_close):
    avg_vol = PRIORITY_ASSETS[symbol]['avg_vol']
    vol_ratio = (current_vol / avg_vol) * 100
    price_change = ((current_price - prev_close) / prev_close) * 100
    
    # Trigger: Volume > 140% and Price Drop > 3%
    if vol_ratio > 140 and price_change < -3:
        return (f"🚨 **VOLATILITY DUMP: ${symbol}**\n"
                f"Volume Spike: `{vol_ratio:.1f}%` of average.\n"
                f"Price Action: `{price_change:.1f}%` intraday dump.\n"
                f"Status: Institutional 'Whale' footprint detected.")
    return None

# --- SHIELD 3: THE PREMIUM THRESHOLD ---
def check_premium_anchor(symbol, price):
    # This logic assumes you will manually update a 'nav.json' or use a scraper
    # For now, it serves as the logic gate for the 25% High Premium alert
    return None

def run_monitor():
    tz = pytz.timezone('US/Aleutian')
    print(f"--- 🛡️ SENTRY ACTIVE: {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    while True:
        # 1. SHIELD 1: SEC SCAN (Highest Priority)
        check_sec_filings()
        
        # 2. MARKET DATA SCANS
        for ticker in PRIORITY_ASSETS:
            try:
                url = f"https://api.twelvedata.com/quote?symbol={ticker}&apikey={TWELVE_DATA_KEY}"
                r = requests.get(url, timeout=15)
                data = r.json()
                
                if "close" in data:
                    price = float(data['close'])
                    vol = float(data['volume'])
                    prev_close = float(data['previous_close'])
                    
                    # SHIELD 2: WHALE MONITOR
                    whale_msg = check_whale_activity(ticker, vol, price, prev_close)
                    if whale_msg:
                        broadcast_alert("CRITICAL", f"WHALE DUMP: {ticker}", whale_msg)
                        
                    # SHIELD 3: PREMIUM CHECK
                    # Placeholder for NAV-based premium calculation
                    
            except: pass 

        # Silent console heartbeat
        sys.stdout.write(".")
        sys.stdout.flush()
        
        # 15-minute resolution: Perfect for rights offerings and institutional shifts
        time.sleep(900) 

if __name__ == "__main__":
    run_monitor()
