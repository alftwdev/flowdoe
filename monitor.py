import os
import requests
import json
import time
import datetime
import smtplib
from email.message import EmailMessage
from edgar import Company, set_identity
from dotenv import load_dotenv

# Shared Intelligence Tools
try:
    from essentials_tools import send_essentials_embed, get_institutional_conviction
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- 1. CONFIGURATION ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
PUSHOVER_USER = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_TOKEN = os.getenv("PUSHOVER_API_TOKEN")

# Anchor NAVs - Rockefeller standard for CLM/CRF
ANCHOR_NAV = {"CLM": 6.58, "CRF": 6.45} 

# --- 2. TACTICAL INTELLIGENCE ---

def get_posture_metrics(symbol):
    """Calculates macro metrics and Premium to NAV."""
    try:
        rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&outputsize=1&apikey={TD_API_KEY}"
        quote_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
        
        rsi_data = requests.get(rsi_url).json()
        quote_data = requests.get(quote_url).json()
        
        price = float(quote_data['close'])
        rsi = float(rsi_data['values'][0]['rsi'])
        
        nav = ANCHOR_NAV.get(symbol, price)
        premium = ((price - nav) / nav) * 100
        
        return price, rsi, premium
    except:
        return None, None, None

def check_sec_shield(tickers):
    """Shield: The N-2 SEC Early Warning System."""
    set_identity("Alwin Almazan alwin.almazan@gmail.com") 
    found_threats = []
    for ticker in tickers:
        try:
            company = Company(ticker)
            filings = company.get_filings(form=["N-2", "N-2/A", "424B3"]).latest(1)
            if filings:
                found_threats.append(f"🚨 {ticker} SEC ALERT: {filings.form} detected.")
        except: continue
    return found_threats

def dispatch_report(symbol, price, rsi, premium, threats, conviction):
    """Unified Posture Dispatcher (Replaces nav_tracker.py)."""
    
    # 1. Determine Posture & Strategy
    if threats:
        posture = "🚨 RED (EXIT / AVOID)"
        color = 0xe74c3c
        income_strategy = "⚠️ EXIT: Capital at risk. Do not DRIP."
        verdict = "SEC Dilution Risk detected. Strategic liquidation recommended."
    elif premium < 15 and 30 < rsi < 50:
        posture = "✅ GREEN (ACCUMULATE)"
        color = 0x2ecc71
        income_strategy = "💎 DRIP: High efficiency zone."
        verdict = "Premium reset & RSI stabilizing. Ideal for accumulation."
    elif rsi < 30:
        posture = "🔵 BLUE (A+ BUY / OVERSOLD)"
        color = 0x3498db
        income_strategy = "💰 BUY: Maximum capital efficiency."
        verdict = "Extreme panic detected. Optimal re-entry point."
    elif premium > 25:
        posture = "🛡️ NEUTRAL (HOLD)"
        color = 0x95a5a6
        income_strategy = "💵 CASH: Take dividend in cash (Premium too high)."
        verdict = "Market price is significantly above NAV. Hold position."
    else:
        posture = "🛡️ NEUTRAL (HOLD)"
        color = 0x95a5a6
        income_strategy = "💎 DRIP: Standard efficiency."
        verdict = "No active threats. Premium is in a normal range."

    # 2. Build Message
    msg = (
        f"### **Rockefeller Posture Report: {symbol}**\n"
        f"**Current Posture**: `{posture}`\n"
        f"┣ Price: `${price:.2f}`\n"
        f"┣ Premium to NAV: `{premium:.1f}%`\n"
        f"┣ RSI (1D): `{rsi:.1f}`\n"
        f"┣ Income Note: **{income_strategy}**\n"
        f"┗ Whale Flow: `{conviction}`\n\n"
        f"**Strategy Verdict**: {verdict}"
    )

    # 3. Dispatch
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        send_essentials_embed(WEBHOOK_CORNERSTONE, f"🛡️ Sentry Pulse: {symbol}", msg, color)
    
    # Priority alert for high-risk or high-opportunity events
    if PUSHOVER_TOKEN and PUSHOVER_USER and (threats or rsi < 30 or premium > 30):
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": PUSHOVER_TOKEN, "user": PUSHOVER_USER,
            "title": f"Sentry: {symbol} {posture}", "message": msg.replace("**", ""), "priority": 1
        })

# --- 3. MAIN SENTRY LOOP ---

def run_sentry_monitor():
    print("🏛️ RO Sentry: Unified Lifecycle Manager Online")
    watchlist = ["CLM", "CRF"]
    
    while True:
        sec_threats = check_sec_shield(watchlist)
        
        for ticker in watchlist:
            price, rsi, premium = get_posture_metrics(ticker)
            label, _, is_high_vol = get_institutional_conviction(ticker, TD_API_KEY)
            
            if price:
                ticker_threats = [t for t in sec_threats if ticker in t]
                dispatch_report(ticker, price, rsi, premium, ticker_threats, label)

        # 4-hour reporting cycle
        time.sleep(14400)

if __name__ == "__main__":
    run_sentry_monitor()
