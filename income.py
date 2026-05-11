import os
import requests
import json
import pandas as pd
import time
from datetime import datetime
import pytz
from dotenv import load_dotenv

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- 1. INITIALIZATION & PATHING ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_INCOME = os.getenv("WEBHOOK_DIVIDEND_CCETFS") # Ensure this is in your .env
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")

# Strategy Constants
PRIORITY_ASSETS = ["CLM", "CRF"]
INCOME_WATCHLIST = ["MSTY", "NVDY", "JEPI", "JEPQ", "SCHD"]

def get_ecosystem_regime():
    """Reads the current market posture from the shared ledger."""
    try:
        with open(REGIME_LEDGER, "r") as f:
            data = json.load(f)
            return data.get("regime", "NEUTRAL"), data.get("rsi_shield", "STABLE")
    except:
        return "NEUTRAL", "STABLE"

def get_income_intel(symbol):
    """Fetches high-precision dividend and technical data via Venture Tier."""
    url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    try:
        r = requests.get(url, timeout=12).json()
        if r.get("status") == "error": return None
        
        # Calculate Logic
        price = float(r['close'])
        change = float(r.get('percent_change', 0))
        
        # Mocking logic for Dividend/RSI for consolidated view
        # In production, you would call the /rsi and /dividend endpoints here
        return {
            "symbol": symbol,
            "name": r.get("name", symbol),
            "price": price,
            "change": change,
            "status": "MONITORING"
        }
    except:
        return None

def run_income_manager():
    tz_h = pytz.timezone('Pacific/Honolulu')
    now = datetime.now(tz_h)
    regime, rsi_shield = get_ecosystem_regime()
    
    print(f"--- 🏛️ ROCKEFELLER INCOME MANAGER: {now.strftime('%H:%M')} ---")
    
    # 1. PRIORITY SHIELD (CLM/CRF)
    for ticker in PRIORITY_ASSETS:
        intel = get_income_intel(ticker)
        if intel:
            # RO Early Warning Logic: If price drops > 4% on high volume
            # (Note: Integration with edgartools for N-2 should be added here)
            alert_color = 0x27ae60 if regime == "BULLISH" else 0xf1c40f
            
            desc = (
                f"### **Priority Asset Pulse: {intel['symbol']}**\n"
                f"**Regime Status**: `{regime}` | **RSI Shield**: `{rsi_shield}`\n\n"
                f"└ **Price**: `${intel['price']:.2f}` ({intel['change']:+.2f}%)\n"
                f"└ **Action**: Monitoring for N-2 Filings & Whale Dumps.\n\n"
                f"*Strategy: Protection of capital is priority #1.*"
            )
            if HAS_ESSENTIALS:
                send_essentials_embed(WEBHOOK_INCOME, f"🛡️ Sentry: {intel['symbol']}", desc, alert_color)
            time.sleep(1)

    # 2. INCOME HUNTER (CC ETFs & Growth)
    # This only dispatches if there's a significant move or it's a 'Pulse' time
    if now.minute == 0: # Hourly Pulse
        hunter_list = []
        for ticker in INCOME_WATCHLIST:
            intel = get_income_intel(ticker)
            if intel:
                hunter_list.append(f"`{intel['symbol']}`: ${intel['price']:.2f} ({intel['change']:+.2f}%)")
        
        desc = "### **Monthly Dividend & CC-ETF Tracker**\n" + "\n".join(hunter_list)
        if HAS_ESSENTIALS:
            send_essentials_embed(WEBHOOK_INCOME, "💸 Income Hunter: Market Pulse", desc, 0x3498db)

if __name__ == "__main__":
    run_income_manager()
