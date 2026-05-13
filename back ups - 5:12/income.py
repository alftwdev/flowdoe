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
WEBHOOK_INCOME = os.getenv("WEBHOOK_DIVIDEND_CCETFS")
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")

PRIORITY_ASSETS = ["CLM", "CRF"]
INCOME_WATCHLIST = ["MSTY", "NVDY", "JEPI", "JEPQ", "SCHD"]

def get_ecosystem_regime():
    try:
        with open(REGIME_LEDGER, "r") as f:
            data = json.load(f)
            return data.get("regime", "NEUTRAL"), data.get("rsi_shield", "STABLE")
    except:
        return "NEUTRAL", "STABLE"

def get_detailed_intel(symbol):
    """Fetches full institutional snapshot using Venture Tier endpoints."""
    # 1. Fetch Quote (Price + 52-Week Range)
    quote_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    # 2. Fetch Last Dividend (Amount + Date)
    div_url = f"https://api.twelvedata.com/dividends?symbol={symbol}&apikey={TD_API_KEY}"
    
    try:
        q_res = requests.get(quote_url, timeout=10).json()
        d_res = requests.get(div_url, timeout=10).json()
        
        if q_res.get("status") == "error": return None

        # Extract Dividend Data
        last_div_amt = "N/A"
        last_div_date = "N/A"
        if d_res.get("status") != "error" and d_res.get("revisions"):
            last_div = d_res['revisions'][0]
            last_div_amt = f"${float(last_div['amount']):.4f}"
            last_div_date = last_div['payment_date']

        return {
            "symbol": symbol,
            "name": q_res.get("name", symbol),
            "price": float(q_res['close']),
            "change": float(q_res.get('percent_change', 0)),
            "high_52": q_res.get('fifty_two_week', {}).get('high', 'N/A'),
            "low_52": q_res.get('fifty_two_week', {}).get('low', 'N/A'),
            "div_amt": last_div_amt,
            "div_date": last_div_date
        }
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

def run_income_manager():
    tz_h = pytz.timezone('Pacific/Honolulu')
    now = datetime.now(tz_h)
    regime, rsi_shield = get_ecosystem_regime()
    
    print(f"--- 🏛️ ROCKEFELLER INCOME MANAGER: {now.strftime('%H:%M')} ---")
    
    # Process watchlist for the Hourly Pulse
    if now.minute == 0:
        for ticker in (PRIORITY_ASSETS + INCOME_WATCHLIST):
            intel = get_detailed_intel(ticker)
            if not intel: continue

            # Determine color based on performance
            color = 0x27ae60 if intel['change'] >= 0 else 0xe74c3c
            
            # Formulating the Intelligence Hub Message
            desc = (
                f"### **Institutional Intel: ${intel['symbol']}**\n"
                f"**Posturing**: `{regime}` | **RSI Shield**: `{rsi_shield}`\n\n"
                f"**Market Snapshot**:\n"
                f"┣ Price: **`${intel['price']:.2f}`** ({intel['change']:+.2f}%)\n"
                f"┗ 52W Range: `${intel['low_52']}` — `${intel['high_52']}`\n\n"
                f"**Dividend Intelligence**:\n"
                f"┣ Last Payment: **{intel['div_amt']}**\n"
                f"┗ Pay Date: `{intel['div_date']}`\n\n"
                f"*Data provided via Twelve Data Venture Tier.*"
            )

            if HAS_ESSENTIALS:
                title = f"🛡️ Sentry Pulse: {intel['symbol']}" if ticker in PRIORITY_ASSETS else f"💸 Income Hub: {intel['symbol']}"
                send_essentials_embed(WEBHOOK_INCOME, title, desc, color)
            
            # Rate limiting safety for Twelve Data
            time.sleep(2)

if __name__ == "__main__":
    run_income_manager()
