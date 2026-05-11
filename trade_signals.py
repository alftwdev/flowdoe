import os
import time
import requests
import datetime
import json
import pandas as pd
import pytz
import sys
from dotenv import load_dotenv

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- 1. CONFIGURATION ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

TD_API_KEY = str(os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")).strip()
WEBHOOK_URL = os.getenv("WEBHOOK_TRADE_SIGNALS")
REGIME_LEDGER = os.path.join(BASE_PATH, "market_regime.json")

# Strategy Constants
WATCHLIST = ["NVDA", "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL", "AMD", "META", "XLC"]

def get_ecosystem_data():
    """Reads cross-script data to gain top-tier conviction."""
    try:
        with open(REGIME_LEDGER, "r") as f:
            data = json.load(f)
            # Default to conservative 50 if ledger is missing/corrupt during high vol
            return data.get("regime", "NEUTRAL"), data.get("rsi_shield_limit", 66), data.get("vix_status", "STABLE")
    except:
        return "NEUTRAL", 66, "STABLE"

def is_market_open():
    tz_et = pytz.timezone('US/Eastern')
    now_et = datetime.datetime.now(tz_et)
    if now_et.weekday() > 4: return False
    market_open = now_et.replace(hour=9, minute=30, second=0)
    market_close = now_et.replace(hour=16, minute=0, second=0)
    return market_open <= now_et <= market_close

def fetch_signals_data(symbol):
    """Venture Tier: Pulls Price, RSI, and VWAP for A+ setup detection."""
    try:
        # Combined request for technicals
        url = f"https://api.twelvedata.com/complex_data?apikey={TD_API_KEY}"
        payload = {
            "symbols": [symbol],
            "intervals": ["15min"],
            "methods": ["quote", "rsi", "vwap"],
            "outputsize": 1
        }
        r = requests.post(url, json=payload).json()
        
        res = r['data'][0]
        price = float(res['res']['quote']['close'])
        rsi = float(res['res']['rsi']['values'][0]['rsi'])
        vwap = float(res['res']['vwap']['values'][0]['vwap'])
        name = res['res']['quote']['name']
        
        return {"price": price, "rsi": rsi, "vwap": vwap, "name": name}
    except:
        return None

def run_trade_signals():
    print(f"--- 🏛️ A+ SIGNAL HUNTER START: {datetime.datetime.now().strftime('%H:%M')} ---")
    alerted_today = []
    
    while True:
        if not is_market_open():
            print("   [INFO] Market Closed. Sleeping...")
            time.sleep(600)
            continue

        # CROSS-SCRIPT COMMUNICATION
        regime, rsi_limit, vix_status = get_ecosystem_data()
        
        for symbol in WATCHLIST:
            if symbol in alerted_today: continue
            
            intel = fetch_signals_data(symbol)
            if not intel: continue

            # A+ CONVICTION LOGIC
            setup_found = False
            # Bullish Reclaim: Price > VWAP AND RSI < Limit (Dynamic Shield)
            if intel['rsi'] < rsi_limit and intel['price'] > intel['vwap']:
                if regime in ["BULLISH", "NEUTRAL"]:
                    setup_found = True
                    strike = round(intel['price'] * 0.90, 2) # 1.5-2 Sigma estimate
                    execution = {
                        "action": "SELL PUT / BUY CALL",
                        "type": "Bullish Reclaim",
                        "strike": f"${strike} (Bottom Shield)",
                        "color": 0x2ecc71
                    }

            if setup_found:
                alerted_today.append(symbol)
                msg = (
                    f"### 🏛️ A+ CONVICTION SIGNAL: ${symbol}\n"
                    f"**Execution**: `{execution['action']}`\n"
                    f"**Setup**: `{execution['type']}`\n\n"
                    f"**Execution Parameters**:\n"
                    f"└ Target Strike: **{execution['strike']}**\n"
                    f"└ **Market Regime**: `{regime}`\n"
                    f"└ **RSI Shield**: `{rsi_limit}` ({vix_status})\n\n"
                    f"**Institutional Note**: *Stars have aligned. Price has reclaimed VWAP while in the {vix_status} safety zone.*"
                )
                if HAS_ESSENTIALS:
                    send_essentials_embed(WEBHOOK_URL, f"A+ Setup: {intel['name']}", msg, execution['color'])
                    print(f"🎯 Broadcast Sent: {symbol}")

        time.sleep(300) # 5-minute cycle

if __name__ == "__main__":
    run_trade_signals()
