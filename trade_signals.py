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
SIGNAL_MEMORY = os.path.join(BASE_PATH, "trade_memory.json")

# Strategy Constants
WATCHLIST = ["NVDA", "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL", "AMD", "META", "XLC"]

# --- 2. ELITE UTILITIES ---

def is_market_open():
    """Checks NYSE market hours (ET) for active trading."""
    tz_et = pytz.timezone('US/Eastern')
    now_et = datetime.datetime.now(tz_et)
    if now_et.weekday() > 4: return False
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now_et <= market_close

def get_ecosystem_data():
    """Reads cross-script data from macro_radar.py."""
    try:
        if os.path.exists(REGIME_LEDGER):
            with open(REGIME_LEDGER, "r") as f:
                data = json.load(f)
                return data.get("regime", "NEUTRAL"), data.get("rsi_shield_limit", 66), data.get("vix_status", "STABLE")
    except:
        pass
    return "NEUTRAL", 66, "STABLE"

def has_been_alerted(symbol):
    """Checks if a signal for this symbol was already sent today."""
    today = datetime.date.today().isoformat()
    if os.path.exists(SIGNAL_MEMORY):
        with open(SIGNAL_MEMORY, "r") as f:
            memory = json.load(f)
            if memory.get(symbol) == today:
                return True
    return False

def mark_as_alerted(symbol):
    """Records the alert to prevent daily duplicates."""
    today = datetime.date.today().isoformat()
    memory = {}
    if os.path.exists(SIGNAL_MEMORY):
        with open(SIGNAL_MEMORY, "r") as f:
            memory = json.load(f)
    
    memory[symbol] = today
    with open(SIGNAL_MEMORY, "w") as f:
        json.dump(memory, f)

# --- 3. DATA & EXECUTION ---

def fetch_signals_data(symbol):
    try:
        url = f"https://api.twelvedata.com/complex_data?apikey={TD_API_KEY}"
        payload = {
            "symbols": [symbol],
            "intervals": ["15min"],
            "methods": ["quote", "rsi", "vwap"],
            "outputsize": 1
        }
        r = requests.post(url, json=payload, timeout=15).json()
        res = r['data'][0]
        
        return {
            "price": float(res['res']['quote']['close']),
            "rsi": float(res['res']['rsi']['values'][0]['rsi']),
            "vwap": float(res['res']['vwap']['values'][0]['vwap']),
            "name": res['res']['quote']['name']
        }
    except Exception as e:
        print(f"   [ERROR] Fetching {symbol}: {e}")
        return None

def run_trade_signals():
    print(f"--- 🏛️ A+ SIGNAL HUNTER START ---")
    
    while True:
        # 1. MARKET HOURS GATE
        if not is_market_open():
            print(f"💤 Market Closed. Entering Deep Sleep (1 Hour).")
            time.sleep(3600)
            continue

        # 2. CONTEXT RETRIEVAL
        regime, rsi_limit, vix_status = get_ecosystem_data()
        print(f"🔍 Scanning Watchlist | Regime: {regime} | RSI Shield: {rsi_limit}")

        for symbol in WATCHLIST:
            # 3. DUPLICATE GATE
            if has_been_alerted(symbol):
                continue
            
            intel = fetch_signals_data(symbol)
            if not intel: continue

            # 4. A+ CONVICTION LOGIC
            setup_found = False
            # Bullish Reclaim Logic
            if intel['rsi'] < rsi_limit and intel['price'] > intel['vwap']:
                if regime in ["BULLISH", "NEUTRAL"]:
                    setup_found = True
                    strike = round(intel['price'] * 0.90, 2)
                    execution = {
                        "action": "SELL PUT / BUY CALL",
                        "type": "Bullish Reclaim",
                        "strike": f"${strike} (Bottom Shield)",
                        "color": 0x2ecc71
                    }

            if setup_found:
                mark_as_alerted(symbol)
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

        # 5. CYCLE FREQUENCY
        print(f"✅ Scan Complete. Standing by for 5 minutes...")
        time.sleep(300) 

if __name__ == "__main__":
    run_trade_signals()
