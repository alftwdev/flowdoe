import os
import time
import requests
import datetime
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
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")

def is_market_open():
    """Checks if the NYSE is currently open (9:30 AM - 4:00 PM ET)."""
    tz_et = pytz.timezone('US/Eastern')
    now_et = datetime.datetime.now(tz_et)
    if now_et.weekday() > 4: return False # Weekend
    market_open = now_et.replace(hour=9, minute=30, second=0)
    market_close = now_et.replace(hour=16, minute=0, second=0)
    return market_open <= now_et <= market_close

def get_market_regime():
    try:
        if os.path.exists(HISTORY_FILE):
            df = pd.read_csv(HISTORY_FILE, on_bad_lines='skip')
            return df.iloc[-1]['Regime'].upper().strip()
    except:
        return "NEUTRAL"
    return "NEUTRAL"

def get_dynamic_hunters():
    """Venture Tier: Scans for real-time momentum movers."""
    url = f"https://api.twelvedata.com/market_movers/stocks?direction=all&apikey={TD_API_KEY}"
    core_watchlist = ["NVDA", "TSLA", "TQQQ", "AMD", "AAPL", "MSFT", "IWM"]
    try:
        resp = requests.get(url, timeout=10).json()
        discovered = [item['symbol'] for item in resp.get('values', []) if item['symbol'].isalpha()]
        return list(dict.fromkeys(core_watchlist + discovered))[:15]
    except:
        return core_watchlist

def get_advanced_intel(symbol):
    """Real-time data fetch for A+ Setup Verification."""
    try:
        # Standard Venture Tier real-time endpoints
        base = "https://api.twelvedata.com"
        q = requests.get(f"{base}/quote?symbol={symbol}&apikey={TD_API_KEY}").json()
        r = requests.get(f"{base}/rsi?symbol={symbol}&interval=15min&time_period=14&apikey={TD_API_KEY}").json()
        v = requests.get(f"{base}/vwap?symbol={symbol}&interval=15min&apikey={TD_API_KEY}").json()
        a = requests.get(f"{base}/atr?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}").json()

        return {
            "price": float(q['close']),
            "rsi": float(r['values'][0]['rsi']),
            "vwap": float(v['values'][0]['vwap']),
            "atr": float(a['values'][0]['atr']),
            "change": float(q['percent_change']),
            "name": q.get("name", symbol)
        }
    except:
        return None

def execute_hunter_loop():
    print(f"--- 🏛️ SENTRY ACTIVE: CONTINUOUS MARKET MONITORING ---")
    
    # Track symbols we already alerted on to avoid 'spamming' the same move
    alerted_today = []

    while True:
        # 1. Market Hours Check
        if not is_market_open() and "test" not in sys.argv:
            print("休 Market Closed. Sleeping for 15 minutes...")
            time.sleep(900)
            alerted_today = [] # Reset alerts for next day
            continue

        regime = get_market_regime()
        tickers = get_dynamic_hunters()
        
        for symbol in tickers:
            if symbol in alerted_today and "test" not in sys.argv:
                continue

            intel = get_advanced_intel(symbol)
            if not intel: continue

            price, rsi, vwap, atr = intel['price'], intel['rsi'], intel['vwap'], intel['atr']
            setup_found = False
            execution = {}

            # 🟢 THE A+ BULLISH SETUP (The "Stars Aligned" Long)
            if regime in ["BULLISH", "RISK-ON", "NEUTRAL"] and rsi < 32 and price > vwap:
                setup_found = True
                strike = round(price - (atr * 1.5), 0)
                execution = {
                    "action": "BUY CALL / SELL PUT",
                    "type": "Bullish Confluence",
                    "strike": f"${strike}",
                    "color": 0x2ecc71
                }

            # 🔴 THE A+ BEARISH SETUP (The "Stars Aligned" Short)
            elif regime in ["BEARISH", "RISK-OFF"] and rsi > 68 and price < vwap:
                setup_found = True
                strike = round(price + (atr * 1.5), 0)
                execution = {
                    "action": "BUY PUT / SELL CALL",
                    "type": "Bearish Rejection",
                    "strike": f"${strike}",
                    "color": 0xe74c3c
                }

            if setup_found:
                alerted_today.append(symbol)
                msg = (
                    f"### 🏛️ A+ CONVICTION SIGNAL: ${symbol}\n"
                    f"**Execution**: `{execution['action']}`\n"
                    f"**Setup**: `{execution['type']}`\n\n"
                    f"**Execution Parameters**:\n"
                    f"└ Target Strike: **{execution['strike']}**\n"
                    f"└ Recommended DTE: **7-14 Days**\n"
                    f"└ **Market Regime**: `{regime}`\n\n"
                    f"**Institutional Note**: *Stars have aligned. Price has reclaimed VWAP while in a deep value RSI zone. "
                    f"Standard Deviation (1.5σ) strike provides optimal safety.* \n\n"
                    f"⚠️ *Personal use only. Not financial advice.*"
                )
                if HAS_ESSENTIALS:
                    send_essentials_embed(WEBHOOK_URL, f"A+ Setup: {intel['name']}", msg, execution['color'])
                    print(f"🎯 Broadcast Sent: {symbol}")

        # Scan every 5 minutes during market hours
        print(f"   [SCAN COMPLETE] {datetime.datetime.now().strftime('%H:%M:%S')} - No new A+ setups found. Monitoring...")
        time.sleep(300)

if __name__ == "__main__":
    execute_hunter_loop()
