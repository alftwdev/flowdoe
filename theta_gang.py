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

# --- 1. CONFIG & PATHING ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

TD_API_KEY = str(os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")).strip()
WEBHOOK_URL = os.getenv("WEBHOOK_THETA_GANG")
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")

def is_market_open():
    """Checks if NYSE is open (9:30 AM - 4:00 PM ET)."""
    tz_et = pytz.timezone('US/Eastern')
    now_et = datetime.datetime.now(tz_et)
    if now_et.weekday() > 4: return False
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now_et <= market_close

def get_market_regime():
    """Reads the 'Brain' for direction alignment."""
    try:
        if os.path.exists(HISTORY_FILE):
            df = pd.read_csv(HISTORY_FILE, on_bad_lines='skip')
            return df.iloc[-1]['Regime'].upper().strip()
    except:
        return "NEUTRAL"
    return "NEUTRAL"

def get_dynamic_movers():
    """Venture Tier: Scans for momentum and volume active tickers."""
    try:
        url = f"https://api.twelvedata.com/market_movers/stocks?direction=all&apikey={TD_API_KEY}"
        data = requests.get(url, timeout=15).json()
        return [item['symbol'] for item in data.get('values', [])[:12]]
    except:
        return []

def get_theta_intel(symbol, regime):
    """The 'Stars Aligned' Logic: RSI + StdDev + VWAP + Regime."""
    try:
        # Fetching the 4 Pillars of Data
        base = "https://api.twelvedata.com"
        q = requests.get(f"{base}/quote?symbol={symbol}&apikey={TD_API_KEY}").json()
        r = requests.get(f"{base}/rsi?symbol={symbol}&interval=15min&time_period=14&apikey={TD_API_KEY}").json()
        s = requests.get(f"{base}/stddev?symbol={symbol}&interval=1day&time_period=20&apikey={TD_API_KEY}").json()
        v = requests.get(f"{base}/vwap?symbol={symbol}&interval=15min&apikey={TD_API_KEY}").json()

        if "error" in [q.get("status"), r.get("status"), s.get("status"), v.get("status")]: return None

        price = float(q['close'])
        rsi = float(r['values'][0]['rsi'])
        std = float(s['values'][0]['stddev'])
        vwap = float(v['values'][0]['vwap'])
        
        is_setup = False
        action = ""
        strike = 0
        color = 0x95a5a6 # Default Gray

        # 🟢 A+ BULLISH THETA: Market Bullish + Oversold + Price > VWAP (Reclaim)
        if regime in ["BULLISH", "RISK-ON", "NEUTRAL"]:
            if rsi < 35 and price > vwap:
                is_setup = True
                action = "SELL PUT (CASH SECURED)"
                # 1.5 Sigma safety margin for Rockefeller protection
                strike = price - (std * 1.5)
                color = 0x27ae60

        # 🔴 A+ BEARISH THETA: Market Bearish + Overbought + Price < VWAP (Rejection)
        elif regime in ["BEARISH", "RISK-OFF"]:
            if rsi > 65 and price < vwap:
                is_setup = True
                action = "SELL CALL (CREDIT SPREAD)"
                strike = price + (std * 1.5)
                color = 0xe74c3c

        if not is_setup: return None

        return {
            "symbol": symbol,
            "price": price,
            "strike": strike,
            "rsi": rsi,
            "action": action,
            "color": color,
            "name": q.get("name", symbol),
            "regime": regime
        }
    except:
        return None

def execute_sentry_loop():
    print(f"--- 🎡 THETA SENTINEL ACTIVE: CONTINUOUS SCANNING ---")
    alerted_today = []

    while True:
        # 1. Market Hours Control
        if not is_market_open() and "test" not in sys.argv:
            print(f"[{datetime.datetime.now().strftime('%H:%M')}] Market Closed. Sleeping...")
            time.sleep(900) # Check every 15 mins
            alerted_today = [] # Reset alerts for the new day
            continue

        regime = get_market_regime()
        watchlist = ["MSTY", "NVDY", "CONY", "TSLY", "IWM"] + get_dynamic_movers()
        watchlist = list(dict.fromkeys(watchlist))

        for symbol in watchlist:
            if symbol in alerted_today and "test" not in sys.argv:
                continue

            print(f"   [SCAN] {symbol}...", end="\r")
            intel = get_theta_intel(symbol, regime)
            
            if intel:
                alerted_today.append(symbol)
                msg = (
                    f"### 🏛️ Elite Theta Setup: ${intel['symbol']}\n"
                    f"**Posturing**: `{intel['action']}`\n\n"
                    f"**Execution Parameters**:\n"
                    f"└ Current Price: `${intel['price']:.2f}`\n"
                    f"└ **1.5σ (Sigma) Strike**: **`${intel['strike']:.2f}`**\n"
                    f"└ RSI (15m): `{intel['rsi']:.1f}`\n\n"
                    f"**Institutional Confluence**:\n"
                    f"└ Regime: `{intel['regime']}`\n"
                    f"└ **VWAP Alignment**: `Institutional Floor Confirmed`\n\n"
                    f"*Note: Strike calculated for high-probability decay. Personal use only.*"
                )
                
                if HAS_ESSENTIALS:
                    send_essentials_embed(WEBHOOK_URL, f"Theta Intel: {intel['name']}", msg, intel['color'])
                    print(f"🎯 Dispatched A+ Setup for {symbol}")

        # Scan every 10 minutes to allow for "Theta" setups to develop
        print(f"   [PULSE] Scan complete. Next hunt in 10 minutes...          ")
        time.sleep(600)

if __name__ == "__main__":
    execute_sentry_loop()
