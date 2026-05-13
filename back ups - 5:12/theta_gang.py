import os
import time
import requests
import datetime
import pandas as pd
import pytz
import sys
from dotenv import load_dotenv

# --- 1. INITIALIZATION & SHARED LOGIC ---
try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

# Aligning Webhook naming with your .env standards
TD_API_KEY = str(os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")).strip()
WEBHOOK_URL = os.getenv("WEBHOOK_THETA_GANG")
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")

def is_market_open():
    tz_et = pytz.timezone('US/Eastern')
    now_et = datetime.datetime.now(tz_et)
    if now_et.weekday() > 4: return False
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now_et <= market_close

def get_market_regime():
    try:
        if os.path.exists(HISTORY_FILE):
            df = pd.read_csv(HISTORY_FILE, on_bad_lines='skip')
            return df.iloc[-1]['Regime'].upper().strip()
    except:
        return "NEUTRAL"
    return "NEUTRAL"

def get_dynamic_movers():
    """Sync with trade_signals logic for dynamic discovery."""
    url = f"https://api.twelvedata.com/market_movers/stocks?direction=all&apikey={TD_API_KEY}"
    try:
        data = requests.get(url, timeout=10).json()
        return [item['symbol'] for item in data.get('values', []) if item['symbol'].isalpha()][:12]
    except:
        return []

def get_theta_intel(symbol, regime):
    """Refined Rockefeller Logic: Synchronized with trade_signals & income_ccetfs."""
    try:
        base = "https://api.twelvedata.com"
        q = requests.get(f"{base}/quote?symbol={symbol}&apikey={TD_API_KEY}").json()
        r = requests.get(f"{base}/rsi?symbol={symbol}&interval=15min&time_period=14&apikey={TD_API_KEY}").json()
        s = requests.get(f"{base}/stddev?symbol={symbol}&interval=1day&time_period=20&apikey={TD_API_KEY}").json()
        v = requests.get(f"{base}/vwap?symbol={symbol}&interval=15min&apikey={TD_API_KEY}").json()

        if any(x.get("status") == "error" for x in [q, r, s, v]): return None

        price = float(q['close'])
        rsi = float(r['values'][0]['rsi'])
        std = float(s['values'][0]['stddev'])
        vwap = float(v['values'][0]['vwap'])
        
        setup_found = False
        action = ""
        strike = 0
        color = 0x95a5a6 

        # BROADENED PARAMETERS (RSI < 42) for Scheduled Tasks
        # Ensures we catch high-quality setups that aren't 'extreme' oversold
        if regime in ["BULLISH", "RISK-ON", "NEUTRAL"]:
            # Logic: Oversold but reclaiming Institutional Floor (VWAP)
            if rsi < 42 and price > (vwap * 0.995): # Allows for 0.5% margin near VWAP
                setup_found = True
                action = "SELL PUT (CASH SECURED)"
                strike = price - (std * 1.5)
                color = 0x27ae60

        elif regime in ["BEARISH", "RISK-OFF"]:
            # Logic: Overbought and rejecting Institutional Ceiling
            if rsi > 58 and price < (vwap * 1.005):
                setup_found = True
                action = "SELL CALL (CREDIT SPREAD)"
                strike = price + (std * 1.5)
                color = 0xe74c3c

        if not setup_found:
            if "test" in sys.argv:
                print(f"      [DEBUG] {symbol} RSI: {rsi:.1f} | Price: {price} | VWAP: {vwap}")
            return None

        return {
            "symbol": symbol, "price": price, "strike": strike, "rsi": rsi,
            "action": action, "color": color, "name": q.get("name", symbol), "regime": regime
        }
    except:
        return None

def run_scheduled_theta_hunt():
    print(f"--- 🎡 THETA TACTICAL SCAN: {datetime.datetime.now().strftime('%H:%M')} ---")
    
    # 1. Market & Test Check
    is_test = "test" in sys.argv
    if not is_market_open() and not is_test:
        print("Market Closed. Exiting.")
        return

    regime = get_market_regime()
    print(f"   [BRAIN] Current Regime: {regime}")

    # Core Assets (MSTY/CONY/NVDY) + Real-time Movers
    watchlist = ["MSTY", "NVDY", "CONY", "TSLY", "IWM", "NVDA", "AAPL"] + get_dynamic_movers()
    watchlist = list(dict.fromkeys(watchlist))

    signals_sent = 0
    for symbol in watchlist:
        print(f"   [SCAN] {symbol}...", end="\r")
        intel = get_theta_intel(symbol, regime)
        
        if intel or (is_test and symbol == "MSTY"):
            # Force a signal for MSTY if testing to verify Discord
            if is_test and not intel:
                intel = {"symbol": "MSTY", "price": 25.88, "strike": 22.50, "rsi": 47.5, "action": "TEST BROADCAST", "color": 0x3498db, "name": "MicroStrategy Yield ETF", "regime": regime}

            signals_sent += 1
            msg = (
                f"### 🏛️ Elite Theta Setup: ${intel['symbol']}\n"
                f"**Posturing**: `{intel['action']}`\n\n"
                f"**Execution Parameters**:\n"
                f"└ Current Price: `${intel['price']:.2f}`\n"
                f"└ **1.5σ (Sigma) Strike**: **`${intel['strike']:.2f}`**\n"
                f"└ RSI (15m): `{intel['rsi']:.1f}`\n\n"
                f"**Institutional Confluence**:\n"
                f"└ Regime: `{intel['regime']}`\n"
                f"└ **VWAP Alignment**: `Strategic Floor Confirmed`\n\n"
                f"*Note: Calculated for high-probability decay. Data synced via Rockefeller Macro Engine.*"
            )
            
            if HAS_ESSENTIALS and WEBHOOK_URL:
                send_essentials_embed(WEBHOOK_URL, f"Theta Intel: {intel['name']}", msg, intel['color'])
                print(f"🎯 Dispatched {intel['symbol']} to Discord.                       ")
        
        time.sleep(0.4)

    print(f"\n--- 🎡 SCAN COMPLETE: {signals_sent} SIGNALS DISPATCHED ---")

if __name__ == "__main__":
    run_scheduled_theta_hunt()
