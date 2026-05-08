import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

# --- 1. CONFIG & PATHING ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

TD_API_KEY = str(os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")).strip()
WEBHOOK_URL = os.getenv("WEBHOOK_THETA_GANG")
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")

def get_market_regime():
    """Reads the 'Brain' (macro_history.csv) to determine defensive vs aggressive posture."""
    try:
        if os.path.exists(HISTORY_FILE):
            df = pd.read_csv(HISTORY_FILE)
            return df.iloc[-1]['Regime'].upper().strip()
    except Exception as e:
        print(f"   [!] History Read Error: {e}")
    return "NEUTRAL"

def get_dynamic_movers():
    """Venture Tier: Scans for the top 10 most active stocks today."""
    try:
        url = f"https://api.twelvedata.com/market_movers/stocks?direction=all&apikey={TD_API_KEY}"
        data = requests.get(url, timeout=15).json()
        if data.get("status") == "error": return []
        return [item['symbol'] for item in data['values'][:10]]
    except:
        return []

def get_theta_intel(symbol, regime):
    """Calculates an A+ setup using RSI, StdDev, and Regime alignment."""
    try:
        # Fetch Quote, RSI, and Standard Deviation (Expected Move)
        quote_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
        rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}"
        std_url = f"https://api.twelvedata.com/stddev?symbol={symbol}&interval=1day&time_period=20&apikey={TD_API_KEY}"
        
        q = requests.get(quote_url).json()
        r = requests.get(rsi_url).json()
        s = requests.get(std_url).json()

        if "error" in [q.get("status"), r.get("status"), s.get("status")]: return None

        price = float(q['close'])
        rsi = float(r['values'][0]['rsi'])
        std = float(s['values'][0]['stddev'])
        
        # Calculate Strike based on 1 Standard Deviation (68% probability)
        # If Bullish/Neutral: Sell Puts below (Price - Std)
        # If Bearish: Sell Calls above (Price + Std)
        
        is_setup = False
        action = ""
        strike = 0
        color = 0x2ecc71 # Green

        if regime in ["BULLISH", "RISK-ON", "NEUTRAL"]:
            if rsi < 45: # Oversold / Value area
                is_setup = True
                action = "SELL PUT (CASH SECURED)"
                strike = price - (std * 1.5) # Aggressive safety margin
                color = 0x27ae60
        else: # BEARISH / RISK-OFF
            if rsi > 55: # Overbought / Resistance
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
            "name": q.get("name", symbol)
        }
    except:
        return None

def run_theta_gang():
    print(f"--- 🎡 THETA GANG: DYNAMIC HUNTER START ---")
    regime = get_market_regime()
    print(f"   [BRAIN] Current Regime: {regime}")

    # Combine Priority Assets + Dynamic Movers
    watchlist = ["MSTY", "NVDY", "TSLY", "IWM"] + get_dynamic_movers()
    # Remove duplicates
    watchlist = list(dict.fromkeys(watchlist))

    signals_found = 0
    for symbol in watchlist:
        print(f"   [SCAN] Analyzing {symbol}...")
        intel = get_theta_intel(symbol, regime)
        
        if intel:
            signals_found += 1
            msg = (
                f"### 🏛️ Elite Theta Setup: ${intel['symbol']}\n"
                f"**Posturing**: `{intel['action']}`\n\n"
                f"**Tactical Intel**:\n"
                f"└ Current Price: `${intel['price']:.2f}`\n"
                f"└ **Expected Move Strike**: `${intel['strike']:.2f}`\n"
                f"└ Relative Strength (RSI): `{intel['rsi']:.1f}`\n\n"
                f"**Rockefeller Mandate**: *Strike calculated at 1.5σ (Sigma) deviation. High probability of expiry OTM based on {regime} regime.*"
            )
            send_essentials_embed(WEBHOOK_URL, f"Theta Intel: {intel['name']}", msg, intel['color'])
            time.sleep(2)

    print(f"--- 🎡 THETA GANG: {signals_found} SIGNALS DISPATCHED ---")

if __name__ == "__main__":
    run_theta_gang()
