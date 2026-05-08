import os
import time
import requests
import datetime
import pandas as pd
import pytz
from dotenv import load_dotenv

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- 1. INITIALIZATION ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_URL = os.getenv("WEBHOOK_TRADE_SIGNALS")
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")

def get_market_regime():
    """Reads the latest regime from macro_history.csv for regime-aware trading."""
    try:
        df = pd.read_csv(HISTORY_FILE)
        if not df.empty:
            return df.iloc[-1]['Regime'].upper()
    except:
        return "NEUTRAL"
    return "NEUTRAL"

def get_dynamic_movers():
    """Venture Tier: Scans for high-volume momentum tickers."""
    url = f"https://api.twelvedata.com/market_movers/stocks?direction=all&apikey={TD_API_KEY}"
    try:
        resp = requests.get(url, timeout=10).json()
        discovered = [item['symbol'] for item in resp.get('values', [])]
        core = ["NVDA", "TSLA", "AAPL", "AMD", "TQQQ", "MSFT"]
        return list(set(discovered + core))[:12]
    except:
        return ["NVDA", "TSLA", "AAPL", "AMD", "TQQQ", "MSFT"]

def get_trade_setup(symbol):
    endpoints = {
        "quote": f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}",
        "rsi": f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=15min&time_period=14&apikey={TD_API_KEY}",
        "vwap": f"https://api.twelvedata.com/vwap?symbol={symbol}&interval=15min&apikey={TD_API_KEY}"
    }
    results = {}
    try:
        for key, url in endpoints.items():
            data = requests.get(url).json()
            if key == "quote":
                results['price'] = float(data['close'])
            elif key == "rsi":
                results['rsi'] = float(data['values'][0]['rsi'])
            elif key == "vwap":
                results['vwap'] = float(data['values'][0]['vwap'])
        return results
    except:
        return None

def run_trade_hunter():
    tz_honolulu = pytz.timezone('Pacific/Honolulu')
    now = datetime.datetime.now(tz_honolulu)
    regime = get_market_regime()
    tickers = get_dynamic_movers()
    
    for symbol in tickers:
        setup = get_trade_setup(symbol)
        if not setup: continue

        price, rsi, vwap = setup['price'], setup['rsi'], setup['vwap']
        
        # 🟢 BULLISH CONFLUENCE (Market Bullish + Oversold + Price > VWAP)
        if regime == "BULLISH" and rsi < 38 and price > vwap:
            msg = (f"**A+ BULLISH ENTRY**\nTicker: **${symbol}**\n"
                   f"└ Price: `${price:.2f}` | RSI: `{rsi:.1f}`\n"
                   f"└ Regime: `BULLISH` | Logic: `VWAP Reclaim`")
            send_essentials_embed(WEBHOOK_URL, "🏛️ Essentials: High Conviction Buy", msg, 0x2ecc71)

        # 🔴 BEARISH CONFLUENCE (Market Bearish + Overbought + Price < VWAP)
        elif regime == "BEARISH" and rsi > 62 and price < vwap:
            msg = (f"**A+ BEARISH ENTRY**\nTicker: **${symbol}**\n"
                   f"└ Price: `${price:.2f}` | RSI: `{rsi:.1f}`\n"
                   f"└ Regime: `BEARISH` | Logic: `VWAP Rejection`")
            send_essentials_embed(WEBHOOK_URL, "🏛️ Essentials: High Conviction Short", msg, 0xe74c3c)

        time.sleep(1)

if __name__ == "__main__":
    run_trade_hunter()
