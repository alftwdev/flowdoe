import os
import time
import requests
import datetime
import json
import pandas as pd
import pytz
import sys
from PIL import Image, ImageDraw, ImageFont
from dotenv import load_dotenv

try:
    from essentials_tools import send_essentials_embed, get_institutional_conviction, get_trend_alignment
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

# --- 2. THE INTEL ENGINE ---

def fetch_unified_data(symbol):
    """One call to fetch price, technicals, and statistical volatility."""
    try:
        url = f"https://api.twelvedata.com/complex_data?apikey={TD_API_KEY}"
        payload = {
            "symbols": [symbol],
            "intervals": ["15min"],
            "methods": ["quote", "rsi", "vwap", "statistics"],
            "outputsize": 1
        }
        res = requests.post(url, json=payload).json()
        data = res['data'][0]
        
        quote = data['quote']
        stats = data['statistics']
        
        return {
            "symbol": symbol,
            "price": float(quote['close']),
            "rsi": float(data['rsi']['values'][0]['rsi']),
            "vwap": float(data['vwap']['values'][0]['vwap']),
            "change": float(quote['percent_change']),
            "std_dev": float(stats['statistics']['price_range']['standard_deviation']),
            "trend": get_trend_alignment(symbol),
            "conviction": get_institutional_conviction(symbol)
        }
    except:
        return None

def generate_signal_card(symbol, intel, setup, regime):
    """Generates the branded Rockefeller card with dynamic data."""
    img = Image.new('RGB', (800, 500), color=(10, 10, 12))
    d = ImageDraw.Draw(img)
    
    # Header
    d.rectangle([0, 0, 800, 80], fill=(20, 20, 25))
    d.text((30, 25), f"ROCKEFELLER SENTRY: ${symbol}", fill=(212, 175, 55))
    
    # Context
    d.text((30, 100), f"REGIME: {regime}", fill=(200, 200, 200))
    d.text((30, 140), f"CONVICTION: {intel['conviction']}", fill=(255, 255, 255))
    d.text((30, 180), f"TREND: {intel['trend']}", fill=(255, 255, 255))

    # Execution Box
    d.rectangle([400, 100, 770, 260], outline=(60, 60, 70), width=2)
    d.text((420, 120), "EXECUTION STRATEGY", fill=(212, 175, 55))
    d.text((420, 160), f"TYPE: {setup['type']}", fill=(255, 255, 255))
    d.text((420, 200), f"ACTION: {setup['action']}", fill=(255, 255, 255))
    d.text((420, 230), f"STRIKE: {setup['strike']}", fill=(46, 204, 113))

    # Stats
    d.text((30, 320), f"PRICE: ${intel['price']:.2f}", fill=(255, 255, 255))
    d.text((30, 360), f"RSI: {intel['rsi']:.1f}", fill=(255, 255, 255))
    d.text((30, 400), f"VWAP: ${intel['vwap']:.2f}", fill=(255, 255, 255))
    
    # Footer
    d.text((30, 460), "PROPRIETARY SIGNAL | DISPATCHED VIA SENTRY ENGINE", fill=(70, 70, 80))
    
    path = os.path.join(BASE_PATH, f"card_{symbol}.png")
    img.save(path)
    return path

# --- 3. THE ANALYST (Logic Hub) ---

def run_trade_signals():
    print(f"📡 Sentry Engine Active: {datetime.datetime.now()}")
    
    try:
        with open(REGIME_LEDGER, "r") as f:
            mkt = json.load(f)
            regime = mkt.get("regime", "NEUTRAL")
            rsi_limit = mkt.get("rsi_shield_limit", 66)
    except:
        regime, rsi_limit = "NEUTRAL", 66

    # Balanced Watchlist for Growth & Income
    watchlist = ["NVDA", "MSTY", "NVDY", "TSLA", "AAPL", "COIN", "MARA", "CLM", "CRF"]

    for symbol in watchlist:
        intel = fetch_unified_data(symbol)
        if not intel: continue
        
        setup = None
        
        # LOGIC 1: DIRECTIONAL (Buyers)
        # RSI Reset + Price > VWAP + Bullish/Neutral Sentiment
        if intel['rsi'] < rsi_limit and intel['price'] > intel['vwap'] and regime in ["BULLISH", "NEUTRAL"]:
            if "NORMAL" not in intel['conviction']:
                setup = {
                    "action": "BUY CALL / SELL PUT",
                    "type": "Bullish Reclaim",
                    "strike": f"${round(intel['price'] * 0.97, 2)} (Near-Money)",
                    "color": 0x2ecc71
                }

        # LOGIC 2: INCOME (Sellers)
        # If no directional play, look for 1.5 Sigma Credit Spreads
        if not setup and regime != "BEARISH":
            # Safety Zone Calculation
            strike_target = intel['price'] - (1.5 * intel['std_dev'])
            
            # Premium Hunter setup: Only sell when price is stable/rising (RSI > 45)
            if intel['rsi'] > 45:
                setup = {
                    "action": "SELL PUT SPREAD (Premium)",
                    "type": "Theta Income Hunter",
                    "strike": f"${round(strike_target, 2)} (1.5σ OTM)",
                    "color": 0x3498db
                }

        if setup:
            # Memory Check to prevent spam
            if not is_already_alerted(symbol, setup['type']):
                card_path = generate_signal_card(symbol, intel, setup, regime)
                dispatch_alert(symbol, setup, card_path)
                mark_alerted(symbol, setup['type'])
                if card_path and os.path.exists(card_path):
                    os.remove(card_path)
        
        time.sleep(2)

def is_already_alerted(symbol, s_type):
    try:
        with open(SIGNAL_MEMORY, "r") as f:
            mem = json.load(f)
            return mem.get(f"{symbol}_{s_type}") == datetime.date.today().isoformat()
    except: return False

def mark_alerted(symbol, s_type):
    try:
        with open(SIGNAL_MEMORY, "r") as f: mem = json.load(f)
    except: mem = {}
    mem[f"{symbol}_{s_type}"] = datetime.date.today().isoformat()
    with open(SIGNAL_MEMORY, "w") as f: json.dump(mem, f)

def dispatch_alert(symbol, setup, card_path):
    content = (
        f"🎯 **Rockefeller Sentry Signal: ${symbol}**\n"
        f"┣ **Strategy**: `{setup['action']}`\n"
        f"┗ **Class**: `{setup['type']}`"
    )
    try:
        if card_path:
            with open(card_path, 'rb') as f:
                requests.post(WEBHOOK_URL, data={"content": content}, files={'file': f})
        else:
            requests.post(WEBHOOK_URL, json={"content": content})
    except Exception as e:
        print(f"❌ Dispatch Error: {e}")

if __name__ == "__main__":
    # Test mode: run once and exit. Standard: run loop.
    if "test" in sys.argv:
        run_trade_signals()
    else:
        while True:
            run_trade_signals()
            time.sleep(900) # 15-minute intervals
