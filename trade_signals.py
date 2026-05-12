import os
import time
import requests
import datetime
import json
import pandas as pd
import pytz
import sys
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont

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

WATCHLIST = ["NVDA", "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL", "AMD", "META", "XLC"]

# --- 2. IMAGE GENERATION ENGINE (ANTI-SCRAPE) ---

def generate_signal_card(symbol, intel, execution, regime):
    """Generates a professional, watermarked image for the trade signal."""
    # Create Canvas (Dark Rockefeller Theme)
    width, height = 800, 450
    bg_color = (10, 10, 10)  # Near black
    img = Image.new('RGB', (width, height), color=bg_color)
    draw = ImageDraw.Draw(img)

    # Attempt to load professional fonts, fallback to default
    try:
        header_f = ImageFont.truetype("arialbd.ttf", 50)
        body_f = ImageFont.truetype("arial.ttf", 28)
        trace_f = ImageFont.truetype("arial.ttf", 14)
    except:
        header_f = ImageFont.load_default()
        body_f = ImageFont.load_default()
        trace_f = ImageFont.load_default()

    # Draw Branded Header
    draw.rectangle([0, 0, 800, 80], fill=(212, 175, 55)) # Gold Bar
    draw.text((30, 15), "🏛️ ROCKEFELLER STRATEGIC INTELLIGENCE", fill=(0, 0, 0), font=header_f)

    # Draw Signal Data
    draw.text((40, 120), f"TICKER: ${symbol}", fill=(255, 255, 255), font=body_f)
    draw.text((40, 170), f"ACTION: {execution['action']}", fill=(46, 204, 113), font=body_f)
    draw.text((40, 220), f"SETUP: {execution['type']}", fill=(255, 255, 255), font=body_f)
    draw.text((40, 270), f"STRIKE: {execution['strike']}", fill=(212, 175, 55), font=body_f)
    
    # Draw Market Context Footer
    draw.line((40, 330, 760, 330), fill=(50, 50, 50), width=2)
    footer_text = f"Regime: {regime} | RSI: {intel['rsi']:.1f} | VWAP: ${intel['vwap']:.2f}"
    draw.text((40, 350), footer_text, fill=(150, 150, 150), font=body_f)

    # INVISIBLE WATERMARK (The Traceable Seed)
    # This ID can be updated to include a specific user ID or timestamp
    trace_id = f"REF-{int(time.time())}-SIG"
    draw.text((700, 420), trace_id, fill=(15, 15, 15), font=trace_f) # Nearly invisible

    file_path = os.path.join(BASE_PATH, f"{symbol}_signal.png")
    img.save(file_path)
    return file_path

# --- 3. CORE LOGIC & UTILITIES ---

def is_market_open():
    tz_et = pytz.timezone('US/Eastern')
    now_et = datetime.datetime.now(tz_et)
    if now_et.weekday() > 4: return False
    return now_et.replace(hour=9, minute=30, second=0) <= now_et <= now_et.replace(hour=16, minute=0, second=0)

def get_ecosystem_data():
    try:
        if os.path.exists(REGIME_LEDGER):
            with open(REGIME_LEDGER, "r") as f:
                data = json.load(f)
                return data.get("regime", "NEUTRAL"), data.get("rsi_shield_limit", 66)
    except: pass
    return "NEUTRAL", 66

def has_been_alerted(symbol):
    today = datetime.date.today().isoformat()
    if os.path.exists(SIGNAL_MEMORY):
        with open(SIGNAL_MEMORY, "r") as f:
            mem = json.load(f)
            return mem.get(symbol) == today
    return False

def mark_as_alerted(symbol):
    today = datetime.date.today().isoformat()
    mem = {}
    if os.path.exists(SIGNAL_MEMORY):
        with open(SIGNAL_MEMORY, "r") as f: mem = json.load(f)
    mem[symbol] = today
    with open(SIGNAL_MEMORY, "w") as f: json.dump(mem, f)

def fetch_signals_data(symbol):
    try:
        url = f"https://api.twelvedata.com/complex_data?apikey={TD_API_KEY}"
        payload = {"symbols": [symbol], "intervals": ["15min"], "methods": ["quote", "rsi", "vwap"], "outputsize": 1}
        r = requests.post(url, json=payload, timeout=15).json()
        res = r['data'][0]['res']
        return {
            "price": float(res['quote']['close']),
            "rsi": float(res['rsi']['values'][0]['rsi']),
            "vwap": float(res['vwap']['values'][0]['vwap']),
            "name": res['quote']['name']
        }
    except: return None

def run_trade_signals():
    print("🏛️ Rockefeller Trade Sentry: Online")
    
    while True:
        if not is_market_open():
            print("💤 Market Closed. Deep Sleep (1 Hour).")
            time.sleep(3600)
            continue

        regime, rsi_limit = get_ecosystem_data()

        for symbol in WATCHLIST:
            if has_been_alerted(symbol): continue
            
            intel = fetch_signals_data(symbol)
            if not intel: continue

            setup_found = False
            # Bullish Reclaim Logic
            if intel['rsi'] < rsi_limit and intel['price'] > intel['vwap'] and regime in ["BULLISH", "NEUTRAL"]:
                setup_found = True
                strike = round(intel['price'] * 0.90, 2)
                execution = {"action": "SELL PUT / BUY CALL", "type": "Bullish Reclaim", "strike": f"${strike}", "color": 0x2ecc71}

            if setup_found:
                # 1. Generate Image Card
                image_file = generate_signal_card(symbol, intel, execution, regime)
                
                # 2. Dispatch to Discord
                try:
                    with open(image_file, 'rb') as f:
                        payload = {"content": f"🎯 **A+ Conviction Signal: ${symbol}**"}
                        requests.post(WEBHOOK_URL, data=payload, files={'file': f})
                    
                    mark_as_alerted(symbol)
                    print(f"🎯 Image Signal Dispatched: {symbol}")
                    os.remove(image_file) # Cleanup
                except Exception as e:
                    print(f"❌ Dispatch Error: {e}")

        time.sleep(300) # 5-minute cycle

if __name__ == "__main__":
    run_trade_signals()
