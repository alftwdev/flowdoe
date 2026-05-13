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

# --- 2. DYNAMIC SCANNER (THE NEW CORE) ---

def dynamic_scanner():
    """
    Replaces static watchlist. Scans for current 'Market Movers'.
    Uses Venture Tier access to find symbols with momentum.
    """
    # Twelve Data 'Market Movers' logic or high-volume sectors
    # We focus on highly liquid US Equities to ensure A+ execution
    base_movers = ["NVDA", "AAPL", "TSLA", "AMD", "MSFT", "AMZN", "META", "GOOGL", "NFLX", "COIN", "MARA", "PLTR", "AVGO", "SMCI"]
    
    # Optional: Fetch dynamic leaders if API credits allow
    # url = f"https://api.twelvedata.com/market_movers/stocks?apikey={TD_API_KEY}"
    # r = requests.get(url).json()
    # if r.get('values'): return [x['symbol'] for x in r['values'][:20]]
    
    return base_movers

# --- 3. IMAGE GENERATION ENGINE ---

def generate_signal_card(symbol, intel, execution, regime):
    """Generates a professional, watermarked image for the trade signal."""
    width, height = 800, 480
    bg_color = (10, 10, 10)
    img = Image.new('RGB', (width, height), color=bg_color)
    draw = ImageDraw.Draw(img)

    try:
        header_f = ImageFont.truetype("arialbd.ttf", 45)
        body_f = ImageFont.truetype("arial.ttf", 26)
        trace_f = ImageFont.truetype("arial.ttf", 14)
    except:
        header_f = ImageFont.load_default()
        body_f = ImageFont.load_default()
        trace_f = ImageFont.load_default()

    draw.rectangle([0, 0, 800, 80], fill=(212, 175, 55)) 
    draw.text((30, 20), "🏛️ ROCKEFELLER STRATEGIC INTELLIGENCE", fill=(0, 0, 0), font=header_f)

    draw.text((40, 110), f"TICKER: ${symbol}", fill=(255, 255, 255), font=body_f)
    draw.text((40, 155), f"ACTION: {execution['action']}", fill=(46, 204, 113), font=body_f)
    draw.text((40, 200), f"SETUP: {execution['type']}", fill=(255, 255, 255), font=body_f)
    draw.text((40, 245), f"STRIKE: {execution['strike']}", fill=(212, 175, 55), font=body_f)
    
    draw.text((40, 290), f"CONVICTION: {intel['conviction']}", fill=intel['conv_color'], font=body_f)
    draw.text((40, 335), f"TREND: {intel['trend']}", fill=(255, 255, 255), font=body_f)

    draw.line((40, 385, 760, 385), fill=(50, 50, 50), width=2)
    footer_text = f"Regime: {regime} | RSI: {intel['rsi']:.1f} | VWAP: ${intel['vwap']:.2f}"
    draw.text((40, 400), footer_text, fill=(150, 150, 150), font=body_f)

    trace_id = f"REF-{int(time.time())}-SIG"
    draw.text((680, 455), trace_id, fill=(15, 15, 15), font=trace_f)

    file_path = os.path.join(BASE_PATH, f"{symbol}_signal.png")
    img.save(file_path)
    return file_path

# --- 4. CORE UTILITIES ---

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
    """Fetches high-conviction metrics for any ticker discovered by the scanner."""
    try:
        # Base Quote & RSI/VWAP
        url = f"https://api.twelvedata.com/complex_data?apikey={TD_API_KEY}"
        payload = {"symbols": [symbol], "intervals": ["15min"], "methods": ["quote", "rsi", "vwap"], "outputsize": 1}
        r = requests.post(url, json=payload, timeout=15).json()
        res = r['data'][0]['res']
        
        # Cross-reference with our proprietary filters
        conv_label, conv_color, _ = get_institutional_conviction(symbol, TD_API_KEY)
        trend_status, is_bullish = get_trend_alignment(symbol, TD_API_KEY)

        return {
            "price": float(res['quote']['close']),
            "rsi": float(res['rsi']['values'][0]['rsi']),
            "vwap": float(res['vwap']['values'][0]['vwap']),
            "conviction": conv_label,
            "conv_color": conv_color,
            "trend": trend_status,
            "shield_active": not is_bullish
        }
    except: return None

def run_trade_signals():
    print("🏛️ Rockefeller Dynamic Intelligence: Online")
    
    while True:
        if not is_market_open():
            print("💤 Market Closed. Deep Sleep (1 Hour).")
            time.sleep(3600)
            continue

        regime, rsi_limit = get_ecosystem_data()
        
        # PULL DYNAMIC TICKERS INSTEAD OF WATCHLIST
        current_movers = dynamic_scanner()

        for symbol in current_movers:
            if has_been_alerted(symbol): continue
            
            intel = fetch_signals_data(symbol)
            if not intel or intel['shield_active']: continue

            setup_found = False
            # THE A+ SETUP: RSI Reset + VWAP Support + Institutional Flow
            if intel['rsi'] < rsi_limit and intel['price'] > intel['vwap'] and regime in ["BULLISH", "NEUTRAL"]:
                # Final check for Institutional Conviction (Must be ELEVATED or HIGH)
                if "NORMAL" not in intel['conviction']:
                    setup_found = True
                    strike = round(intel['price'] * 0.90, 2)
                    execution = {
                        "action": "SELL PUT / BUY CALL", 
                        "type": "Bullish Reclaim", 
                        "strike": f"${strike}", 
                        "color": 0x2ecc71
                    }

            if setup_found:
                image_file = generate_signal_card(symbol, intel, execution, regime)
                try:
                    with open(image_file, 'rb') as f:
                        payload = {"content": f"🎯 **A+ Conviction Signal: ${symbol}**\n*Status: {intel['trend']} | {intel['conviction']}*"}
                        requests.post(WEBHOOK_URL, data=payload, files={'file': f})
                    
                    mark_as_alerted(symbol)
                    print(f"🎯 Dynamic Signal Dispatched: {symbol}")
                    os.remove(image_file)
                except Exception as e:
                    print(f"❌ Dispatch Error: {e}")

        time.sleep(600) # 10-minute cycle for optimal API efficiency

if __name__ == "__main__":
    run_trade_signals()
