import os
import csv
import requests
import pytz
from datetime import datetime
from dotenv import load_dotenv

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- 1. CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
HISTORY_FILE = os.path.join(BASE_DIR, "macro_history.csv")

def get_market_pillar(symbol):
    """Fetches quote with fallback to historical time_series for weekend support."""
    # Attempt 1: Standard Quote
    url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("status") != "error" and res.get("close"):
            return float(res["close"]), float(res.get("percent_change", 0))
    except:
        pass

    # Attempt 2: Time Series (The Weekend Shield)
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=1&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        if res.get("status") != "error" and "values" in res:
            return float(res["values"][0]["close"]), 0.0
    except:
        pass
    
    return None, 0.0

def run_macro_radar():
    print(f"--- 🏛️ MACRO RADAR: FINAL ALIGNMENT ---")
    
    # 1. SPY - The Trend Pillar
    spy_p, spy_chg = get_market_pillar("SPY")
    
    # 2. VIX - The Fear Pillar (Testing Multiple Symbol Formats)
    vix_p = None
    for sym in ["VIX:CBOE", "VIX", "$VIX"]:
        val, _ = get_market_pillar(sym)
        if val:
            vix_p = val
            break
    
    # Emergency Fallback for VIX to prevent Logic Crashes
    if vix_p is None:
        print("    ⚠️ VIX API Failure. Defaulting to Neutral (20.0) for logic continuity.")
        vix_p = 20.0

    # 3. Technicals
    ema_url = f"https://api.twelvedata.com/ema?symbol=SPY&interval=1day&time_period=200&apikey={TD_API_KEY}"
    rsi_url = f"https://api.twelvedata.com/rsi?symbol=SPY&interval=1day&time_period=14&apikey={TD_API_KEY}"
    
    try:
        ema_v = float(requests.get(ema_url).json()['values'][0]['ema'])
        rsi_v = float(requests.get(rsi_url).json()['values'][0]['rsi'])
    except Exception as e:
        print(f"❌ Technical Pillar Error: {e}")
        return

    # 4. REGIME LOGIC (Rockefeller Matrix)
    if spy_p > ema_v and vix_p < 22:
        regime, color = "BULLISH", 0x2ecc71
    elif spy_p < (ema_v * 0.98) or vix_p > 28:
        regime, color = "BEARISH", 0xe74c3c
    else:
        regime, color = "NEUTRAL", 0xf1c40f

    # 5. DATA PERSISTENCE (The Ledger)
    tz_h = pytz.timezone('Pacific/Honolulu')
    date_str = datetime.now(tz_h).strftime('%Y-%m-%d')
    
    file_exists = os.path.isfile(HISTORY_FILE)
    with open(HISTORY_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Date", "Regime", "VIX", "SPY_Price", "RSI"])
        writer.writerow([date_str, regime, vix_p, spy_p, rsi_v])

    print(f"✅ SUCCESS: Market is {regime} | VIX: {vix_p} | SPY: {spy_p}")

    # 6. ECOSYSTEM DISPATCH
    if HAS_ESSENTIALS and WEBHOOK_MARKET:
        desc = (
            f"### **Market Posture: {regime}**\n"
            f"└ **SPY**: `${spy_p:,.2f}`\n"
            f"└ **VIX**: `{vix_p:.2f}`\n"
            f"└ **RSI**: `{rsi_v:.1f}`\n\n"
            f"**Strategy Note**: All sub-scripts (`trade_signals.py`, `monitor.py`) "
            f"are now aligned to the **{regime}** posture for the next 24 hours."
        )
        send_essentials_embed(WEBHOOK_MARKET, "🏛️ Rockefeller Macro Radar", desc, color)

if __name__ == "__main__":
    run_macro_radar()
