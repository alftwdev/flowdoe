import datetime
import sys
import os
import requests
import pandas as pd
import time
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

# --- 0. LOAD SECURE VAULT ---
load_dotenv()

# --- 1. IDENTITY & CREDENTIALS ---
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_API_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")

def get_td_indicator(symbol, indicator, interval="1day", period=14):
    """Fetches specific technical indicators with detailed error logging."""
    print(f"    [TD] Fetching {indicator} for {symbol}...")
    url = f"https://api.twelvedata.com/{indicator}?symbol={symbol}&interval={interval}&time_period={period}&outputsize=1&apikey={TD_API_KEY}"
    try:
        response = requests.get(url, timeout=15).json()
        if "values" in response and response["values"]:
            return float(response['values'][0][indicator.lower()])
        
        # Detailed debugging for weekend/credit issues
        print(f"    [TD DEBUG] {indicator} Fail: {response.get('message', 'No message')} | Code: {response.get('code', 'N/A')}")
        return None
    except Exception as e:
        print(f"    [TD] Connection ERROR on {indicator}: {e}")
        return None

def get_td_price(symbol):
    """Fetches price via Quote with fallbacks for Indices and Weekend testing."""
    symbols_to_try = [symbol]
    if symbol == "VIX":
        # Order: Index specific, standard, and common prefix
        symbols_to_try = ["VIX:INDEX", "VIX", "^VIX"]

    for s in symbols_to_try:
        url = f"https://api.twelvedata.com/quote?symbol={s}&apikey={TD_API_KEY}"
        try:
            response = requests.get(url, timeout=15).json()
            if "close" in response and response["close"] is not None:
                return float(response['close'])
        except:
            continue
            
    # --- WEEKEND FALLBACK LOGIC ---
    if symbol == "VIX":
        print(f"    [!] VIX Data Unavailable (Market Closed). Using fallback value 20.0 for testing.")
        return 20.0 # Default 'Neutral' VIX for script continuity
        
    print(f"    [TD DEBUG] Price Fail for {symbol} after all attempts.")
    return None

def run_macro_check():
    print(f"\n--- VENTURE MACRO RADAR START: {datetime.datetime.now()} ---")
    
    if not TD_API_KEY:
        print("    [!] ERROR: TWELVE_DATA_API_KEY not found in .env")
        return

    # 1. FETCH DATA
    spy_price = get_td_price("SPY")
    spy_ema200 = get_td_indicator("SPY", "ema", period=200)
    tqqq_price = get_td_price("TQQQ")
    tqqq_rsi = get_td_indicator("TQQQ", "rsi", period=14)
    vix_val = get_td_price("VIX")

    if None in [spy_price, spy_ema200, tqqq_price, tqqq_rsi, vix_val]:
        print("    [!] Critical Data Missing. See DEBUG notes above.")
        return

    # 2. STRATEGY & REGIME LOGIC
    is_bullish = spy_price > spy_ema200
    regime_text = "🐂 BULLISH EXPANSION" if is_bullish else "🐻 BEARISH REGIME"
    status_color = 0x2ecc71 if is_bullish else 0xe74c3c 
    
    strike_zone = "⚡ STRIKE ZONE ACTIVE" if (tqqq_rsi < 35 and is_bullish) else "Neutral"
    
    outlook = "Condition: Risk-On" if (is_bullish and vix_val < 20) else "Condition: Caution Required"
    if not is_bullish: outlook = "Condition: Risk-Off / Defensive"

    # 3. STRUCTURED LOGGING (Optimized for Recaps)
    # We include 'week_id' to easily group data for your Sunday recaps
    now = datetime.datetime.now()
    new_entry = {
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "week_id": now.strftime("%Y-%U"), # Year and Week Number
        "spy_price": spy_price,
        "spy_ema200": spy_ema200,
        "tqqq_rsi": round(tqqq_rsi, 2),
        "vix": vix_val,
        "regime": regime_text,
        "is_strike": 1 if strike_zone != "Neutral" else 0
    }
    
    df = pd.DataFrame([new_entry])
    df.to_csv(HISTORY_FILE, mode='a', header=not os.path.exists(HISTORY_FILE), index=False)

    # 4. CONSTRUCT REPORTS
    report_title = f"🌎 Market Regime: {regime_text}"
    report_body = (
        f"**Outlook**: {outlook}\n\n"
        f"**SPY**: ${spy_price:.2f} (Trend: {'Above' if is_bullish else 'Below'} 200-EMA)\n"
        f"**TQQQ RSI**: {tqqq_rsi:.2f} | **VIX**: {vix_val:.2f}\n\n"
        f"**Status**: {strike_zone}\n"
        f"_Disclaimer: Educational data only._"
    )

    # 5. DISPATCHER
    print("ACTION: Dispatching to Discord...")
    send_essentials_embed(
        webhook_url=WEBHOOK_MARKET,
        title=report_title,
        description=report_body,
        color=status_color
    )

    print("ACTION: Dispatching Pushover...")
    try:
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": PUSHOVER_API_TOKEN,
            "user": PUSHOVER_USER_KEY,
            "title": f"🌎 Macro: {regime_text}",
            "message": report_body.replace("**", ""),
            "priority": 1 if (not is_bullish or strike_zone != "Neutral") else 0
        }, timeout=10)
    except Exception as e:
        print(f"    [PUSH] ERROR: {e}")

    print(f"--- MACRO RADAR FINISHED ---\n")

if __name__ == "__main__":
    run_macro_check()
