import datetime
import sys
import os
import requests
import pandas as pd
import time
from dotenv import load_dotenv
# Import the professional formatting tool
from essentials_tools import send_essentials_embed

# --- 0. LOAD SECURE VAULT ---
load_dotenv()

# --- 1. IDENTITY & CREDENTIALS ---
# Updated to match your .env key exactly
ALPHA_VANTAGE_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_API_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

def get_av_data(symbol, function="TIME_SERIES_DAILY"):
    """Fetches data with Free Tier rate-limit awareness."""
    print(f"    [AV] Fetching {symbol} data...")
    url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={ALPHA_VANTAGE_KEY}&outputsize=full"
    try:
        response = requests.get(url, timeout=15)
        data = response.json()
        
        # Check for the common 'Note' which indicates rate limiting
        if "Note" in data:
            print(f"    [AV] RATE LIMIT REACHED: {data['Note']}")
            return None
            
        if "Time Series (Daily)" in data:
            df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient='index').astype(float)
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            return df
            
        print(f"    [AV] Error or No Data for {symbol}: {data.get('Error Message', 'Unknown Error')}")
        return None
    except Exception as e:
        print(f"    [AV] Connection ERROR fetching {symbol}: {e}")
        return None

def calculate_rsi(df, period=14):
    """Calculates Relative Strength Index for strike zone detection."""
    delta = df['4. close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1+rs))

def run_macro_check():
    print(f"\n--- MACRO RADAR START: {datetime.datetime.now()} ---")
    
    if not ALPHA_VANTAGE_KEY:
        print("    [!] ERROR: ALPHAVANTAGE_API_KEY not found in .env")
        return

    # 1. FETCH CORE ENGINE DATA 
    # Added 15s delays between calls to stay safe on Free Tier
    spy_df = get_av_data("SPY")
    time.sleep(15) 
    
    tqqq_df = get_av_data("TQQQ")
    time.sleep(15)
    
    # Note: VIX may require a different function or premium depending on AV's current free tier status
    vix_df = get_av_data("VIX") 
    
    if spy_df is None or tqqq_df is None:
        print("    [!] Critical Data Missing. Aborting.")
        return

    # 2. STRATEGY CALCULATIONS
    current_spy = spy_df['4. close'].iloc[-1]
    spy_200ma = spy_df['4. close'].rolling(window=200).mean().iloc[-1]
    current_tqqq = tqqq_df['4. close'].iloc[-1]
    tqqq_rsi = calculate_rsi(tqqq_df).iloc[-1]
    
    # VIX Logic (Fallback if VIX data is restricted on Free Tier)
    vix_val = vix_df['4. close'].iloc[-1] if vix_df is not None else 0.0
    
    # Market Regime Detection
    regime = "Bullish (Above 200MA)" if current_spy > spy_200ma else "Bearish (Below 200MA)"
    status_color = 0x2ecc71 if current_spy > spy_200ma else 0xe74c3c 
    
    # Strike Zone Logic (RSI < 35 in Bull Regime)
    strike_zone = "⚡ STRIKE ZONE ACTIVE" if (tqqq_rsi < 35 and current_spy > spy_200ma) else "Neutral"
    
    # 3. CONSTRUCT REPORT
    report_title = f"Market Intelligence: {regime}"
    report_body = (
        f"**SPY**: ${current_spy:.2f} (200MA: ${spy_200ma:.2f})\n"
        f"**TQQQ**: ${current_tqqq:.2f} | **RSI**: {tqqq_rsi:.2f}\n"
        f"**VIX**: {vix_val:.2f}\n"
        f"**Status**: {strike_zone}"
    )

    # 4. DISPATCHER
    print("ACTION: Dispatching to Discord #market-analysis...")
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
            "title": f"🌎 Macro: {regime}",
            "message": report_body.replace("**", ""),
            "priority": 1 if strike_zone != "Neutral" else 0
        }, timeout=10)
    except Exception as e:
        print(f"    [PUSH] ERROR: {e}")

    print(f"--- MACRO RADAR FINISHED: {datetime.datetime.now()} ---\n")

if __name__ == "__main__":
    run_macro_check()
