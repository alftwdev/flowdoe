import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

# Import the broadcast tool from your local essentials_tools.py
try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- 1. CONFIG & SANITIZATION ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

# .strip() is critical here to remove hidden characters/newlines from .env
TD_API_KEY = str(os.getenv("TD_API_KEY")).strip()
WEBHOOK_URL = str(os.getenv("WEBHOOK_THETA_GANG") or os.getenv("WEBHOOK_THETAGANG")).strip()
HISTORY_FILE = os.path.join(BASE_PATH, "macro_history.csv")

def get_market_regime():
    """Bridges data from macro_radar.py history file."""
    try:
        if os.path.exists(HISTORY_FILE):
            df = pd.read_csv(HISTORY_FILE, on_bad_lines='skip')
            df.columns = [c.strip() for c in df.columns]
            if 'Regime' in df.columns:
                # Returns the most recent regime entry
                return df.iloc[-1]['Regime'].upper().strip()
    except Exception as e:
        print(f"⚠️ [FILE ERROR] Could not read macro_history.csv: {e}")
    return "NEUTRAL"

def get_theta_intel(symbol, regime):
    """Calculates strikes using 20-day Standard Deviation."""
    try:
        # 1. Fetch Quote (Price)
        q_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
        q_resp = requests.get(q_url, timeout=15)
        
        # 2. Fetch StDev (Volatility)
        s_url = f"https://api.twelvedata.com/stdev?symbol={symbol}&interval=1day&time_period=20&apikey={TD_API_KEY}"
        s_resp = requests.get(s_url, timeout=15)

        # JSON SAFETY CHECK: Capture raw text if parsing fails
        try:
            quote_data = q_resp.json()
            sd_data = s_resp.json()
        except Exception:
            print(f"   [API ERROR] Non-JSON Response: {q_resp.text[:100]}")
            return None

        # CHECK FOR API ERRORS (e.g., Invalid Key, Rate Limit)
        if 'close' not in quote_data:
            msg = quote_data.get('message', 'Unknown API Error')
            print(f"   [DATA ERROR] {symbol}: {msg}")
            return None

        price = float(quote_data['close'])
        stdev = float(sd_data['values'][0]['stdev'])
        
        # --- THETA GANG LOGIC: REGIME-BASED BUFFER ---
        # Current Context detected: RISK-OFF
        if "OFF" in regime or "BEAR" in regime:
            strike = price - (stdev * 2.25)  # Defensive (2.25 Sigma)
            label, color = "🛡️ DEFENSIVE (Risk-Off)", 0xe67e22 # Orange
        elif "ON" in regime or "BULL" in regime:
            strike = price - (stdev * 0.85)  # Aggressive (0.85 Sigma)
            label, color = "⚡ AGGRESSIVE (Risk-On)", 0x2ecc71 # Green
        else:
            strike = price - (stdev * 1.5)   # Balanced (1.5 Sigma)
            label, color = "⚖️ BALANCED", 0x3498db # Blue

        return {
            "symbol": symbol,
            "price": price,
            "strike": strike,
            "label": label,
            "color": color
        }
    except Exception as e:
        print(f"   [SYSTEM ERROR] {symbol}: {e}")
        return None

def run_theta_gang():
    print(f"--- 🎡 THETA GANG START (HST) ---")
    
    if not TD_API_KEY or TD_API_KEY == "None":
        print("❌ CRITICAL: TD_API_KEY is missing from .env")
        return

    regime = get_market_regime()
    print(f"   [SYNC] Market Context: {regime}")

    # Your core high-yield watchlist
    tickers = ["MSTY", "NVDY", "TSLY", "IWM", "NVDA", "TSLA", "TQQQ", "CONY"]
    
    for symbol in tickers:
        print(f"   [SCAN] {symbol}...", end=" ", flush=True)
        intel = get_theta_intel(symbol, regime)
        
        if intel and HAS_ESSENTIALS:
            msg = (
                f"**Income Alert: ${intel['symbol']}**\n"
                f"└ Price: `${intel['price']:.2f}`\n"
                f"└ **Target Put Strike**: `${intel['strike']:.2f}`\n"
                f"└ **Strategy**: {intel['label']}\n\n"
                f"*Market Intel: Strike adjusted for {regime} regime.*"
            )
            # Sends the broadcast to Discord
            send_essentials_embed(WEBHOOK_URL, "🏛️ Essentials: Theta Intel", msg, intel['color'])
            print("✅ Sent.")
        else:
            print("❌ Skipped.")
        
        # Rate limiting for Venture Tier (keeps you under the per-minute limit)
        time.sleep(2) 

    print(f"--- THETA GANG FINISHED ---")

if __name__ == "__main__":
    run_theta_gang()
