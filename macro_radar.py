import os
import json
import requests
import sys
from datetime import datetime
from dotenv import load_dotenv

# --- 1. INITIALIZATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# File paths for state management
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")
STATE_FILE = os.path.join(BASE_DIR, "last_alert.json") 
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")

def get_last_state():
    """Loads the last broadcasted state from memory."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            try:
                return json.load(f)
            except:
                return {"regime": None, "rsi": 0.0}
    return {"regime": None, "rsi": 0.0}

def save_current_state(regime, rsi):
    """Saves the current state to memory after a broadcast."""
    with open(STATE_FILE, "w") as f:
        json.dump({
            "regime": regime, 
            "rsi": rsi, 
            "timestamp": datetime.now().isoformat()
        }, f)

def fetch_live_market_data(symbol="SPY"):
    """Fetches live Price and RSI from Twelve Data."""
    try:
        # Fetch Price
        price_url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}"
        p_resp = requests.get(price_url).json()
        price = float(p_resp['price'])

        # Fetch RSI (Daily)
        rsi_url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1day&time_period=14&apikey={TD_API_KEY}"
        r_resp = requests.get(rsi_url).json()
        rsi = float(r_resp['values'][0]['rsi'])

        return price, rsi
    except Exception as e:
        print(f"❌ API Data Acquisition Error: {e}")
        return None, None

def should_broadcast(current_regime, current_rsi, vix_status, force=False):
    """Elite Logic: Only broadcast on changes or extremes."""
    if force:
        return True, "Manual System Test"

    last_state = get_last_state()
    
    # 1. THE SHIELD GATE
    if vix_status in ["STORM", "ELEVATED"] and current_regime == "BULLISH":
        return False, f"Shield Muzzle ({vix_status})"

    # 2. REGIME SHIFT CHECK
    if current_regime != last_state.get("regime"):
        return True, f"Regime Shift: {current_regime}"

    # 3. RSI EXTREME CHECK
    if current_rsi > 70 and last_state.get("rsi", 0) <= 70:
        return True, "Overbought RSI (Over 70)"
    if current_rsi < 30 and last_state.get("rsi", 100) >= 30:
        return True, "Oversold RSI (Under 30)"

    return False, None

def run_macro_radar(force_test=False):
    print(f"📡 Rockefeller Radar: Scanning Market Structure... {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # --- A. LOAD CENTRAL LEDGER ---
    try:
        with open(REGIME_LEDGER, "r") as f:
            ledger = json.load(f)
            vix_status = ledger.get("vix_status", "STABLE")
            current_regime = ledger.get("regime", "NEUTRAL")
    except Exception as e:
        vix_status = "STABLE"
        current_regime = "NEUTRAL"

    # --- B. LIVE DATA ACQUISITION ---
    spy_price, current_rsi = fetch_live_market_data("SPY")
    
    if spy_price is None:
        print("🛑 Data fetch failed.")
        return

    # --- C. ELITE FILTERING ---
    trigger_detected, reason = should_broadcast(current_regime, current_rsi, vix_status, force=force_test)

    if trigger_detected:
        print(f"🚀 Trigger Detected: {reason}. Dispatching to Discord...")
        
        color = 0x2ecc71 if current_regime == "BULLISH" else 0xe74c3c if current_regime == "BEARISH" else 0xf1c40f
        
        intelligence_report = {
            "title": "🏛️ Rockefeller Market Intelligence",
            "description": (
                f"**Event**: `{reason}`\n\n"
                f"**Current Metrics**:\n"
                f"┣ **Regime**: `{current_regime}`\n"
                f"┣ **SPY**: `${spy_price:,.2f}`\n"
                f"┣ **RSI**: `{current_rsi:.1f}`\n"
                f"┗ **VIX Sentry**: `{vix_status}`\n\n"
                f"*Note: System is now in Silent Standby until the next major shift.*"
            ),
            "color": color,
            "footer": {"text": f"Rockefeller Strategic Intelligence • {datetime.now().strftime('%H:%M HST')}"}
        }
        
        try:
            response = requests.post(WEBHOOK_MARKET, json={"embeds": [intelligence_report]})
            if response.status_code == 204:
                print("✅ Broadcast Successful.")
                save_current_state(current_regime, current_rsi)
        except Exception as e:
            print(f"❌ Webhook Error: {e}")
    else:
        print(f"💤 Status: {current_regime} | RSI: {current_rsi:.1f} | Reason: Holding")

if __name__ == "__main__":
    # Check if 'test' was passed as a command line argument
    is_test = True if len(sys.argv) > 1 and sys.argv[1] == "test" else False
    run_macro_radar(force_test=is_test)
