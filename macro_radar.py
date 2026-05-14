import os
import json
import requests
import pandas as pd
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
            return json.load(f)
    return {"regime": None, "rsi": 0.0}

def save_current_state(regime, rsi):
    """Saves the current state to memory after a broadcast."""
    with open(STATE_FILE, "w") as f:
        json.dump({
            "regime": regime, 
            "rsi": rsi, 
            "timestamp": datetime.now().isoformat()
        }, f)

def should_broadcast(current_regime, current_rsi, vix_status):
    """
    Elite Logic: Only broadcast if:
    1. The SHIELD is not in STORM/ELEVATED mode (for Bullish alerts).
    2. The Market Regime has changed (e.g., BULLISH -> NEUTRAL).
    3. RSI has crossed into an extreme zone (>70 or <30).
    """
    last_state = get_last_state()
    
    # 1. THE SHIELD GATE
    # Block BULLISH broadcasts if VIX is high to prevent "Bull Traps"
    if vix_status in ["STORM", "ELEVATED"] and current_regime == "BULLISH":
        print(f"🛡️ Shield Active ({vix_status}): Suppressing Bullish Alert.")
        return False, f"Shield Muzzle ({vix_status})"

    # 2. REGIME SHIFT CHECK
    if current_regime != last_state.get("regime"):
        return True, f"Regime Shift: {current_regime}"

    # 3. RSI EXTREME CHECK
    # Only alert if it's a new extreme we haven't flagged yet
    if current_rsi > 70 and last_state.get("rsi", 0) <= 70:
        return True, "Overbought RSI (Over 70)"
    if current_rsi < 30 and last_state.get("rsi", 100) >= 30:
        return True, "Oversold RSI (Under 30)"

    return False, None

def run_macro_radar():
    print(f"📡 Rockefeller Radar: Scanning Market Structure... {datetime.now()}")
    
    # --- A. LOAD CENTRAL LEDGER ---
    try:
        with open(REGIME_LEDGER, "r") as f:
            ledger = json.load(f)
            vix_status = ledger.get("vix_status", "STABLE")
    except Exception as e:
        print(f"⚠️ Ledger Read Error: {e}. Defaulting to STABLE.")
        vix_status = "STABLE"

    # --- B. DATA ACQUISITION (Simulated for logic flow) ---
    # In production, replace with actual TwelveData/Finnhub calls
    current_regime = "BULLISH" 
    current_rsi = 72.5         
    spy_price = 520.50
    vix_price = 14.2           # This would normally be fetched live

    # --- C. ELITE FILTERING ---
    trigger_detected, reason = should_broadcast(current_regime, current_rsi, vix_status)

    if trigger_detected:
        print(f"🚀 Trigger Detected: {reason}. Dispatching to Discord...")
        
        # Determine Color based on Regime
        color = 0x2ecc71 if current_regime == "BULLISH" else 0xe74c3c if current_regime == "BEARISH" else 0xf1c40f
        
        # Build the "Elite" Embed
        intelligence_report = {
            "title": "🏛️ Rockefeller Market Intelligence",
            "description": (
                f"**Event**: `{reason}`\n\n"
                f"**Current Metrics**:\n"
                f"┣ **Regime**: `{current_regime}`\n"
                f"┣ **SPY**: `${spy_price:,.2f}`\n"
                f"┣ **RSI**: `{current_rsi:.1f}`\n"
                f"┗ **VIX Sentry**: `{vix_status}`\n\n"
                f"*System Note: Logic threshold met. Next update on regime shift or RSI reset.*"
            ),
            "color": color,
            "footer": {"text": f"Rockefeller Strategic Intelligence • {datetime.now().strftime('%H:%M HST')}"}
        }
        
        # Webhook Payload
        payload = {"embeds": [intelligence_report]}
        
        try:
            response = requests.post(WEBHOOK_MARKET, json=payload)
            if response.status_code == 204:
                print("✅ Broadcast Successful.")
                save_current_state(current_regime, current_rsi)
        except Exception as e:
            print(f"❌ Webhook Error: {e}")
    else:
        print(f"💤 Radar Standby: {reason if reason else 'No changes detected.'}")

if __name__ == "__main__":
    run_macro_radar()
