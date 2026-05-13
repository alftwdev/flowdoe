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
STATE_FILE = os.path.join(BASE_DIR, "last_alert.json") # The Bot's "Memory"
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
        json.dump({"regime": regime, "rsi": rsi, "timestamp": datetime.now().isoformat()}, f)

def should_broadcast(current_regime, current_rsi):
    """
    Elite Logic: Only broadcast if:
    1. The Market Regime has changed (e.g., BULLISH -> NEUTRAL)
    2. RSI has crossed into an extreme zone (>70 or <30) and we haven't alerted yet.
    """
    last_state = get_last_state()
    
    # Trigger 1: Regime Change
    if current_regime != last_state.get("regime"):
        return True, f"Regime Shift: {last_state.get('regime')} ➔ {current_regime}"
    
    # Trigger 2: RSI Extreme Breach
    # We only alert if it's the FIRST time entering the zone since the last reset
    if current_rsi >= 70 and last_state.get("rsi") < 70:
        return True, f"RSI Overbought: {current_rsi:.1f}"
    if current_rsi <= 30 and last_state.get("rsi") > 30:
        return True, f"RSI Oversold: {current_rsi:.1f}"

    return False, None

def run_macro_radar():
    # ... (Standard data fetching logic for SPY, VIX, RSI here) ...
    # For this example, let's assume we've fetched:
    current_regime = "BULLISH" # This would be calculated by your logic
    current_rsi = 76.1         # This would be fetched from TwelveData
    spy_price = 520.50
    vix_price = 14.2

    # --- ELITE FILTER ---
    trigger_detected, reason = should_broadcast(current_regime, current_rsi)

    if trigger_detected:
        print(f"🚀 Trigger Detected: {reason}. Dispatching to Discord...")
        
        # Build the "Elite" Embed
        intelligence_report = (
            f"### 🏛️ Rockefeller Market Intelligence\n"
            f"**Event**: `{reason}`\n\n"
            f"**Current Metrics**:\n"
            f"└ **Regime**: `{current_regime}`\n"
            f"└ **SPY**: `${spy_price:,.2f}`\n"
            f"└ **RSI**: `{current_rsi:.1f}`\n"
            f"└ **VIX**: `{vix_price:.2f}`\n\n"
            f"*System Note: Intelligence threshold met. No further alerts until regime shift or RSI reset.*"
        )
        
        # Replace this with your actual webhook call
        # send_essentials_embed(WEBHOOK_MARKET, "Market Alert", intelligence_report, 0x2ecc71)
        
        # Update memory so we don't spam the same info next time
        save_current_state(current_regime, current_rsi)
    else:
        print(f"⚖️ Market Stable. RSI @ {current_rsi:.1f} (No change-only trigger). Standing by.")

if __name__ == "__main__":
    run_macro_radar()
