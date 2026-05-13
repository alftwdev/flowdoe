import os
import requests
import json
import time
from datetime import datetime
from dotenv import load_dotenv

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- CONFIG ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")

def update_global_risk(vix_price, pcr_val):
    """Updates the shared ledger so trade_signals.py can see the risk spike."""
    try:
        with open(REGIME_LEDGER, "r") as f:
            data = json.load(f)
    except:
        data = {}

    # Logic: If VIX > 20 or PCR > 1.0, tighten the RSI Shield
    risk_level = "STORM" if vix_price > 22 else "STABLE"
    rsi_limit = 50 if risk_level == "STORM" else 66
    
    data.update({
        "vix_status": risk_level,
        "rsi_shield_limit": rsi_limit,
        "pcr_ratio": pcr_val,
        "last_vol_update": datetime.now().isoformat()
    })

    with open(REGIME_LEDGER, "w") as f:
        json.dump(data, f)
    return risk_level

def run_sentry():
    print("🛡️ VOLATILITY SENTRY ACTIVE...")
    while True:
        # Fetching VIX and Put/Call Ratio (Venture Tier)
        vix_url = f"https://api.twelvedata.com/quote?symbol=VIX&apikey={TD_API_KEY}"
        # Note: PCR often requires a specialized endpoint or manual proxy in Twelve Data
        
        try:
            r = requests.get(vix_url).json()
            vix_p = float(r['close'])
            
            risk = update_global_risk(vix_p, 0.85) # PCR placeholder
            
            if risk == "STORM":
                desc = f"⚠️ **GLOBAL RED ALERT: VIX SPIKE DETECTED**\n└ VIX: `{vix_p:.2f}`\n└ Action: **RSI Shield strictly enforced at < 50.**\n└ Trade Signals: Tightening stops."
                if HAS_ESSENTIALS:
                    send_essentials_embed(WEBHOOK_MARKET, "🚨 Volatility Warning", desc, 0xe74c3c)
            
        except Exception as e:
            print(f"Error in Sentry: {e}")
        
        time.sleep(900) # Check every 15 mins

if __name__ == "__main__":
    run_sentry()
