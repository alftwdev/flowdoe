import os
import requests
import json
import time
import datetime
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
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")

def update_global_risk(vix_price, pcr_val):
    """
    The Central Logic: Writes to the shared ledger.
    Every other bot checks this before firing an alert.
    """
    try:
        with open(REGIME_LEDGER, "r") as f:
            data = json.load(f)
    except:
        data = {}

    # Define Risk Thresholds
    if vix_price > 25:
        risk_level = "STORM"
        rsi_limit = 45 # Extreme caution
    elif vix_price > 19:
        risk_level = "ELEVATED"
        rsi_limit = 55 # Tightened shield
    else:
        risk_level = "STABLE"
        rsi_limit = 66 # Full offensive mode

    data.update({
        "vix_status": risk_level,
        "rsi_shield_limit": rsi_limit,
        "vix_price": round(vix_price, 2),
        "last_vol_update": datetime.datetime.now().isoformat()
    })

    with open(REGIME_LEDGER, "w") as f:
        json.dump(data, f)
        
    return risk_level, rsi_limit

def run_volatility_sentry():
    print("🛡️ Rockefeller Volatility Sentry: Active")
    
    last_broadcast_risk = None
    
    while True:
        try:
            # 1. Fetch VIX Data
            vix_url = f"https://api.twelvedata.com/quote?symbol=VIX&apikey={TD_API_KEY}"
            r = requests.get(vix_url, timeout=15).json()
            vix_p = float(r['close'])
            
            # 2. Update Ecosystem Ledger
            risk, limit = update_global_risk(vix_p, 0.85)
            
            # 3. Broadcast Major Shifts
            if risk != last_broadcast_risk:
                print(f"⚠️ Market Risk Shift: {risk} (RSI Shield: {limit})")
                
                if HAS_ESSENTIALS and WEBHOOK_MARKET:
                    color = 0xe74c3c if risk == "STORM" else 0xf1c40f if risk == "ELEVATED" else 0x2ecc71
                    title = "🛡️ Global Volatility Shift"
                    desc = (
                        f"### **Market Condition: {risk}**\n"
                        f"The Rockefeller Shield has adjusted parameters to preserve capital.\n\n"
                        f"┣ **VIX Index**: `{vix_p:.2f}`\n"
                        f"┣ **RSI Shield**: `Set to < {limit}`\n"
                        f"┗ **Posturing**: {'Defensive' if risk != 'STABLE' else 'Offensive'}"
                    )
                    send_essentials_embed(WEBHOOK_MARKET, title, desc, color)
                
                last_broadcast_risk = risk

        except Exception as e:
            print(f"❌ Volatility Scan Error: {e}")

        # Sync every 10 minutes (Venture Tier efficient)
        time.sleep(600)

if __name__ == "__main__":
    run_volatility_sentry()
