import os
import json
import requests
import sys
from datetime import datetime
from dotenv import load_dotenv

# --- 1. INITIALIZATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")
STATE_FILE = os.path.join(BASE_DIR, "last_alert.json") 
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
# NEW: Ensure this is added to your .env
WEBHOOK_CRYPTO = os.getenv("WEBHOOK_CRYPTO") 

# [Existing State Management Functions: get_last_state, save_current_state, fetch_live_market_data remain untouched]

# --- NEW: CRYPTO INTELLIGENCE ADDITION ---

def fetch_crypto_intelligence():
    """Fetches essential institutional data and dynamic charts for BTC and ETH."""
    if not WEBHOOK_CRYPTO:
        return

    targets = ["BTC/USD", "ETH/USD"]
    for symbol in targets:
        try:
            # 1. Gather Quote & Visuals
            quote_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
            logo_url = f"https://api.twelvedata.com/logo?symbol={symbol}&apikey={TD_API_KEY}"
            
            # Dynamic Screenshot (Option A)
            chart_url = f"https://api.twelvedata.com/screenshot?symbol={symbol}&apikey={TD_API_KEY}"
            
            data = requests.get(quote_url).json()
            logo_data = requests.get(logo_url).json()
            
            price = float(data.get("close", 0))
            change = float(data.get("percent_change", 0))
            logo_thumb = logo_data.get("url", "")

            color = 0x2ecc71 if change > 0 else 0xe74c3c
            
            embed = {
                "title": f"₿ {symbol} Institutional Pulse",
                "description": (
                    f"┣ **Current Price**: `${price:,.2f}`\n"
                    f"┣ **24h Change**: `{change:+.2f}%`\n"
                    f"┗ **Source**: `Twelve Data Venture`"
                ),
                "color": color,
                "thumbnail": {"url": logo_thumb},
                "image": {"url": chart_url},
                "footer": {"text": f"Rockefeller Crypto Intelligence • {datetime.now().strftime('%H:%M HST')}"}
            }
            
            requests.post(WEBHOOK_CRYPTO, json={"embeds": [embed]})
            
        except Exception as e:
            print(f"⚠️ Crypto Radar Error for {symbol}: {e}")
           
 # --- ADDED TO macro_radar.py ---

def broadcast_flowstate_pulse():
    """Fires at 09:35 AM EST to confirm system status regardless of triggers."""
    now = datetime.now()
    # Check for 09:35 AM EST (which is 03:35 AM HST for you)
    if now.hour == 3 and now.minute == 35:
        targets = ["BTC/USD", "ETH/USD"]
        for symbol in targets:
            pulse = fetch_crypto_pulse(symbol) # Uses our Option A logic
            if not pulse: continue
            
            embed = {
                "title": f"🏛️ {symbol.split('/')[0]} Flowstate Update",
                "description": (
                    f"**Status**: `SCANNING / STABLE`\n\n"
                    f"**Current Context**:\n"
                    f"┣ **Price**: `${pulse['price']:,.2f}`\n"
                    f"┣ **24h Change**: `{pulse['change']}%`\n"
                    f"┗ **Shield Status**: `ACTIVE`\n\n"
                    f"**Sentry Note**: BTC/ETH are being monitored for high-conviction entries. "
                    f"The engine is currently in 'Silent Standby' awaiting a regime shift."
                ),
                "color": 0x3498db, # Professional Blue for Flowstate
                "thumbnail": {"url": pulse['logo']},
                "image": {"url": pulse['chart']},
                "footer": {"text": "Rockefeller Crypto Intelligence"}
            }
            requests.post(WEBHOOK_CRYPTO, json={"embeds": [embed]})          

def run_radar_cycle():
    """Main execution loop modified to include Crypto Pulse."""
    # [Existing SPY/VIX/RSI Logic remains here]
    
    # Logic to trigger crypto pulse (e.g., every 60 minutes or on specific conditions)
    if datetime.now().minute == 0: 
        fetch_crypto_intelligence()
    
    # [Rest of the existing radar logic continues...]
