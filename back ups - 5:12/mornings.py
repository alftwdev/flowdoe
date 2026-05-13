import os
import json
import pytz
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

WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")

def generate_morning_brief():
    tz_h = pytz.timezone('Pacific/Honolulu')
    now = datetime.now(tz_h)
    
    # 1. Load Data from Ecosystem Ledger
    try:
        with open(REGIME_LEDGER, "r") as f:
            data = json.load(f)
            regime = data.get("regime", "NEUTRAL")
            vix_status = data.get("vix_status", "STABLE")
            rsi_limit = data.get("rsi_shield_limit", 66)
    except:
        regime, vix_status, rsi_limit = "NEUTRAL", "UNKNOWN", 66

    # 2. Determine Rockefeller Tone
    color = 0x2ecc71 if regime == "BULLISH" else 0xe74c3c if regime == "BEARISH" else 0xf1c40f
    
    # 3. Build Brief
    title = "🌅 Rockefeller Morning Intelligence"
    description = (
        f"### **Daily Battle Plan: {now.strftime('%b %d, %Y')}**\n"
        f"The market is opening with a **{regime}** posture.\n\n"
        f"**Risk Parameters**:\n"
        f"┣ **Volatility**: `{vix_status}`\n"
        f"┣ **RSI Shield**: `Active at < {rsi_limit}`\n"
        f"┗ **Conviction**: {'High' if vix_status == 'STABLE' else 'Measured'}\n\n"
        "**Tactical Objectives**:\n"
        "1. Monitor #trade-signals for VWAP reclaims.\n"
        "2. Income focused in #dividend-ccetfs.\n"
        "3. Protect capital at all costs.\n\n"
        f"*Status: Engine firing. Sentry scripts are always-on.*"
    )

    if HAS_ESSENTIALS and WEBHOOK_MARKET:
        send_essentials_embed(WEBHOOK_MARKET, title, description, color)
        print(f"✅ Morning Brief Dispatched at {now.strftime('%H:%M')} HST")

if __name__ == "__main__":
    generate_morning_brief()
