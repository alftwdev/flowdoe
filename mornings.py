import os
import sys
import json
import pytz
import logging
import datetime
from dotenv import load_dotenv
from ecosys import EcosystemState, log_event, logger as base_logger

# 1. Initialize Child Logger
logger = logging.getLogger("Morning_Brief")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

def validate_environment():
    required_keys = ["WEBHOOK_MARKET_ANALYSIS"]
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        logger.error(f"CRITICAL: Missing environment variables: {missing}")
        sys.exit(1)

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

validate_environment()

WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
db.get_state("market_regime")

def load_crypto_state_context():
    context = {"btc_price": "N/A", "eth_price": "N/A", "has_crypto": False}
    if not os.path.exists(STATE_FILE):
        return context
    try:
        with open(STATE_FILE, "r") as f:
            state_data = json.load(f)
            
        btc_raw = state_data.get("crypto_BTC_USD")
        eth_raw = state_data.get("crypto_ETH_USD")
        
        if btc_raw:
            price_val = float(btc_raw)
            est_mo = (price_val * 0.125) / 12.0
            context["btc_price"] = f"${price_val:,.2f} (Est: ${est_mo:,.2f}/mo premium)"
            context["has_crypto"] = True
            
        if eth_raw:
            price_val = float(eth_raw)
            est_mo = (price_val * 0.138) / 12.0
            context["eth_price"] = f"${price_val:,.2f} (Est: ${est_mo:,.2f}/mo premium)"
            context["has_crypto"] = True
            
        return context
    except Exception as e:
        logger.warning(f"Crypto State Ingestion Omitted: {e}")
        return context

def check_market_status():
    """Validates if the NYSE is open today."""
    td_key = os.getenv("TWELVE_DATA_API_KEY")
    try:
        res = requests.get(f"https://api.twelvedata.com/market_state?apikey={td_key}", timeout=5).json()
        for market in res:
            if market.get("country") == "United States" and market.get("code") == "NYSE":
                return market.get("is_market_open"), market.get("time_to_open", "Closed")
    except Exception as e:
        logger.error(f"Market state fetch failed: {e}")
    return True, "" # Default to assuming open if API fails

# Inside generate_morning_brief():
is_open, time_to_open = check_market_status()
if not is_open:
    title = "🌅 Rockefeller Morning Intelligence Briefing [MARKET CLOSED]"
    description = f"### **System Standby: Market Holiday**\nCurrently, markets are closed, enjoy the day & some nature. Normal operations will resume at the next active trading bell."
    send_essentials_embed(WEBHOOK_MARKET, title, description, 0x95a5a6)
    return  

def generate_morning_brief():
    tz_h = pytz.timezone('Pacific/Honolulu')
    now = datetime.datetime.now(tz_h)
    
    try:
        with open(REGIME_LEDGER, "r") as f:
            data = json.load(f)
            regime = data.get("regime", "NEUTRAL")
            vix_status = data.get("vix_status", "STABLE")
            rsi_limit = data.get("rsi_shield_limit", 66)
            futures_pulse = data.get("futures_pulse", "🟡 NEUTRAL")
            system_override = data.get("system_override", False)
    except Exception as e:
        logger.warning(f"Core Ledger Load Failed: {e}")
        regime, vix_status, rsi_limit, system_override = "NEUTRAL", "UNKNOWN", 66, False
        futures_pulse = "⚪ UNKNOWN"

    crypto_ctx = load_crypto_state_context()

    if system_override or vix_status == "STORM":
        color = 0xe74c3c 
        posture = "🛡️ DEFENSIVE: Shield is ACTIVE. Capital preservation priority."
        objectives = ["1. NO NEW ENTRIES: Volatility exceeds safety thresholds.", "2. CASH IS A POSITION: Wait for VIX to mean-revert."]
    elif vix_status == "ELEVATED":
        color = 0xf1c40f 
        posture = "⚖️ MEASURED: Volatility is rising. Size positions at 50%."
        objectives = ["1. TIGHT SHIELD: Only entries with RSI < 55.", "2. MONITOR VIX: Any spike above 22 triggers lockdown."]
    else:
        color = 0x2ecc71 
        posture = "🚀 OFFENSIVE: Conditions are STABLE. Execute at full size."
        objectives = ["1. ALPHA HUNT: Follow signals for high-conviction entries.", "2. RSI LIMIT: Standard < 66 threshold applies."]

    if vix_status == "STABLE" and crypto_ctx["has_crypto"]:
        objectives.append("3. THETA HARVEST: Prioritize premium credit options over spot equity chase.")

    title = "🌅 Rockefeller Morning Intelligence Briefing"
    objective_text = "\n".join(objectives)
    
    desc_lines = [
        f"### **Daily Battle Plan: {now.strftime('%b %d, %Y')}**",
        f"**Tactical Posture**: {posture}\n",
        f"📊 **Core Equity Index Metrics**:",
        f"┣ **Market Regime**: `{regime}`",
        f"┣ **Volatility Sentry**: `{vix_status}`",
        f"┗ **RSI Shield**: `Active < {rsi_limit}`\n"
    ]

    if crypto_ctx["has_crypto"]:
        desc_lines.extend([f"🏛️ **Sovereign Liquidity Snapshot**:", f"┣ **BTC Core Matrix**: `{crypto_ctx['btc_price']}`", f"┗ **ETH Core Matrix**: `{crypto_ctx['eth_price']}`\n"])

    desc_lines.extend([f"🛡️ **Tactical Objectives**:", f"{objective_text}\n", f"*Generated by Rockefeller Cross-Asset Architecture.*"])

    description = "\n".join(desc_lines)

    if HAS_ESSENTIALS and WEBHOOK_MARKET:
        if send_essentials_embed(WEBHOOK_MARKET, title, description, color):
            logger.info(f"Cross-Asset Morning Intelligence brief dispatched for {vix_status} regime.")
        else:
            logger.error("Failed to dispatch intelligence brief to Discord.")
    else:
        logger.warning("Dispatch skipped: Webhooks disconnected or Essentials Tools missing.")

if __name__ == "__main__":
    generate_morning_brief()
