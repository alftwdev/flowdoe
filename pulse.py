import os
import sys
import time
import logging
from datetime import datetime, timedelta
import pytz
import requests
import numpy as np
from dotenv import load_dotenv
from database import EcosystemDatabase
from ai import generate_ai_macro_brief
from essentials_tools import send_essentials_embed

logger = logging.getLogger("Pulse_Engine")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

db = EcosystemDatabase()

# Webhooks and API Keys
PUSHOVER_APP_TOKEN = os.getenv("PUSHOVER_API_TOKEN")
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")

WEBHOOKS = {
    "options": os.getenv("WEBHOOK_TRADE_SIGNALS"),
    "crypto": os.getenv("WEBHOOK_CRYPTO"),
    "tsp": os.getenv("WEBHOOK_FED"),
    "macro": os.getenv("WEBHOOK_FOREX")
}

# Silence thresholds (in seconds)
SILENCE_THRESHOLDS = {
    "options": 14400,  # 4 hours
    "crypto": 21600,   # 6 hours
    "tsp": 28800,      # 8 hours
    "macro": 14400     # 4 hours
}

def notify_admin_pushover(message):
    if not PUSHOVER_APP_TOKEN or not PUSHOVER_USER_KEY:
        return
    try:
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": PUSHOVER_APP_TOKEN,
            "user": PUSHOVER_USER_KEY,
            "message": message,
            "title": "Pulse Engine Alert"
        }, timeout=5)
    except Exception as e:
        logger.error(f"Pushover failure: {e}")

def get_fear_and_greed():
    """Fetches real-time Crypto Sentiment."""
    try:
        url = "https://api.alternative.me/fng/?limit=1"
        response = requests.get(url, timeout=5).json()
        data = response['data'][0]
        return f"{data['value_classification']} ({data['value']})"
    except Exception:
        return "N/A"

def get_crypto_supplements():
    """Extracts Z-Score and Correlation primitives for the Crypto Pulse."""
    try:
        fng = get_fear_and_greed()
        z_score = db.get_state("btc_vol_z_score", 0.0)
        z_status = "COMPRESSION" if z_score < -2.0 else "EXPANSION" if z_score > 2.0 else "NOMINAL"
        return fng, z_score, z_status
    except Exception:
        return "N/A", 0.0, "NOMINAL"

def check_and_dispatch_pulse():
    current_time = time.time()
    tz_h = pytz.timezone('Pacific/Honolulu')
    now_hst = datetime.now(tz_h)
    
    if not (6 <= now_hst.hour <= 16):
        return

    try:
        vix_iv = db.get_state("vix_iv_index", 20.0)
        net_liq = db.get_state("net_liquidity", 7000.0)
    except Exception as e:
        logger.error(f"Database access fault: {e}")
        return
    
    for sector, webhook in WEBHOOKS.items():
        if not webhook: continue
        
        try:
            last_ping = db.get_state(f"last_ping_{sector}", 0.0)
        except Exception:
            continue
            
        time_silent = current_time - last_ping
        
        if time_silent > SILENCE_THRESHOLDS[sector]:
            logger.info(f"Silence threshold breached for {sector}. Generating AI Pulse...")
            
            history_data = f"Sector: {sector.upper()}. No actionable mathematical setups detected in the last {SILENCE_THRESHOLDS[sector]//3600} hours. VIX at {vix_iv}."
            ai_intel = generate_ai_macro_brief(history_data, net_liq, credit_spread=3.5)
            
            # --- SUPPLEMENTAL INJECTS ---
            supplemental_text = ""
            if sector == "crypto":
                fng, z_score, z_status = get_crypto_supplements()
                supplemental_text = (
                    f"🔮 **Crypto Sentiment**: `{fng}`\n"
                    f"⚡ **BTC Volatility Z-Score**: `{z_score:.2f}` ({z_status})\n"
                )
            
            payload = (
                f"### 📡 Sector Update: {sector.upper()} Matrix\n"
                f"{ai_intel.get('discord_embed_brief', 'Awaiting structural market development.')}\n\n"
                f"{supplemental_text}"
                f"**System Status**: `NO ACTIONABLE SETUPS` | **VIX**: `{vix_iv}`\n"
                f"*The quantitative engine is suppressing signals to protect capital during sub-optimal conditions.*"
            )
            
            try:
                send_essentials_embed(webhook, f"Sector Pulse & Analysis", payload, 0x34495e)
                db.update_state(f"last_ping_{sector}", time.time())
                logger.info(f"Successfully dispatched pulse to {sector}.")
            except Exception as e:
                logger.error(f"Failed to dispatch pulse to {sector}: {e}")

if __name__ == "__main__":
    logger.info("Pulse Engine initialized. Monitoring ecosystem silence...")
    notify_admin_pushover("Pulse Engine successfully deployed to the Always-On slot.")
    
    while True:
        try:
            check_and_dispatch_pulse()
            time.sleep(3600)
        except Exception as e:
            logger.error(f"Pulse Engine Fault: {e}")
            # Suppressing rapid notification spam on DB faults
            if "malformed" not in str(e).lower():
                notify_admin_pushover(f"Pulse Engine Fault: {e}")
            time.sleep(300)
