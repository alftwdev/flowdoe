import os
import time
import logging
from datetime import datetime
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase
from essentials_tools import send_essentials_embed
from ai import generate_ai_macro_brief

logger = logging.getLogger("Pulse_Engine")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

db = EcosystemDatabase()

WEBHOOKS = {
    "options": os.getenv("WEBHOOK_TRADE_SIGNALS"),
    "crypto": os.getenv("WEBHOOK_CRYPTO"),
    "tsp": os.getenv("WEBHOOK_FED"),
    "macro": os.getenv("WEBHOOK_FOREX")
}

SILENCE_THRESHOLDS = {
    "options": 14400,
    "crypto": 21600,
    "tsp": 28800,
    "macro": 14400
}

def get_crypto_supplements():
    z_score = float(db.get_state("btc_vol_z_score", 0.0))
    z_status = "COMPRESSION" if z_score < -2.0 else "EXPANSION" if z_score > 2.0 else "NOMINAL"
    return z_score, z_status

def check_and_dispatch_pulse():
    current_time = time.time()
    tz_h = pytz.timezone('Pacific/Honolulu')
    now_hst = datetime.now(tz_h)
    
    if not (6 <= now_hst.hour <= 16):
        return

    net_liq = float(db.get_state("net_liquidity", 7000.0))
    cred_spread = float(db.get_state("credit_spread", 3.5))
    
    for sector, webhook in WEBHOOKS.items():
        if not webhook: continue
        
        last_ping = float(db.get_state(f"last_ping_{sector}", 0.0))
        if (current_time - last_ping) > SILENCE_THRESHOLDS[sector]:
            logger.info(f"Silence threshold breached for {sector}. Generating Quantitative Pulse...")
            
            ai_intel = generate_ai_macro_brief(fred_liquidity=net_liq, credit_spread=cred_spread)
            
            supplemental_text = ""
            if sector == "crypto":
                z_score, z_status = get_crypto_supplements()
                supplemental_text = f"⚡ **BTC Volatility Z-Score**: `{z_score:.2f}` ({z_status})\n\n"
            
            payload = (
                f"### 📡 Sector Update: {sector.upper()} Matrix\n"
                f"{ai_intel.get('discord_embed_brief')}\n\n"
                f"{supplemental_text}"
                f"**System Status**: `NO ACTIONABLE SETUPS`\n"
                f"*The quantitative engine is suppressing signals to protect capital during sub-optimal conditions.*"
            )
            
            try:
                send_essentials_embed(webhook, f"Sector Pulse & Analysis", payload, 0x34495e)
                db.update_state(f"last_ping_{sector}", current_time)
            except Exception as e:
                logger.error(f"Failed to dispatch pulse to {sector}: {e}")

if __name__ == "__main__":
    logger.info("Quantitative Pulse Engine initialized. Bypassing LLM.")
    while True:
        try:
            check_and_dispatch_pulse()
            time.sleep(3600)
        except Exception as e:
            logger.error(f"Pulse Engine Fault: {e}")
            time.sleep(300)
