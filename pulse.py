import os
import time
import logging
from datetime import datetime
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase
from essentials_tools import send_essentials_embed
from ai import generate_retail_translation

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
    vix_iv = float(db.get_state("vix_iv_index", 20.0))
    vrp = float(db.get_state("SPY_vrp_latest", 0.0))
    
    # Mathematical State Hash for the Gatekeeper Shift Detection
    vix_regime = "HIGH" if vix_iv > 24 else "MED" if vix_iv > 19 else "LOW"
    vrp_regime = "POS" if vrp > 0 else "NEG"
    cred_regime = "STRESS" if cred_spread > 4.5 else "STABLE"
    macro_state_hash = f"{vix_regime}_{vrp_regime}_{cred_regime}"
    
    for sector, webhook in WEBHOOKS.items():
        if not webhook: continue
        
        # --- 3-Strike Dynamic Gatekeeper Logic ---
        last_state = db.get_state(f"gatekeeper_state_{sector}", "")
        strikes = int(db.get_state(f"gatekeeper_strikes_{sector}", 0))
        
        if macro_state_hash != last_state:
            logger.info(f"[{sector.upper()}] Math shifted ({last_state} -> {macro_state_hash}). Resetting Gatekeeper.")
            strikes = 0
            db.update_state(f"gatekeeper_state_{sector}", macro_state_hash)
        
        if strikes >= 3:
            continue # Prop-firm silence active
            
        last_ping = float(db.get_state(f"last_ping_{sector}", 0.0))
        if (current_time - last_ping) > SILENCE_THRESHOLDS[sector]:
            logger.info(f"Dispatching translation pulse for {sector} (Strike {strikes + 1}/3)...")
            
            intel = generate_retail_translation(sector, net_liq, cred_spread, vix_iv, vrp)
            
            if sector == "crypto":
                z_score, z_status = get_crypto_supplements()
                intel['payload'] += f"\n\n⚡ **BTC Volatility Z-Score**: `{z_score:.2f}` ({z_status})"
            
            try:
                send_essentials_embed(webhook, intel['title'], intel['payload'], intel['color'])
                db.update_state(f"last_ping_{sector}", current_time)
                db.update_state(f"gatekeeper_strikes_{sector}", strikes + 1)
            except Exception as e:
                logger.error(f"Failed to dispatch pulse to {sector}: {e}")

if __name__ == "__main__":
    logger.info("Quantitative Pulse Engine initialized. Gatekeeper & Translation Layer Active.")
    while True:
        try:
            check_and_dispatch_pulse()
            time.sleep(3600)
        except Exception as e:
            logger.error(f"Pulse Engine Fault: {e}")
            time.sleep(300)
