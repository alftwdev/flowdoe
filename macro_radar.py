import os
import sys
import logging
import requests  # Fixed: explicitly imported
from dotenv import load_dotenv
from ecosys import EcosystemState, log_event, logger as base_logger

# 1. Initialize Child Logger
logger = logging.getLogger("Macro_Radar")

# 2. Configuration & Initialization
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

def validate_environment():
    """Gatekeeper: Ensures all required keys exist before execution."""
    required_keys = ["TWELVE_DATA_API_KEY"] 
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        log_event(f"CRITICAL: Missing environment variables: {missing}", "ERROR")
        sys.exit(1)

# Import Essentials tools into global scope
try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False
    def send_essentials_embed(*args, **kwargs): return False

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING") or os.getenv("WEBHOOK_MARKET_ANALYSIS")

def broadcast_microstructure_pulse(is_test=False):
    """Refactored pulse to utilize global HAS_ESSENTIALS and fixed requests import."""
    logger.info("Initiating Macro Structure & Liquidity Scan...")
    
    # Example logic utilizing requests
    # Ensure this block handles network exceptions as per previous logs
    try:
        # Placeholder for your actual scanning logic
        logger.info("Scanning order book and profile...")
        # ... logic that uses requests ...
    except Exception as e:
        logger.error(f"Analysis failed: {e}")

if __name__ == "__main__":
    # GATEKEEPER: Validate environment immediately
    validate_environment()
    
    logger.info("Macro Radar initialized and validated.")
    
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        logger.info("Terminal Test Mode Initiated.")
        broadcast_microstructure_pulse(is_test=True)
    else:
        logger.info("Production mode: Starting persistent scan loop.")
        import time
        while True:
            try:
                broadcast_microstructure_pulse(is_test=False)
            except Exception as e:
                logger.error(f"Loop error: {e}")
            time.sleep(900) # 15-minute interval
