import os
import sys
import logging
from dotenv import load_dotenv
from ecosys import EcosystemState, log_event, logger as base_logger

# 1. Initialize Child Logger
logger = logging.getLogger("Trade_Signals")

# 2. Configuration & Initialization
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Import required tools into global scope
try:
    from essentials_tools import send_essentials_embed, get_trend_alignment, get_institutional_conviction
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False
    # Define a dummy function to prevent NameError if import fails
    def send_essentials_embed(*args, **kwargs): return False

def validate_environment():
    """Gatekeeper: Ensures required keys exist before execution begins."""
    required_keys = ["WEBHOOK_FUTURES_TRADING", "WEBHOOK_TRADE_SIGNALS"]
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        log_event(f"CRITICAL: Missing environment variables: {missing}", "ERROR")
        sys.exit(1)

STATE_FILE = os.path.join(BASE_DIR, "last_trade_alerts.json")
WEBHOOKS = {
    "FUTURES": os.getenv("WEBHOOK_FUTURES_TRADING"),
    "OPTIONS": os.getenv("WEBHOOK_TRADE_SIGNALS"),
    "SENTRY": os.getenv("WEBHOOK_MARKET_ANALYSIS")
}

def execute_signal_scan(is_test=False):
    """Execution pipeline using validated tools."""
    webhook = WEBHOOKS.get("OPTIONS")
    
    if not HAS_ESSENTIALS:
        logger.warning("Essentials tools unavailable.")
        return

    if is_test:
        logger.info("Executing test broadcast...")
        if webhook:
            success = send_essentials_embed(webhook, "TEST", "Diagnostic Pulse: Connection Verified.")
            logger.info(f"Test broadcast status: {success}")
        else:
            logger.error("Broadcast aborted: WEBHOOK_TRADE_SIGNALS missing in .env.")
    
    # ... rest of your signal logic
    logger.info("Signal scan complete.")

if __name__ == "__main__":
    validate_environment()
    
    logger.info("Trade_Signals initialized and validated.")

    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        logger.info("Terminal Test Mode Initiated.")
        execute_signal_scan(is_test=True)
    else:
        logger.info("Production mode: Starting persistent signal loop.")
        import time
        while True:
            try:
                execute_signal_scan(is_test=False)
            except Exception as e:
                logger.error(f"Loop error: {e}")
            time.sleep(900)
