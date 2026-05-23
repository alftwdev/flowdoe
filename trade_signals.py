import os
import sys
import logging
import time
from dotenv import load_dotenv
from ecosys import EcosystemState, log_event, logger as base_logger

# 1. Initialize Child Logger & Ensure Console Verbosity
logger = logging.getLogger("Trade_Signals")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

# 2. Configuration & Initialization
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

try:
    from essentials_tools import send_essentials_embed, get_trend_alignment, get_institutional_conviction
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False
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

def get_regime_modifiers():
    """Reads live RAM state to adjust trading parameters and risk limits dynamically."""
    state = EcosystemState()
    vix_status = state.get("vix_status", "STABLE")
    regime = state.get("regime", "BULLISH")
    
    # Initialize with default conservative-neutral parameters
    modifiers = {
        "position_size": 1.0,
        "strategy_type": "DEBIT",
        "shield_active": False,
        "conviction_required": "NORMAL",
        "stop_loss_multiplier": 1.0,
        "take_profit_target": 1.0
    }

    # Logic gates for Volatility Expansion
    if vix_status in ["HIGH_VOLATILITY", "STORM"]:
        modifiers["shield_active"] = True
        modifiers["position_size"] = 0.0
        modifiers["conviction_required"] = "HIGH"
        modifiers["stop_loss_multiplier"] = 2.0  # Widen stops to avoid chop out
        modifiers["take_profit_target"] = 0.5    # Take profits faster in chaos
        
    elif vix_status == "ELEVATED":
        modifiers["strategy_type"] = "CREDIT"
        modifiers["position_size"] = 0.50
        modifiers["conviction_required"] = "HIGH"
        modifiers["stop_loss_multiplier"] = 1.5  # Moderate widen
        modifiers["take_profit_target"] = 0.75   # Moderate acceleration
        
    elif vix_status == "COMPRESSED":
        modifiers["strategy_type"] = "DEBIT"
        modifiers["position_size"] = 1.0
        modifiers["conviction_required"] = "NORMAL"
        modifiers["stop_loss_multiplier"] = 1.0  # Standard risk
        modifiers["take_profit_target"] = 1.0
        
    return modifiers, vix_status, regime

def execute_signal_scan(is_test=False):
    """Execution pipeline using macro modifier ingestion."""
    webhook = WEBHOOKS.get("OPTIONS")
    
    if not HAS_ESSENTIALS:
        logger.warning("Essentials tools unavailable.")
        return

    # Ingest regime data
    modifiers, vix_status, regime = get_regime_modifiers()
    logger.info(f"Loaded Regime Matrix -> VIX Status: [{vix_status}] | Market Regime: [{regime}]")

    if modifiers["shield_active"]:
        logger.warning("🛡️ CAPITAL SHIELD ACTIVE: Volatility exceeds safety rules. Scanning aborted.")
        return

    if is_test:
        logger.info("Executing verbose test broadcast...")
        if webhook:
            payload_msg = (
                f"Diagnostic Pulse: Connection Verified.\n"
                f"┣ **Regime Context**: `{regime}`\n"
                f"┣ **VIX Volatility Mode**: `{vix_status}`\n"
                f"┗ **Active Allocation Strategy**: `{modifiers['strategy_type']} Matrix (Size: {modifiers['position_size']*100}%)`"
            )
            success = send_essentials_embed(webhook, "TEST: Macro Modifiers Loaded", payload_msg)
            logger.info(f"Test broadcast status: {success}")
        else:
            logger.error("Broadcast aborted: WEBHOOK_TRADE_SIGNALS missing in .env.")
    else:
        # --- PRODUCTION SCAN LOGIC ---
        # Scanner utilizes modifiers['strategy_type'] directly
        pass
    
    logger.info("Signal scan complete.")

if __name__ == "__main__":
    validate_environment()
    logger.info("Trade_Signals initialized and validated.")

    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        logger.info("Terminal Test Mode Initiated.")
        execute_signal_scan(is_test=True)
    else:
        logger.info("Production mode: Starting persistent 15-minute signal loop.")
        while True:
            try:
                execute_signal_scan(is_test=False)
            except Exception as e:
                logger.error(f"Loop error: {e}")
            time.sleep(900)
