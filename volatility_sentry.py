import os
import sys
import json
import time
import logging
import websocket
from datetime import datetime
import pytz
from dotenv import load_dotenv
from ecosys import EcosystemState, log_event, logger as base_logger

# 1. Initialize Child Logger
logger = logging.getLogger("Volatility_Sentry")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

def validate_environment():
    """Gatekeeper: Prevents 404 Cloudflare websocket errors by ensuring API key exists."""
    required_keys = ["TWELVE_DATA_API_KEY"]
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        logger.error(f"CRITICAL: Missing environment variables: {missing}")
        sys.exit(1)

# GATEKEEPER: Validate before assigning variables
validate_environment()

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"

class RockefellerSentry:
    def __init__(self):
        self.vix_last = 0.0
        self.tz = pytz.timezone('Pacific/Honolulu')

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get("event") == "price" and "price" in data:
                price = float(data.get("price"))
                self.process_volatility_shift(price)
        except Exception as e:
            logger.error(f"Data parse exception: {e}")

    def on_open(self, ws):
        subscribe_payload = {"action": "subscribe", "params": {"symbols": "VIX"}}
        ws.send(json.dumps(subscribe_payload))
        logger.info("Distributed stream handshake opened. Subscription parameters transmitted.")

    def process_volatility_shift(self, current_vix):
        state = EcosystemState()
        state.update({"vix_close": current_vix})
        
        if current_vix > 25.0 and self.vix_last <= 25.0:
            state.update({"vix_status": "HIGH_VOLATILITY"})
            payload = {
                "embeds": [{
                    "title": "⚠️ ROCKEFELLER CRITICAL RISK WARNING: VOLATILITY SPIKE",
                    "description": (f"┣ **Current VIX Value**: `{current_vix:.2f}`\n"
                                    f"┣ **Directional Velocity**: `ACCELERATING`\n"
                                    f"┗ **Ecosystem Action**: `Chop suppression active.`"),
                    "color": 0xe74c3c,
                    "timestamp": datetime.now(pytz.utc).isoformat()
                }]
            }
            try:
                import requests
                if WEBHOOK_MARKET:
                    requests.post(WEBHOOK_MARKET, json=payload, timeout=5)
            except Exception as e:
                logger.error(f"Failed to post velocity warning to Discord: {e}")
        elif current_vix <= 20.0:
            state.update({"vix_status": "COMPRESSED"})
        else:
            state.update({"vix_status": "STABLE"})
            
        self.vix_last = current_vix

    def on_error(self, ws, error):
        logger.error(f"Sentry Error Boundary encountered: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.warning("Stream disconnected. Forcing reconnect sequence in 5s...")
        time.sleep(5)
        self.start_sentry()

    def start_sentry(self):
        logger.info("Initiating Stream Handshake...")
        ws = websocket.WebSocketApp(WS_URL, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close, on_open=self.on_open)
        ws.run_forever()

if __name__ == "__main__":
    logger.info("Rockefeller Volatility Sentry Armed.")
    sentry = RockefellerSentry()
    sentry.start_sentry()
