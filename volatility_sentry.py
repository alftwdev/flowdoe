import os
import json
import time
import websocket
from datetime import datetime
import pytz
from dotenv import load_dotenv
from ecosys import EcosystemState, log_event

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")

# FIXED: Replaced legacy path with strict Distributed WebSocket System route to resolve 404 Handshake Failure
WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"

class RockefellerSentry:
    def __init__(self):
        self.vix_last = 0.0
        self.tz = pytz.timezone('Pacific/Honolulu')
        self.macro_windows = ["02:25", "02:30", "02:35", "08:00"]

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            # Standard price update message frame validation
            if data.get("event") == "price" and "price" in data:
                price = float(data.get("price"))
                self.process_volatility_shift(price)
        except Exception as e:
            log_event(f"Volatility Sentry data parse exception: {e}", "ERROR")

    def on_open(self, ws):
        """Dispatches accurate subscription dictionary frame directly upon handshake success."""
        subscribe_payload = {
            "action": "subscribe",
            "params": {
                "symbols": "VIX"
            }
        }
        ws.send(json.dumps(subscribe_payload))
        log_event("Volatility Sentry distributed stream handshake opened. Subscription parameters transmitted.")

    def process_volatility_shift(self, current_vix):
        """Processes shifts across Natenberg Volatility Surface thresholds without disk overhead."""
        state = EcosystemState()
        
        old_velocity = state.get("vix_velocity", "NORMAL")
        state.update({"vix_close": current_vix})
        
        now_hst = datetime.now(self.tz)
        current_time_str = now_hst.strftime("%H:%M")
        
        # Systemic Volatility Surface Risk Profiling
        if current_vix > 25.0 and self.vix_last <= 25.0:
            state.update({"vix_status": "HIGH_VOLATILITY"})
            payload = {
                "embeds": [{
                    "title": "⚠️ ROCKEFELLER CRITICAL RISK WARNING: VOLATILITY SPIKE",
                    "description": (
                        f"Real-time tracking indicates rapid momentum shifts across volatility matrices.\n\n"
                        f"┣ **Current VIX Value**: `{current_vix:.2f}`\n"
                        f"┣ **Directional Velocity**: `ACCELERATING`\n"
                        f"┗ **Ecosystem Action**: `Chop suppression active. Sizing criteria restricted.`"
                    ),
                    "color": 0xe74c3c,
                    "timestamp": datetime.now(pytz.utc).isoformat()
                }]
            }
            try:
                import requests
                if WEBHOOK_MARKET:
                    requests.post(WEBHOOK_MARKET, json=payload, timeout=5)
            except Exception as e:
                log_event(f"Failed to post velocity warning to Discord matrix: {e}", "ERROR")
        elif current_vix <= 20.0:
            state.update({"vix_status": "COMPRESSED"})
        else:
            state.update({"vix_status": "STABLE"})
            
        self.vix_last = current_vix

    def on_error(self, ws, error):
        log_event(f"Sentry Error Boundary encountered: {error}", "ERROR")

    def on_close(self, ws, close_status_code, close_msg):
        log_event("Sentry stream disconnected. Forcing reconnect sequence...")
        time.sleep(5)
        self.start_sentry()

    def start_sentry(self):
        log_event("Rockefeller Volatility Sentry: Initiating Stream Handshake...")
        ws = websocket.WebSocketApp(
            WS_URL,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        ws.run_forever()

if __name__ == "__main__":
    sentry = RockefellerSentry()
    sentry.start_sentry()
