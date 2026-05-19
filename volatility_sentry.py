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
WS_URL = f"wss://ws.twelvedata.com/v1/quotes?apikey={TD_API_KEY}"

class RockefellerSentry:
    def __init__(self):
        self.vix_last = 0.0
        self.tz = pytz.timezone('Pacific/Honolulu')
        self.macro_windows = ["02:25", "02:30", "02:35", "08:00"]

    def on_message(self, ws, message):
        data = json.loads(message)
        if data.get("event") == "price" and "price" in data:
            try:
                price = float(data.get("price"))
                self.process_volatility_shift(price)
            except (ValueError, TypeError) as e:
                pass

    def process_volatility_shift(self, current_vix):
        """Processes shifts across Natenberg Volatility Surface thresholds without disk overhead."""
        state = EcosystemState()
        
        # Retrieve state from memory cache to check for threshold adjustments
        old_velocity = state.get("vix_velocity", "STABLE")
        
        status = "STABLE"
        rsi_limit = 66
        if current_vix > 19.5:
            status = "ELEVATED"
            rsi_limit = 55

        vix_velocity = "STABLE"
        if self.vix_last > 0.0:
            vix_delta = current_vix - self.vix_last
            if vix_delta < -0.4:
                vix_velocity = "DECAYING"
            elif vix_delta > 0.5:
                vix_velocity = "ACCELERATING"

        self.vix_last = current_vix
        now_hst = datetime.now(self.tz).strftime("%H:%M")
        macro_muted = any(now_hst == window for window in self.macro_windows)

        # Update singleton. Disk writes occur automatically if entries changed.
        state.update({
            "vix_status": status,
            "vix_current": current_vix,
            "vix_velocity": vix_velocity,
            "rsi_shield_limit": rsi_limit,
            "macro_muted": macro_muted,
            "realized_vol_pulse": "STREAMING",
            "last_handshake": datetime.now(self.tz).isoformat()
        })

        # REGIME AWARENESS GATEWAY: Alert only on structural baseline shifts
        if vix_velocity != old_velocity:
            log_event(f"Volatility Sentry detected systemic velocity transition: {old_velocity} -> {vix_velocity}")
            if WEBHOOK_MARKET and vix_velocity == "ACCELERATING":
                payload = {
                    "embeds": [{
                        "title": "⚠️ Volatility Guardrail Alert: Acceleration Detected",
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
                    requests.post(WEBHOOK_MARKET, json=payload, timeout=5)
                except Exception as e:
                    log_event(f"Failed to post velocity warning to Discord matrix: {e}", "ERROR")

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
            on_close=self.on_close
        )
        ws.run_forever()

if __name__ == "__main__":
    sentry = RockefellerSentry()
    sentry.start_sentry()
