import os
import json
import time
import websocket
from datetime import datetime
import pytz
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")
WS_URL = f"wss://ws.twelvedata.com/v1/quotes?apikey={TD_API_KEY}"

class RockefellerSentry:
    def __init__(self):
        self.vix_last = 0.0
        self.tz = pytz.timezone('Pacific/Honolulu')
        # Macro news release windows (HST) where execution must be paused automatically
        self.macro_windows = ["02:25", "02:30", "02:35", "08:00"]

    def on_message(self, ws, message):
        data = json.loads(message)
        
        # Protect against unhandled non-numeric strings during connection setups
        if data.get("event") == "price" and "price" in data:
            try:
                price = float(data.get("price"))
                self.process_volatility_shift(price)
            except (ValueError, TypeError) as e:
                print(f"⚠️ Parsing pass skipped: {e}")

    def process_volatility_shift(self, current_vix):
        """Processes real-time shifts across Natenberg Volatility Surface thresholds."""
        try:
            with open(REGIME_LEDGER, "r") as f:
                ledger = json.load(f)
        except Exception: 
            ledger = {}

        # 1. Evaluate Risk Posture Surface Limits
        if current_vix > 30:
            status, rsi_limit = "CRITICAL (System Lockdown)", 40
        elif current_vix > 20:
            status, rsi_limit = "ELEVATED (Defensive Scalp)", 52
        else:
            status, rsi_limit = "STABLE (Full Offensive)", 68

        # 2. Track Speed Velocity Spikes
        vix_velocity = "NOMINAL"
        if self.vix_last > 0:
            vix_delta = current_vix - self.vix_last
            if vix_delta > 1.5:
                vix_velocity = "CRITICAL_SPIKE"
            elif vix_delta > 0.5:
                vix_velocity = "ACCELERATING"

        self.vix_last = current_vix

        # 3. Assess Current Clock State Against Macro Constraints
        now_hst = datetime.now(self.tz).strftime("%H:%M")
        macro_muted = any(now_hst == window for window in self.macro_windows)

        # 4. Strict State Persistence (Applies updates non-destructively)
        ledger.update({
            "vix_status": status,
            "vix_current": current_vix,
            "vix_velocity": vix_velocity,
            "rsi_shield_limit": rsi_limit,
            "macro_muted": macro_muted,
            "realized_vol_pulse": "STREAMING",
            "last_handshake": datetime.now(self.tz).isoformat()
        })

        with open(REGIME_LEDGER, "w") as f:
            json.dump(ledger, f, indent=4)

    def on_error(self, ws, error):
        print(f"🛡️ Sentry Error Boundary: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print("🛡️ Sentry Stream disconnected. Forcing reconnect cycle...")
        time.sleep(5)
        self.start_sentry()

    def start_sentry(self):
        print("🛡️ Rockefeller Volatility Sentry: Initiating Stream Handshake...")
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
