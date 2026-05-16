import os
import json
import time
import websocket
from datetime import datetime
import pytz
from dotenv import load_dotenv

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")
WS_URL = f"wss://ws.twelvedata.com/v1/quotes?apikey={TD_API_KEY}"

class RockefellerSentry:
    def __init__(self):
        self.vix_last = 0.0
        self.tz = pytz.timezone('Pacific/Honolulu')
        # Dynamic macro release filters (HST) - Blocks critical economic windows (e.g., 08:30 EST data releases)
        self.macro_windows = ["02:25", "02:26", "02:27", "02:28", "02:29", "02:30", "02:31", "02:32", "02:33", "02:34", "02:35", "08:00", "08:05"]

    def on_message(self, ws, message):
        data = json.loads(message)
        
        # Hardened filter: Process streaming pricing data only, ignoring connection handshakes
        if data.get("event") == "price" and "price" in data:
            try:
                price = float(data.get("price"))
                self.process_volatility_shift(price)
            except (ValueError, TypeError) as e:
                print(f"⚠️ Sentry Parsing Pass: {e}")

    def process_volatility_shift(self, current_vix):
        """Applies dynamic risk thresholds utilizing Natenberg's Volatility Surface benchmarks."""
        try:
            with open(REGIME_LEDGER, "r") as f:
                ledger = json.load(f)
        except Exception: 
            ledger = {}

        # 1. Evaluate Surface Thresholds
        if current_vix > 30:
            status, rsi_limit = "CRITICAL (System Lockdown)", 40
        elif current_vix > 20:
            status, rsi_limit = "ELEVATED (Defensive Scalp)", 52
        else:
            status, rsi_limit = "STABLE (Full Offensive)", 68

        # 2. Calculate Intraday Volatility Velocity Spikes
        vix_velocity = "NOMINAL"
        if self.vix_last > 0:
            vix_delta = current_vix - self.vix_last
            if vix_delta > 1.5:
                vix_velocity = "CRITICAL_SPIKE"
            elif vix_delta > 0.5:
                vix_velocity = "ACCELERATING"

        self.vix_last = current_vix

        # 3. Assess Current Clock State against Macro Kill-Switch Windows
        now_hst = datetime.now(self.tz).strftime("%H:%M")
        macro_muted = any(now_hst == window for window in self.macro_windows)

        # 4. State Persistence: Non-destructive update to preserve structural tracking indices
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
        print(f"🛡️ SENTRY ERROR: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print("🛡️ SENTRY CLOSED: Reconnecting in 5 seconds...")
        time.sleep(5)
        self.start_sentry()

    def start_sentry(self):
        """Deploys a persistent stream connection to the Twelve Data index WebSocket."""
        print("🛡️ Rockefeller Sentry: Activating WebSocket Stream...")
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
