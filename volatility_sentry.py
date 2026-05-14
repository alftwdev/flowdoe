import os
import json
import time
import websocket
from dotenv import load_dotenv

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")
# Use the Twelve Data WebSocket URL for Futures/Indices
WS_URL = f"wss://ws.twelvedata.com/v1/quotes?apikey={TD_API_KEY}"

class RockefellerSentry:
    def __init__(self):
        self.vix_last = 0
        self.skew_alert = False

    def on_message(self, ws, message):
        data = json.loads(message)
        if data.get("event") == "price":
            # Extract real-time volatility proxy (e.g., VIX or /ES movement)
            price = float(data.get("price", 0))
            self.process_volatility_shift(price)

    def process_volatility_shift(self, current_vix):
        """Dynamic Risk Adjustment based on Natenberg's Volatility Surface."""
        try:
            with open(REGIME_LEDGER, "r") as f:
                ledger = json.load(f)
        except: ledger = {}

        # 1. DEFINE NATENBERG REGIMES
        if current_vix > 30:
            status, rsi_limit = "CRITICAL (System Lockdown)", 40
        elif current_vix > 20:
            status, rsi_limit = "ELEVATED (Defensive Scalp)", 52
        else:
            status, rsi_limit = "STABLE (Full Offensive)", 68

        # 2. UPDATE ECOSYSTEM LEDGER
        ledger.update({
            "vix_status": status,
            "rsi_shield_limit": rsi_limit,
            "realized_vol_pulse": "STREAMING",
            "last_handshake": time.strftime('%Y-%m-%dT%H:%M:%S')
        })

        with open(REGIME_LEDGER, "w") as f:
            json.dump(ledger, f, indent=4)

    def start_sentry(self):
        """Initialize the Twelve Data WebSocket."""
        print("🛡️ Rockefeller Sentry: Activating WebSocket Stream...")
        ws = websocket.WebSocketApp(
            WS_URL,
            on_message=self.on_message,
            on_open=lambda ws: ws.send(json.dumps({"action": "subscribe", "params": {"symbols": "VIX"}}))
        )
        ws.run_forever()

if __name__ == "__main__":
    sentry = RockefellerSentry()
    sentry.start_sentry()
