import os
import sys
import json
import time
import logging
import numpy as np
import websocket
from dotenv import load_dotenv
from database import EcosystemDatabase

logger = logging.getLogger("Proximity_Sentry")
logging.basicConfig(level=logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

WEBHOOK_OPTIONS = os.getenv("WEBHOOK_OPTIONS_SIGNALS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_CRYPTO = os.getenv("WEBHOOK_CRYPTO")
WEBHOOK_FOREX = os.getenv("WEBHOOK_FOREX")

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False
    def send_essentials_embed(*args, **kwargs): pass

class ConsolidatedSentry:
    def __init__(self):
        self.ws_url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={os.getenv('TWELVE_DATA_API_KEY')}"
        self.btc_window = []
        self.strike_tracker = {}

    def enforce_gatekeeper(self, alert_id, key_hash, price):
        """Standardized 3-Strike Rule with 4-Hour Reset Architecture"""
        current_time = time.time()
        tracker = self.strike_tracker.get(alert_id, {"hash": "", "strikes": 0, "time": 0})
        
        if tracker["hash"] != key_hash or (current_time - tracker["time"] > 14400):
            tracker = {"hash": key_hash, "strikes": 1, "time": current_time}
            self.strike_tracker[alert_id] = tracker
            return True
            
        if tracker["strikes"] < 3:
            tracker["strikes"] += 1
            tracker["time"] = current_time
            self.strike_tracker[alert_id] = tracker
            return True
            
        return False

    def evaluate_proximity_metrics(self, symbol, price):
        if symbol not in ["SPY", "QQQ", "XAU/USD"]: return
        
        if symbol in ["SPY", "QQQ"]:
            upper = float(db.get_state(f"{symbol}_expected_upper", 0.0))
            lower = float(db.get_state(f"{symbol}_expected_lower", 0.0))
            target_webhook = WEBHOOK_OPTIONS
        else:
            upper = float(db.get_state("XAU/USD_upper_noise", 0.0))
            lower = float(db.get_state("XAU/USD_lower_noise", 0.0))
            target_webhook = WEBHOOK_FOREX

        if upper == 0 or lower == 0: return

        # Perimeter Breach Evaluation Mechanics
        if price >= upper * 0.9985:
            key_hash = f"{symbol}_CEILING_BREACH"
            if self.enforce_gatekeeper(symbol, key_hash, price) and target_webhook:
                payload = f"🎯 **[{symbol} Flowstate]**\n┣ Spot: `${price:,.2f}` | Volatility Ceiling Compression reached.\n┗ *Risk Rule: Confinement boundary active. Momentum longs restricted.*"
                send_essentials_embed(target_webhook, f"🚨 Boundary Hit: {symbol}", payload, 0xe74c3c)
                
        elif price <= lower * 1.0015:
            key_hash = f"{symbol}_FLOOR_BREACH"
            if self.enforce_gatekeeper(symbol, key_hash, price) and target_webhook:
                payload = f"🎯 **[{symbol} Flowstate]**\n┣ Spot: `${price:,.2f}` | Volatility Floor Compression reached.\n┗ *Risk Rule: Potential well base active. Short execution high-risk.*"
                send_essentials_embed(target_webhook, f"🚨 Boundary Hit: {symbol}", payload, 0x2ecc71)

    def process_crypto_volatility(self, price):
        current_time = time.time()
        self.btc_window.append((current_time, price))
        while self.btc_window and (current_time - self.btc_window[0][0] > 3600):
            self.btc_window.pop(0)
            
        pct_change = (price - self.btc_window[0][1]) / self.btc_window[0][1]
        if abs(pct_change) >= 0.025:
            key_hash = f"BTC_VOL_{round(pct_change, 3)}"
            if self.enforce_gatekeeper("BTC_VOL_ALERT", key_hash, price) and WEBHOOK_CRYPTO:
                payload = f"🪙 **[BTC/USD Telemetry]**\n┣ Spot Rate: `${price:,.2f}`\n┗ ⚠️ Rolling Hourly Velocity Breach: `{pct_change*100:+.2f}%` directional momentum detected."
                send_essentials_embed(WEBHOOK_CRYPTO, "⚡ Volatility Sentry Trigger", payload, 0xf39c12)

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            symbol = data.get("symbol")
            price = float(data.get("price", 0.0))
            if not symbol or price == 0: return

            if symbol in ["SPY", "QQQ", "XAU/USD"]:
                self.evaluate_proximity_metrics(symbol, price)
            elif symbol == "BTC/USD":
                self.process_crypto_volatility(price)
            elif symbol == "VIX":
                db.update_state("vix_iv_index", price)
        except Exception: pass

    def on_open(self, ws):
        logger.info("WS Pipeline active. Monitoring SPY, QQQ, VIX, XAU/USD, BTC/USD...")
        ws.send(json.dumps({"action": "subscribe", "params": {"symbols": "SPY,QQQ,VIX,XAU/USD,BTC/USD"}}))

    def start_sentry(self):
        ws = websocket.WebSocketApp(self.ws_url, on_message=self.on_message, on_open=self.on_open)
        ws.run_forever()

if __name__ == "__main__":
    sentry = ConsolidatedSentry()
    sentry.start_sentry()
