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

    def evaluate_proximity_metrics(self, symbol, price):
        if symbol not in ["SPY", "QQQ", "XAU/USD", "EUR/USD", "GBP/USD", "USD/JPY"]: 
            return
        
        # Map asset vectors to their proper database keys and targets
        if symbol in ["SPY", "QQQ"]:
            upper = float(db.get_state(f"{symbol}_expected_upper", 0.0))
            lower = float(db.get_state(f"{symbol}_expected_lower", 0.0))
            target_webhook = WEBHOOK_OPTIONS
            precision_pct = 0.0015
        else:
            upper = float(db.get_state(f"{symbol}_upper_noise", 0.0))
            lower = float(db.get_state(f"{symbol}_lower_noise", 0.0))
            target_webhook = WEBHOOK_FOREX
            precision_pct = 0.0005

        if upper == 0 or lower == 0 or not target_webhook: 
            return

        # Perimeter Breach Evaluation Mechanics
        if price >= upper * (1.0 - precision_pct):
            state_token = f"{symbol}_CEILING_BREACH_AT_{price:.4f}"
            should_broadcast = db.track_and_limit_alerts(
                alert_id=f"SENTRY_{symbol}_UPPER",
                current_state=state_token,
                current_trigger=price,
                max_broadcasts=3,
                threshold_pct=0.002
            )
            if should_broadcast and HAS_ESSENTIALS:
                payload = f"🎯 **[{symbol} Perimeter Alert]**\n┣ Spot Level: `{price:,.4f}`\n┗ ⚠️ Volatility Ceiling Compression reached. Long momentum restricted."
                send_essentials_embed(target_webhook, f"🚨 Volatility Boundary Hit: {symbol}", payload, 0xe74c3c)
                
        elif price <= lower * (1.0 + precision_pct):
            state_token = f"{symbol}_FLOOR_BREACH_AT_{price:.4f}"
            should_broadcast = db.track_and_limit_alerts(
                alert_id=f"SENTRY_{symbol}_LOWER",
                current_state=state_token,
                current_trigger=price,
                max_broadcasts=3,
                threshold_pct=0.002
            )
            if should_broadcast and HAS_ESSENTIALS:
                payload = f"🎯 **[{symbol} Perimeter Alert]**\n┣ Spot Level: `{price:,.4f}`\n┗ ⚠️ Volatility Floor Compression reached. Short execution high-risk."
                send_essentials_embed(target_webhook, f"🚨 Volatility Boundary Hit: {symbol}", payload, 0x2ecc71)

    def process_crypto_volatility(self, price):
        current_time = time.time()
        self.btc_window.append((current_time, price))
        while self.btc_window and (current_time - self.btc_window[0][0] > 3600):
            self.btc_window.pop(0)
            
        pct_change = (price - self.btc_window[0][1]) / self.btc_window[0][1]
        if abs(pct_change) >= 0.025:
            state_token = f"BTC_SPEED_{round(pct_change, 4)}"
            should_broadcast = db.track_and_limit_alerts(
                alert_id="SENTRY_BTC_VOL",
                current_state=state_token,
                current_trigger=price,
                max_broadcasts=3,
                threshold_pct=0.01
            )
            if should_broadcast and WEBHOOK_CRYPTO and HAS_ESSENTIALS:
                payload = f"🪙 **[BTC/USD Telemetry]**\n┣ Spot Rate: `${price:,.2f}`\n┗ ⚠️ Rolling Hourly Velocity Breach: `{pct_change*100:+.2f}%` directional momentum detected."
                send_essentials_embed(WEBHOOK_CRYPTO, "⚡ Volatility Sentry Trigger", payload, 0xf39c12)

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            symbol = data.get("symbol")
            price = float(data.get("price", 0.0))
            if not symbol or price == 0: 
                return

            if symbol in ["SPY", "QQQ", "XAU/USD", "EUR/USD", "GBP/USD", "USD/JPY"]:
                self.evaluate_proximity_metrics(symbol, price)
            elif symbol == "BTC/USD":
                self.process_crypto_volatility(price)
            elif symbol == "VIX":
                db.update_state("vix_iv_index", price)
        except: 
            pass

    def on_open(self, ws):
        logger.info("Websocket pipeline connected. Initializing unified stream monitor...")
        subscription_message = {
            "action": "subscribe",
            "params": {"symbols": "SPY,QQQ,VIX,XAU/USD,EUR/USD,GBP/USD,USD/JPY,BTC/USD"}
        }
        ws.send(json.dumps(subscription_message))

    def start_sentry(self):
        ws = websocket.WebSocketApp(self.ws_url, on_message=self.on_message, on_open=self.on_open)
        ws.run_forever()

if __name__ == "__main__":
    sentry = ConsolidatedSentry()
    sentry.start_sentry()
