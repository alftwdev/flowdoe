import os
import sys
import json
import time
import logging
import requests
import websocket
from datetime import datetime
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase

logger = logging.getLogger("Volatility_Crypto_Sentry")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_CRYPTO = os.getenv("WEBHOOK_CRYPTO") or WEBHOOK_MARKET

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False
    def send_essentials_embed(*args, **kwargs): return False

db = EcosystemDatabase()
WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"

class ConsolidatedSentry:
    def __init__(self):
        self.state_memory = {
            "vix_last": 0.0,
            "btc_last": 0.0,
            "eth_last": 0.0,
            "last_write_time": 0,
            "last_alpha_post_time": 0
        }
        self.tz = pytz.timezone('Pacific/Honolulu')
        self.btc_window = []
        self.eth_window = []
        self.last_alert_time = {"BTC": 0, "ETH": 0}
        self.volatility_threshold = 0.025 
        self.reconnect_attempts = 0

    def write_to_state(self, btc_price, eth_price):
        current_time = time.time()
        if current_time - self.state_memory["last_write_time"] > 60:
            data = {
                "btc_price": f"${btc_price:,.2f}", 
                "eth_price": f"${eth_price:,.2f}",
                "timestamp": datetime.now(self.tz).isoformat()
            }
            db.update_state("crypto_live_state", data)
            self.state_memory["last_write_time"] = current_time

    def process_vix(self, price):
        self.state_memory["vix_last"] = price
        vix_status = "CRITICAL SPARK" if price > 24.0 else "ELEVATED" if price > 19.0 else "NOMINAL"
        db.update_state("market_regime", {"vix_status": vix_status, "vix_price": price})
        db.update_state("vix_iv_index", price)

    def process_rolling_volatility(self, symbol, current_price):
        current_time = time.time()
        window = self.btc_window if symbol == "BTC" else self.eth_window
        window.append((current_time, current_price))
        
        while window and (current_time - window[0][0] > 3600):
            window.pop(0)
            
        if len(window) < 2: return

        initial_price = window[0][1]
        pct_change = (current_price - initial_price) / initial_price
        
        if abs(pct_change) >= self.volatility_threshold:
            if current_time - self.last_alert_time[symbol] > 3600:
                self.last_alert_time[symbol] = current_time
                self.dispatch_volatility_alert(symbol, current_price, pct_change * 100)

    def dispatch_volatility_alert(self, symbol, current_price, velocity_pct):
        emoji = "📈" if velocity_pct > 0 else "📉"
        direction = "EXPANSION" if velocity_pct > 0 else "RETRACTION"
        title = f"🚨 Volatility Sentry: {symbol}/USD Momentum Trigger"
        payload = (
            f"⚠️ **Proprietary Rolling 60-Min Velocity Scan Breached**\n"
            f"┣ **Asset Class Token**: `{symbol}/USD`\n"
            f"┣ **Current Spot Rate**: `${current_price:,.2f}`\n"
            f"┣ **Rolling Hourly Delta**: `{emoji} {velocity_pct:+.2f}%`\n"
            f"┗ **Structural Vector**: `Momentum {direction} Identified`\n\n"
            f"*Post Subscriptions Notice: Sizing allocation and systemic risk boundaries can be updated dynamically via standard quantamental interfaces.*"
        )
        if HAS_ESSENTIALS and WEBHOOK_CRYPTO:
            send_essentials_embed(WEBHOOK_CRYPTO, title, payload, 0xe74c3c if velocity_pct < 0 else 0x2ecc71)
            db.update_state("last_ping_crypto", time.time())

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            symbol = data.get('symbol')
            price = float(data.get('price', 0))

            if symbol == 'VIX':
                self.process_vix(price)
            elif symbol == "BTC/USD":
                self.state_memory["btc_last"] = price
                self.process_rolling_volatility("BTC", price)
            elif symbol == "ETH/USD":
                self.state_memory["eth_last"] = price
                self.process_rolling_volatility("ETH", price)
                self.write_to_state(self.state_memory["btc_last"], price)
        except Exception as e:
            logger.error(f"Stream parsing error: {e}")

    def on_error(self, ws, error):
        logger.error(f"Sentry Boundary encountered: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        backoff_time = min(5 * (2 ** self.reconnect_attempts), 300)
        logger.warning(f"Stream disconnected. Reconnecting in {backoff_time}s...")
        time.sleep(backoff_time)
        self.reconnect_attempts += 1
        self.start_sentry()

    def on_open(self, ws):
        logger.info("Connected to Twelve Data WebSockets. Establishing telemetry...")
        self.reconnect_attempts = 0 
        ws.send(json.dumps({"action": "subscribe", "params": {"symbols": "VIX,BTC/USD,ETH/USD"}}))

    def start_sentry(self):
        ws = websocket.WebSocketApp(WS_URL, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close, on_open=self.on_open)
        ws.run_forever()

if __name__ == "__main__":
    sentry = ConsolidatedSentry()
    sentry.start_sentry()
