import os
import sys
import json
import time
import logging
import numpy as np
import websocket
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase

logger = logging.getLogger("Proximity_Sentry")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_CRYPTO = os.getenv("WEBHOOK_CRYPTO") or os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_OPTIONS_SIGNALS = os.getenv("WEBHOOK_OPTIONS_SIGNALS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_FOREX = os.getenv("WEBHOOK_FOREX") or os.getenv("WEBHOOK_MARKET_ANALYSIS")

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
        self.state_memory = {"vix_last": 0.0, "btc_last": 0.0, "eth_last": 0.0, "last_write_time": 0}
        self.btc_window = []
        self.eth_window = []
        self.last_alert_time = {"BTC": 0, "ETH": 0}
        self.proximity_cooldowns = {} # Prevents proximity spam
        self.volatility_threshold = 0.025 
        self.reconnect_attempts = 0

    # --- PROXIMITY EXECUTION ENGINE ---
    def check_proximity(self, symbol, price):
        """Event-Driven checker: validates live ticks against DB boundaries."""
        levels = {}
        if symbol == "SPY":
            levels = {
                "Point of Control (POC)": db.get_state("SPY_poc", 0),
                "Intraday Liquidity Floor": db.get_state("SPY_vol_floor", 0),
                "Gamma Resistance Ceiling": db.get_state("SPY_gamma_ceiling", 0),
                "0DTE Expected Move Ceiling": db.get_state("SPY_expected_upper", 0),
                "0DTE Expected Move Floor": db.get_state("SPY_expected_lower", 0),
            }
        elif symbol == "XAU/USD":
            levels = {
                "ATR Exhaustion Ceiling": db.get_state("XAU/USD_upper_noise", 0),
                "ATR Exhaustion Floor": db.get_state("XAU/USD_lower_noise", 0),
            }

        for level_name, level_price in levels.items():
            if not level_price or float(level_price) == 0: continue
            
            # Trigger threshold: within 0.15% of the boundary
            proximity_pct = abs(price - float(level_price)) / float(level_price)
            if proximity_pct <= 0.0015:
                alert_key = f"{symbol}_{level_name}"
                last_alert = self.proximity_cooldowns.get(alert_key, 0)
                current_time = time.time()

                # 4-hour cooldown per specific level interaction to prevent chop-spam
                if current_time - last_alert > 14400:
                    self.dispatch_proximity_alert(symbol, price, level_name, float(level_price))
                    self.proximity_cooldowns[alert_key] = current_time

    def dispatch_proximity_alert(self, symbol, price, level_name, level_price):
        title = f"🚨 TACTICAL PROXIMITY ALERT: {symbol}"
        
        # Determine actionable bias context natively
        if "Floor" in level_name or "Lower" in level_name:
            bias, color = "mean-reversion LONG", 0x2ecc71
        elif "Ceiling" in level_name or "Upper" in level_name:
            bias, color = "rejection SHORT", 0xe74c3c
        else:
            bias, color = "high-volume consolidation", 0xf1c40f

        payload = (
            f"**Strategic Boundary Reached**\n"
            f"The spot price of `{symbol}` is currently at `${price:,.2f}`, testing the mathematically defined **{level_name}** (`${level_price:,.2f}`).\n\n"
            f"🧠 **Execution Directive**: High probability for {bias} mechanics. Validate order flow and volume before triggering entry."
        )
        
        target_webhook = WEBHOOK_OPTIONS_SIGNALS if symbol == "SPY" else WEBHOOK_FOREX
        if HAS_ESSENTIALS and target_webhook:
            send_essentials_embed(target_webhook, title, payload, color)
            logger.info(f"Proximity Execution Fired: {symbol} at {level_name}")

    # --- CRYPTO & VIX ENGINES (Existing Logic) ---
    def calculate_volatility_z_score(self, current_vol):
        hist_vol = db.get_state("btc_historical_volatility", [])
        hist_vol.append(current_vol)
        if len(hist_vol) > 90: hist_vol.pop(0)
        db.update_state("btc_historical_volatility", hist_vol)
        
        if len(hist_vol) < 10: return 0.0
        std_rv = np.std(hist_vol)
        if std_rv == 0: return 0.0
        
        z_score = (current_vol - np.mean(hist_vol)) / std_rv
        db.update_state("btc_vol_z_score", float(z_score))
        return z_score

    def write_to_state(self, btc_price, eth_price):
        current_time = time.time()
        if current_time - self.state_memory["last_write_time"] > 60:
            db.update_state("crypto_live_state", {"btc_price": f"${btc_price:,.2f}", "eth_price": f"${eth_price:,.2f}"})
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
        
        while window and (current_time - window[0][0] > 3600): window.pop(0)
        if len(window) < 2: return

        pct_change = (current_price - window[0][1]) / window[0][1]
        
        if symbol == "BTC":
            z_score = self.calculate_volatility_z_score(abs(pct_change))
            if z_score < -2.0 and current_time - self.last_alert_time.get("BTC_COMPRESSION", 0) > 14400:
                self.dispatch_compression_alert()
                self.last_alert_time["BTC_COMPRESSION"] = current_time
        
        if abs(pct_change) >= self.volatility_threshold:
            if current_time - self.last_alert_time[symbol] > 3600:
                self.last_alert_time[symbol] = current_time
                self.dispatch_volatility_alert(symbol, current_price, pct_change * 100)

    def dispatch_compression_alert(self):
        if HAS_ESSENTIALS and WEBHOOK_CRYPTO:
            send_essentials_embed(WEBHOOK_CRYPTO, "⚡ Crypto Matrix: Extreme Volatility Compression", "⚠️ **Z-Score Boundary Alert**\nBTC realized volatility has fallen over 2 standard deviations below its rolling mean. Historical probability mathematically indicates imminent, violent directional expansion.", 0x9b59b6)

    def dispatch_volatility_alert(self, symbol, current_price, velocity_pct):
        emoji, direction = ("📈", "EXPANSION") if velocity_pct > 0 else ("📉", "RETRACTION")
        payload = f"⚠️ **Proprietary Rolling 60-Min Velocity Scan Breached**\n┣ **Asset Class Token**: `{symbol}/USD`\n┣ **Current Spot Rate**: `${current_price:,.2f}`\n┣ **Rolling Hourly Delta**: `{emoji} {velocity_pct:+.2f}%`\n┗ **Structural Vector**: `Momentum {direction} Identified`"
        if HAS_ESSENTIALS and WEBHOOK_CRYPTO:
            send_essentials_embed(WEBHOOK_CRYPTO, f"🚨 Volatility Sentry: {symbol}/USD Momentum Trigger", payload, 0xe74c3c if velocity_pct < 0 else 0x2ecc71)

    # --- WEBSOCKET HANDLERS ---
    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            symbol = data.get('symbol')
            price = float(data.get('price', 0))

            if symbol == 'VIX': self.process_vix(price)
            elif symbol == "BTC/USD":
                self.state_memory["btc_last"] = price
                self.process_rolling_volatility("BTC", price)
            elif symbol == "ETH/USD":
                self.state_memory["eth_last"] = price
                self.process_rolling_volatility("ETH", price)
                self.write_to_state(self.state_memory["btc_last"], price)
            elif symbol in ["SPY", "XAU/USD"]:
                # TRIGGER THE EVENT-DRIVEN PROXIMITY WATCHER
                self.check_proximity(symbol, price)
        except Exception: pass

    def on_error(self, ws, error): logger.error(f"Sentry Boundary encountered: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        backoff_time = min(5 * (2 ** self.reconnect_attempts), 300)
        time.sleep(backoff_time)
        self.reconnect_attempts += 1
        self.start_sentry()

    def on_open(self, ws):
        logger.info("Connected to WebSocket. Establishing telemetry for VIX, Crypto, Equities, and Forex...")
        self.reconnect_attempts = 0 
        # Added SPY and XAU/USD to the live stream
        ws.send(json.dumps({"action": "subscribe", "params": {"symbols": "VIX,BTC/USD,ETH/USD,SPY,XAU/USD"}}))

    def start_sentry(self):
        ws = websocket.WebSocketApp(WS_URL, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close, on_open=self.on_open)
        ws.run_forever()

if __name__ == "__main__":
    sentry = ConsolidatedSentry()
    sentry.start_sentry()
