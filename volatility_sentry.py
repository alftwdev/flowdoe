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

# Setup institutional-grade logging
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

# Initialize database link
db = EcosystemDatabase()

# Multiplexed streaming link
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
        
        # NEW: Network Jitter Tracking
        self.reconnect_attempts = 0

    def write_to_state(self, btc_price, eth_price):
        """Throttled database writing to prevent I/O bottlenecks."""
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
        """Monitors VIX states and writes systemic risk anomalies into memory."""
        self.state_memory["vix_last"] = price
        vix_status = "CRITICAL SPARK" if price > 24.0 else "ELEVATED" if price > 19.0 else "NOMINAL"
        db.update_state("market_regime", {"vix_status": vix_status, "vix_price": price})

    def process_rolling_volatility(self, symbol, current_price):
        """Surgically tracks rolling 60-minute variations without clogging channels."""
        current_time = time.time()
        window = self.btc_window if symbol == "BTC" else self.eth_window
        window.append((current_time, current_price))
        
        # Prune elements older than 60 minutes (3600 seconds)
        while window and (current_time - window[0][0] > 3600):
            window.pop(0)
            
        if len(window) < 2:
            return

        # Calculate variance from the baseline anchor of the current hour block
        initial_price = window[0][1]
        pct_change = (current_price - initial_price) / initial_price
        
        if abs(pct_change) >= self.volatility_threshold:
            # Check alert cooldown threshold
            if current_time - self.last_alert_time[symbol] > 3600:
                self.last_alert_time[symbol] = current_time
                self.dispatch_volatility_alert(symbol, current_price, pct_change * 100)

    def dispatch_volatility_alert(self, symbol, current_price, velocity_pct):
        """Dispatches automated velocity warnings directly into the execution cluster."""
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

    def dispatch_crypto_income_alpha(self):
        """Injects non-speculative structural cash flow data into the channel daily."""
        current_time = time.time()
        # Broadcast once every 24 hours (86400 seconds) to ensure baseline server throughput
        if current_time - self.state_memory["last_alpha_post_time"] < 86400:
            return
            
        self.state_memory["last_alpha_post_time"] = current_time
        
        title = "₿ Crypto Structural Cash Flow & Yield Matrix"
        payload = (
            f"### 🛡️ **Capital Preservation & Alternative Income Layer**\n"
            f"Our architecture suppresses speculative trade chasing in favor of institutional basis capture and audited protocols:\n\n"
            f"🏦 **Tier-1 Staking Yield Reference Indices**\n"
            f"┣ **Base-Layer Native ETH Protocol Staking**: `~3.30% APY`\n"
            f"┣ **Coinbase Institutional Custodial Staking**: `~2.85% APY`\n"
            f"┗ **Risk-Mitigated DeFi Protocol Aggregators**: `~3.65% APY`\n\n"
            f"💎 **The Basis / Funding Rate Arbitrage Blueprint**\n"
            f"When speculative retail leverage expands funding rates upward, members can optimize a delta-neutral cash-and-carry framework:\n"
            f"1. **Acquire Spot Collateral** (Long Spot Underlying Index via Coinbase tier resources).\n"
            f"2. **Sell Equivalent Perpetual Contracts** short across primary liquidity providers.\n"
            f"3. **Capture Net Premium Flow** while keeping price exposure completely insulated from drawdowns.\n\n"
            f"*[Ecosystem Directives: No programmatic direction triggers are dispatched for digital assets. Focus is isolated to compounding cash flow mechanics.]*"
        )
        if HAS_ESSENTIALS and WEBHOOK_CRYPTO:
            send_essentials_embed(WEBHOOK_CRYPTO, title, payload, 0xf1c40f)

# volatility_sentry.py - Update the on_message handler
    def on_message(self, ws, message):
        data = json.loads(message)
        # Assuming TwelveData provides IV or we use VIX as proxy for SPY
        if data['symbol'] == 'VIX':
            self.state_memory['vix_last'] = float(data['price'])
            # PUSH to DB for consumption by edge.py and income.py
            db.update_state("vix_iv_index", self.state_memory['vix_last'])                elif "BTC/USD" in symbol:
                    self.state_memory["btc_last"] = price
                    self.process_rolling_volatility("BTC", price)
                elif symbol == "ETH/USD":
                    self.state_memory["eth_last"] = price
                    self.process_rolling_volatility("ETH", price)
                    self.write_to_state(self.state_memory["btc_last"], price)
                    
                # Evaluate alternative income alpha cadence
                self.dispatch_crypto_income_alpha()
        except Exception as e:
            logger.error(f"Stream parsing error: {e}")

    def on_error(self, ws, error):
        logger.error(f"Sentry Boundary encountered: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        # NEW: Exponential Backoff Logic (5s, 10s, 20s... capped at 300s)
        backoff_time = min(5 * (2 ** self.reconnect_attempts), 300)
        logger.warning(f"Stream disconnected: {close_msg} ({close_status_code}). Reconnecting in {backoff_time}s...")
        time.sleep(backoff_time)
        self.reconnect_attempts += 1
        self.start_sentry()

    def on_open(self, ws):
        logger.info("Connected to Twelve Data Enterprise WebSockets Frame. Establishing telemetry...")
        self.reconnect_attempts = 0 # Reset jitter tracking on successful connection
        subscribe_payload = {
            "action": "subscribe",
            "params": {
                "symbols": "VIX,BTC/USD,ETH/USD"
            }
        }
        ws.send(json.dumps(subscribe_payload))

    def start_sentry(self):
        ws = websocket.WebSocketApp(
            WS_URL, 
            on_message=self.on_message, 
            on_error=self.on_error, 
            on_close=self.on_close, 
            on_open=self.on_open
        )
        ws.run_forever()

if __name__ == "__main__":
    if not TD_API_KEY:
        logger.error("CRITICAL: Environment variable TWELVE_DATA_API_KEY is undefined. Execution halted.")
        sys.exit(1)
        
    sentry = ConsolidatedSentry()
    logger.info("Bootstrapping Sentry Core. Initiating persistent tasks...")
    sentry.start_sentry()
