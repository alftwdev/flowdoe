import os
import sys
import json
import time
import logging
import websocket
from datetime import datetime
import pytz
from dotenv import load_dotenv

logger = logging.getLogger("Volatility_Crypto_Sentry")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
db.get_state("market_regime")

# Multiplexed connection: Listening to VIX and Crypto simultaneously
WS_URL = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"

class ConsolidatedSentry:
    def __init__(self):
        self.state_memory = {
            "vix_last": 0.0,
            "btc_last": 0.0,
            "eth_last": 0.0,
            "last_write_time": 0
        }
        self.tz = pytz.timezone('Pacific/Honolulu')

    def write_to_state(self, btc_price, eth_price):
        """Throttled disk writing to prevent I/O bottlenecks."""
        current_time = time.time()
        # Only write to disk every 60 seconds to save resources
        if current_time - self.state_memory["last_write_time"] > 60:
            data = {"btc_price": f"${btc_price:,.2f}", "eth_price": f"${eth_price:,.2f}", "has_crypto": True}
            try:
                with open(STATE_FILE, "w") as f:
                    json.dump(data, f)
                self.state_memory["last_write_time"] = current_time
            except Exception as e:
                logger.error(f"State write failed: {e}")

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get("event") == "price" and "price" in data:
                symbol = data.get("symbol")
                price = float(data.get("price"))

                if symbol == "VIX" or symbol == "IXIC": # Depending on TD access
                    self.process_vix(price)
                elif symbol == "BTC/USD":
                    self.state_memory["btc_last"] = price
                    self.process_crypto(symbol, price)
                elif symbol == "ETH/USD":
                    self.state_memory["eth_last"] = price
                    self.write_to_state(self.state_memory["btc_last"], price)

        except Exception as e:
            logger.error(f"Stream parsing error: {e}")

    def process_vix(self, current_vix):
        """Original VIX logic."""
        if current_vix > 25.0 and self.state_memory["vix_last"] <= 25.0:
            logger.warning(f"VIX Spike Detected: {current_vix}")
            # Dispatch to Discord logic here...
        self.state_memory["vix_last"] = current_vix

    def process_crypto(self, symbol, current_price):
        """Crypto specific velocity tracking."""
        # Insert velocity math here. For example, if price drops 3% in 5 minutes.
        pass

    def on_open(self, ws):
        logger.info("Connection established. Multiplexing VIX, BTC, and ETH...")
        subscribe_msg = {
            "action": "subscribe",
            "params": {"symbols": "VIX,BTC/USD,ETH/USD"}
        }
        ws.send(json.dumps(subscribe_msg))

    def on_error(self, ws, error):
        logger.error(f"Sentry Boundary encountered: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.warning("Stream disconnected. Forcing reconnect in 5s...")
        time.sleep(5)
        self.start_sentry()

    def start_sentry(self):
        ws = websocket.WebSocketApp(WS_URL, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close, on_open=self.on_open)
        ws.run_forever()

if __name__ == "__main__":
    if not TD_API_KEY:
        logger.error("Missing TwelveData API Key.")
        sys.exit(1)
    
    sentry = ConsolidatedSentry()
    sentry.start_sentry()

# Inside ConsolidatedSentry class in volatility_sentry.py

def on_open(self, ws):
    logger.info("Multiplexing VIX and Exchange-Specific Crypto...")
    # Target specific CEXs to calculate real spreads
    subscribe_msg = {
        "action": "subscribe",
        "params": {"symbols": "VIX,BTC/USD:Binance,BTC/USD:Coinbase"}
    }
    ws.send(json.dumps(subscribe_msg))

def process_crypto_arbitrage(self, exchange, current_price):
    """Calculates Net Arbitrage across CEX venues."""
    self.state_memory[f"btc_{exchange}"] = current_price
    
    binance_price = self.state_memory.get("btc_Binance", 0)
    coinbase_price = self.state_memory.get("btc_Coinbase", 0)
    
    if binance_price > 0 and coinbase_price > 0:
        # Determine Buy/Sell venues
        p_buy = min(binance_price, coinbase_price)
        p_sell = max(binance_price, coinbase_price)
        
        # Calculate Fees (0.1% Taker, 0.1% Maker + estimated network proxy of $5)
        fee_sum = (p_buy * 0.001) + (p_sell * 0.001) + 5.00
        
        # The Mathematical Spread
        net_profit = (p_sell - p_buy) - fee_sum
        
        # Depth/Slippage Buffer: Only alert if net profit > $40 (filters out thin liquidity noise)
        if net_profit > 40.00:
            buy_venue = "Binance" if p_buy == binance_price else "Coinbase"
            sell_venue = "Coinbase" if buy_venue == "Binance" else "Binance"
            
            payload_msg = (
                f"**CEX Arbitrage Opportunity Detected**\n"
                f"┣ **Buy**: `{buy_venue}` at `${p_buy:,.2f}`\n"
                f"┣ **Sell**: `{sell_venue}` at `${p_sell:,.2f}`\n"
                f"┣ **Gross Spread**: `${(p_sell - p_buy):,.2f}`\n"
                f"┗ **Net Arb (Post-Fees)**: `${net_profit:,.2f}`"
            )
            # Dispatch embed to premium channel  
