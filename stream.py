import os
import sys
import json
import time
import logging
import threading
import websocket
import requests
from dotenv import load_dotenv
from database import EcosystemDatabase
from analytics import HighFidelityAnalyticsEngine

logger = logging.getLogger("Unified_Stream_Sentry")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_TRADE_SIGNALS = os.getenv("WEBHOOK_TRADE_SIGNALS")
WEBHOOK_MARKET_ANALYSIS = os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_FOREX = os.getenv("WEBHOOK_FOREX")
WEBHOOK_CRYPTO = os.getenv("WEBHOOK_CRYPTO")

try:
    from essentials_tools import send_essentials_embed, send_essentials_embed_with_chart, generate_candlestick_chart
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False
    def send_essentials_embed(*args, **kwargs): pass
    def send_essentials_embed_with_chart(*args, **kwargs): pass
    def generate_candlestick_chart(*args, **kwargs): return None

# =====================================================================
# WebSocket Agent — real-time BTC volatility, SPY/QQQ perimeter alerts,
# VIXY proxy price updates for monitor.py. Event-driven: near-zero CPU.
# =====================================================================
class RealTimeTickAgent:
    def __init__(self):
        self.ws_url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TWELVE_DATA_API_KEY}"
        self.btc_window = []

    def evaluate_proximity_metrics(self, symbol, price):
        if symbol not in ["SPY", "QQQ", "XAU/USD", "EUR/USD", "GBP/USD", "USD/JPY"]: return
        if symbol in ["SPY", "QQQ"]:
            upper = float(db.get_state(f"{symbol}_expected_upper", 0.0))
            lower = float(db.get_state(f"{symbol}_expected_lower", 0.0))
            target_webhook = WEBHOOK_TRADE_SIGNALS
            precision_pct = 0.0015
        else:
            upper = float(db.get_state(f"{symbol}_upper_noise", 0.0))
            lower = float(db.get_state(f"{symbol}_lower_noise", 0.0))
            target_webhook = WEBHOOK_FOREX
            precision_pct = 0.0005

        if upper == 0 or lower == 0 or not target_webhook: return

        if price >= upper * (1.0 - precision_pct):
            alert_id = f"perimeter_breach_{symbol.upper()}_UPPER"
            if db.track_and_limit_alerts(alert_id, "VOLATILITY_CEILING_COMPRESSION", price, max_broadcasts=1, threshold_pct=0.005):
                payload = (
                    f"┣ Spot Level: `{price:,.4f}`\n"
                    f"┣ Volatility Ceiling Compression reached.\n"
                    f"┗ ⚠️ Market trajectory: bearish (upside extended, mean-reversion risk into the ceiling)"
                )
                send_essentials_embed(target_webhook, f"🚨 Volatility Boundary Hit: {symbol}", payload, 0xe74c3c)

        elif price <= lower * (1.0 + precision_pct):
            alert_id = f"perimeter_breach_{symbol.upper()}_LOWER"
            if db.track_and_limit_alerts(alert_id, "VOLATILITY_FLOOR_COMPRESSION", price, max_broadcasts=1, threshold_pct=0.005):
                payload = (
                    f"┣ Spot Level: `{price:,.4f}`\n"
                    f"┣ Volatility Floor Compression reached.\n"
                    f"┗ ⚠️ Market trajectory: bullish (downside extended, mean-reversion bounce risk off the floor)"
                )
                send_essentials_embed(target_webhook, f"🚨 Volatility Boundary Hit: {symbol}", payload, 0x2ecc71)

    def process_crypto_volatility(self, price):
        current_time = time.time()
        self.btc_window.append((current_time, price))
        while self.btc_window and (current_time - self.btc_window[0][0] > 3600): self.btc_window.pop(0)

        pct_change = (price - self.btc_window[0][1]) / self.btc_window[0][1]
        if abs(pct_change) >= 0.025:
            if db.track_and_limit_alerts("BTC_USD_VOL_STREAM", "HOURLY_VELOCITY_BREACH", price, max_broadcasts=1, threshold_pct=0.03):
                webhook = WEBHOOK_CRYPTO or WEBHOOK_MARKET_ANALYSIS
                if webhook and HAS_ESSENTIALS:
                    payload = f"🪙 **[BTC/USD Telemetry]**\n┣ Spot Rate: `${price:,.2f}`\n┗ ⚠️ Hourly Velocity Breach: `{pct_change*100:+.2f}%` directional momentum."
                    try:
                        ohlc = HighFidelityAnalyticsEngine().fetch_crypto_ohlc("BTC/USD", outputsize=60)
                        if ohlc is not None and not ohlc.empty:
                            chart_bytes = generate_candlestick_chart("BTC/USD", ohlc, last_change=price - self.btc_window[0][1], last_change_pct=pct_change * 100)
                            send_essentials_embed_with_chart(webhook, "⚡ Volatility Sentry Trigger", payload, chart_bytes, color=0xf39c12)
                            return
                    except Exception as e:
                        logger.error(f"BTC chart attach failed, falling back to text-only: {e}")
                    send_essentials_embed(webhook, "⚡ Volatility Sentry Trigger", payload, 0xf39c12)

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            symbol = data.get("symbol")
            price = float(data.get("price", 0.0))
            if not symbol or price == 0: return

            if symbol in ["SPY", "QQQ", "XAU/USD", "EUR/USD", "GBP/USD", "USD/JPY"]:
                self.evaluate_proximity_metrics(symbol, price)
            elif symbol == "BTC/USD":
                self.process_crypto_volatility(price)
            elif symbol == "VIXY":
                # Real-time proxy for VIX (VIX 404s on this Twelve Data plan tier).
                # Written to DB so monitor.py can read it on its 15-min loop.
                db.update_state("vixy_price_realtime", price)
        except Exception: pass

    def on_open(self, ws):
        logger.info("Websocket pipeline connected.")
        self._connected = True
        self._backoff = 30.0  # reset on confirmed connection, not on clean return
        ws.send(json.dumps({"action": "subscribe", "params": {"symbols": "SPY,QQQ,VIXY,XAU/USD,BTC/USD"}}))

    def on_error(self, ws, error):
        logger.error(f"WS error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        self._connected = False
        logger.debug("Stream dropped.")

    def execution_loop(self):
        self._connected = False
        self._backoff = 30.0  # start conservative — Twelve Data may rate-limit rapid reconnects
        while True:
            try:
                ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_message=self.on_message,
                    on_open=self.on_open,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                ws.run_forever(ping_interval=60, ping_timeout=15)
            except Exception as e:
                logger.error(f"WS exception: {e}")
            # Always sleep before reconnecting — whether clean disconnect or exception.
            # Resets to 30s after a confirmed on_open; stays elevated if we never connected.
            delay = 30.0 if self._connected else self._backoff
            logger.info(f"Reconnecting in {delay:.0f}s...")
            time.sleep(delay)
            self._connected = False
            self._backoff = min(self._backoff * 2, 300.0)

# =====================================================================
# DAEMON ORCHESTRATOR
# =====================================================================
if __name__ == "__main__":
    logger.info("Initializing Stream Sentry (WebSocket only)...")

    ws_agent = RealTimeTickAgent()
    t1 = threading.Thread(target=ws_agent.execution_loop, name="WS_Streamer", daemon=True)

    try:
        t1.start()
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Operator triggered shutdown.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Unhandled system crash: {e}")
        sys.exit(1)
