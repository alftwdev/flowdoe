import os
import sys
import json
import time
import logging
import threading
import websocket
import requests
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from database import EcosystemDatabase
from analytics import HighFidelityAnalyticsEngine

# Setup unified logging architecture
logger = logging.getLogger("Unified_Stream_Sentry")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

# API and Webhook Keys
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_TRADE_SIGNALS = os.getenv("WEBHOOK_TRADE_SIGNALS")
WEBHOOK_MARKET_ANALYSIS = os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_DIVIDEND_CCETFS = os.getenv("WEBHOOK_DIVIDEND_CCETFS")
WEBHOOK_FOREX = os.getenv("WEBHOOK_FOREX")
WEBHOOK_CRYPTO = os.getenv("WEBHOOK_CRYPTO") # Explicit Crypto Allocation

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False
    def send_essentials_embed(*args, **kwargs): pass

# =====================================================================
# THREAD 1: REST Polling Agent
# =====================================================================
class StructuralBreakoutAgent:
    def __init__(self):
        self.watchlist = ["SPY", "QQQ", "AAPL", "NVDA", "MSFT"]
        self.base_url = "https://api.twelvedata.com/time_series"

    def _fetch_candles(self, symbol, interval, outputsize):
        params = {"symbol": symbol, "interval": interval, "outputsize": str(outputsize), "apikey": TWELVE_DATA_API_KEY}
        try:
            response = requests.get(self.base_url, params=params, timeout=15)
            if response.status_code == 429:
                logger.warning("Twelve Data API rate limit hit. Backing off for 15 seconds...")
                time.sleep(15)
                return None
            response.raise_for_status()
            data = response.json()
            if "values" not in data: return None
            df = pd.DataFrame(data["values"])
            df["close"], df["high"], df["low"] = df["close"].astype(float), df["high"].astype(float), df["low"].astype(float)
            return df.iloc[::-1].reset_index(drop=True)
        except Exception: return None

    def calculate_atr(self, df, period=14):
        high_low = df['high'] - df['low']
        high_cp = np.abs(df['high'] - df['close'].shift())
        low_cp = np.abs(df['low'] - df['close'].shift())
        df_tr = pd.DataFrame({'hl': high_low, 'hcp': high_cp, 'lcp': low_cp})
        return df_tr.max(axis=1).rolling(window=period).mean()

    def process_telemetry(self, symbol):
        df_daily = self._fetch_candles(symbol, interval="1day", outputsize=60)
        if df_daily is None or len(df_daily) < 50: return

        df_daily["ma50"] = df_daily["close"].rolling(window=50).mean()
        htf_trend = "UP" if df_daily["ma50"].iloc[-1] > df_daily["ma50"].iloc[-2] else "DOWN"

        df_4h = self._fetch_candles(symbol, interval="4h", outputsize=30)
        if df_4h is None or len(df_4h) < 20: return

        df_4h["basis"] = df_4h["close"].rolling(window=20).mean()
        df_4h["std"] = df_4h["close"].rolling(window=20).std()
        df_4h["bb_upper"] = df_4h["basis"] + (2.0 * df_4h["std"])
        df_4h["bb_lower"] = df_4h["basis"] - (2.0 * df_4h["std"])
        df_4h["bbw"] = (df_4h["bb_upper"] - df_4h["bb_lower"]) / df_4h["basis"]
        df_4h["atr"] = self.calculate_atr(df_4h, period=14)
        df_4h["atr_pct"] = (df_4h["atr"] / df_4h["close"]) * 100

        close_4h = df_4h["close"].iloc[-1]
        bbw = df_4h["bbw"].iloc[-1]
        atr_pct = df_4h["atr_pct"].iloc[-1]
        std_4h = df_4h["std"].iloc[-1]

        if std_4h == 0: return
        sd_position = (close_4h - df_4h["basis"].iloc[-1]) / std_4h

        upper_breakout = close_4h > df_4h["bb_upper"].iloc[-1]
        lower_breakout = close_4h < df_4h["bb_lower"].iloc[-1]
        volatility_expanding = bbw > 0.05
        momentum_validated = atr_pct > 1.0

        if upper_breakout:
            if sd_position >= 2.2:
                self.broadcast_trap(symbol, "BULLISH LIQUIDITY GRAB (FAKE OUT)", close_4h, sd_position, bbw, atr_pct)
            elif volatility_expanding and momentum_validated and htf_trend == "UP":
                self.broadcast_signal(symbol, "VALID LONG BREAKOUT", close_4h, sd_position, bbw, atr_pct, htf_trend)
        elif lower_breakout:
            if sd_position <= -2.2:
                self.broadcast_trap(symbol, "BEARISH LIQUIDITY GRAB (FAKE OUT)", close_4h, sd_position, bbw, atr_pct)
            elif volatility_expanding and momentum_validated and htf_trend == "DOWN":
                self.broadcast_signal(symbol, "VALID SHORT BREAKOUT", close_4h, sd_position, bbw, atr_pct, htf_trend)

    def broadcast_signal(self, symbol, status, price, sd, bbw, atr, trend):
        alert_id = f"breakout_{symbol}_{datetime.now().strftime('%Y%m%d')}"
        if not db.track_and_limit_alerts(alert_id, f"SIGNAL_{status}_PRC", current_trigger=price, max_broadcasts=2, threshold_pct=0.002): return
        payload = (f"⚡ **System Trend Momentum Entry Confirmed**\n┣ **Asset ID:** `{symbol}`\n┣ **Action Type:** `{status}`\n"
                   f"┣ **Execution Spot Price:** `${price:,.2f}`\n┣ **Standard Deviation Factor:** `{sd:+.2f}σ`\n"
                   f"┣ **Bandwidth Expansion (BBW):** `{bbw:.4f}`\n┗ **Filter (Daily MA):** `{trend}TREND`")
        if HAS_ESSENTIALS and WEBHOOK_TRADE_SIGNALS: send_essentials_embed(WEBHOOK_TRADE_SIGNALS, f"🟢 Genuine Trend Structure Breakout: {symbol}", payload, 0x2ecc71)

    def broadcast_trap(self, symbol, status, price, sd, bbw, atr):
        alert_id = f"trap_{symbol}_{datetime.now().strftime('%Y%m%d')}"
        if not db.track_and_limit_alerts(alert_id, f"TRAP_{status}_PRC", current_trigger=price, max_broadcasts=1, threshold_pct=0.005): return
        payload = (f"🚨 **Counter-Trend Institutional Trap Detected**\n┣ **Asset ID:** `{symbol}`\n┣ **Calculated Event:** `{status}`\n"
                   f"┣ **Current Spot Position:** `${price:,.2f}`\n┣ **Exhaustion Boundary Z-Score:** `{sd:+.2f}σ` *(Overextended)*\n"
                   f"┗ **Volatility Index Force (ATR %):** `{atr:.2f}%`\n\n⚠️ *Retail momentum is being absorbed here.*")
        if HAS_ESSENTIALS and WEBHOOK_TRADE_SIGNALS: send_essentials_embed(WEBHOOK_TRADE_SIGNALS, f"🛑 High Probability False Breakout Trap: {symbol}", payload, 0xe74c3c)

    def run_wheel_discovery(self):
        engine = HighFidelityAnalyticsEngine()
        candidates = engine.generate_wheel_candidates()
        if not candidates: return
        payload = "### **⚙️ Automated Wheel Strategy Discovery**\n*Highly curated 30-45 DTE Cash-Secured Put setups (~0.40 Delta).*\n\n"
        for c in candidates: payload += f"**{c['symbol']}** | Spot: `${c['spot']:,.2f}`\n┣ **Target Expiration**: `{c['expiration']}` ({c['dte']} DTE)\n┣ **Optimal Strike**: `STO ${c['strike']:.1f} Put`\n┗ **Capital Efficiency**: Est. `{c['annualized_roi']}%` Annualized ROI\n\n"
        if HAS_ESSENTIALS and WEBHOOK_DIVIDEND_CCETFS: send_essentials_embed(WEBHOOK_DIVIDEND_CCETFS, "🎡 PRIME WHEEL CANDIDATES", payload, 0x9b59b6)

    def execution_loop(self):
        logger.info("REST Polling Agent Initialized.")
        loop_counter = 0
        while True:
            for symbol in self.watchlist:
                self.process_telemetry(symbol)
                time.sleep(2) 
            if loop_counter % 5 == 0:
                try: self.run_wheel_discovery()
                except Exception as e: logger.error(f"Wheel Discovery Failed: {e}")
            loop_counter += 1
            time.sleep(120)

# =====================================================================
# THREAD 2: Real-Time WebSocket Agent 
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
            state_str = "VOLATILITY_CEILING_COMPRESSION"
            if db.track_and_limit_alerts(alert_id, state_str, price, max_broadcasts=1, threshold_pct=0.005):
                payload = f"🎯 **[{symbol} Perimeter Alert]**\n┣ Spot Level: `{price:,.4f}`\n┗ ⚠️ Volatility Ceiling Compression reached."
                send_essentials_embed(target_webhook, f"🚨 Volatility Boundary Hit: {symbol}", payload, 0xe74c3c)

        elif price <= lower * (1.0 + precision_pct):
            alert_id = f"perimeter_breach_{symbol.upper()}_LOWER"
            state_str = "VOLATILITY_FLOOR_COMPRESSION"
            if db.track_and_limit_alerts(alert_id, state_str, price, max_broadcasts=1, threshold_pct=0.005):
                payload = f"🎯 **[{symbol} Perimeter Alert]**\n┣ Spot Level: `{price:,.4f}`\n┗ ⚠️ Volatility Floor Compression reached."
                send_essentials_embed(target_webhook, f"🚨 Volatility Boundary Hit: {symbol}", payload, 0x2ecc71)

    def process_crypto_volatility(self, price):
        current_time = time.time()
        self.btc_window.append((current_time, price))
        while self.btc_window and (current_time - self.btc_window[0][0] > 3600): self.btc_window.pop(0)
            
        pct_change = (price - self.btc_window[0][1]) / self.btc_window[0][1]
        if abs(pct_change) >= 0.025:
            # Shifted tolerance threshold from 1.5% to 3.0% buffer before DB lockout breaks
            if db.track_and_limit_alerts("BTC_USD_VOL_STREAM", "HOURLY_VELOCITY_BREACH", price, max_broadcasts=1, threshold_pct=0.03):
                webhook = WEBHOOK_CRYPTO or WEBHOOK_MARKET_ANALYSIS
                if webhook and HAS_ESSENTIALS:
                    payload = f"🪙 **[BTC/USD Telemetry]**\n┣ Spot Rate: `${price:,.2f}`\n┗ ⚠️ Hourly Velocity Breach: `{pct_change*100:+.2f}%` directional momentum."
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
            elif symbol == "VIX":
                db.update_state("vix_iv_index", price)
        except Exception: pass

    def on_open(self, ws):
        logger.info("Websocket pipeline connected. Initializing unified stream monitor...")
        ws.send(json.dumps({"action": "subscribe", "params": {"symbols": "SPY,QQQ,VIX,XAU/USD,EUR/USD,GBP/USD,USD/JPY,BTC/USD"}}))

    def on_error(self, ws, error): pass
    def on_close(self, ws, close_status_code, close_msg): logger.debug("Stream dropped. Re-establishing...")

    def execution_loop(self):
        backoff = 1.0
        while True:
            try:
                ws = websocket.WebSocketApp(self.ws_url, on_message=self.on_message, on_open=self.on_open, on_error=self.on_error, on_close=self.on_close)
                ws.run_forever()
                backoff = 1.0 
            except Exception as e:
                logger.error(f"WS disconnected. Reconnecting in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

# =====================================================================
# DAEMON ORCHESTRATOR
# =====================================================================
if __name__ == "__main__":
    logger.info("Initializing Unified Stream Ecosystem...")
    
    rest_agent = StructuralBreakoutAgent()
    ws_agent = RealTimeTickAgent()

    t1 = threading.Thread(target=rest_agent.execution_loop, name="REST_Poller", daemon=True)
    t2 = threading.Thread(target=ws_agent.execution_loop, name="WS_Streamer", daemon=True)

    try:
        t1.start()
        t2.start()
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Operator triggered shutdown. Halting threads gracefully.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Unhandled system crash: {e}")
        sys.exit(1)
