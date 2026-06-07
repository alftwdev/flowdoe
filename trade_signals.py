import os
import sys
import time
import logging
import requests
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from database import EcosystemDatabase

# Setup logging architecture
logger = logging.getLogger("FalseBreakout_Sentry")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

# API and Routing Keys
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_TRADE_SIGNALS = os.getenv("WEBHOOK_TRADE_SIGNALS")
WEBHOOK_DIVIDEND_CCETFS = os.getenv("WEBHOOK_DIVIDEND_CCETFS")

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

class FalseBreakoutSentry:
    def __init__(self):
        self.watchlist = ["SPY", "QQQ", "AAPL", "NVDA", "MSFT"]
        self.base_url = "https://api.twelvedata.com/time_series"

    def _fetch_candles(self, symbol, interval, outputsize):
        """Fetches candle vectors from Twelve Data with defensive rate limit handling."""
        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": str(outputsize),
            "apikey": TWELVE_DATA_API_KEY
        }
        try:
            response = requests.get(self.base_url, params=params, timeout=15)
            if response.status_code == 429:
                logger.warning("Twelve Data API rate limit hit. Backing off for 15 seconds...")
                time.sleep(15)
                return None
            response.raise_for_status()
            data = response.json()
            if "values" not in data:
                logger.error(f"Invalid data structure for {symbol}: {data.get('message')}")
                return None
            df = pd.DataFrame(data["values"])
            df["close"] = df["close"].astype(float)
            df["high"] = df["high"].astype(float)
            df["low"] = df["low"].astype(float)
            return df.iloc[::-1].reset_index(drop=True) # Return chronologically ordered DF
        except Exception as e:
            logger.error(f"Network error pulling {symbol} ({interval}): {e}")
            return None

    def calculate_atr(self, df, period=14):
        """Calculates standard Average True Range."""
        high_low = df['high'] - df['low']
        high_cp = np.abs(df['high'] - df['close'].shift())
        low_cp = np.abs(df['low'] - df['close'].shift())
        df_tr = pd.DataFrame({'hl': high_low, 'hcp': high_cp, 'lcp': low_cp})
        true_range = df_tr.max(axis=1)
        return true_range.rolling(window=period).mean()

    def process_telemetry(self, symbol):
        """Runs the complete multi-channel confirmation matrix for structural breakouts."""
        # 1. Higher Timeframe Trend Alignment Matrix (Daily Chart)
        df_daily = self._fetch_candles(symbol, interval="1day", outputsize=60)
        if df_daily is None or len(df_daily) < 50: return

        df_daily["ma50"] = df_daily["close"].rolling(window=50).mean()
        daily_ma_current = df_daily["ma50"].iloc[-1]
        daily_ma_prior = df_daily["ma50"].iloc[-2]
        
        htf_trend = "UP" if daily_ma_current > daily_ma_prior else "DOWN"

        # 2. Strategy Execution Timeline Matrix (4-Hour Chart)
        df_4h = self._fetch_candles(symbol, interval="4h", outputsize=30)
        if df_4h is None or len(df_4h) < 20: return

        # Bollinger Band Computations
        df_4h["basis"] = df_4h["close"].rolling(window=20).mean()
        df_4h["std"] = df_4h["close"].rolling(window=20).std()
        df_4h["bb_upper"] = df_4h["basis"] + (2.0 * df_4h["std"])
        df_4h["bb_lower"] = df_4h["basis"] - (2.0 * df_4h["std"])
        df_4h["bbw"] = (df_4h["bb_upper"] - df_4h["bb_lower"]) / df_4h["basis"]
        
        # Velocity Computations
        df_4h["atr"] = self.calculate_atr(df_4h, period=14)
        df_4h["atr_pct"] = (df_4h["atr"] / df_4h["close"]) * 100

        # Current Operational Values
        close_4h = df_4h["close"].iloc[-1]
        upper_bb = df_4h["bb_upper"].iloc[-1]
        lower_bb = df_4h["bb_lower"].iloc[-1]
        bbw = df_4h["bbw"].iloc[-1]
        atr_pct = df_4h["atr_pct"].iloc[-1]
        basis_4h = df_4h["basis"].iloc[-1]
        std_4h = df_4h["std"].iloc[-1]

        if std_4h == 0: return
        sd_position = (close_4h - basis_4h) / std_4h

        # Parsing Structural Conditions
        upper_breakout = close_4h > upper_bb
        lower_breakout = close_4h < lower_bb
        volatility_expanding = bbw > 0.05
        momentum_validated = atr_pct > 1.0

        # 3. Dynamic Alert Evaluation State Engine
        if upper_breakout:
            if sd_position >= 2.2:
                # Trap Condition Checked: High-velocity overextension
                self.broadcast_trap(symbol, "BULLISH LIQUIDITY GRAB (FAKE OUT)", close_4h, sd_position, bbw, atr_pct)
            elif volatility_expanding and momentum_validated and htf_trend == "UP":
                # Confirmed Momentum Entry Checked
                self.broadcast_signal(symbol, "VALID LONG BREAKOUT", close_4h, sd_position, bbw, atr_pct, htf_trend)

        elif lower_breakout:
            if sd_position <= -2.2:
                # Trap Condition Checked: Low-velocity overextension
                self.broadcast_trap(symbol, "BEARISH LIQUIDITY GRAB (FAKE OUT)", close_4h, sd_position, bbw, atr_pct)
            elif volatility_expanding and momentum_validated and htf_trend == "DOWN":
                # Confirmed Momentum Entry Checked
                self.broadcast_signal(symbol, "VALID SHORT BREAKOUT", close_4h, sd_position, bbw, atr_pct, htf_trend)

    def broadcast_signal(self, symbol, status, price, sd, bbw, atr, trend):
        alert_id = f"breakout_{symbol}_{datetime.now().strftime('%Y%m%d')}"
        state_string = f"SIGNAL_{status}_PRC_{price}"
        
        if not db.track_and_limit_alerts(alert_id, state_string, current_trigger=price, max_broadcasts=2, threshold_pct=0.002):
            return

        payload = (
            f"⚡ **System Trend Momentum Entry Confirmed**\n"
            f"┣ **Asset ID:** `{symbol}`\n"
            f"┣ **Action Type:** `{status}`\n"
            f"┣ **Execution Spot Price:** `${price:,.2f}`\n"
            f"┣ **Standard Deviation Factor:** `{sd:+.2f}σ` *(Room to Run)*\n"
            f"┣ **Bollinger Band Expansion Width (BBW):** `{bbw:.4f}`\n"
            f"┣ **Volatility Vector (ATR %):** `{atr:.2f}%`\n"
            f"┗ **Higher Timeframe Filter (Daily MA):** `{trend}TREND`"
        )
        logger.info(f"Firing Breakout Signal: {symbol} - {status}")
        if HAS_ESSENTIALS and WEBHOOK_TRADE_SIGNALS:
            send_essentials_embed(WEBHOOK_TRADE_SIGNALS, f"🟢 Genuine Trend Structure Breakout: {symbol}", payload, 0x2ecc71)

    def broadcast_trap(self, symbol, status, price, sd, bbw, atr):
        alert_id = f"trap_{symbol}_{datetime.now().strftime('%Y%m%d')}"
        state_string = f"TRAP_{status}_PRC_{price}"
        
        if not db.track_and_limit_alerts(alert_id, state_string, current_trigger=price, max_broadcasts=1, threshold_pct=0.005):
            return

        payload = (
            f"🚨 **Counter-Trend Institutional Trap Detected**\n"
            f"┣ **Asset ID:** `{symbol}`\n"
            f"┣ **Calculated Event:** `{status}`\n"
            f"┣ **Current Spot Position:** `${price:,.2f}`\n"
            f"┣ **Exhaustion Boundary Z-Score:** `{sd:+.2f}σ` *(Overextended)*\n"
            f"┣ **Bandwidth Compression Factor:** `{bbw:.4f}`\n"
            f"┗ **Volatility Index Force (ATR %):** `{atr:.2f}%`\n\n"
            f"⚠️ *Trading Guidance: Retail momentum is being absorbed here. Stand down from buying/selling the break.*"
        )
        logger.info(f"Firing Trap Alert: {symbol} - {status}")
        if HAS_ESSENTIALS and WEBHOOK_TRADE_SIGNALS:
            send_essentials_embed(WEBHOOK_TRADE_SIGNALS, f"🛑 High Probability False Breakout Trap: {symbol}", payload, 0xe74c3c)

    def run_wheel_discovery(self):
        """Triggers the Analytics Engine and routes Prime Wheel Candidates."""
        from analytics import HighFidelityAnalyticsEngine
        engine = HighFidelityAnalyticsEngine()
        
        logger.info("Executing 10-Minute Wheel Discovery Scan...")
        candidates = engine.generate_wheel_candidates()
        if not candidates: return

        payload = "### **⚙️ Automated Wheel Strategy Discovery**\n*Highly curated 30-45 DTE Cash-Secured Put setups (~0.40 Delta).*\n\n"
        
        for c in candidates:
            payload += (
                f"**{c['symbol']}** | Spot: `${c['spot']:,.2f}`\n"
                f"┣ **Target Expiration**: `{c['expiration']}` ({c['dte']} DTE)\n"
                f"┣ **Optimal Strike**: `STO ${c['strike']:.1f} Put`\n"
                f"┗ **Capital Efficiency**: Est. `{c['annualized_roi']}%` Annualized ROI\n\n"
            )
            
        payload += "💡 *Directive: Sell puts on assets you are willing to own. If assigned, wheel into Covered Calls.*"
        
        if HAS_ESSENTIALS and WEBHOOK_DIVIDEND_CCETFS:
            send_essentials_embed(WEBHOOK_DIVIDEND_CCETFS, "🎡 PRIME WHEEL CANDIDATES", payload, 0x9b59b6)

    def execution_loop(self):
        logger.info("Starting Dual-Core Sentry daemon (Breakouts + Wheel Discovery)...")
        loop_counter = 0
        
        while True:
            # 1. False Breakout Sentry (Runs every loop ~ 2 mins)
            for symbol in self.watchlist:
                self.process_telemetry(symbol)
                time.sleep(2) # API pacing
            
            # 2. Wheel Discovery Scanner (Runs every 5th loop ~ 10 mins)
            if loop_counter % 5 == 0:
                try:
                    self.run_wheel_discovery()
                except Exception as e:
                    logger.error(f"Wheel Discovery Failed: {e}")
                
            loop_counter += 1
            time.sleep(120) # 2-minute base structural delay

if __name__ == "__main__":
    sentry = FalseBreakoutSentry()
    try:
        sentry.execution_loop()
    except KeyboardInterrupt:
        logger.info("Sentry daemon manually interrupted by operator. Shutting down gracefully.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Unhandled system crash: {e}")
        sys.exit(1)
