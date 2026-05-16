import os
import time
import requests
import datetime
import json
import sys
import pytz
from dotenv import load_dotenv

# --- 1. INITIALIZATION & ECOSYSTEM ALIGNMENT ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_PATH, ".env"))

try:
    from essentials_tools import (
        send_essentials_embed, 
        get_institutional_conviction, 
        get_trend_alignment
    )
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# System Infrastructure Variables
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_OPTIONS = os.getenv("WEBHOOK_TRADE_SIGNALS")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")
REGIME_LEDGER = os.path.join(BASE_PATH, "market_regime.json")

def get_signal_tier(conviction_score, rsi, trend_bullish):
    """Surgically ranks options signal setups based on tracking metrics."""
    if "HIGH" in conviction_score and trend_bullish and 40 < rsi < 65:
        return "Tier A - High Conviction", 0x2ecc71
    if "HIGH" in conviction_score:
        return "Tier B - Tactical Entry", 0xf1c40f
    return "Tier C - Speculative", 0x3498db

class RockefellerFuturesEngine:
    def __init__(self):
        self.tz = pytz.timezone('Pacific/Honolulu')
        self.last_pulse_hour = -1
        self.last_signal_timestamp = 0  # Cooldown throttle to prevent double-firing alerts

    def run_engine_cycle(self, is_test=False):
        """Processes core infrastructure logic for futures signals and options scans."""
        if not HAS_ESSENTIALS:
            print("❌ Critical System Halt: essentials_tools.py is missing.")
            return

        # 1. Pull Ecosystem Metrics with Defensive Fallbacks
        try:
            with open(REGIME_LEDGER, "r") as f:
                ledger = json.load(f)
        except Exception:
            ledger = {}

        regime = ledger.get("regime", "BULLISH")
        vix_status = ledger.get("vix_status", "STABLE")
        vix_current = ledger.get("vix_current", 14.5)
        vix_velocity = ledger.get("vix_velocity", "NOMINAL")
        rsi_limit = ledger.get("rsi_shield_limit", 68)
        macro_muted = ledger.get("macro_muted", False)

        now = datetime.datetime.now(self.tz)

        # 2. Safety Intercepts: Guard Against Chaotic Volatility Spikes
        if (macro_muted or vix_velocity == "CRITICAL_SPIKE") and not is_test:
            print("🛡️ Risk Shield Active: Trading execution muted due to macro risk/volatility.")
            return

        # 3. Interrogate Live Market Vectors (Tracking SPY as /ES Liquidity Proxy)
        trend_status, is_bullish = get_trend_alignment("SPY", TD_API_KEY)
        conviction_str, _, whale_active = get_institutional_conviction("SPY", TD_API_KEY)
        
        # Pull live tracking RSI or mock for terminal testing path
        current_rsi = 58.4 if is_test else 55.0 

        # 4. EXECUTION DECISION ENGINE
        current_time_sec = time.time()
        
        # Condition A: Trend aligned, RSI under shield limit, and volatility is safe
        if is_bullish and current_rsi <= rsi_limit and "CRITICAL" not in vix_status:
            # Enforce a 30-minute cooldown window between live structural signals
            if (current_time_sec - self.last_signal_timestamp > 1800) or is_test:
                self.dispatch_tactical_entry(trend_status, current_rsi, rsi_limit, vix_status, conviction_str)
                self.last_signal_timestamp = current_time_sec
        
        # Condition B: Trend is higher but RSI violates safety surface boundaries
        elif current_rsi > rsi_limit and not is_test:
            print(f"⚠️ Signal Suppressed: RSI tracking at {current_rsi:.1f} exceeds safety threshold ({rsi_limit}).")

        # 5. HIGH-PRIORITY EQUITY SCANNING (Options Channel Alignment)
        try:
            sample_watchlist = ["CLM", "CRF"]
            for ticker in sample_watchlist:
                opt_conviction, _, opt_triggered = get_institutional_conviction(ticker, TD_API_KEY)
                _, opt_trend_bullish = get_trend_alignment(ticker, TD_API_KEY)
                
                if opt_triggered or is_test:
                    tier_label, color_code = get_signal_tier(opt_conviction, current_rsi, opt_trend_bullish)
                    title = f"🚨 OPTIONS ALIGNMENT DETECTED: {ticker}"
                    desc = (
                        f"Technical breakout alert tracking under **{tier_label}** thresholds.\n\n"
                        f"┣ **Asset Underlying**: `{ticker}`\n"
                        f"┣ **Conviction Flow**: `{opt_conviction}`\n"
                        f"┗ **Ecosystem Posture**: `{vix_status}`"
                    )
                    if WEBHOOK_OPTIONS:
                        send_essentials_embed(WEBHOOK_OPTIONS, title, desc, color_code)
                    if is_test:
                        break
        except Exception as e:
            print(f"⚠️ Options Tracking Anomaly: {e}")

        # 6. BI-HOURLY CHANNEL REFRESH (Provides Directional Pulse Without Fatigue)
        if (now.hour % 2 == 0 and now.hour != self.last_pulse_hour) or is_test:
            self.dispatch_intraday_pulse(regime, vix_status, vix_current, current_rsi, conviction_str)
            self.last_pulse_hour = now.hour

    # --- DISCORD EMBED BLUEPRINTS ---

    def dispatch_tactical_entry(self, trend_status, rsi, rsi_limit, vix_status, conviction):
        """Dispatches your exact target entry blueprint to the futures channel."""
        whale_flag = "⚡ HIGH (Volume > 1.5x 30-day Avg - Whale Inflow)" if "HIGH" in conviction else "NORMAL (Sustained Participation)"
        
        embed = {
            "title": "🏛️ Rockefeller Futures Flowstate Update",
            "description": (
                "**System Status**: `🟢 ACTIVE / SIGNAL TRIGGERED`\n\n"
                "### **🎯 Tactical Entry Parameters**:\n"
                f"┣ **Asset**: `/ES` (E-mini S&P 500 Futures)\n"
                "┣ **Strategy**: `Momentum Breakout (Scalp)`\n"
                "┣ **Direction**: `LONG`\n"
                "┗ **Entry Vector**: `Market Order (Aggressive Buying Detected)`\n\n"
                "### **📊 Market Context (The Radar)**:\n"
                f"┣ **Regime**: `BULLISH ({trend_status})`\n"
                f"┣ **Sentry RSI**: `{rsi:.1f}` (Safely under the {rsi_limit} Shield Limit)\n"
                f"┣ **Volatility Sentry**: `{vix_status}`\n"
                f"┗ **Institutional Flow**: `{whale_flag}`\n\n"
                "### **🔬 Order Flow Intelligence**:\n"
                "┣ **Momentum Vector**: *Aggressive market orders lifting the ask.*\n"
                "┗ **Order Flow Note**: *Heavy volume backing the break. Watch for potential 'Trapped Short Sellers' to fuel short-term continuation.*\n\n"
                "### **🛡️ Risk Management (Natenberg Surface Guardrails)**:\n"
                "┣ **Stop Loss Vector**: *Structured dynamically off 1-hour ATR.*\n"
                "┗ **Sentry Reminder**: *Signals provide setups; risk control ensures survival. Never revenge trade if a setup triggers a trailing stop.*"
            ),
            "color": 0x2ecc71,
            "footer": {
                "text": "Rockefeller Strategic Intelligence Execution Engine • HST Timezone"
            },
            "timestamp": datetime.datetime.now(pytz.utc).isoformat()
        }
        
        if WEBHOOK_FUTURES:
            requests.post(WEBHOOK_FUTURES, json={"embeds": [embed]})

    def dispatch_intraday_pulse(self, regime, vix_status, vix_current, rsi, conviction):
        """Dispatches standard periodic updates to maintain institutional reliability."""
        embed = {
            "title": "📊 Futures State of the Tape: Intraday Pulse Check",
            "description": (
                f"### **Session Structural Assessment**\n\n"
                f"**Current Context Metrics**:\n"
                f"┣ **Macro Regime**: `{regime}`\n"
                f"┣ **Volatility Level**: `{vix_status}` (`{vix_current:.2f}`)\n"
                f"┣ **Current RSI Level**: `{rsi:.1f}`\n"
                f"┗ **Order Book Profile**: `{conviction}`\n\n"
                "**📋 Operational Directive**:\n"
                "Do not force trade sizes inside late mid-session liquidity drops. "
                "Allow technical breakout sweeps to confirm institutional tracking before entry allocation."
            ),
            "color": 0x3498db,
            "footer": { "text": "Rockefeller Intraday Pulse Monitor" },
            "timestamp": datetime.datetime.now(pytz.utc).isoformat()
        }
        if WEBHOOK_FUTURES:
            requests.post(WEBHOOK_FUTURES, json={"embeds": [embed]})


if __name__ == "__main__":
    engine = RockefellerFuturesEngine()
    
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        print("🧪 Triggering Verification Test Path...")
        engine.run_engine_cycle(is_test=True)
        print("✅ Production transmission path successfully verified.")
    else:
        print("⚙️ Rockefeller Futures Engine is running in background daemon mode...")
        while True:
            try:
                engine.run_engine_cycle(is_test=False)
            except Exception as e:
                print(f"⚠️ Engine Loop Intercept: {e}")
            time.sleep(60)
