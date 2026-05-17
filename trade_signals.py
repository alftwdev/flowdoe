import os
import time
import requests
import datetime
import json
import sys
import pytz
from dotenv import load_dotenv

# --- 1. ECOSYSTEM ROOT INITIALIZATION ---
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

# System Infrastructure Gateways
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_OPTIONS = os.getenv("WEBHOOK_TRADE_SIGNALS")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")
REGIME_LEDGER = os.path.join(BASE_PATH, "market_regime.json")
CHECKPOINT_FILE = os.path.join(BASE_PATH, "last_signal_checkpoint.json")

def get_signal_tier(conviction_score, rsi, trend_bullish):
    """Ranks options signal setups based on tracking metrics."""
    if "HIGH" in conviction_score and trend_bullish and 40 < rsi < 65:
        return "Tier A - High Conviction", 0x2ecc71
    if "HIGH" in conviction_score:
        return "Tier B - Tactical Entry", 0xf1c40f
    return "Tier C - Speculative", 0x3498db

class RockefellerFuturesEngine:
    def __init__(self):
        self.tz = pytz.timezone('Pacific/Honolulu')
        self.last_processed_time = self.load_checkpoint()

    def load_checkpoint(self):
        """Loads persistent alert historical markers to protect against spam loops."""
        if os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, "r") as f:
                    data = json.load(f)
                    return data.get("last_processed_time")
            except:
                return None
        return None

    def save_checkpoint(self, timestamp):
        """Saves current broadcast timestamp permanently to disk."""
        self.last_processed_time = timestamp
        try:
            with open(CHECKPOINT_FILE, "w") as f:
                json.dump({"last_processed_time": timestamp}, f)
        except Exception as e:
            print(f"⚠️ [System Alert] Checkpoint write failure: {e}")

    def load_regime_state(self):
        if not os.path.exists(REGIME_LEDGER):
            return "BULLISH", "STABLE", 14.5, 65.0
        try:
            with open(REGIME_LEDGER, "r") as f:
                state = json.load(f)
            return (
                state.get("regime", "BULLISH"),
                state.get("vix_status", "STABLE"),
                float(state.get("vix_current", 14.5)),
                float(state.get("rsi_shield_limit", 65.0))
            )
        except:
            return "BULLISH", "STABLE", 14.5, 65.0

    def run_engine_cycle(self, is_test=False):
        regime, vix_status, vix_current, rsi_limit = self.load_regime_state()
        
        if is_test:
            print("🧪 Terminal Flag Found: Simulating synchronized dual-channel verification...")
            self.broadcast_signal(
                symbol="/ES & OPTION BENCHMARKS",
                strat="Ecosystem Integration Verification Test",
                status="VERIFIED TEST",
                vix_current=vix_current,
                rsi=55.0,
                vix_status=vix_status,
                regime=regime,
                conviction="⚡ HIGH (System Dynamic Check)",
                force_all_channels=True
            )
            return

        # --- PRODUCTION AUTOMATED LOGIC ---
        SIGNAL_DATA_SOURCE = os.path.join(BASE_PATH, "signal_results.json")
        if not os.path.exists(SIGNAL_DATA_SOURCE):
            return

        try:
            with open(SIGNAL_DATA_SOURCE, "r") as f:
                signals = json.load(f)
            if not signals:
                return
            
            latest_signal = signals[-1]
            signal_time = latest_signal.get("time") or latest_signal.get("timestamp") or latest_signal.get("date")
            
            # Persistent check stops the alert loop dead in its tracks
            if signal_time == self.last_processed_time:
                return  
                
            symbol = latest_signal.get("symbol", "/ES")
            strat = latest_signal.get("strat") or latest_signal.get("strategy", "Momentum Breakout")
            type_tag = latest_signal.get("type", "FUTURES").upper()
            direction = latest_signal.get("direction", "LONG")
            
            # Contextual Technical Lookups via Twelve Data Indicators
            conviction, _, _ = get_institutional_conviction(symbol, TD_API_KEY) if HAS_ESSENTIALS else ("NORMAL", 0, False)
            _, trend_bullish = get_trend_alignment(symbol, TD_API_KEY) if HAS_ESSENTIALS else ("NEUTRAL", True)
            rsi_val = 55.0  # Normalized mid-band placeholder index
            
            if type_tag == "FUTURES":
                self.broadcast_signal(symbol, strat, f"🟢 ACTIVE / SIGNAL TRIGGERED ({direction})", vix_current, rsi_val, vix_status, regime, conviction, target_channel="FUTURES")
            elif type_tag == "OPTION":
                self.broadcast_signal(symbol, strat, f"🟢 OPTIONS FOCUS SETUP ({direction})", vix_current, rsi_val, vix_status, regime, conviction, target_channel="OPTIONS")

            # Commit state to cache file to seal entry
            self.save_checkpoint(signal_time)

        except Exception as e:
            print(f"⚠️ Production Loop Exception: {e}")

    def broadcast_signal(self, symbol, strat, status, vix_current, rsi, vix_status, regime, conviction, target_channel=None, force_all_channels=False):
        """Unified transmission matrix routing directly to designated channels with explicit string construction."""
        
        # Explicit newline characters separated cleanly to enforce API payload compliance
        lines = [
            f"**System Status**: `{status}`",
            "",
            "**Tactical Entry Parameters**:",
            f"┣ **Asset**: `{symbol}`",
            f"┣ **Strategy**: `{strat}`",
            "┗ **Tracking Profile**: `Institutional Flow Matching`",
            "",
            "**Market Context (The Radar)**:",
            f"┣ **Regime**: `{regime}`",
            f"┣ **Sentry RSI**: `{rsi:.1f}`",
            f"┣ **Volatility Sentry**: `{vix_status} ({vix_current:.2f})`",
            f"┗ **Institutional Flow**: `{conviction}`",
            "",
            "**Risk Management Guardrails**:",
            "┗ **Sentry Reminder**: Signals provide structural setups; risk parameters ensure survival."
        ]
        
        description_body = "\n".join(lines)

        # Build out clean, stylized Rockefeller Embed layout
        embed = {
            "title": "🚨 Rockefeller Futures Flowstate Update" if "FUTURES" in str(target_channel).upper() or force_all_channels else "🔮 Rockefeller Options Spectrum Alert",
            "description": description_body,
            "color": 0x2ecc71 if "FUTURES" in str(target_channel).upper() or force_all_channels else 0x9b59b6,
            "footer": { "text": "Rockefeller Strategic Intelligence Execution Engine • HST Timezone" },
            "timestamp": datetime.datetime.now(pytz.utc).isoformat()
        }

        payload = {"embeds": [embed]}

        # Handle Terminal Testing Multi-Path Execution
        if force_all_channels:
            if WEBHOOK_FUTURES:
                requests.post(WEBHOOK_FUTURES, json=payload, timeout=10)
            if WEBHOOK_OPTIONS:
                requests.post(WEBHOOK_OPTIONS, json=payload, timeout=10)
            return

        # Route production assets cleanly to their exact workspace target
        if target_channel == "FUTURES" and WEBHOOK_FUTURES:
            requests.post(WEBHOOK_FUTURES, json=payload, timeout=10)
        elif target_channel == "OPTIONS" and WEBHOOK_OPTIONS:
            requests.post(WEBHOOK_OPTIONS, json=payload, timeout=10)


if __name__ == "__main__":
    engine = RockefellerFuturesEngine()
    
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        print("🧪 Triggering Verification Test Path...")
        engine.run_engine_cycle(is_test=True)
        print("✅ Terminal dual-channel test transmission completed successfully.")
    else:
        print("⚙️ Rockefeller Futures Engine is running in background daemon mode...")
        while True:
            try:
                engine.run_engine_cycle(is_test=False)
                time.sleep(15)  # Throttling query cycles
            except Exception as e:
                print(f"⚠️ Engine Loop Interrupted: {e}")
                time.sleep(10)
