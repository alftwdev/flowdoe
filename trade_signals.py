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

# SECURITY DETECTOR: Paste your direct Discord image URL link here (Must end in .png or .jpg)
ESSENTIALS_BRAND_WATERMARK = "https://images-ext-1.discordapp.net/external/.../your_image.png"

def get_rsi_context_gauge(rsi_val):
    """Dynamically converts a raw RSI float into an actionable market zone gauge."""
    if rsi_val <= 30:
        return f"`{rsi_val:.1f}` 🔴 OVERSOLD (Exhaustion Sweep)"
    elif 30 < rsi_val <= 40:
        return f"`{rsi_val:.1f}` 🟡 BEARISH DISPLACEMENT"
    elif 40 < rsi_val <= 60:
        return f"`{rsi_val:.1f}` 🔵 NEUTRAL BALANCE (Fair Value Zone)"
    elif 60 < rsi_val <= 70:
        return f"`{rsi_val:.1f}` 🟢 BULLISH ACCELERATION"
    else:
        return f"`{rsi_val:.1f}` 🔴 OVERBOUGHT (Extension Risk)"

def get_risk_allocation_matrix(vix_status, regime):
    """Evaluates systemic volatility to deliver structural risk parameters."""
    vix_clean = str(vix_status).upper()
    regime_clean = str(regime).upper()
    
    if "SPIKING" in vix_clean or "CRITICAL" in vix_clean:
        return "🚨 HIGH RISK (Throttled Sizes / Trailing Stops Mandated)"
    elif "EXPANDING" in vix_clean or "BEARISH" in regime_clean:
        return "⚠️ MODERATE RISK (Standard Tactical Sizing / Tight Safeguards)"
    else:
        return "🛡️ LOW RISK (Optimal Market Environment for Trend Scaling)"

def generate_canary_fingerprint(base_text, timestamp_str):
    """Surgically injects a unique, invisible tracking signature into text block strings."""
    if not timestamp_str:
        return base_text
    hash_val = sum(ord(c) for c in timestamp_str)
    selector = hash_val % 4
    
    zw_space = "\u200b"
    zw_non_joiner = "\u200c"
    zw_joiner = "\u200d"
    
    if selector == 0:
        fingerprint = zw_space + zw_non_joiner
    elif selector == 1:
        fingerprint = zw_joiner + zw_space
    elif selector == 2:
        fingerprint = zw_non_joiner + zw_joiner
    else:
        fingerprint = zw_space + zw_joiner + zw_non_joiner
        
    if base_text.endswith("."):
        return base_text[:-1] + fingerprint + "."
    return base_text + fingerprint

class RockefellerFuturesEngine:
    def __init__(self):
        self.tz = pytz.timezone('Pacific/Honolulu')
        self.last_processed_time = self.load_checkpoint()

    def load_checkpoint(self):
        if os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, "r") as f:
                    data = json.load(f)
                    return data.get("last_processed_time")
            except:
                return None
        return None

    def save_checkpoint(self, timestamp):
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
            print("🧪 Terminal Flag Found: Simulating updated trader layout execution...")
            self.broadcast_signal(
                symbol="/ES FUTURES CONTRACT (JUNE DESK)",
                strat="Momentum Order-Book Expansion",
                status="VERIFIED SYSTEM UPDATE",
                vix_current=vix_current,
                rsi=55.4,
                vix_status=vix_status,
                regime=regime,
                conviction="⚡ HIGH (System Dynamic Check)",
                timestamp_marker="TEST_VERIFICATION_MATRIX",
                force_all_channels=True
            )
            return

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
            
            if signal_time == self.last_processed_time:
                return  
                
            symbol = latest_signal.get("symbol", "/ES")
            strat = latest_signal.get("strat") or latest_signal.get("strategy", "Momentum Breakout")
            type_tag = latest_signal.get("type", "FUTURES").upper()
            direction = latest_signal.get("direction", "LONG")
            
            conviction, _, _ = get_institutional_conviction(symbol, TD_API_KEY) if HAS_ESSENTIALS else ("NORMAL", 0, False)
            rsi_val = 52.3  # Dynamic technical lookup placeholder
            
            # Append execution context modifiers based on order flow tracking
            formatted_strat = f"{strat} (Directional {direction})"
            
            if type_tag == "FUTURES":
                self.broadcast_signal(f"{symbol} CONTRACT", formatted_strat, f"🟢 DISPATCHING ACTIVE SETUP", vix_current, rsi_val, vix_status, regime, conviction, timestamp_marker=signal_time, target_channel="FUTURES")
            elif type_tag == "OPTION":
                self.broadcast_signal(symbol, formatted_strat, f"🔮 OPTIONS FOCUS REGISTRATION", vix_current, rsi_val, vix_status, regime, conviction, timestamp_marker=signal_time, target_channel="OPTIONS")

            self.save_checkpoint(signal_time)

        except Exception as e:
            print(f"⚠️ Production Loop Exception: {e}")

    def broadcast_signal(self, symbol, strat, status, vix_current, rsi, vix_status, regime, conviction, timestamp_marker=None, target_channel=None, force_all_channels=False):
        """Dispatches refined, branded analytics directly to targeted ecosystem endpoints."""
        
        # Interpret raw numeric inputs into institutional gauges
        rsi_gauge = get_rsi_context_gauge(rsi)
        risk_gauge = get_risk_allocation_matrix(vix_status, regime)
        
        # Inject invisible security trace marker into system baseline parameter
        protected_risk = generate_canary_fingerprint(risk_gauge, timestamp_marker)

        lines = [
            f"**System Status**: `{status}`",
            "",
            "**Tactical Entry Parameters**:",
            f"┣ **Trading Vehicle**: `{symbol}`",
            f"┣ **Execution Strategy**: `{strat}`",
            "┗ **Tracking Profile**: `Institutional Tape Matching`",
            "",
            "**Market Context (The Radar)**:",
            f"┣ **Market Outlook**: `{regime} REGIME`",
            f"┣ **Sentry RSI Vector**: {rsi_gauge}",
            f"┣ **Volatility Surface**: `{vix_status} ({vix_current:.2f})`",
            f"┗ **Order Book Flow**: `{conviction}`",
            "",
            "**Risk Management Guardrails**:",
            f"┗ **Allocation Parameter**: {protected_risk}"
        ]
        
        description_body = "\n".join(lines)
        channel_title = "🚨 ESSENTIALS Futures Flowstate Update" if "FUTURES" in str(target_channel).upper() or force_all_channels else "🔮 ESSENTIALS Options Spectrum Alert"

        embed = {
            "title": channel_title,
            "description": description_body,
            "color": 0x2ecc71 if "FUTURES" in str(target_channel).upper() or force_all_channels else 0x9b59b6,
            "author": {
                "name": "ESSENTIALS Systems",
                "icon_url": ESSENTIALS_BRAND_WATERMARK
            },
            "thumbnail": {
                "url": ESSENTIALS_BRAND_WATERMARK
            },
            "footer": { "text": "ESSENTIALS Execution Engine • HST Timezone" },
            "timestamp": datetime.datetime.now(pytz.utc).isoformat()
        }

        payload = {"embeds": [embed]}

        if force_all_channels:
            if WEBHOOK_FUTURES:
                requests.post(WEBHOOK_FUTURES, json=payload, timeout=10)
            if WEBHOOK_OPTIONS:
                requests.post(WEBHOOK_OPTIONS, json=payload, timeout=10)
            return

        if target_channel == "FUTURES" and WEBHOOK_FUTURES:
            requests.post(WEBHOOK_FUTURES, json=payload, timeout=10)
        elif target_channel == "OPTIONS" and WEBHOOK_OPTIONS:
            requests.post(WEBHOOK_OPTIONS, json=payload, timeout=10)


if __name__ == "__main__":
    engine = RockefellerFuturesEngine()
    
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        engine.run_engine_cycle(is_test=True)
    else:
        while True:
            try:
                engine.run_engine_cycle(is_test=False)
                time.sleep(15)
            except Exception as e:
                time.sleep(10)
