import os
import json
import requests
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from ecosys import EcosystemState, log_event

# --- 1. INITIALIZATION & CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

STATE_FILE = os.path.join(BASE_DIR, "last_alert.json") 
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_CRYPTO = os.getenv("WEBHOOK_CRYPTO") or os.getenv("WEBHOOK_MARKET_ANALYSIS")

# Pushover Integration Credentials
PUSHOVER_APP_TOKEN = os.getenv("PUSHOVER_APP_TOKEN")
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")

ESSENTIALS_BRAND_WATERMARK = "https://images-ext-1.discordapp.net/external/.../your_image.png"

def get_last_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def save_current_state(state_data):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state_data, f, indent=4)
    except Exception as e:
        log_event(f"Failed to record state metadata: {e}", "ERROR")

def send_pushover_alert(message):
    """Dispatches emergency notifications straight to your Pushover application."""
    if not PUSHOVER_APP_TOKEN or not PUSHOVER_USER_KEY:
        log_event("Pushover credentials missing from ecosystem variables.", "ERROR")
        return
    try:
        url = "https://api.pushover.net/1/messages.json"
        data = {
            "token": PUSHOVER_APP_TOKEN,
            "user": PUSHOVER_USER_KEY,
            "message": message,
            "title": "🚨 Rockefeller Ecosystem Sentry"
        }
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        log_event(f"Pushover notification failure: {e}", "ERROR")

def broadcast_system_health():
    """Generates and dispatches a comprehensive state-of-the-market intelligence pulse."""
    if not WEBHOOK_MARKET:
        log_event("Market analysis webhook variable unassigned. Aborting pulse.", "ERROR")
        return

    state = EcosystemState()
    regime = state.get("regime", "BULLISH")
    vix_status = state.get("vix_status", "STABLE")
    vix_current = state.get("vix_current", 14.50)
    vix_velocity = state.get("vix_velocity", "STABLE")
    futures_pulse = state.get("futures_pulse", "🟢 RISK-ON")

    # Generate Tactical Intelligence Narrative
    outlook_text = "Market conditions remain ideal for structured strategic parameters. Implied volatility parameters are compressing across standard levels."
    
    # Surgical Inversion Check: Determine if the current environment shifts posture
    if vix_velocity == "ACCELERATING" or vix_current > 19.5:
        outlook_text = "System detects dynamic expansion. Risk-mitigation matrices recommend shifting focus toward defensive covered calls and premium options pricing adjustments to capture decay thresholds."

    # Surgical Double Tagging Optimization Rules
    # If the narrative contains 'options', append an explicit callout to the signal channel natively
    content_payload = ""
    if "options" in outlook_text.lower():
        content_payload = "📢 **System Notice**: Options-focused market posture identified. Cross-referencing telemetry stream with `#options-signal`."

    embed = {
        "title": "🏛️ Rockefeller Ecosystem Pulse Snapshot",
        "description": (
            f"### **Ecosystem Structural Posture**\n"
            f"┣ **Market Regime**: `{regime}`\n"
            f"┣ **Futures Pulse**: {futures_pulse}\n"
            f"┣ **Volatility Index**: `{vix_current:.2f}` (`{vix_status}`)\n"
            f"┗ **Volatility Velocity**: `{vix_velocity}`\n\n"
            f"📊 **Market Outlook Matrix**\n"
            f"{outlook_text}\n\n"
            f"*State update executed by background radar daemons. Muted parameters active.*"
        ),
        "color": 0x34495e if vix_velocity != "ACCELERATING" else 0xe74c3c,
        "timestamp": datetime.now(pytz.utc).isoformat(),
        "footer": {"text": "Rockefeller Asset Management Engine"}
    }

    try:
        payload = {"embeds": [embed]}
        if content_payload:
            payload["content"] = content_payload
            
        requests.post(WEBHOOK_MARKET, json=payload, timeout=12)
        log_event("Ecosystem health summary broadcasted to Discord matrix.")
    except Exception as e:
        log_event(f"Failed to transmit pulse telemetry: {e}", "ERROR")

def fetch_crypto_intelligence(is_test=False):
    """Executes structural scans over background digital asset valuations."""
    log_event("Executing background crypto asset volume valuation sweep...")
    if is_test:
        broadcast_system_health()

def run_radar_cycle():
    """Main background loop tracking state handshakes and managing execution intervals."""
    state = EcosystemState()
    
    # Deadman Switch Evaluation Check
    last_handshake_str = state.get("last_handshake")
    if last_handshake_str:
        try:
            last_handshake = datetime.fromisoformat(last_handshake_str)
            tz_hst = pytz.timezone('Pacific/Honolulu')
            now_hst = datetime.now(tz_hst)
            
            # If the background tracking stream goes completely silent for more than 1 hour, trigger Pushover
            if (now_hst - last_handshake).total_seconds() > 3600:
                send_pushover_alert("⚠️ SYSTEM FAILURE: Volatility sentinel handshake lost for over 60 minutes. Check daemon runtime status.")
        except Exception as e:
            log_event(f"Deadman framework validation exception: {e}", "ERROR")

    # 4-Hour System Pulse Interval Trigger (Broadcasts strictly on 4-hour cycle boundaries)
    now = datetime.now()
    if now.hour % 4 == 0 and now.minute == 0:
        broadcast_system_health()

    if now.minute == 0:
        fetch_crypto_intelligence(is_test=False)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        log_event("Initiating structural validation checks via testing harness parameters...")
        fetch_crypto_intelligence(is_test=True)
    else:
        log_event("Macro Radar core engine is executing in systemic background configuration...")
        import time
        while True:
            try:
                run_radar_cycle()
                time.sleep(60)
            except Exception as e:
                log_event(f"Radar loop tracking error intercepted: {e}", "ERROR")
                time.sleep(30)
