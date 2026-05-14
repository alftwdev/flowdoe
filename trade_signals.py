import os
import time
import requests
import datetime
import json
import sys
from dotenv import load_dotenv

# --- 1. INITIALIZATION ---
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

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_OPTIONS = os.getenv("WEBHOOK_OPTIONS_SIGNALS")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")
REGIME_LEDGER = os.path.join(BASE_PATH, "market_regime.json")
SIGNAL_LOG = os.path.join(BASE_PATH, "signal_results.json")

# --- 2. ECOSYSTEM HANDSHAKE & SHIELD LOGIC ---

def update_regime_weather(pulse, change):
    """Updates shared ledger for mornings.py and other ecosystem members."""
    try:
        data = {}
        if os.path.exists(REGIME_LEDGER):
            with open(REGIME_LEDGER, "r") as f: 
                data = json.load(f)
        
        data.update({
            "futures_pulse": pulse,
            "futures_change": change,
            "last_handshake": datetime.datetime.now().isoformat()
        })
        with open(REGIME_LEDGER, "w") as f: 
            json.dump(data, f, indent=4)
    except: 
        pass

def check_shield_compliance(symbol, current_rsi=None):
    """
    Validates if the RSI is within the limits set by the Volatility Sentry.
    If current_rsi is not provided, it fetches it.
    """
    try:
        # 1. Fetch current RSI if not provided
        if current_rsi is None:
            url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=15min&outputsize=1&apikey={TD_API_KEY}"
            res = requests.get(url).json()
            current_rsi = float(res['values'][0]['rsi'])

        # 2. Check against Ledger
        with open(REGIME_LEDGER, "r") as f:
            data = json.load(f)
            rsi_limit = data.get("rsi_shield_limit", 66)
            vix_status = data.get("vix_status", "STABLE")
            
        if current_rsi > rsi_limit:
            print(f"🛡️ Shield Block: {symbol} RSI ({current_rsi:.1f}) exceeds {vix_status} limit ({rsi_limit})")
            return False, current_rsi
        return True, current_rsi
    except Exception as e:
        return True, 0.0 # Fail-safe: allow if ledger/API is unreadable

def get_market_shield():
    """SPY as proxy for global sentiment and /ES pulse."""
    try:
        url = f"https://api.twelvedata.com/quote?symbol=SPY&apikey={TD_API_KEY}"
        r = requests.get(url).json()
        change = float(r.get('percent_change', 0))
        
        if change <= -1.0: 
            status, safe = "🔴 BLEEDING (Risk-Off)", False
        elif change >= 0.5: 
            status, safe = "🟢 RISK-ON (High Conviction)", True
        else: 
            status, safe = "🟡 NEUTRAL", True
            
        update_regime_weather(status, change)
        return status, safe
    except:
        return "⚪ UNKNOWN", True

# --- 3. ALPHA ROUTING ---

def dispatch_alpha_signal(symbol, asset_type, strategy):
    """Dual-channel router with institutional conviction and RSI Shield checks."""
    
    # Strip slash for API calls
    api_symbol = symbol.replace("/", "") if asset_type == "FUTURES" else symbol

    # 1. Market-Wide Pulse Check
    shield_status, is_safe = get_market_shield()
    
    # 2. RSI Shield Compliance Check
    is_compliant, rsi_val = check_shield_compliance(api_symbol)
    if not is_compliant:
        return # Suppress alert if RSI is too high for current volatility

    # 3. Intelligence Gathering
    try:
        conviction, color, is_whale = get_institutional_conviction(api_symbol, TD_API_KEY)
        trend, is_bullish = get_trend_alignment(api_symbol, TD_API_KEY)
    except:
        conviction, trend, color, is_whale, is_bullish = "NORMAL", "NEUTRAL", 0x95a5a6, False, True

    target_webhook = WEBHOOK_FUTURES if asset_type == "FUTURES" else WEBHOOK_OPTIONS
    
    # Safety Filter: Only block Options if market is bleeding
    if not is_safe and asset_type == "OPTIONS":
        print(f"    [SENTRY] Signal for {symbol} suppressed: Market Pulse is Risk-Off.")
        return

    title = f"🏛️ Rockefeller Alpha: ${symbol}"
    
    if is_whale and is_bullish and is_safe:
        verdict = "⚡ **TOP-TIER CONVICTION**: Institutional Flow + Market Shield Alignment."
        color = 0x2ecc71
    else:
        verdict = "⚖️ **MEASURED SETUP**: Technical alignment present. Size appropriately."

    description = (
        f"### **Strategy: {strategy}**\n"
        f"**Conviction Matrix**:\n"
        f"┣ **Whale Activity**: `{conviction}`\n"
        f"┣ **Trend Shield**: `{trend}`\n"
        f"┣ **Futures Pulse**: `{shield_status}`\n"
        f"┗ **RSI Level**: `{rsi_val:.1f}`\n\n"
        f"**Tactical Verdict**: {verdict}"
    )

    if HAS_ESSENTIALS and target_webhook:
        send_essentials_embed(target_webhook, title, description, color)
        log_for_audit(symbol, asset_type, strategy)
        print(f"    [DISPATCH] {symbol} alpha sent to Discord.")

def log_for_audit(symbol, asset_type, strategy):
    """Feeds weekly_digest.py for statistical reward percentages."""
    entry = {
        "time": str(datetime.datetime.now()), 
        "symbol": symbol, 
        "type": asset_type, 
        "strat": strategy
    }
    try:
        log = []
        if os.path.exists(SIGNAL_LOG):
            with open(SIGNAL_LOG, "r") as f: 
                log = json.load(f)
        log.append(entry)
        with open(SIGNAL_LOG, "w") as f: 
            json.dump(log, f, indent=4)
    except: 
        pass

if __name__ == "__main__":
    if "test" in sys.argv:
        print("--- [ECOSYSTEM TEST] ---")
        # Testing a standard stock/option
        dispatch_alpha_signal("MSTY", "OPTIONS", "Yield-Capture Strategy")
        time.sleep(1)
        # Testing a futures contract
        dispatch_alpha_signal("/ES", "FUTURES", "Momentum Breakout")
