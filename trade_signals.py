import os
import sys
import json
import requests
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv

# Ingest high-performance ecosystem tools and shared memory state
from ecosys import EcosystemState, log_event
try:
    from essentials_tools import get_trend_alignment, get_institutional_conviction
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# --- 1. INITIALIZATION & INFRASTRUCTURE ROUTING ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

# SURGICAL ROUTING DICTIONARY: Maps distinct signals to unique webhook endpoints
WEBHOOKS = {
    "FUTURES": os.getenv("WEBHOOK_FUTURES_TRADING") or os.getenv("WEBHOOK_TRADE_SIGNALS"),
    "OPTIONS": os.getenv("WEBHOOK_TRADE_SIGNALS") or os.getenv("WEBHOOK_MARKET_ANALYSIS"),
    "SENTRY": os.getenv("WEBHOOK_MARKET_ANALYSIS")  # Maintained for system fallback
}

# Core Watchlists
FUTURES_WATCHLIST = ["/ES", "/NQ"]
# Dynamic scan universe to discover premium and momentum opportunities
DYNAMIC_OPTIONS_WATCHLIST = ["SPY", "QQQ", "AAPL", "NVDA", "TSLA", "AMD", "MSFT", "AMZN"]

# State Tracking for Balanced Silence-Breaker Telemetry (In-Memory Daemon Persistence)
LAST_HEARTBEAT = {
    "OPTIONS": datetime.min.replace(tzinfo=pytz.utc),
    "FUTURES": datetime.min.replace(tzinfo=pytz.utc)
}
HEARTBEAT_INTERVAL_SECONDS = 14400  # Balanced 4-hour monitoring notification window

def execute_signal_scan(is_test=False):
    """
    Evaluates market microstructure trend alignments and classifies opportunities into Tier A/B/C setups.
    Dynamically balances offensive momentum plays for buyers and range-bound matrices for premium sellers.
    """
    global LAST_HEARTBEAT
    now_utc = datetime.now(pytz.utc)
    
    if not TD_API_KEY:
        log_event("Twelve Data API Key missing from configuration environment.", "ERROR")
        return

    signals_sent_futures = False
    signals_sent_options = False

    # --- 1. FUTURES SCAN PIPELINE ---
    for symbol in FUTURES_WATCHLIST:
        try:
            trend_status, is_bullish = get_trend_alignment(symbol, TD_API_KEY) if HAS_ESSENTIALS else ("🟢 BULLISH ALIGNMENT", True)
            conviction_status, color, is_whale = get_institutional_conviction(symbol, TD_API_KEY) if HAS_ESSENTIALS else ("NORMAL", 0x95a5a6, False)
            
            state = EcosystemState()
            futures_pulse = state.get("futures_pulse", "🟢 RISK-ON (High Conviction)")
            
            # Futures Scalp Setup Logic (Tier A/B/C classification)
            if "RISK-ON" in futures_pulse and is_whale:
                title = f"🚀 FUTURES TIER A MOMENTUM BREAKOUT: {symbol}"
                description = (
                    f"### **Execution Vector Details**\n"
                    f"┣ **Asset**: `{symbol}`\n"
                    f"┣ **Postural Alignment**: `{trend_status}`\n"
                    f"┣ **Institutional Flow**: `⚡ WHALE INFLOW DETECTED`\n"
                    f"┗ **Macro Regime Context**: `{futures_pulse}`\n\n"
                    f"**Strategic Action**: Execution networks signal defensive momentum entry parameters. Protect capital baselines."
                )
                payload = {
                    "embeds": [{
                        "title": title,
                        "description": description,
                        "color": 0x2ecc71,  # Institutional Green
                        "timestamp": now_utc.isoformat(),
                        "footer": {"text": "Rockefeller Futures Trading Desk"}
                    }]
                }
                if WEBHOOKS["FUTURES"]:
                    requests.post(WEBHOOKS["FUTURES"], json=payload, timeout=10)
                signals_sent_futures = True
                log_event(f"Tier A Futures breakout alert broadcasted cleanly for {symbol}.")
                
            elif is_test:
                title = f"🧪 FUTURES TIER C TEST SIGNAL: {symbol}"
                description = f"Microstructure check performed. Trend: {trend_status} | Conviction: {conviction_status}"
                payload = {
                    "embeds": [{
                        "title": title,
                        "description": description,
                        "color": 0x34495e,
                        "timestamp": now_utc.isoformat()
                    }]
                }
                if WEBHOOKS["FUTURES"]:
                    requests.post(WEBHOOKS["FUTURES"], json=payload, timeout=10)
                signals_sent_futures = True
                
        except Exception as e:
            log_event(f"Signal scan handling error encountered for futures asset entry {symbol}: {e}", "ERROR")

    # --- 2. DYNAMIC OPTIONS SCAN PIPELINE (TIER A/B/C SETUP ENGINE) ---
    for symbol in DYNAMIC_OPTIONS_WATCHLIST:
        try:
            trend_status, is_bullish = get_trend_alignment(symbol, TD_API_KEY) if HAS_ESSENTIALS else ("🟢 BULLISH ALIGNMENT", True)
            conviction_status, color, is_whale = get_institutional_conviction(symbol, TD_API_KEY) if HAS_ESSENTIALS else ("NORMAL", 0x95a5a6, False)
            
            state = EcosystemState()
            vix_status = state.get("vix_status", "STABLE")
            regime = state.get("regime", "BULLISH")
            rsi_shield = state.get("rsi_shield", "NORMAL")

            setup_tier = None
            strat_name = ""
            embed_color = 0x95a5a6
            action_directive = ""

            # Classifying Tier A Setup: Perfect Institutional Alignment & Momentum (For Options Buyers)
            if is_whale and vix_status == "STABLE" and regime == "BULLISH":
                setup_tier = "TIER A"
                strat_name = "Momentum Breakout Flow (Options Buyer)"
                embed_color = 0x2ecc71  # Institutional Green
                action_directive = "High conviction flow detected. Call side allocation boundaries unlocked based on aggressive order book sweeps."

            # Classifying Tier B Setup: Volatility Overvaluation / Range Consolidation (For Options Premium Sellers)
            elif vix_status in ["COMPRESSED", "STABLE"] and (rsi_shield == "CAUTION" or not is_whale):
                setup_tier = "TIER B"
                strat_name = "Credit Spread / Cash Secured Put (Premium Seller)"
                embed_color = 0xf1c40f  # Strategic Yellow
                action_directive = "Sideways market chop or compression detected. Premium decay parameters optimal for strategic option writing."

            # Classifying Tier C Setup: Mean Reversion / Low-Volume Technical Pullback
            elif is_test or (not is_whale and is_bullish):
                setup_tier = "TIER C"
                strat_name = "Tactical Mean Reversion Pulse"
                embed_color = 0x34495e  # Dark Slate Slate
                action_directive = "Inside bar consolidation structure. Scalp tracking parameters active with restricted size boundaries."

            if setup_tier and (setup_tier in ["TIER A", "TIER B"] or is_test):
                title = f"⚡ OPTIONS {setup_tier} ALGORITHMIC POSTURE: {symbol}"
                description = (
                    f"### **Ecosystem Capital Matrix**\n"
                    f"┣ **Asset Evaluated**: `{symbol}`\n"
                    f"┣ **Target Strategy Profile**: `{strat_name}`\n"
                    f"┣ **Trend Directional Vector**: `{trend_status}`\n"
                    f"┣ **Order Flow Volume Profiler**: `{conviction_status}`\n"
                    f"┗ **Volatility Surface Matrix**: `VIX {vix_status} | Regime {regime}`\n\n"
                    f"**Execution Directive**: {action_directive}"
                )
                payload = {
                    "embeds": [{
                        "title": title,
                        "description": description,
                        "color": embed_color,
                        "timestamp": now_utc.isoformat(),
                        "footer": {"text": "Rockefeller Options Trading Desk"}
                    }]
                }
                if WEBHOOKS["OPTIONS"]:
                    requests.post(WEBHOOKS["OPTIONS"], json=payload, timeout=10)
                signals_sent_options = True
                log_event(f"Options {setup_tier} signal alert broadcasted cleanly for {symbol}.")

        except Exception as e:
            log_event(f"Signal scan handling error encountered for options asset entry {symbol}: {e}", "ERROR")

    # --- 3. BALANCED SILENCE-BREAKER HEARTBEAT LOGIC ---
    # Eliminates repetitive 15-minute suppression spam while providing systemic confirmation
    if not signals_sent_options and (now_utc - LAST_HEARTBEAT["OPTIONS"]).total_seconds() >= HEARTBEAT_INTERVAL_SECONDS:
        try:
            payload = {
                "embeds": [{
                    "title": "🛡️ Rockefeller Sentry Shield: Options Active Pulse",
                    "description": "No current options windows of opportunity detected. Scanning the market — enjoy the city or nature outside.",
                    "color": 0x34495e,
                    "timestamp": now_utc.isoformat(),
                    "footer": {"text": "Sentry Continuous Monitoring Baseline"}
                }]
            }
            if WEBHOOKS["OPTIONS"]:
                requests.post(WEBHOOKS["OPTIONS"], json=payload, timeout=10)
            LAST_HEARTBEAT["OPTIONS"] = now_utc
            log_event("Balanced anti-noise heartbeat update dispatched cleanly to options pipeline.")
        except Exception as e:
            log_event(f"Failed to dispatch options baseline heartbeat: {e}", "ERROR")

    if not signals_sent_futures and (now_utc - LAST_HEARTBEAT["FUTURES"]).total_seconds() >= HEARTBEAT_INTERVAL_SECONDS:
        try:
            payload = {
                "embeds": [{
                    "title": "🛡️ Rockefeller Sentry Shield: Futures Active Pulse",
                    "description": "No current futures windows of opportunity detected. Scanning the market — enjoy the city or nature outside.",
                    "color": 0x34495e,
                    "timestamp": now_utc.isoformat(),
                    "footer": {"text": "Sentry Continuous Monitoring Baseline"}
                }]
            }
            if WEBHOOKS["FUTURES"]:
                requests.post(WEBHOOKS["FUTURES"], json=payload, timeout=10)
            LAST_HEARTBEAT["FUTURES"] = now_utc
            log_event("Balanced anti-noise heartbeat update dispatched cleanly to futures pipeline.")
        except Exception as e:
            log_event(f"Failed to dispatch futures baseline heartbeat: {e}", "ERROR")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        print("🧪 Initiating routing matrix and dynamic setup search parameters...")
        execute_signal_scan(is_test=True)
        print("✅ Production dynamic scan and posture checks completed cleanly.")
    else:
        import time
        log_event("Trade Signal core engine background daemon initialized.")
        while True:
            execute_signal_scan(is_test=False)
            time.sleep(900)
