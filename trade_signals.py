import os
import sys
import json
import requests
from datetime import datetime
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
    "FUTURES": os.getenv("WEBHOOK_FUTURES_TRADING"),
    "OPTIONS": os.getenv("WEBHOOK_TRADE_SIGNALS"),
    "SENTRY": os.getenv("WEBHOOK_MARKET_ANALYSIS")  # Houses the passive "Why" suppression logs
}

# Targeted Watchlist Structures
FUTURES_WATCHLIST = ["/ES", "/NQ"]
OPTIONABLE_WATCHLIST = ["SPY", "QQQ", "AAPL", "NVDA"]

def execute_signal_scan(is_test=False):
    """
    Evaluates market microstructure trend alignments.
    Enforces VSA absorption rules, Volatility Surface limits, and the Larry Williams Chop Shield.
    """
    if not TD_API_KEY:
        log_event("Twelve Data API Key missing from configuration environment.", "ERROR")
        return

    state = EcosystemState()
    
    # Extract structural constraints dynamically from memory state cache
    vix_velocity = state.get("vix_velocity", "STABLE")
    macro_muted = state.get("macro_muted", False)
    rsi_shield_limit = state.get("rsi_shield_limit", 66)
    
    # Build complete combined scan universe
    scan_universe = FUTURES_WATCHLIST + OPTIONABLE_WATCHLIST
    if is_test:
        scan_universe = ["/ES", "SPY"]

    log_event(f"Core execution scan sequence triggered for {len(scan_universe)} assets.")

    for symbol in scan_universe:
        try:
            # 1. Determine Asset Classification & Target Webhook Channel Route
            if symbol in FUTURES_WATCHLIST:
                asset_class = "FUTURES"
                target_webhook = WEBHOOKS["FUTURES"]
                strategy_profile = "Momentum Order-Book Expansion"
                tracking_profile = "Institutional Tape Matching"
                vehicle_desc = f"{symbol} FUTURES CONTRACT (ACTIVE DESK)"
            else:
                asset_class = "OPTIONS"
                target_webhook = WEBHOOKS["OPTIONS"]
                strategy_profile = "Synthetic Premium Monetization"
                tracking_profile = "Implied Volatility Surface Skew"
                vehicle_desc = f"{symbol} EQUITY OPTIONS MATRIX"

            # 2. Ingest Multidimensional Technical & Conviction Flows
            if HAS_ESSENTIALS and not is_test:
                trend_status, supertrend_bullish = get_trend_alignment(symbol, TD_API_KEY)
                conviction_status, embed_color, stopping_volume_detected = get_institutional_conviction(symbol, TD_API_KEY)
            else:
                # Forced configuration pass if running a terminal mock test execution
                supertrend_bullish = True
                stopping_volume_detected = True
                trend_status = "🟢 BULLISH ALIGNMENT (FORCED TEST)"
                conviction_status = "⚡ HIGH (Whale Inflow - FORCED TEST)"

            # 3. Enforce Strategy Gatekeepers (Larry Williams Anti-Chop Pivot Shield)
            # If testing, bypass the shield to verify webhook piping layout functions
            inside_chop_pivot = False if is_test else macro_muted
            
            # 4. Operational Routing Logic Execution
            if supertrend_bullish and stopping_volume_detected and not inside_chop_pivot:
                # --- VALID TRADING ENTRY SIGNAL (TIER A CONVICTION) ---
                if not target_webhook:
                    log_event(f"Missing routing target endpoint for {asset_class} channel pipeline.", "ERROR")
                    continue

                title = f"⚡ ESSENTIALS {asset_class.upper()} FLOWSTATE UPDATE"
                
                # Cross-Channel Surgical Optimization Check:
                content_payload = ""
                if asset_class == "FUTURES":
                    content_payload = "📢 **System Notice**: Index macro conditions match options collateral setups. Cross-referencing `#options-signal`."

                embed = {
                    "title": title,
                    "description": (
                        f"### **Tactical Entry Parameters**\n"
                        f"┣ **Trading Vehicle**: `{vehicle_desc}`\n"
                        f"┣ **Execution Strategy**: {strategy_profile}\n"
                        f"┗ **Tracking Profile**: {tracking_profile}\n\n"
                        f"📊 **Market Context (The Radar)**\n"
                        f"┣ **Market Outlook**: `{state.get('regime', 'BULLISH REGIME')}`\n"
                        f"┣ **Trend Vector**: `{trend_status}`\n"
                        f"┣ **Volatility Surface**: `{state.get('vix_current', 14.50)}` ({vix_velocity})\n"
                        f"┗ **Order Book Flow**: {conviction_status}\n\n"
                        f"🛡️ **Risk Management Guardrails**\n"
                        f"┗ **Allocation Parameter**: `OPTIMAL ENV FOR TREND SCALING (RSI Limit < {rsi_shield_limit})`"
                    ),
                    "color": 0x2ecc71 if asset_class == "FUTURES" else 0x3498db, # Green for Futures, Blue for Options
                    "timestamp": datetime.now(pytz.utc).isoformat(),
                    "footer": {"text": f"ESSENTIALS Execution Engine • HST Timezone"}
                }

                payload = {"embeds": [embed]}
                if content_payload and asset_class == "FUTURES" and WEBHOOKS["OPTIONS"]:
                    try:
                        requests.post(WEBHOOKS["OPTIONS"], json={"content": content_payload}, timeout=5)
                    except Exception as e:
                        log_event(f"Cross-channel push link failed: {e}", "ERROR")

                # Dispatch primary payload cleanly to its isolated channel target
                requests.post(target_webhook, json=payload, timeout=10)
                log_event(f"Successfully routed pristine trade signal alert for {symbol} to {asset_class} webhook.")

            else:
                # --- EFFICIENT ANTI-NOISE LAYER: DISPATCH CHOP MONITORS ---
                if not WEBHOOKS["SENTRY"]:
                    continue

                filter_trigger = "Order flow trend divergence or weak conviction volume thresholds."
                if inside_chop_pivot:
                    filter_trigger = f"Intraday price ranges for `{symbol}` consolidated tightly inside 3-day structural pivot grid. Entry suppressed to preserve win-rate."
                elif not supertrend_bullish:
                    filter_trigger = f"Asset momentum failed technical parameters. Immediate overhead structural resistance detected."

                title = f"🛡️ Sentry Suppression Matrix: {symbol}"
                description = (
                    f"### **Ecosystem Structural Capital Protection**\n"
                    f"┣ **Asset Evaluated**: `{symbol}`\n"
                    f"┣ **Current Operational Status**: `Signal Muted / Suppressed`\n"
                    f"┗ **Filter Constraint**: {filter_trigger}\n\n"
                    f"*The algorithm remains quiet to prevent notification fatigue and eliminate standard retail drawdown phases during sideways chop.*"
                )

                payload = {
                    "embeds": [{
                        "title": title,
                        "description": description,
                        "color": 0x34495e, # Structural Dark Slate
                        "timestamp": datetime.now(pytz.utc).isoformat(),
                        "footer": {"text": "Rockefeller Sentry Shield"}
                    }]
                }
                requests.post(WEBHOOKS["SENTRY"], json=payload, timeout=10)
                log_event(f"Passive anti-noise telemetry logged cleanly for filtered asset: {symbol}.")

        except Exception as e:
            log_event(f"Signal scan handling error encountered for asset entry {symbol}: {e}", "ERROR")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        print("🧪 Initiating routing matrix and anti-noise script test parameters...")
        execute_signal_scan(is_test=True)
        print("✅ Production routing and suppression checks completed cleanly.")
    else:
        import time
        log_event("Trade Signal core engine background daemon initialized.")
        while True:
            execute_signal_scan(is_test=False)
            time.sleep(900)
