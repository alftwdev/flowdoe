#!/usr/bin/env python3
"""
ESSENTIALS Macro-Quant Architecture — Central Pulse Engine
Standardized Master Template System for Cross-Sector Performance Reporting.
"""

import os
import sys
import json
import time
import requests
import argparse
from datetime import datetime
from dotenv import load_dotenv
from database import EcosystemDatabase
from analytics import HighFidelityAnalyticsEngine

# Load environment configurations
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Centralized Webhook Mapping
WEBHOOKS = {
    "gex_macro": os.getenv("WEBHOOK_MARKET_ANALYSIS"),
    "gex_options": os.getenv("WEBHOOK_TRADE_SIGNALS"),
    "futures": os.getenv("WEBHOOK_FUTURES_TRADING"),
    "crypto": os.getenv("WEBHOOK_CRYPTO"),
    "tsp_daily": os.getenv("WEBHOOK_FED"),
    "tsp_weekly": os.getenv("WEBHOOK_FED"),
    "forex": os.getenv("WEBHOOK_FOREX"),
    "announcements": os.getenv("WEBHOOK_ANNOUNCEMENTS"),
    "income": os.getenv("WEBHOOK_DIVIDEND_CCETFS")
}

# Initialize Database Connection
db = EcosystemDatabase()

def evaluate_gatekeeper(channel, current_metric, major_threshold=2.0):
    """
    Implements the 3-Strike Dynamic Gatekeeper protocol.
    Prevents notification fatigue by silencing minor fluctuations while highlighting major moves.
    """
    state_key = f"gatekeeper_{channel}_pulse"
    channel_state = db.get_state(state_key, {"strike_count": 0, "last_value": 0.0})
    
    last_value = channel_state.get("last_value", 0.0)
    strike_count = channel_state.get("strike_count", 0)
    
    delta = abs(current_metric - last_value)
    is_major_move = delta >= major_threshold
    
    if is_major_move or strike_count == 0:
        strike_count = 1
        status = "🟢 NEW STRUCTURAL REGIME DETECTED"
        should_send = True
    elif strike_count < 3:
        strike_count += 1
        status = f"🟡 REGIME PERSISTENCE REMINDER [{strike_count}/3]"
        should_send = True
    else:
        status = "🔒 FATIGUE SILENCE ACTIVE — BOUNDARY NOT BREACHED"
        should_send = False
        
    db.update_state(state_key, {"strike_count": strike_count, "last_value": current_metric})
    return should_send, status

def dispatch_webhook(channel_key, payload_text, color_hex=0x2ecc71):
    """
    Dispatches clean, embedded payload formats to the target premium audience.
    """
    url = WEBHOOKS.get(channel_key)
    if not url:
        print(f"[-] Webhook mapping missing for channel: {channel_key}")
        return False
        
    embed = {
        "description": payload_text,
        "color": color_hex,
        "footer": {
            "text": f"⚡ ESSENTIALS Macro-Quant Architecture | Data Secured • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        }
    }
    
    try:
        response = requests.post(url, json={"embeds": [embed]}, timeout=10)
        if response.status_code in [200, 204]:
            print(f"[+] Successfully dispatched report to {channel_key}")
            return True
        else:
            print(f"[-] Discord API Error ({response.status_code}): {response.text}")
            return False
    except Exception as e:
        print(f"[-] Network connection error while dispatching webhook: {e}")
        return False

# =====================================================================
# MASTER UNIFORM TEMPLATE ENGINE BUILDERS
# =====================================================================

def build_gex_pulse(data, status_tag):
    gex_data = data.get("gex", {}).get("SPY", {"spot": 0.0, "flip": 0.0, "net_oi": 0})
    return (
        f"⚡ **ESSENTIALS QUANT CELL | OPTIONS & GAMMA EXPOSURE**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"┣ Status: {status_tag}\n"
        f"┣ Spot Price: ${gex_data['spot']:,.2f}\n"
        f"┣ Gamma Flip Level: ${gex_data['flip']:,.2f}\n"
        f"┣ Net Open Interest Position: {gex_data['net_oi']:,} contracts\n"
        f"┗ Final Actionable Posture: {'BULLISH IMMUNITY REGIME' if gex_data['spot'] > gex_data['flip'] else 'VOLATILITY ACCELERATION DANGER ZONE'}\n"
    )

def build_forex_pulse(data, status_tag):
    fx_data = data.get("forex", [{"pair": "EUR/USD", "spot": 0.0, "change": "0.0%"}])[0]
    return (
        f"⚡ **ESSENTIALS MACRO TERMINAL | FOREIGN EXCHANGE MATRIX**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"┣ Status: {status_tag}\n"
        f"┣ Asset / Current Spot: {fx_data['pair']} @ {fx_data['spot']}\n"
        f"┣ Velocity Change: {fx_data['change']}\n"
        f"┣ Institutional Order Flow Focus: Major Key Support / Resistance Pivot\n"
        f"┗ Final Actionable Posture: MACRO HEDGE REBALANCING INITIATED\n"
    )

def build_crypto_pulse(data, status_tag):
    crypto_data = data.get("crypto", {"symbol": "BTC/USD", "spot": 0.0, "velocity": "0.0%"})
    return (
        f"⚡ **ESSENTIALS DIGITAL ASSET DESK | CRYPTO VELOCITY SYSTEM**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"┣ Status: {status_tag}\n"
        f"┣ Core Asset Spot: {crypto_data['symbol']} @ ${crypto_data['spot']:,.2f}\n"
        f"┣ Volume Velocity Multiplier: {crypto_data['velocity']}\n"
        f"┣ Market Structure State: High-Beta Capital Expansion Framework\n"
        f"┗ Final Actionable Posture: EXPLOIT DISLOCATION ON INTRADAY DRAWDOWNS\n"
    )

def build_futures_pulse(data, status_tag):
    fut_data = data.get("futures", {"symbol": "ES_F", "spot": 0.0, "posture": "Inside Value"})
    return (
        f"⚡ **ESSENTIALS MICROSTRUCTURE PROFILE | GLOBAL FUTURES**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"┣ Status: {status_tag}\n"
        f"┣ Contract Value / Spot: {fut_data['symbol']} @ {fut_data['spot']}\n"
        f"┣ Algorithmic Profile Range: VAH/VAL Distribution Fields\n"
        f"┣ Market Order Imbalance: Structural Liquidity Block Trapped\n"
        f"┗ Final Actionable Posture: FADE VALUE BOUNDARY EXTENSIONS\n"
    )

def build_tsp_pulse(data, interval_label):
    tsp_data = data.get("tsp", {"fund": "C Fund", "change": "0.0%"})
    return (
        f"⚡ **ESSENTIALS SOVEREIGN TERMINAL | TSP ALLOCATION METRIX**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"┣ Report Interval: {interval_label.upper()} STRUCTURAL SNAPSHOT\n"
        f"┣ Leading Fund Baseline: {tsp_data['fund']} Performance Change: {tsp_data['change']}\n"
        f"┣ Trailing Benchmark Dislocation: Adjusted Risk-Premium Divergence Metric\n"
        f"┣ Macro Systemic Signal: Institutional Liquidity Backstop Retained\n"
        f"┗ Final Actionable Posture: MAIN HOLDING ALLOCATION UNCHANGED\n"
    )

# =====================================================================
# LIVE DATA INGESTION LAYER
# =====================================================================

_analytics_engine = None

def get_analytics():
    global _analytics_engine
    if _analytics_engine is None:
        _analytics_engine = HighFidelityAnalyticsEngine()
    return _analytics_engine

def fetch_live_payload():
    """Fetches live market data via HighFidelityAnalyticsEngine each daemon iteration."""
    engine = get_analytics()
    payload = {
        "gex": {"SPY": {"spot": 0.0, "flip": 0.0, "net_oi": 0}},
        "forex": [{"pair": "EUR/USD", "spot": 0.0, "change": "+0.00%"}],
        "crypto": {"symbol": "BTC/USD", "spot": 0.0, "velocity": "+0.00%"},
        "futures": {"symbol": "ES", "spot": 0.0, "posture": "Inside Value"},
        "tsp": {"fund": "C Fund", "change": "+0.00%"}
    }
    try:
        gex = engine.calculate_gex_profile("SPY")
        if gex.get("current_spot", 0.0) > 0:
            payload["gex"]["SPY"].update({
                "spot": gex["current_spot"],
                "flip": gex["flip_strike"]
            })
    except Exception as e:
        print(f"[-] Live GEX fetch failed: {e}")
    try:
        quotes = engine._fetch_twelve_data_quotes(["EUR/USD", "BTC/USD", "SPY"])
        eur = quotes.get("EUR/USD", {})
        if "close" in eur:
            pct = float(eur.get("percent_change", 0.0))
            payload["forex"] = [{"pair": "EUR/USD", "spot": float(eur["close"]), "change": f"{pct:+.2f}%"}]
        btc = quotes.get("BTC/USD", {})
        if "close" in btc:
            pct = float(btc.get("percent_change", 0.0))
            payload["crypto"] = {"symbol": "BTC/USD", "spot": float(btc["close"]), "velocity": f"{pct:+.2f}%"}
        spy = quotes.get("SPY", {})
        if "close" in spy:
            spot = float(spy["close"])
            vah = float(db.get_state("SPY_vah", spot * 1.005))
            val_level = float(db.get_state("SPY_val", spot * 0.995))
            posture = ("Outside Value Up" if spot > vah else
                       ("Outside Value Down" if spot < val_level else "Inside Value Regime"))
            chg = float(spy.get("percent_change", 0.0))
            payload["futures"] = {"symbol": "ES", "spot": spot, "posture": posture}
            payload["tsp"] = {"fund": "C Fund", "change": f"{chg:+.2f}%"}
    except Exception as e:
        print(f"[-] Live quote batch fetch failed: {e}")
    return payload

# =====================================================================
# ENGINE EXECUTION ROUTER
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="ESSENTIALS Pulse Production Engine Dashboard.")
    
    # FIX: required=False allows the script to survive in the Always-On tab.
    # Added "daemon" to choices and set it as the default.
    parser.add_argument("--mode", type=str, required=False, default="daemon",
                        choices=["gex", "forex", "crypto", "futures", "tsp_daily", "tsp_weekly", "daemon"])
    args = parser.parse_args()

    live_payload = fetch_live_payload()

    if args.mode == "daemon":
        print(f"[+] Launching Ecosystem Pulse Daemon: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        while True:
            try:
                live_payload = fetch_live_payload()
                # GEX Sweep
                g_metric = abs(live_payload["gex"]["SPY"]["spot"] - live_payload["gex"]["SPY"]["flip"])
                should_send, status = evaluate_gatekeeper("gex", g_metric, major_threshold=2.0)
                if should_send:
                    color = 0x2ecc71 if "NEW" in status else 0xf1c40f
                    dispatch_webhook("gex_macro", build_gex_pulse(live_payload, status), color_hex=color)

                # Forex Sweep
                f_metric = float(live_payload["forex"][0]["change"].replace("%", ""))
                should_send, status = evaluate_gatekeeper("forex", f_metric, major_threshold=0.5)
                if should_send:
                    color = 0x3498db if "NEW" in status else 0xf1c40f
                    dispatch_webhook("forex", build_forex_pulse(live_payload, status), color_hex=color)

                # Crypto Sweep
                c_metric = float(live_payload["crypto"]["velocity"].replace("%", ""))
                should_send, status = evaluate_gatekeeper("crypto", c_metric, major_threshold=1.5)
                if should_send:
                    color = 0x9b59b6 if "NEW" in status else 0xf1c40f
                    dispatch_webhook("crypto", build_crypto_pulse(live_payload, status), color_hex=color)

                # Futures Sweep
                spy_spot = live_payload["futures"]["spot"]
                spy_poc = float(db.get_state("SPY_poc", spy_spot))
                f_spot_delta = abs(spy_spot - spy_poc)
                should_send, status = evaluate_gatekeeper("futures", f_spot_delta, major_threshold=5.0)
                if should_send:
                    dispatch_webhook("futures", build_futures_pulse(live_payload, status), color_hex=0xe67e22)

            except Exception as e:
                print(f"[-] Daemon execution error: {e}")
            
            # Sleep 15 minutes to prevent loop exhaustion
            time.sleep(900)

    elif args.mode == "gex":
        metric = abs(live_payload["gex"]["SPY"]["spot"] - live_payload["gex"]["SPY"]["flip"])
        should_send, status = evaluate_gatekeeper("gex", metric, major_threshold=2.0)
        if should_send:
            color = 0x2ecc71 if "NEW" in status else 0xf1c40f
            dispatch_webhook("gex_macro", build_gex_pulse(live_payload, status), color_hex=color)

    elif args.mode == "forex":
        metric = float(live_payload["forex"][0]["change"].replace("%", ""))
        should_send, status = evaluate_gatekeeper("forex", metric, major_threshold=0.5)
        if should_send:
            color = 0x3498db if "NEW" in status else 0xf1c40f
            dispatch_webhook("forex", build_forex_pulse(live_payload, status), color_hex=color)

    elif args.mode == "crypto":
        metric = float(live_payload["crypto"]["velocity"].replace("%", ""))
        should_send, status = evaluate_gatekeeper("crypto", metric, major_threshold=1.5)
        if should_send:
            color = 0x9b59b6 if "NEW" in status else 0xf1c40f
            dispatch_webhook("crypto", build_crypto_pulse(live_payload, status), color_hex=color)

    elif args.mode == "futures":
        spy_spot = live_payload["futures"]["spot"]
        spy_poc = float(db.get_state("SPY_poc", spy_spot))
        f_spot_delta = abs(spy_spot - spy_poc)
        should_send, status = evaluate_gatekeeper("futures", f_spot_delta, major_threshold=5.0)
        if should_send:
            dispatch_webhook("futures", build_futures_pulse(live_payload, status), color_hex=0xe67e22)

    elif args.mode == "tsp_daily":
        dispatch_webhook("tsp_daily", build_tsp_pulse(live_payload, "Daily Baseline"), color_hex=0x1abc9c)

    elif args.mode == "tsp_weekly":
        metric = float(live_payload["tsp"]["change"].replace("%", ""))
        should_send, status = evaluate_gatekeeper("tsp_weekly", metric, major_threshold=1.0)
        if should_send:
            dispatch_webhook("tsp_weekly", build_tsp_pulse(live_payload, f"Weekly {status}"), color_hex=0x2c3e50)

if __name__ == "__main__":
    main()
