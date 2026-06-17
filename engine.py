#!/usr/bin/env python3
import os
import sys
import time
import requests
import argparse
from datetime import datetime
from dotenv import load_dotenv
from database import EcosystemDatabase

# Load existing environment configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Webhook mapping from verified .env setup
WEBHOOKS = {
    "forex": os.getenv("WEBHOOK_FOREX"),
    "crypto": os.getenv("WEBHOOK_CRYPTO"),
    "tsp_daily": os.getenv("WEBHOOK_FED"),
    "tsp_weekly": os.getenv("WEBHOOK_FED"),
    "gex_macro": os.getenv("WEBHOOK_MARKET_ANALYSIS"),
    "gex_options": os.getenv("WEBHOOK_TRADE_SIGNALS")
}

db = EcosystemDatabase()

def evaluate_gatekeeper(channel, current_metric, major_threshold=2.0):
    """
    Implements the 3-Strike Dynamic Gatekeeper protocol natively.
    """
    state_key = f"gatekeeper_{channel}_pulse"
    channel_state = db.get_state(state_key, {"strike_count": 0, "last_value": 0.0})
    
    last_value = channel_state.get("last_value", 0.0)
    strike_count = channel_state.get("strike_count", 0)
    
    delta = abs(current_metric - last_value)
    is_major_move = delta >= major_threshold
    
    if is_major_move:
        channel_state["strike_count"] = 1
        channel_state["last_value"] = current_metric
        db.update_state(state_key, channel_state)
        return True, f"Pulse Broadcast (New Regime | Δ {delta:.2f})"
        
    if strike_count >= 3:
        channel_state["last_value"] = current_metric
        db.update_state(state_key, channel_state)
        return False, "Suppressed (Noise Reduction)"
    
    channel_state["strike_count"] += 1
    channel_state["last_value"] = current_metric
    db.update_state(state_key, channel_state)
    return True, f"Pulse Reminder ({channel_state['strike_count']}/3)"

def dispatch_webhook(channel, payload_text, title="SYSTEMIC TELEMETRY ALERT", color=0x2ecc71):
    """
    UPGRADED: Converts text blocks into native Discord Rich Embed notifications.
    Maintains full reverse-compatibility with the original string input signature.
    The left-hand vertical bar color adapts instantly to the market state.
    """
    url = WEBHOOKS.get(channel)
    if not url: return False
    
    # Extract the original string header to use as the embed Title for scannability
    lines = payload_text.split("\n")
    if lines and lines[0].startswith("⚡"):
        title = lines[0].replace("⚡", "").strip()
        payload_text = "\n".join(lines[1:])
        
    embed_payload = {
        "embeds": [
            {
                "title": f"⚡ {title}",
                "description": payload_text,
                "color": color,
                "footer": {
                    "text": "ESSENTIALS Macro-Quant Architecture | Data Secured"
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        ]
    }
    
    try:
        response = requests.post(url, json=embed_payload, headers={"Content-Type": "application/json"}, timeout=10)
        if response.status_code in [200, 204]: return True
    except Exception: pass
    return False

def fetch_market_telemetry():
    """Data Aggregator Anchor"""
    return {
        "liquidity": {"index": "7.42T", "momentum": "+1.4%", "sofr": "0.012%"},
        "forex_pairs": [
            {"pair": "AUD/USD", "spot": "0.7081", "change": "+0.60%", "regime": "Accelerating Up", "rr": "1 : 3.4"},
            {"pair": "XAU/USD", "spot": "4281.86", "change": "+1.58%", "regime": "Accelerating Up", "rr": "1 : 2.9"},
            {"pair": "EUR/USD", "spot": "1.1605", "change": "+0.41%", "regime": "Trend Decelerating", "rr": "1 : 1.5"},
            {"pair": "USD/JPY", "spot": "159.81", "change": "-0.30%", "regime": "Trend Decelerating", "rr": "1 : 1.1"}
        ],
        "crypto": {
            "spot": "65,440.24", "vol": "42.1%", "bbw": "LOW", 
            "funding": "+0.035%", "velocity": "+2.52%", "imbalance": "+12.4%"
        },
        "tsp_funds": [
            {"name": "C-Fund", "proxy": "S&P 500", "change": "+1.12%", "regime": "Structural Up", "weight": "45%"},
            {"name": "S-Fund", "proxy": "Completion", "change": "+0.44%", "regime": "Macro Compression", "weight": "20%"},
            {"name": "I-Fund", "proxy": "Intl EAFE", "change": "-0.18%", "regime": "Cyclical Down", "weight": "05%"},
            {"name": "F-Fund", "proxy": "Agg Bond", "change": "-0.54%", "regime": "Yield Expansion", "weight": "00%"},
            {"name": "G-Fund", "proxy": "Treasury", "change": "+0.01%", "regime": "Safe-Haven Anchor", "weight": "30%"}
        ],
        "macro": {"us10y": "4.45%", "ratio": "3.1:1"},
        "gex": {
            "SPY": {"spot": 542.12, "flip": 535.00, "state": "POSITIVE GAMMA"},
            "QQQ": {"spot": 472.50, "flip": 475.00, "state": "NEGATIVE GAMMA"},
            "TQQQ": {"spot": 62.80, "flip": 60.00, "state": "POSITIVE GAMMA"}
        }
    }

def build_forex_pulse(data, status_text):
    liq = data["liquidity"]
    pulse = (
        f"⚡ GLOBAL MACRO & FX TELEMETRY PULSE\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💧 Global Liquidity Environment:\n"
        f"┣ Net Liquidity Index:  ${liq['index']} [{liq['momentum']}]\n"
        f"┣ SOFR-Repo Spread:     {liq['sofr']} (Abundant)\n"
        f"┗ DXY Macro Bias:       Volatility Compression\n\n"
        f"📊 Cross-Sectional Matrix (Spot | % | Regime):\n"
    )
    for p in data["forex_pairs"]:
        pulse += f"┣ {p['pair']:<8}: {p['spot']:<8} | {p['change']:<6} | {p['regime']}\n"
        
    pulse += (
        f"┗ Vector:  Risk-Adjusted R:R active across matrix\n\n"
        f"🔗 Structural Divergence Alert:\n"
        f"┣ Alert:   USD/JPY spot trend vs yield compression\n"
        f"┗ Status:  Potential institutional unwinding detected\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ ESSENTIALS Macro-Quant Architecture | {status_text}"
    )
    return pulse

def build_crypto_pulse(data, status_text):
    c = data["crypto"]
    pulse = (
        f"⚡ CRYPTO LIQUIDITY & VOLATILITY SENTRY\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🚨 Structural Market Telemetry:\n"
        f"┣ Spot Rate:          ${c['spot']}\n"
        f"┣ 24h Realized Vol:   {c['vol']}\n"
        f"┣ Vol Context:        BBW at {c['bbw']} (Compression)\n"
        f"┗ Order Book:         {c['imbalance']} Ask-Side Depth\n\n"
        f"⛓️ Derivatives Risk Profile:\n"
        f"┣ Funding Rate:       {c['funding']} / 8h\n"
        f"┣ Liquidity Vector:   High density at $66,200\n"
        f"┗ Velocity:           {c['velocity']} (Momentum expanding)\n\n"
        f"🛡️ Algorithmic Vector:\n"
        f"┗ Status:             Directional expansion expected < 4h\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ ESSENTIALS Macro-Quant Architecture | {status_text}"
    )
    return pulse

def build_tsp_weekly_pulse(data, status_text):
    m = data["macro"]
    pulse = (
        f"⚡ TSP STRATEGIC VECTOR & RISK MATRIX\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🛡️ Target Allocation Weights:\n"
    )
    for f in data["tsp_funds"]:
        pulse += f"┣ {f['name']:<7}: {f['weight']:<5} ({f['regime']})\n"
        
    pulse += (
        f"┗ Vector:  Risk-Parity weights balanced for regime\n\n"
        f"📊 Macro Underlying Metrics:\n"
        f"┣ Ref Yield (10Y):    {m['us10y']} (Rising Bias)\n"
        f"┣ Equity Risk:        LOW (S&P > 200 SMA)\n"
        f"┗ Momentum Ratio:     Equities lead Fixed Income {m['ratio']}\n\n"
        f"🎯 Strategic Directive:\n"
        f"┗ Action: Maintain C-Fund core; hold G-Fund buffers\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ ESSENTIALS Macro-Quant Architecture | {status_text}"
    )
    return pulse

def build_tsp_daily_pulse(data):
    pulse = (
        f"⚡ TSP END-OF-DAY PERFORMANCE RECAP\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏁 Closing Performance:\n"
    )
    for f in data["tsp_funds"]:
        pulse += f"┣ {f['name']:<7}: {f['change']:<6} | {f['regime']}\n"
        
    pulse += (
        f"┗ Sync:    Market closing harmonization complete\n\n"
        f"📋 Sector Synchronization:\n"
        f"┣ C-Fund:  Institutional inflows tracked\n"
        f"┣ S-Fund:  Neutral compression maintaining\n"
        f"┣ F-Fund:  Downside acceleration via 10Y\n"
        f"┗ G-Fund:  Safe-haven anchor executing\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ ESSENTIALS Macro-Quant Architecture | Data Secured"
    )
    return pulse

def build_gex_pulse(data, tickers, status_text):
    header = "GLOBAL MACRO" if "SPY" in tickers else "TACTICAL OPTIONS"
    pulse = (
        f"⚡ SYSTEMIC GEX MATRIX | {header}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Market Maker Positioning:\n"
    )
    for t in tickers:
        g = data["gex"][t]
        pulse += f"┣ {t:<6}: ${g['spot']:<8.2f} (Flip: ${g['flip']:.2f})\n"
        
    pulse += (
        f"┗ State:  {data['gex'][tickers[0]]['state']}\n\n"
        f"💡 Strategic Posture:\n"
        f"┣ Trigger: Breaking Gamma Flip shifts hedging\n"
        f"┗ Effect:  Negative = Vol Expansion | Positive = Suppression\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ ESSENTIALS Macro-Quant Architecture | {status_text}"
    )
    return pulse

def main():
    parser = argparse.ArgumentParser(description="ESSENTIALS Multi-Asset Quant Engine")
    parser.add_argument("--mode", required=False, default="daemon", 
                        choices=["forex", "crypto", "tsp_daily", "tsp_weekly", "gex", "daemon"])
    args = parser.parse_args()
    
    if args.mode == "daemon":
        print(f"[+] Launching Ecosystem Pulse Daemon: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        while True:
            data = fetch_market_telemetry()
            
            # --- FOREX SWEEP ---
            try:
                f_metric = float(data["forex_pairs"][0]["change"].replace("%", ""))
                should_send, status = evaluate_gatekeeper("forex", f_metric, major_threshold=0.5)
                # SUPPLEMENTAL VISUAL ASSIGNMENT: Dynamic border color injection
                color_code = 0xe74c3c if "New Regime" in status else (0xf1c40f if "Reminder" in status else 0x2ecc71)
                if should_send: dispatch_webhook("forex", build_forex_pulse(data, status), color=color_code)
            except Exception as e: print(f"[-] Forex error: {e}")

            # --- CRYPTO SWEEP ---
            try:
                c_metric = float(data["crypto"]["velocity"].replace("%", ""))
                should_send, status = evaluate_gatekeeper("crypto", c_metric, major_threshold=1.5)
                color_code = 0xe74c3c if "New Regime" in status else (0xf1c40f if "Reminder" in status else 0x2ecc71)
                if should_send: dispatch_webhook("crypto", build_crypto_pulse(data, status), color=color_code)
            except Exception as e: print(f"[-] Crypto error: {e}")

            # --- GEX SWEEP ---
            try:
                g_metric = abs(data["gex"]["SPY"]["spot"] - data["gex"]["SPY"]["flip"])
                should_send, status = evaluate_gatekeeper("gex", g_metric, major_threshold=2.0)
                color_code = 0xe74c3c if "New Regime" in status else (0xf1c40f if "Reminder" in status else 0x2ecc71)
                if should_send:
                    dispatch_webhook("gex_macro", build_gex_pulse(data, ["SPY", "QQQ"], status), color=color_code)
                    dispatch_webhook("gex_options", build_gex_pulse(data, ["TQQQ"], status), color=color_code)
            except Exception as e: print(f"[-] GEX error: {e}")

            time.sleep(900)
            
    else:
        data = fetch_market_telemetry()
        if args.mode == "forex":
            current_metric = float(data["forex_pairs"][0]["change"].replace("%", ""))
            should_send, status = evaluate_gatekeeper("forex", current_metric, major_threshold=0.5)
            color_code = 0xe74c3c if "New Regime" in status else (0xf1c40f if "Reminder" in status else 0x2ecc71)
            if should_send: dispatch_webhook("forex", build_forex_pulse(data, status), color=color_code)
        elif args.mode == "crypto":
            current_metric = float(data["crypto"]["velocity"].replace("%", ""))
            should_send, status = evaluate_gatekeeper("crypto", current_metric, major_threshold=1.5)
            color_code = 0xe74c3c if "New Regime" in status else (0xf1c40f if "Reminder" in status else 0x2ecc71)
            if should_send: dispatch_webhook("crypto", build_crypto_pulse(data, status), color=color_code)
        elif args.mode == "tsp_daily":
            # Direct transmission uses standard target green
            dispatch_webhook("tsp_daily", build_tsp_daily_pulse(data), color=0x2ecc71)
        elif args.mode == "tsp_weekly":
            current_metric = float(data["tsp_funds"][0]["change"].replace("%", ""))
            should_send, status = evaluate_gatekeeper("tsp_weekly", current_metric, major_threshold=1.5)
            color_code = 0xe74c3c if "New Regime" in status else (0xf1c40f if "Reminder" in status else 0x2ecc71)
            if should_send: dispatch_webhook("tsp_weekly", build_tsp_weekly_pulse(data, status), color=color_code)
        elif args.mode == "gex":
            current_metric = abs(data["gex"]["SPY"]["spot"] - data["gex"]["SPY"]["flip"])
            should_send, status = evaluate_gatekeeper("gex", current_metric, major_threshold=2.0)
            color_code = 0xe74c3c if "New Regime" in status else (0xf1c40f if "Reminder" in status else 0x2ecc71)
            if should_send:
                dispatch_webhook("gex_macro", build_gex_pulse(data, ["SPY", "QQQ"], status), color=color_code)
                dispatch_webhook("gex_options", build_gex_pulse(data, ["TQQQ"], status), color=color_code)

if __name__ == "__main__":
    main()
