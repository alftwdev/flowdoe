#!/usr/bin/env python3
import os
import sys
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from database import EcosystemDatabase

# Load existing environment configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Webhook mapping from your verified .env setup
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
    Implements the 3-Strike Dynamic Gatekeeper protocol natively using EcosystemDatabase.
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

def dispatch_webhook(channel, payload_text):
    url = WEBHOOKS.get(channel)
    if not url: return False
    try:
        response = requests.post(url, json={"content": payload_text}, headers={"Content-Type": "application/json"}, timeout=10)
        if response.status_code in [200, 204]: return True
    except Exception: pass
    return False

def fetch_market_telemetry():
    """Data Aggregator Anchor (Replace mock loops with direct streams)"""
    return {
        "liquidity": {"index": "7.42T", "momentum": "+1.4%", "sofr": "0.012%"},
        "forex_pairs": [
            {"pair": "AUD/USD", "spot": "0.7081", "change": "+0.60%", "regime": "Accelerating Up", "rr": "1 : 3.4 (Long)"},
            {"pair": "XAU/USD", "spot": "4281.86", "change": "+1.58%", "regime": "Accelerating Up", "rr": "1 : 2.9 (Long)"},
            {"pair": "EUR/USD", "spot": "1.1605", "change": "+0.41%", "regime": "Trend Decelerating", "rr": "1 : 1.5 (Range)"},
            {"pair": "USD/JPY", "spot": "159.81", "change": "-0.30%", "regime": "Trend Decelerating", "rr": "1 : 1.1 (Squeeze)"}
        ],
        "crypto": {
            "spot": "65,440.24", "vol": "42.1%", "bbw": "LOW (Compression Target)", 
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
            "SPY": {"spot": 542.12, "flip": 535.00, "state": "POSITIVE GAMMA (Volatility Suppressed)"},
            "QQQ": {"spot": 472.50, "flip": 475.00, "state": "NEGATIVE GAMMA (Volatility Amplified)"},
            "TQQQ": {"spot": 62.80, "flip": 60.00, "state": "POSITIVE GAMMA (Accelerated Momentum)"}
        }
    }

def build_forex_pulse(data, status_text):
    liq = data["liquidity"]
    pulse = (
        f"⚡ GLOBAL MACRO & FX TELEMETRY PULSE\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💧 Global Liquidity Environment:\n"
        f"┣ Net Liquidity Index:  ${liq['index']} [{liq['momentum']} / 20d Momentum]\n"
        f"┣ SOFR-Repo Spread:     {liq['sofr']} [Liquidity Abundant]\n"
        f"┗ DXY Macro Bias:       Bearish Volatility Compression\n\n"
        f"📊 Cross-Sectional FX Momentum Matrix:\n"
        f"Pair       | Spot Price | Day %  | RS Momentum Regime       | Dynamic R:R\n"
        f"───────────+────────────+────────+──────────────────────────+─────────────\n"
    )
    for p in data["forex_pairs"]:
        pulse += f"{p['pair']:<10} | {p['spot']:<10} | {p['change']:<6} | {p['regime']:<24} | {p['rr']}\n"
        
    pulse += (
        f"\n🔗 Structural Divergence Alerts:\n"
        f"┗ ALERT [USD/JPY]: Spot price maintaining upward trend despite\n"
        f"  compression in yield differential. Potential institutional unwinding.\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ ESSENTIALS Macro-Quant Architecture | {status_text}"
    )
    return pulse

def build_crypto_pulse(data, status_text):
    c = data["crypto"]
    pulse = (
        f"⚡ CRYPTO LIQUIDITY & VOLATILITY SENTRY | BTC/USD\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🚨 Structural Market Telemetry:\n"
        f"┣ Current Spot Rate:   ${c['spot']}\n"
        f"┣ 24h Realized Vol:    {c['vol']}\n"
        f"┣ Volatility Context:  COMPRESSION MAXIMUM (BBW at {c['bbw']})\n"
        f"┗ Order Book Imbal:    {c['imbalance']} Ask-Side Depth\n\n"
        f"⛓️ Derivatives & Leverage Risk Profile:\n"
        f"┣ Agg. Funding Rate:   {c['funding']} / 8h\n"
        f"┣ 1H Liq. Vector:      High density resting at $66,200\n"
        f"┗ Velocity Profile:    Momentum expanding away from VWAP\n\n"
        f"🛡️ Algorithmic Telemetry Vector:\n"
        f"┗ SENTRY TRIGGER: Velocity Vector moved {c['velocity']}. BBW compressed.\n"
        f"  Expect aggressive directional volume expansion within 2-4 hours.\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ ESSENTIALS Macro-Quant Architecture | {status_text}"
    )
    return pulse

def build_tsp_weekly_pulse(data, status_text):
    m = data["macro"]
    pulse = (
        f"⚡ GOVERNMENT & MILITARY WEALTH MATRIX | TSP STRATEGIC VECTOR\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🛡️ Risk-Parity Matrix & Target Weights:\n"
        f"Fund    | Proxy Tracked | Momentum Regime    | Target Vector\n"
        f"────────+───────────────+────────────────────+───────────────\n"
    )
    for f in data["tsp_funds"]:
        pulse += f"{f['name']:<7} | {f['proxy']:<13} | {f['regime']:<18} | ALLOC ({f['weight']})\n"
        
    pulse += (
        f"\n📊 Macro Underlying Metrics:\n"
        f"┣ Reference Yield (10Y): {m['us10y']} [Bias: Rising]\n"
        f"┣ System Equity Risk:    LOW (S&P 500 > 200 SMA)\n"
        f"┗ Cross-Asset Momentum:  Equities outpacing Fixed Income {m['ratio']}\n\n"
        f"🎯 Strategic Directive:\n"
        f"┗ Maintain equity core in C-Fund. Hold cash buffers in G-Fund\n"
        f"  (Zero-Volatility Anchor proxy via Treasury yield curve).\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ ESSENTIALS Macro-Quant Architecture | {status_text}"
    )
    return pulse

def build_tsp_daily_pulse(data):
    pulse = (
        f"⚡ TSP END-OF-DAY RECAP | MARKET HARMONIZATION\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏁 TSP Fund Closing Performance:\n"
    )
    for f in data["tsp_funds"]:
        pulse += f"┣ {f['name']} ({f['proxy']:<13}) : {f['change']:<6} | Status: {f['regime']}\n"
        
    pulse += (
        f"\n📋 Sector Synchronization Analysis:\n"
        f"┣ Large-Cap Equities (C-Fund) tracking institutional inflows.\n"
        f"┣ Small-Cap Equities (S-Fund) maintaining neutral compression.\n"
        f"┣ Fixed Income (F-Fund) observing downside acceleration via 10Y.\n"
        f"┗ Risk-Free Yield (G-Fund) executing safe-haven functions.\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ ESSENTIALS Macro-Quant Architecture | Data Secured"
    )
    return pulse

def build_gex_pulse(data, tickers, status_text):
    header_tag = "GLOBAL MACRO" if "SPY" in tickers else "TACTICAL OPTIONS"
    pulse = (
        f"⚡ SYSTEMIC GEX MATRIX PROFILE | {header_tag}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Automated Market Maker Positioning:\n"
        f"Ticker     | Spot Price | Gamma Flip | Structural Posture Context\n"
        f"───────────+────────────+────────────+──────────────────────────────\n"
    )
    for t in tickers:
        g = data["gex"][t]
        pulse += f"{t:<10} | ${g['spot']:<9.2f} | ${g['flip']:<10.2f} | {g['state']}\n"
        
    pulse += (
        f"\n💡 Strategic Posture Directive:\n"
        f"┗ Fading or breaking Gamma Flip lines shifts institutional hedging.\n"
        f"  Negative Gamma = Volatility Expansion. Positive = Suppression.\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ ESSENTIALS Macro-Quant Architecture | {status_text}"
    )
    return pulse

def main():
    import argparse
    parser = argparse.ArgumentParser(description="ESSENTIALS Multi-Asset Quant Engine")
    parser.add_argument("--mode", required=True, choices=["forex", "crypto", "tsp_daily", "tsp_weekly", "gex"])
    args = parser.parse_args()
    
    data = fetch_market_telemetry()
    
    if args.mode == "forex":
        current_metric = float(data["forex_pairs"][0]["change"].replace("%", ""))
        should_send, status = evaluate_gatekeeper("forex", current_metric, major_threshold=0.5)
        if should_send:
            dispatch_webhook("forex", build_forex_pulse(data, status))
            
    elif args.mode == "crypto":
        current_metric = float(data["crypto"]["velocity"].replace("%", ""))
        should_send, status = evaluate_gatekeeper("crypto", current_metric, major_threshold=1.5)
        if should_send:
            dispatch_webhook("crypto", build_crypto_pulse(data, status))
            
    elif args.mode == "tsp_daily":
        dispatch_webhook("tsp_daily", build_tsp_daily_pulse(data))
        
    elif args.mode == "tsp_weekly":
        current_metric = float(data["tsp_funds"][0]["change"].replace("%", ""))
        should_send, status = evaluate_gatekeeper("tsp_weekly", current_metric, major_threshold=1.5)
        if should_send:
            dispatch_webhook("tsp_weekly", build_tsp_weekly_pulse(data, status))

    elif args.mode == "gex":
        current_metric = abs(data["gex"]["SPY"]["spot"] - data["gex"]["SPY"]["flip"])
        should_send, status = evaluate_gatekeeper("gex", current_metric, major_threshold=2.0)
        if should_send:
            dispatch_webhook("gex_macro", build_gex_pulse(data, ["SPY", "QQQ"], status))
            dispatch_webhook("gex_options", build_gex_pulse(data, ["TQQQ"], status))

if __name__ == "__main__":
    main()
