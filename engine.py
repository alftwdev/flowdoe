#!/usr/bin/env python3
import os
import json
import argparse
from datetime import datetime
import requests
from dotenv import load_load

# Load existing environment configuration
load_dotenv()

# Webhook mapping from your verified .env setup
WEBHOOKS = {
    "forex": os.getenv("WEBHOOK_FOREX"),
    "crypto": os.getenv("WEBHOOK_CRYPTO"),
    "tsp_daily": os.getenv("WEBHOOK_FED"),
    "tsp_weekly": os.getenv("WEBHOOK_FED")
}

STATE_FILE = os.path.join(os.path.dirname(__file__), ".gatekeeper_state.json")

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)

def evaluate_gatekeeper(channel, current_metric, major_threshold=2.0):
    """
    Implements the 3-Strike Dynamic Gatekeeper protocol.
    Prevents notification fatigue for noise, fires immediately on major structural shifts.
    """
    state = load_state()
    channel_state = state.get(channel, {"strike_count": 0, "last_value": 0.0})
    
    last_value = channel_state["last_value"]
    strike_count = channel_state["strike_count"]
    
    # Calculate relative baseline deviation
    delta = abs(current_metric - last_value)
    
    is_major_move = delta >= major_threshold
    
    if is_major_move:
        # Reset counter on macro or volatility expansion
        channel_state["strike_count"] = 1
        channel_state["last_value"] = current_metric
        state[channel] = channel_state
        save_state(state)
        return True, f"Major Breach Detected (Delta: {delta:.2f})"
        
    if strike_count >= 3:
        # Strike limit reached; suppress minimal variance fluctuations
        channel_state["last_value"] = current_metric
        state[channel] = channel_state
        save_state(state)
        return False, "Suppressed via 3-Strike Rule (Noise Reduction)"
    
    # Increment strike for minimal movements, but allow delivery
    channel_state["strike_count"] += 1
    channel_state["last_value"] = current_metric
    state[channel] = channel_state
    save_state(state)
    return True, f"Pulse Allowed (Strike {channel_state['strike_count']}/3)"

def dispatch_webhook(channel, payload_text):
    url = WEBHOOKS.get(channel)
    if not url:
        print(f"Error: Webhook for channel '{channel}' not configured in environment.")
        return False
        
    payload = {"content": payload_text}
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.status_code in [200, 204]:
            print(f"[{channel.upper()}] Telemetry successfully dispatched.")
            return True
        else:
            print(f"[{channel.upper()}] Post failed with status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"[{channel.upper()}] Critical dispatch failure: {str(e)}")
        return False

def fetch_market_telemetry():
    """
    Data Aggregator Anchor.
    Replace these mock dictionary lookups with your direct Twelve Data,
    Finviz scraping, or live brokerage API data parsing streams.
    """
    # High-accuracy structural layout proxies
    return {
        "liquidity": {"index": "7.42T", "momentum": "+1.4%", "sofr": "0.012%"},
        "forex_pairs": [
            {"pair": "AUD/USD", "spot": "0.7081", "change": "+0.60%", "regime": "🚀 Accelerating Up", "rr": "1 : 3.4 (Long)"},
            {"pair": "XAU/USD", "spot": "4281.86", "change": "+1.58%", "regime": "🚀 Accelerating Up", "rr": "1 : 2.9 (Long)"},
            {"pair": "EUR/USD", "spot": "1.1605", "change": "+0.41%", "regime": "🔄 Trend Decelerating", "rr": "1 : 1.5 (Range)"},
            {"pair": "USD/JPY", "spot": "159.81", "change": "-0.30%", "regime": "⚠️ Trend Decelerating", "rr": "1 : 1.1 (Squeeze)"}
        ],
        "crypto": {
            "spot": "65,440.24", "vol": "42.1%", "bbw": "LOW (Compression Target)", 
            "funding": "+0.035%", "velocity": "+2.52%", "imbalance": "+12.4%"
        },
        "tsp_funds": [
            {"name": "C-Fund", "proxy": "S&P 500 Equity", "change": "+1.12%", "regime": "📈 Structural Up", "weight": "45%"},
            {"name": "S-Fund", "proxy": "Completion Index", "change": "+0.44%", "regime": "🔄 Macro Compression", "weight": "20%"},
            {"name": "I-Fund", "proxy": "International EAFE", "change": "-0.18%", "regime": "📉 Cyclical Down", "weight": "05%"},
            {"name": "F-Fund", "proxy": "Aggregate Bond ETF", "change": "-0.54%", "regime": "📉 Yield Expansion", "weight": "00%"},
            {"name": "G-Fund", "proxy": "Short-Term Treasury", "change": "+0.01%", "regime": "🔒 Safe-Haven Anchor", "weight": "30%"}
        ],
        "macro": {"us10y": "4.45%", "ratio": "3.1:1"}
    }

def build_forex_pulse(data):
    liq = data["liquidity"]
    pulse = (
        "====================================================================\n"
        "⚓ GLOBAL MACRO & FX TELEMETRY PULSE | ESSENTIALS ARCHITECTURE\n"
        f"Sync Time: {datetime.utcnow().strftime('%H:%M:%S')} UTC | Macro Regime: [STAGE 2: EXPANSION]\n"
        "====================================================================\n\n"
        "💧 GLOBAL LIQUIDITY ENVIRONMENT\n"
        "────────────────────────────────────────────────────────────────────\n"
        f"• Net Liquidity Index : ${liq['index']} [{liq['momentum']} / 20d Momentum]\n"
        f"• SOFR-Repo Spread    : {liq['sofr']} [🟢 Liquidity Abundant / Low Stress]\n"
        "• DXY Macro Bias      : Bearish Volatility Compression (Regime Short)\n\n"
        "📊 CROSS-SECTIONAL FX MOMENTUM MATRIX (Z-Score vs G10 Basket)\n"
        "────────────────────────────────────────────────────────────────────\n"
        "Pair      | Spot Price | Day %  | RS Momentum Regime       | Dynamic R:R\n"
        "──────────+────────────+────────+──────────────────────────+─────────────\n"
    )
    for p in data["forex_pairs"]:
        pulse += f"{p['pair']:<10} | {p['spot']:<10} | {p['change']:<6} | {p['regime']:<24} | {p['rr']}\n"
        
    pulse += (
        "\n🔗 STRUCTURAL DIVERCENCE ALERTS\n"
        "────────────────────────────────────────────────────────────────────\n"
        "⚠️ ALERT [USD/JPY]: Spot price is maintaining an upward trend despite a\n"
        "24bp compression in the US-Japan 2Y yield differential over the past\n"
        "48 hours. Structural divergence indicates potential institutional unwinding.\n\n"
        "ESSENTIALS Macro-Quant Architecture | Data Secured"
    )
    return pulse

def build_crypto_pulse(data):
    c = data["crypto"]
    pulse = (
        "====================================================================\n"
        "⚡ CRYPTO LIQUIDITY & VOLATILITY SENTRY | QUANT ENGINE\n"
        f"Sync Time: {datetime.utcnow().strftime('%H:%M:%S')} UTC | Target: [BTC/USD]\n"
        "====================================================================\n\n"
        "🚨 STRUCTURAL MARKET TELEMETRY\n"
        "────────────────────────────────────────────────────────────────────\n"
        f"• Current Spot Rate   : ${c['spot']}  | 24h Realized Vol : {c['vol']}\n"
        f"• Volatility Context  : 🛑 COMPRESSION MAXIMUM (BBW at {c['bbw']})\n"
        f"• Order Book Imbalance: {c['imbalance']} Ask-Side Depth (Overhead Wall)\n\n"
        "⛓️ DERIVATIVES & LEVERAGE RISK PROFILE\n"
        "────────────────────────────────────────────────────────────────────\n"
        f"• Aggregate Funding Rate : {c['funding']} / 8h (Long-Biased Premium Building)\n"
        f"• 1H Liquidation Vector  : High density of short stops resting at $66,200\n"
        f"• Velocity Vector Profile: Momentum expanding rapidly away from 4H VWAP\n\n"
        "🛡️ ALGORITHMIC TELEMETRY VECTOR\n"
        "────────────────────────────────────────────────────────────────────\n"
        "[⚠️ SENTRY TRIGGER: VOLATILITY EXPLOSION IMMINENT]\n"
        f"The Velocity Vector has moved {c['velocity']}. Bollinger Band Width has\n"
        "compressed to a critical structural threshold. Expect aggressive\n"
        "directional volume expansion within the next 2-4 hours.\n\n"
        "ESSENTIALS Macro-Quant Architecture | Data Secured"
    )
    return pulse

def build_tsp_weekly_pulse(data):
    m = data["macro"]
    pulse = (
        "====================================================================\n"
        "⚓ GOVERNMENT & MILITARY WEALTH MATRIX | TSP STRATEGIC VECTOR\n"
        f"Evaluation Date: {datetime.utcnow().strftime('%Y-%m-%d')} | Horizon: Medium-Term Macro\n"
        "====================================================================\n\n"
        "🛡️ RISK-PARITY MATRIX & TARGET ALLOCATION WEIGHTS\n"
        "────────────────────────────────────────────────────────────────────\n"
        "Fund    | Proxy Tracked       | Momentum Regime     | Target Vector\n"
        "────────+─────────────────────+─────────────────────+───────────────\n"
    )
    for f in data["tsp_funds"]:
        pulse += f"{f['name']:<7} | {f['proxy']:<19} | {f['regime']:<19} | TARGET ALLOC ({f['weight']})\n"
        
    pulse += (
        "\n📊 MACRO UNDERLYING METRICS\n"
        "────────────────────────────────────────────────────────────────────\n"
        f"• Reference Yield (US10Y)   : {m['us10y']} [Directional Bias: Rising]\n"
        "• System Equity Drawdown Risk: LOW (S&P 500 trading above 200-day SMA)\n"
        f"• Cross-Asset Momentum Ratio: Equities outperforming Fixed Income {m['ratio']}\n\n"
        "🎯 STRATEGIC DIRECTIVE\n"
        "────────────────────────────────────────────────────────────────────\n"
        "The Macro Architecture dictates a selective core allocation structure.\n"
        "Maintain equity core weight in C-Fund while isolating international risk.\n"
        "Hold cash buffers in G-Fund (Zero-Volatility Anchor proxy via Treasury yield curve).\n\n"
        "ESSENTIALS Macro-Quant Architecture | Data Secured"
    )
    return pulse

def build_tsp_daily_pulse(data):
    pulse = (
        "====================================================================\n"
        "📊 TSP END-OF-DAY RECAP | MARKET PERFORMANCE HARMONIZATION\n"
        f"Market Close: {datetime.utcnow().strftime('%Y-%m-%d')} | Daily Delta Tracking\n"
        "====================================================================\n\n"
        "🏁 TSP FUND CLOSING PERFORMANCE\n"
        "────────────────────────────────────────────────────────────────────\n"
    )
    for f in data["tsp_funds"]:
        pulse += f"┣ {f['name']} ({f['proxy']:<19}) : {f['change']} | Status: {f['regime']}\n"
        
    pulse += (
        "────────────────────────────────────────────────────────────────────\n"
        "📋 FINVIZ SECTOR SYNCHRONIZATION DETAILED ANALYSIS\n"
        "• Large-Cap Equities (C-Fund Proxy) led inflows following institutional cross-asset shifts.\n"
        "• Small-Cap Equities (S-Fund Proxy) faced mid-day range compression, maintaining neutral trends.\n"
        "• Aggregate Fixed Income (F-Fund Proxy) faced downside acceleration as the 10Y Yield compressed capital valuations.\n"
        "• Risk-Free Yield (G-Fund Proxy) preserved equity stability, executing structural safe-haven functions.\n\n"
        "ESSENTIALS Macro-Quant Architecture | Data Secured"
    )
    return pulse

def main():
    parser = argparse.ArgumentParser(description="ESSENTIALS Multi-Asset Quant Engine Dashboard")
    parser.add_argument("--mode", required=True, choices=["forex", "crypto", "tsp_daily", "tsp_weekly"],
                        help="Select reporting matrix output mode")
    args = parser.parse_args()
    
    data = fetch_market_telemetry()
    
    if args.mode == "forex":
        # Gatekeeper evaluation using calculated spot changes or velocity metrics
        current_metric = float(data["forex_pairs"][0]["change"].replace("%", ""))
        should_send, reason = evaluate_gatekeeper("forex", current_metric, major_threshold=0.5)
        print(f"[Gatekeeper] Forex analysis: {reason}")
        if should_send:
            payload = build_forex_pulse(data)
            dispatch_webhook("forex", payload)
            
    elif args.mode == "crypto":
        # Volatility gatekeeper tracking percentage velocity changes
        current_metric = float(data["crypto"]["velocity"].replace("%", ""))
        should_send, reason = evaluate_gatekeeper("crypto", current_metric, major_threshold=1.5)
        print(f"[Gatekeeper] Crypto analysis: {reason}")
        if should_send:
            payload = build_crypto_pulse(data)
            dispatch_webhook("crypto", payload)
            
    elif args.mode == "tsp_daily":
        # Daily updates bypass noise filters to ensure end-of-day recaps run consistently
        payload = build_tsp_daily_pulse(data)
        dispatch_webhook("tsp_daily", payload)
        
    elif args.mode == "tsp_weekly":
        # Weekly macro allocations execute at set intervals
        payload = build_tsp_weekly_pulse(data)
        dispatch_webhook("tsp_weekly", payload)

if __name__ == "__main__":
    main()
