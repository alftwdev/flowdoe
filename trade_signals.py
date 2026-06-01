import os
import sys
import logging
import time
import math
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase

logger = logging.getLogger("Trade_Signals")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

try:
    from essentials_tools import send_essentials_embed
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False
    def send_essentials_embed(*args, **kwargs): pass

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_FOREX = os.getenv("WEBHOOK_FOREX")
WEBHOOK_OPTIONS_SIGNALS = os.getenv("WEBHOOK_OPTIONS_SIGNALS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")

# --- Institutional Forex Guidelines Dictionary ---
FOREX_TACTICAL_DATA = {
    "XAU/USD": {
        "setup": "Asian Session Liquidity Sweep (Turtle Soup) or Supply/Demand w/ Higher Timeframe Confluence.",
        "orders": "Stop: $3 to $5 | Target: $10 to $15 (1:2 to 1:3 RR).",
        "early_signal": "9-SMA crosses 20-EMA on 4H + Selling Exhaustion (Positive Delta on Dips). Inverse correlation to DXY."
    },
    "EUR/USD": {
        "setup": "Break & Retest or Value Gaps at Heavy Volume Nodes (London/NY overlap).",
        "orders": "Stop: 15-25 pips | Target: 40-60 pips targeting next structural liquidity.",
        "early_signal": "RSI (14) dips to 30-40 holding higher lows. Correlated to DXY weakness."
    },
    "GBP/USD": {
        "setup": "London Session Breakouts or S/R Rejection with 61.8% / 71% Fib levels.",
        "orders": "Stop: 30-40 pips | Target: 80-120 pips.",
        "early_signal": "9 EMA crossing above 21 EMA (London Session 04:00-07:00 GMT) with RSI 50-70."
    },
    "USD/JPY": {
        "setup": "Yield-Driven Pullbacks via EMA crossovers (20/50) or Heavy Volume Zone Retests on 30m.",
        "orders": "Stop: 25-35 pips | Target: 70-120 pips (Trailing for 1:3 RR).",
        "early_signal": "Reversals off key support lines holding structure. Driven by US/Japan yield curve differentials."
    }
}

def fetch_td_indicator(symbol, indicator, interval, **params):
    url = f"https://api.twelvedata.com/{indicator}?symbol={symbol}&interval={interval}&apikey={TD_API_KEY}"
    for k, v in params.items(): url += f"&{k}={v}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" in res and res["values"]:
            return float(res["values"][0].get(indicator, 0.0))
    except Exception as e:
        logger.error(f"Failed to fetch {indicator} for {symbol}: {e}")
    return 0.0

def fetch_price(symbol):
    try:
        res = requests.get(f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
        return float(res.get("price", 0.0))
    except: return 0.0

def execute_options_expected_move(is_test=False):
    logger.info("Calculating Options 0DTE Expected Move Boundary...")
    spy_price = fetch_price("SPY")
    vix_iv = float(db.get_state("vix_iv_index", 15.0))
    max_pain = db.get_state("spy_highest_oi_strike", "N/A")
    vrp_value = float(db.get_state("SPY_vrp_latest", 0.0))
    
    if spy_price > 0 and vix_iv > 0:
        expected_move = spy_price * (vix_iv / 100) * math.sqrt(1 / 252)
        upper_bound = spy_price + expected_move
        lower_bound = spy_price - expected_move
        
        if vrp_value < 0:
            vrp_status = "🔴 NEGATIVE VRP (Underpriced Risk)"
            directive = "🚨 DRAWDOWN WARNING: Market makers are underpricing tail risk. Do NOT sell unhedged or wide premium inside this zone."
            color_hex = 0xe74c3c  
        else:
            vrp_status = "🟢 POSITIVE VRP (Premium Rich)"
            directive = "✅ SPREAD ALPHA ACTIVE: Yield harvesting parameters authorized outside of calculated bounds."
            color_hex = 0x2ecc71  

        state_hash = f"IV_{int(vix_iv)}_VRP_{'NEG' if vrp_value < 0 else 'POS'}_OI_{max_pain}"
        
        should_broadcast = db.track_and_limit_alerts(
            alert_id="OPTIONS_0DTE_SPY",
            current_state=state_hash,
            current_trigger=spy_price,
            max_broadcasts=3,
            threshold_pct=0.012 
        )

        if should_broadcast or is_test:
            payload = (
                f"[SYSTEMIC RISK POSTURE: {vrp_status}]\n\n"
                f"🧠 **Expected Move Boundary (0DTE)**:\n"
                f"┣ **SPY Spot Price**: `${spy_price:,.2f}`\n"
                f"┣ **Implied Daily Move**: `+/- ${expected_move:.2f}`\n"
                f"┣ **Upper Boundary (Ceiling)**: `${upper_bound:.2f}`\n"
                f"┣ **Lower Boundary (Floor)**: `${lower_bound:.2f}`\n"
                f"┗ **Max Pain OI Node**: `${max_pain}`\n\n"
                f"💡 **Tactical Directive**:\n{directive}"
            )
            if HAS_ESSENTIALS and WEBHOOK_OPTIONS_SIGNALS:
                send_essentials_embed(WEBHOOK_OPTIONS_SIGNALS, "🎯 SPY Volatility & OI Boundary", payload, color_hex)

def execute_forex_tactical_scan(is_test=False):
    logger.info("Executing Forex ATR Exhaustion & Tactical Scans...")
    if not TD_API_KEY or not WEBHOOK_FOREX: return

    pairs = ["EUR/USD", "XAU/USD", "GBP/USD", "USD/JPY"]
    net_liq = float(db.get_state("net_liquidity", 0.0))
    
    for pair in pairs:
        price = fetch_price(pair)
        atr_14 = fetch_td_indicator(pair, "atr", "1day", time_period=14)
        
        if not price or not atr_14: continue
            
        upper_noise = price + (atr_14 * 0.5)
        lower_noise = price - (atr_14 * 0.5)
        
        state_hash = f"ATR_RANGE_{pair}_LIQ_{int(net_liq)}"
        
        should_broadcast = db.track_and_limit_alerts(
            alert_id=f"FOREX_{pair}",
            current_state=state_hash,
            current_trigger=price,
            max_broadcasts=3,
            threshold_pct=0.008 
        )

        if should_broadcast or is_test:
            tactics = FOREX_TACTICAL_DATA.get(pair, {})
            
            payload = (
                f"[MACRO TELEMETRY: Systemic Base Liquidity at ${net_liq:,.0f}B]\n\n"
                f"🌐 **{pair} Volatility Matrix**:\n"
                f"┣ **Current Spot**: `{price:,.4f}`\n"
                f"┣ **Daily ATR (14D)**: `{atr_14:,.4f}`\n"
                f"┗ **Noise Band Perimeter**: `{lower_noise:,.4f}` to `{upper_noise:,.4f}`\n\n"
                f"🛡️ **Institutional Execution Framework**:\n"
                f"┣ **Golden Setup**: {tactics.get('setup', 'N/A')}\n"
                f"┣ **Average Orders**: {tactics.get('orders', 'N/A')}\n"
                f"┗ **Early Signal Model**: {tactics.get('early_signal', 'N/A')}\n\n"
                f"⚠️ **Risk Management Rule (3-5-7)**: Max 3% loss per trade | 5% weekly drawdown limit | 7% monthly drawdown limit."
            )
            send_essentials_embed(WEBHOOK_FOREX, f"🌍 Macro Volatility Brief: {pair}", payload, 0x34495e)

if __name__ == "__main__":
    logger.info("Trade_Signals initialized. Always-On Loop Active.")
    tz_h = pytz.timezone('Pacific/Honolulu')

    try:
        from cross_asset import broadcast_futures_snapshot
    except ImportError:
        def broadcast_futures_snapshot(is_test=False): pass

    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        execute_options_expected_move(is_test=True)
        execute_forex_tactical_scan(is_test=True)
        broadcast_futures_snapshot(is_test=True)
    else:
        loop_counter = 0
        while True:
            try:
                now = datetime.now(tz_h)
                current_time_val = int(now.strftime("%H%M"))
                
                execute_forex_tactical_scan()
                
                if loop_counter % 5 == 0:
                    broadcast_futures_snapshot()
                
                if current_time_val == 335:
                    execute_options_expected_move()
                    
                time.sleep(60) 
                loop_counter += 1
            except Exception as e:
                logger.error(f"Ecosystem Main Loop Exception caught: {e}")
                time.sleep(60)
