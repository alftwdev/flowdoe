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

FOREX_TACTICAL_DATA = {
    "XAU/USD": {"setup": "Asian Session Liquidity Sweep", "orders": "Stop: $3-$5 | Target: $10-$15"},
    "EUR/USD": {"setup": "Value Gap Break & Retest Node", "orders": "Stop: 15-25 pips | Target: 40-60 pips"},
    "GBP/USD": {"setup": "London Session S/R Rejection", "orders": "Stop: 30-40 pips | Target: 80-120 pips"},
    "USD/JPY": {"setup": "Yield-Driven EMA Pullback Scan", "orders": "Stop: 25-35 pips | Target: 70-120 pips"}
}

def fetch_td_indicator(symbol, indicator, interval, **params):
    url = f"https://api.twelvedata.com/{indicator}?symbol={symbol}&interval={interval}&apikey={TD_API_KEY}"
    for k, v in params.items(): url += f"&{k}={v}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" in res and res["values"]:
            return float(res["values"][0].get(indicator, 0.0))
    except Exception as e:
        logger.error(f"Indicator Error ({indicator}) for {symbol}: {e}")
    return 0.0

def fetch_price(symbol):
    try:
        res = requests.get(f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
        return float(res.get("price", 0.0))
    except: return 0.0

def calculate_qho_bounds(symbol, spot, poc, atr, vrp):
    """Applies Quantum Harmonic Oscillator Potential-Well Confinement Math"""
    if spot <= 0 or poc <= 0 or atr <= 0:
        return spot * 1.01, spot * 0.99
    k_modifier = math.sqrt(1.0 + math.log1p(abs(vrp)))
    qho_variance = atr * k_modifier
    return poc + qho_variance, poc - qho_variance

def execute_macro_boundary_sync():
    logger.info("Syncing Core VRP & QHO Potential Well Matrices...")
    vix_iv = float(db.get_state("vix_iv_index", 15.0))
    vrp_value = float(db.get_state("SPY_vrp_latest", 0.0))
    
    for sym in ["SPY", "QQQ"]:
        spot = fetch_price(sym)
        poc = float(db.get_state(f"{sym}_poc", spot))
        atr = fetch_td_indicator(sym, "atr", "1day", time_period=14)
        
        if spot <= 0: continue
        
        expected_move = spot * (vix_iv / 100) * math.sqrt(1 / 252)
        q_upper, q_lower = calculate_qho_bounds(sym, spot, poc, atr, vrp_value)
        
        db.update_state(f"{sym}_expected_upper", q_upper)
        db.update_state(f"{sym}_expected_lower", q_lower)
        db.update_state(f"{sym}_live_spot", spot)

def execute_forex_tactical_scan(is_test=False):
    if not TD_API_KEY or not WEBHOOK_FOREX: return
    pairs = ["EUR/USD", "XAU/USD", "GBP/USD", "USD/JPY"]
    net_liq = float(db.get_state("net_liquidity", 0.0))
    
    for pair in pairs:
        price = fetch_price(pair)
        atr_14 = fetch_td_indicator(pair, "atr", "1day", time_period=14)
        if not price or not atr_14: continue
            
        upper_noise = price + (atr_14 * 0.5)
        lower_noise = price - (atr_14 * 0.5)
        
        db.update_state(f"{pair}_upper_noise", upper_noise)
        db.update_state(f"{pair}_lower_noise", lower_noise)

if __name__ == "__main__":
    tz_h = pytz.timezone('Pacific/Honolulu')
    try:
        from cross_asset import broadcast_futures_snapshot
    except ImportError:
        def broadcast_futures_snapshot(is_test=False): pass

    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        execute_macro_boundary_sync()
        execute_forex_tactical_scan(is_test=True)
        broadcast_futures_snapshot(is_test=True)
    else:
        loop_cnt = 0
        while True:
            try:
                execute_macro_boundary_sync()
                execute_forex_tactical_scan()
                if loop_cnt % 10 == 0:
                    broadcast_futures_snapshot()
                time.sleep(180) # Variable time-dilation protection
                loop_cnt += 1
            except Exception as e:
                logger.error(f"Signals Error Loop: {e}")
                time.sleep(60)
