import os
import sys
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed
from database import EcosystemDatabase

logger = logging.getLogger("Cross_Asset_Expansion")
logging.basicConfig(level=logging.INFO)

load_dotenv()
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")
db = EcosystemDatabase()

def fetch_td_indicator(symbol, indicator, interval, **params):
    url = f"https://api.twelvedata.com/{indicator}?symbol={symbol}&interval={interval}&apikey={TD_API_KEY}"
    for k, v in params.items(): url += f"&{k}={v}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" in res: return float(res["values"][0].get(indicator, 0.0))
    except Exception as e:
        logger.error(f"Error fetching {indicator} for {symbol}: {e}")
    return 0.0

def calculate_point_of_control(symbol):
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=5min&outputsize=78&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" not in res: return 0.0
        df = pd.DataFrame(res["values"])
        df['close'] = df['close'].astype(float).round(1)
        df['volume'] = df['volume'].astype(int)
        volume_profile = df.groupby('close')['volume'].sum()
        return float(volume_profile.idxmax())
    except: return 0.0

def broadcast_futures_snapshot(is_test=False):
    logger.info("Mapping Futures Node Matrix...")
    assets = {"SPY": "/ES Profile", "QQQ": "/NQ Profile"}
    
    for sym, label in assets.items():
        price = float(db.get_state(f"{sym}_live_spot", 0.0))
        vwap = fetch_td_indicator(sym, "vwap", "5min")
        poc = calculate_point_of_control(sym)
        
        if price == 0 or vwap == 0: continue
            
        db.update_state(f"{sym}_poc", poc)
        db.update_state(f"{sym}_vwap", vwap)
        
        # Immediate 1-Two Liner Output Generation for Intraday Streams
        if WEBHOOK_FUTURES and is_test:
            payload = f"⚡ **[{label}]**\n┣ Price: `${price:,.2f}` | POC Node: `${poc:,.2f}` | VWAP Dev: `{price-vwap:+.2f}`\n┗ *Directive: Alpha execution optimal inside localized VWAP convergence windows.*"
            send_essentials_embed(WEBHOOK_FUTURES, "Futures Telemetry Update", payload, 0xf39c12)

if __name__ == "__main__":
    broadcast_futures_snapshot(is_test=True)
