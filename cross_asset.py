import os
import time
import logging
import requests
import pandas as pd
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed

logger = logging.getLogger("Cross_Asset_Expansion")
logging.basicConfig(level=logging.INFO)

load_dotenv()
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_TRADING")

def fetch_td_sma(symbol, time_period):
    url = f"https://api.twelvedata.com/sma?symbol={symbol}&interval=1day&time_period={time_period}&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" in res:
            return float(res["values"][0].get("sma", 0.0))
    except Exception as e:
        logger.error(f"Error fetching SMA for {symbol}: {e}")
    return None

def broadcast_futures_snapshot():
    logger.info("Compiling Futures Regime Filter...")
    # Using ETF proxies if exact futures symbols aren't returning valid TD SMA data
    assets = {"SPY": "E-mini S&P 500 Proxy", "QQQ": "E-mini Nasdaq Proxy"}
    payload_lines = ["### 📊 Daily Futures Regime Filter"]
    
    for sym, name in assets.items():
        sma_7 = fetch_td_sma(sym, 7)
        sma_21 = fetch_td_sma(sym, 21)
        
        if sma_7 and sma_21:
            if sma_7 > sma_21:
                trend = "🟢 BULLISH REGIME"
                directive = "SYSTEM OVERRIDE: Long-Bias Only. Short scalps highly dangerous."
            else:
                trend = "🔴 BEARISH REGIME"
                directive = "SYSTEM OVERRIDE: Short-Bias Only. Long scalps disabled."
                
            payload_lines.append(f"┣ **{name} ({sym})**\n┃ ┣ Trend: `{trend}`\n┃ ┗ {directive}")
    
    if WEBHOOK_FUTURES:
        send_essentials_embed(WEBHOOK_FUTURES, "Algorithmic Directional Filter", "\n\n".join(payload_lines), 0xf39c12)

if __name__ == "__main__":
    broadcast_futures_snapshot()
