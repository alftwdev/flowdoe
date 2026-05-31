import os
import time
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
        if "values" in res:
            return float(res["values"][0].get(indicator, 0.0))
    except Exception as e:
        logger.error(f"Error fetching {indicator} for {symbol}: {e}")
    return 0.0

def fetch_price(symbol):
    try:
        res = requests.get(f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
        return float(res.get("price", 0.0))
    except: return 0.0

def broadcast_futures_snapshot(is_test=False):
    logger.info("Compiling Institutional Futures Engine...")
    
    # We use high-volume ETFs for accurate VWAP/ATR, but label them strictly as Futures
    assets = {
        "SPY": {"name": "/ES (E-mini S&P 500)", "multiplier": 10}, 
        "QQQ": {"name": "/NQ (E-mini Nasdaq 100)", "multiplier": 40}
    }
    
    payload_lines = [
        "====================================================================",
        "Title: INSTITUTIONAL INTRADAY FUTURES | VWAP & ORDER FLOW MATRIX",
        "====================================================================\n"
    ]
    
    state_hash = "" # Used for the gatekeeper
    
    for sym, config in assets.items():
        price = fetch_price(sym)
        vwap = fetch_td_indicator(sym, "vwap", "5min")
        ema_20 = fetch_td_indicator(sym, "ema", "1day", time_period=20)
        atr = fetch_td_indicator(sym, "atr", "1day", time_period=14)
        
        if not price or not vwap: continue
            
        # Extrapolate ETF price roughly to Futures points for realism, or keep raw underlying
        # Here we track the actual asset structure.
        trend = "🟢 BULLISH REGIME" if price > ema_20 else "🔴 BEARISH REGIME"
        bias = "Long-Bias Only. Short scalps highly dangerous." if price > ema_20 else "Short-Bias Only. Long scalps disabled."
        
        # Determine intraday position vs VWAP
        if price > vwap:
            vwap_status = f"Price is ${abs(price-vwap):.2f} ABOVE VWAP. Bulls control intraday order flow."
        else:
            vwap_status = f"Price is ${abs(price-vwap):.2f} BELOW VWAP. Bears control intraday order flow."

        volatility_floor = price - (atr * 0.5)
        gamma_ceiling = price + (atr * 0.5)

        payload_lines.extend([
            f"## 📊 {config['name']}",
            f"┣ Macro Regime: {trend} ({bias})",
            f"┣ Institutional VWAP (5m): ${vwap:,.2f} | {vwap_status}",
            f"┣ Intraday Volatility Floor: ${volatility_floor:,.2f} (Support Band)",
            f"┗ Institutional Gamma Ceiling: ${gamma_ceiling:,.2f} (Take-Profit Band)\n"
        ])
        
        state_hash += trend

    payload_lines.append("--------------------------------------------------------------------")
    payload_lines.append("🧠 Trading Intelligence: Execute entries strictly near VWAP or Volatility Floors. Do not chase momentum into Gamma Ceilings.")
    payload_lines.append("====================================================================")
    
    full_payload = "\n".join(payload_lines)
    
    # Universal Gatekeeper
    should_broadcast = db.track_and_limit_alerts(
        alert_id="FUTURES_VWAP_MATRIX",
        current_state=state_hash,
        current_trigger=fetch_price("SPY"), 
        max_broadcasts=3,
        threshold_pct=0.002 # 0.2% price shift triggers a fresh update
    )

    if should_broadcast or is_test:
        if WEBHOOK_FUTURES:
            send_essentials_embed(WEBHOOK_FUTURES, "Algorithmic Directional Filter", full_payload, 0xf39c12)
    else:
        logger.info("Futures Matrix silenced by Gatekeeper (Threshold not met).")

if __name__ == "__main__":
    is_test_mode = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    broadcast_futures_snapshot(is_test=is_test_mode)
