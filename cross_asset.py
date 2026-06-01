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

def calculate_point_of_control(symbol):
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=5min&outputsize=78&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" not in res: return 0.0
        
        df = pd.DataFrame(res["values"])
        df['close'] = df['close'].astype(float).round(1) 
        df['volume'] = df['volume'].astype(int)
        
        volume_profile = df.groupby('close')['volume'].sum()
        poc_price = volume_profile.idxmax()
        return float(poc_price)
    except Exception as e:
        logger.error(f"Failed to calculate POC for {symbol}: {e}")
        return 0.0

def broadcast_futures_snapshot(is_test=False):
    logger.info("Compiling Institutional Futures Engine...")
    
    assets = {
        "SPY": {"name": "/ES (E-mini S&P 500)", "multiplier": 10}, 
        "QQQ": {"name": "/NQ (E-mini Nasdaq 100)", "multiplier": 40}
    }
    
    state_hash = "" 
    payload_lines = []
    
    for sym, config in assets.items():
        price = fetch_price(sym)
        vwap = fetch_td_indicator(sym, "vwap", "5min")
        ema_20 = fetch_td_indicator(sym, "ema", "1day", time_period=20)
        atr = fetch_td_indicator(sym, "atr", "1day", time_period=14)
        poc = calculate_point_of_control(sym)
        
        if not price or not vwap: continue
            
        trend = "🟢 BULLISH" if price > ema_20 else "🔴 BEARISH"
        bias = "Long-Bias Only. Short execution disabled." if price > ema_20 else "Short-Bias Only. Long execution disabled."
        vwap_status = f"${abs(price-vwap):.2f} ABOVE VWAP" if price > vwap else f"${abs(price-vwap):.2f} BELOW VWAP"
        
        volatility_floor = price - (atr * 0.5)
        gamma_ceiling = price + (atr * 0.5)

        payload_lines.extend([
            f"### 📊 {config['name']}",
            f"┣ **Macro Order Flow**: `{trend}` | *{bias}*",
            f"┣ **Institutional VWAP (5m)**: `${vwap:,.2f}` ({vwap_status})",
            f"┣ 🎯 **Point of Control (POC)**: `${poc:,.2f}` *(High Volume Node)*",
            f"┣ **Intraday Liquidity Floor**: `${volatility_floor:,.2f}`",
            f"┗ **Gamma Resistance Ceiling**: `${gamma_ceiling:,.2f}`\n"
        ])
        
        state_hash += f"{sym}_{trend}_{'UP' if price > vwap else 'DN'}"

    full_payload = (
        f"[EXECUTION POSTURE: INTRADAY MARKET PROFILE MATRIX]\n\n" + 
        "\n".join(payload_lines) +
        f"--------------------------------------------------------------------\n"
        f"🧠 **Tactical Directive**: Execute entries strictly adjacent to VWAP, POC, or Liquidity Floors. Do not chase breakout momentum into Gamma Ceilings."
    )
    
    # WIDENED: Now requires a 0.7% move in the broad market to reset the gatekeeper
    should_broadcast = db.track_and_limit_alerts(
        alert_id="FUTURES_MARKET_PROFILE_MATRIX",
        current_state=state_hash,
        current_trigger=fetch_price("SPY"), 
        max_broadcasts=3,
        threshold_pct=0.007 
    )

    if should_broadcast or is_test:
        if WEBHOOK_FUTURES:
            send_essentials_embed(WEBHOOK_FUTURES, "⚡ Algorithmic Market Profile Terminal", full_payload, 0xf39c12)
            logger.info("Futures Matrix alert successfully pushed down the pipeline.")
    else:
        logger.info("Futures matrix silenced by Gatekeeper (Threshold limit active).")

if __name__ == "__main__":
    is_test_mode = len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]
    broadcast_futures_snapshot(is_test=is_test_mode)
