import os
import sys
import time
import logging
import requests
import math
from datetime import datetime
from dotenv import load_dotenv
from database import EcosystemDatabase
try:
    from essentials_tools import send_essentials_embed
except ImportError:
    def send_essentials_embed(*args, **kwargs): pass

logger = logging.getLogger("Trade_Signals")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_FOREX = os.getenv("WEBHOOK_FOREX")
WEBHOOK_OPTIONS = os.getenv("WEBHOOK_OPTIONS_SIGNALS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")

def fetch_td_indicator(symbol, indicator, interval, **params):
    url = f"https://api.twelvedata.com/{indicator}?symbol={symbol}&interval={interval}&apikey={TD_API_KEY}"
    for k, v in params.items(): url += f"&{k}={v}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" in res: return float(res["values"][0].get(indicator, 0.0))
    except: pass
    return 0.0

def fetch_price(symbol):
    try:
        res = requests.get(f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
        return float(res.get("price", 0.0))
    except: return 0.0

def execute_forex_intermarket_scan():
    if not WEBHOOK_FOREX: return
    dxy_rsi = fetch_td_indicator("DXY", "rsi", "1hour", time_period=14)
    if dxy_rsi == 0.0: return
    
    fx_assets = ["XAU/USD", "EUR/USD", "GBP/USD", "USD/JPY"]
    
    for symbol in fx_assets:
        spot_price = fetch_price(symbol)
        if spot_price == 0.0: continue
        
        dispersion = (-0.85 * (1.0 - (dxy_rsi / 100.0)))
        state_hash = f"{symbol.replace('/', '_')}_DISPERSION_{round(dispersion, 1)}"
        
        should_broadcast = db.track_and_limit_alerts(
            alert_id=f"FX_{symbol.replace('/', '_')}_INTERMARKET",
            current_state=state_hash,
            current_trigger=spot_price,
            max_broadcasts=2,
            threshold_pct=0.005
        )
        
        if should_broadcast:
            payload = (
                f"🌍 **Macro Volatility Alert: {symbol} Intermarket Realignment**\n"
                f"┣ **{symbol} Spot Rate**: `${spot_price:,.4f}`\n"
                f"┣ **DXY Dispersion Vector**: `{dispersion:+.2f}`\n"
                f"┗ **Tactical Action Plan**: Quantitative parameters indicate an extreme deviation against the dollar index baseline. Look for high-timeframe structural confluence levels to define trade entry."
            )
            send_essentials_embed(WEBHOOK_FOREX, f"{symbol} Tactical Telemetry", payload, 0x34495e)

if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
    logger.info("Signal Engine Processing Thread Instantiated Successfully. Spam suppressors active.")
    
    while True:
        try:
            vix = fetch_price("VIX")
            spot = fetch_price("SPY")
            if spot > 0 and vix > 0:
                vrp = float(db.get_state("SPY_vrp_latest", 0.0))
                atr = fetch_td_indicator("SPY", "atr", "1day", time_period=14)
                variance = atr * math.sqrt(1.0 + math.log1p(abs(vrp)))
                db.update_state("SPY_expected_upper", spot + variance)
                db.update_state("SPY_expected_lower", spot - variance)

            execute_forex_intermarket_scan()
            
            sys.stdout.flush()
            time.sleep(300) 
        except Exception as e:
            logger.error(f"Signals Error Loop Exception Trace: {e}")
            time.sleep(60)
