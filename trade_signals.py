import os
import sys
import logging
import time
import math
import requests
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
    
    if spy_price > 0 and vix_iv > 0:
        expected_move = spy_price * (vix_iv / 100) * math.sqrt(1/252)
        upper_bound = spy_price + expected_move
        lower_bound = spy_price - expected_move
        
        state_hash = f"IV_{int(vix_iv)}" # Hash changes if VIX whole number changes
        
        should_broadcast = db.track_and_limit_alerts(
            alert_id="OPTIONS_0DTE_SPY",
            current_state=state_hash,
            current_trigger=spy_price,
            max_broadcasts=3,
            threshold_pct=0.005 # 0.5% move in SPY resets the alert
        )

        if should_broadcast or is_test:
            payload = (
                f"### 🎯 SPY 0DTE Volatility Boundary\n"
                f"Institutional pricing models mandate the following 'No-Fly Zones' for short premium sellers today:\n\n"
                f"┣ **SPY Spot Price:** `${spy_price:,.2f}`\n"
                f"┣ **Implied Daily Move:** `+/- ${expected_move:.2f}`\n"
                f"┣ **Upper Expected Boundary:** `${upper_bound:.2f}`\n"
                f"┗ **Lower Expected Boundary:** `${lower_bound:.2f}`\n\n"
                f"⚠️ *Directive: Do not sell naked credit inside this perimeter. Options market makers are pricing in movement to these strikes.*"
            )
            if HAS_ESSENTIALS:
                send_essentials_embed(WEBHOOK_OPTIONS_SIGNALS, "Mathematical Expected Move", payload, 0x9b59b6)

def execute_forex_tactical_scan(is_test=False):
    logger.info("Executing Forex ATR Exhaustion & Tactical Scans...")
    if not TD_API_KEY or not WEBHOOK_FOREX: return

    pairs = ["EUR/USD", "XAU/USD", "GBP/USD", "USD/JPY"]
    
    for pair in pairs:
        price = fetch_price(pair)
        atr_14 = fetch_td_indicator(pair, "atr", "1day", time_period=14)
        
        if not price or not atr_14: continue
            
        upper_noise = price + (atr_14 * 0.5)
        lower_noise = price - (atr_14 * 0.5)
        
        # Check Gatekeeper before building string
        state_hash = f"ATR_RANGE_{pair}"
        should_broadcast = db.track_and_limit_alerts(
            alert_id=f"FOREX_{pair}",
            current_state=state_hash,
            current_trigger=price,
            max_broadcasts=3,
            threshold_pct=0.001 # 0.1% price movement allows a new alert
        )

        if should_broadcast or is_test:
            payload = (
                f"### 🌐 {pair} Volatility Matrix\n"
                f"┣ **Current Spot**: `{price:,.4f}`\n"
                f"┣ **Daily ATR (14D)**: `{atr_14:,.4f}`\n"
                f"┗ **Noise Band Thresholds**: Breakouts must clear `{upper_noise:,.4f}` or `{lower_noise:,.4f}` to negate mathematical noise algorithms.\n\n"
                f"*Execute inside the noise band at your own statistical peril.*"
            )
            send_essentials_embed(WEBHOOK_FOREX, "Macro Volatility Brief", payload, 0x34495e)

if __name__ == "__main__":
    logger.info("Trade_Signals initialized.")
    tz_h = pytz.timezone('Pacific/Honolulu')

    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        execute_options_expected_move(is_test=True)
        execute_forex_tactical_scan(is_test=True)
    else:
        while True:
            try:
                now = datetime.now(tz_h)
                current_time_val = int(now.strftime("%H%M"))
                
                # Check intervals (Execute continuously; Gatekeeper prevents spam)
                execute_forex_tactical_scan()
                
                if current_time_val == 335:
                    execute_options_expected_move()
                    
                time.sleep(300) # Loop every 5 mins. Database protects Discord from spam.
            except Exception as e:
                logger.error(f"Ecosystem Main Loop Exception caught: {e}")
                time.sleep(60)
