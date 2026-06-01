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

# --- Core Webhook & API Routing ---
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_FOREX = os.getenv("WEBHOOK_FOREX")
WEBHOOK_OPTIONS_SIGNALS = os.getenv("WEBHOOK_OPTIONS_SIGNALS") or os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_FUTURES = os.getenv("WEBHOOK_FUTURES_SIGNALS") or os.getenv("WEBHOOK_FUTURES")

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
    
    if spy_price > 0 and vix_iv > 0:
        # Simplified BSM theoretical expected move
        expected_move = spy_price * (vix_iv / 100) * math.sqrt(1/252)
        upper_bound = spy_price + expected_move
        lower_bound = spy_price - expected_move
        
        state_hash = f"IV_{int(vix_iv)}_OI_{max_pain}" 
        
        # 3-Strike Dynamic Gatekeeper
        should_broadcast = db.track_and_limit_alerts(
            alert_id="OPTIONS_0DTE_SPY",
            current_state=state_hash,
            current_trigger=spy_price,
            max_broadcasts=3,
            threshold_pct=0.005 
        )

        if should_broadcast or is_test:
            payload = (
                f"### 🎯 SPY 0DTE Volatility & Open Interest Boundary\n"
                f"Institutional pricing models mandate the following 'No-Fly Zones' based on BSM theoretical pricing and Open Interest flows:\n\n"
                f"┣ **SPY Spot Price:** `${spy_price:,.2f}`\n"
                f"┣ **Implied Daily Move (BSM Model):** `+/- ${expected_move:.2f}`\n"
                f"┣ **Upper Expected Boundary:** `${upper_bound:.2f}`\n"
                f"┣ **Lower Expected Boundary:** `${lower_bound:.2f}`\n"
                f"┗ **Institutional Open Interest (Max Pain):** `${max_pain}`\n\n"
                f"⚠️ *Directive: Do not sell naked credit inside this perimeter. Options market makers are pricing in movement to these strikes.*"
            )
            if HAS_ESSENTIALS and WEBHOOK_OPTIONS_SIGNALS:
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
        
        state_hash = f"ATR_RANGE_{pair}"
        
        # 3-Strike Dynamic Gatekeeper
        should_broadcast = db.track_and_limit_alerts(
            alert_id=f"FOREX_{pair}",
            current_state=state_hash,
            current_trigger=price,
            max_broadcasts=3,
            threshold_pct=0.001 
        )

        if should_broadcast or is_test:
            payload = (
                f"### 🌐 {pair} Volatility Matrix\n"
                f"┣ **Current Spot**: `{price:,.4f}`\n"
                f"┣ **Daily ATR (14D)**: `{atr_14:,.4f}`\n"
                f"┗ **Noise Band Thresholds**: Breakouts must clear `{upper_noise:,.4f}` or `{lower_noise:,.4f}` to negate mathematical noise algorithms.\n\n"
                f"*Execute inside the noise band at your own statistical peril.*"
            )
            if HAS_ESSENTIALS:
                send_essentials_embed(WEBHOOK_FOREX, "Macro Volatility Brief", payload, 0x34495e)

def calculate_wma(series, length):
    """Calculates Weighted Moving Average."""
    weights = np.arange(1, length + 1)
    return series.rolling(length).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)

def execute_futures_conviction_scan(is_test=False):
    """
    Translates Pine Script WMA crossover and Gaussian momentum logic 
    to analyze continuous futures order flow.
    """
    logger.info("⚡ Initiating Institutional Futures Matrix...")
    if not TD_API_KEY:
        logger.error("Aborting Futures Scan: Missing TWELVE_DATA_API_KEY.")
        return

    futures_assets = {"ES": "S&P 500 Futures", "NQ": "Nasdaq Futures"}
    report_lines = []
    regime_states = []
    es_trigger_price = 0.0

    for symbol, name in futures_assets.items():
        try:
            url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1h&outputsize=250&apikey={TD_API_KEY}"
            res = requests.get(url, timeout=10).json()
            
            if "values" not in res:
                logger.warning(f"Could not fetch time series for {symbol}")
                continue
                
            df = pd.DataFrame(res['values'])
            df['close'] = df['close'].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)
            
            close_price = df['close'].iloc[-1]
            if symbol == "ES":
                es_trigger_price = close_price
            
            df['WMA_13'] = calculate_wma(df['close'], 13)
            df['WMA_48'] = calculate_wma(df['close'], 48)
            df['WMA_200'] = calculate_wma(df['close'], 200)
            
            wma_13 = df['WMA_13'].iloc[-1]
            wma_48 = df['WMA_48'].iloc[-1]
            wma_200 = df['WMA_200'].iloc[-1]

            trend_status = "CHOP / CONSOLIDATION"
            emoji = "⏳"
            
            is_up_trend = close_price > wma_200
            is_down_trend = close_price < wma_200
            
            if is_up_trend:
                if wma_13 > wma_48 and wma_48 > wma_200:
                    trend_status = "STRONGER UPTREND (MAX CONVICTION)"
                    emoji = "🔥"
                elif wma_13 > wma_48:
                    trend_status = "CONFIRMED UPTREND"
                    emoji = "🟢"
                else:
                    trend_status = "EARLY UPTREND / REVERSAL"
                    emoji = "🟡"
            elif is_down_trend:
                if wma_13 < wma_48 and wma_48 < wma_200:
                    trend_status = "STRONGER DOWNTREND (MAX CONVICTION)"
                    emoji = "🚨"
                elif wma_13 < wma_48:
                    trend_status = "CONFIRMED DOWNTREND"
                    emoji = "🔴"
                else:
                    trend_status = "EARLY DOWNTREND / DISTRIBUTION"
                    emoji = "🟠"

            regime_states.append(f"{symbol}:{trend_status}")
            
            pct_from_200 = ((close_price - wma_200) / wma_200) * 100
            exhaustion_warning = ""
            if pct_from_200 >= 2.5:
                exhaustion_warning = " ⚠️ LOCAL TOP RISK"
            elif pct_from_200 <= -2.5:
                exhaustion_warning = " ⚠️ LOCAL BOTTOM RISK"
                
            report_lines.append(
                f"┣ **{name} ({symbol})**: `{close_price:,.2f}`\n"
                f"┃  ┣ **Regime**: {emoji} *{trend_status}*{exhaustion_warning}\n"
                f"┃  ┗ **WMA Structure**: 13:`{wma_13:,.2f}` | 48:`{wma_48:,.2f}` | 200:`{wma_200:,.2f}`"
            )
        except Exception as e:
            logger.error(f"Error compiling futures matrix for {symbol}: {e}")

    if not report_lines:
        return

    # 3-Strike Dynamic Gatekeeper implementation for Futures
    combined_state_hash = "|".join(regime_states)
    should_broadcast = db.track_and_limit_alerts(
        alert_id="FUTURES_MATRIX_CONVICTION",
        current_state=combined_state_hash,
        current_trigger=es_trigger_price,
        max_broadcasts=3,
        threshold_pct=0.0015 # Math shifts by 0.15% to reset
    )

    if should_broadcast or is_test:
        payload = (
            f"### 🌐 Institutional Futures Matrix\n"
            f"Cross-verified structural WMA regime mapping for continuous futures contracts:\n\n"
            f"**Asset Consensus Profiles:**\n" + "\n".join(report_lines) + "\n\n"
            f"***\n"
            f"**Rockefeller Strategic Intelligence** • Futures Order Flow Engine"
        )

        title = "📈 Futures Strategic Conviction Broadcast" + (" [TEST]" if is_test else "")
        
        if HAS_ESSENTIALS and WEBHOOK_FUTURES:
            send_essentials_embed(WEBHOOK_FUTURES, title, payload, 0xf1c40f)
            db.log_event("Futures Conviction Matrix successfully dispatched.")
        else:
            logger.info(f"[Local Broadcast Print Due to Webhook Absence]:\n{payload}")

if __name__ == "__main__":
    logger.info("Trade_Signals initialized.")
    tz_h = pytz.timezone('Pacific/Honolulu')

    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        execute_options_expected_move(is_test=True)
        execute_forex_tactical_scan(is_test=True)
        execute_futures_conviction_scan(is_test=True)
    else:
        while True:
            try:
                now = datetime.now(tz_h)
                current_time_val = int(now.strftime("%H%M"))
                
                # Active constant monitoring across requested sectors
                execute_forex_tactical_scan()
                execute_futures_conviction_scan() 
                
                # Time-gated scans (e.g., Options pricing pre-market/close)
                if current_time_val == 335:
                    execute_options_expected_move()
                    
                time.sleep(300) 
            except Exception as e:
                logger.error(f"Ecosystem Main Loop Exception caught: {e}")
                time.sleep(60)
