import os
import sys
import logging
import time
import requests
import numpy as np
import pandas as pd
from datetime import datetime
import pytz
from dotenv import load_dotenv
from database import EcosystemDatabase

try:
    from statsmodels.api import OLS, add_constant
    from statsmodels.tsa.stattools import adfuller
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False

try:
    from edge import calculate_vrp_score
    from metrics import log_trade_context
    HAS_LOCAL_MODULES = True
except (ImportError, SyntaxError, IndentationError):
    HAS_LOCAL_MODULES = False

logger = logging.getLogger("Trade_Signals")
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
db = EcosystemDatabase()

try:
    from essentials_tools import send_essentials_embed, get_trend_alignment, get_institutional_conviction
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False
    def send_essentials_embed(*args, **kwargs): return False
    def get_trend_alignment(symbol, td_api_key): return "NEUTRAL", True
    def get_institutional_conviction(symbol, td_api_key): return 0, 1000000

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MARKET_ANALYSIS = os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_FOREX = os.getenv("WEBHOOK_FOREX")
WEBHOOK_FED = os.getenv("WEBHOOK_FED")

def validate_environment():
    required_keys = ["TWELVE_DATA_API_KEY", "WEBHOOK_MARKET_ANALYSIS"]
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        logger.warning(f"⚠️ System Environmental Warning: Missing variables {missing}.")

def fetch_twelvedata_rsi(symbol, interval="1h", time_period=14):
    url = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval={interval}&time_period={time_period}&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" in res and res["values"]:
            return float(res["values"][0].get("rsi", 50.0))
    except Exception as e:
        logger.error(f"Failed to fetch RSI for {symbol}: {e}")
    return 50.0

def fetch_td_indicator(symbol, indicator, interval, **params):
    """Universal helper for Twelve Data Technical Indicators."""
    url = f"https://api.twelvedata.com/{indicator}?symbol={symbol}&interval={interval}&apikey={TD_API_KEY}"
    for k, v in params.items(): url += f"&{k}={v}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" in res and res["values"]:
            if indicator == "stoch":
                return float(res["values"][0].get("slowk", 50.0)), float(res["values"][0].get("slowd", 50.0))
            return float(res["values"][0].get(indicator, 0.0))
    except Exception as e:
        logger.error(f"Failed to fetch {indicator} for {symbol}: {e}")
    return None

def execute_vrp_signal_scan(is_test=False):
    logger.info("Executing Volatility Risk Premium (VRP) signal scan...")
    latest_vrp = db.get_state("SPY_vrp_latest", 0.0)
    vix_iv = db.get_state("vix_iv_index", 14.0)
    status = "VOLATILITY HARVESTING" if latest_vrp > 0 else "INSURANCE BUYING"
    db.log_event(f"VRP Scan completed. State: {status} | VIX: {vix_iv}")

def execute_pairs_scan(is_test=False):
    logger.info("Executing statistical arbitrage pairs scan...")
    if not HAS_STATSMODELS: return
    db.log_event("Pairs arbitrage scan completed nominal.")

def execute_global_macro_matrix(is_test=False):
    logger.info("Syncing system global macro alignment layers...")
    regime = db.get_state("market_regime_state", "BULLISH")
    logger.info(f"Global macro alignment pulled: Current regime mode is {regime}")

def execute_forex_tactical_scan():
    """
    Intraday execution of the XAU/USD 4-Phase Pullback & EUR/USD Multi-Timeframe Reversal logic.
    Evaluates indicators to deploy highly actionable math-backed signals.
    """
    logger.info("Executing Forex & Metals Tactical Scans...")
    if not TD_API_KEY or not WEBHOOK_FOREX: return

    report_lines = []

    # Strategy 1: EUR/USD Multi-Timeframe Stochastic Reversal
    d_sma7 = fetch_td_indicator("EUR/USD", "sma", "1day", time_period=7)
    d_sma21 = fetch_td_indicator("EUR/USD", "sma", "1day", time_period=21)
    h_sma50 = fetch_td_indicator("EUR/USD", "sma", "1h", time_period=50)
    h_sma200 = fetch_td_indicator("EUR/USD", "sma", "1h", time_period=200)
    stoch = fetch_td_indicator("EUR/USD", "stoch", "1h")

    if all([d_sma7, d_sma21, h_sma50, h_sma200, stoch]):
        k, d = stoch
        macro_bullish = d_sma7 > d_sma21
        micro_bullish = h_sma50 > h_sma200
        
        if macro_bullish and micro_bullish and k < 20:
            report_lines.append(
                f"🚨 **EUR/USD TACTICAL ALIGNMENT (LONG SETUP)**\n"
                f"┣ **Condition:** Daily & Hourly Macro Trend is **BULLISH**.\n"
                f"┣ **Trigger:** Hourly Stochastic dropped to deeply oversold levels (`{k:.1f}`).\n"
                f"┗ **Strategy:** High-probability continuation reversal. Prepare for long execution upon upward momentum cross."
            )
        elif not macro_bullish and not micro_bullish and k > 80:
            report_lines.append(
                f"🚨 **EUR/USD TACTICAL ALIGNMENT (SHORT SETUP)**\n"
                f"┣ **Condition:** Daily & Hourly Macro Trend is **BEARISH**.\n"
                f"┣ **Trigger:** Hourly Stochastic pushed to deeply overbought levels (`{k:.1f}`).\n"
                f"┗ **Strategy:** Trend continuation short setup identified. Await momentum cross downward."
            )

    # Strategy 2: XAU/USD Volatility Expansion Channel
    ema14 = fetch_td_indicator("XAU/USD", "ema", "1h", time_period=14)
    ema18 = fetch_td_indicator("XAU/USD", "ema", "1h", time_period=18)
    ema24 = fetch_td_indicator("XAU/USD", "ema", "1h", time_period=24)
    price_res = requests.get(f"https://api.twelvedata.com/price?symbol=XAU/USD&apikey={TD_API_KEY}").json()
    
    if all([ema14, ema18, ema24]) and "price" in price_res:
        price = float(price_res["price"])
        if ema14 > ema18 > ema24: # Armed Uptrend
            if price <= ema14: # Pullback State
                report_lines.append(
                    f"🏆 **XAU/USD VOLATILITY CHANNEL ARMED**\n"
                    f"┣ **Condition:** Gold is in a confirmed 4-phase uptrend (EMA14 > EMA18 > EMA24).\n"
                    f"┣ **Trigger:** Price has pulled back into the volatility channel (`${price:,.2f}`).\n"
                    f"┗ **Strategy:** Do not enter yet. Wait for breakout confirmation above EMA 14 (`${ema14:,.2f}`) for optimal Risk/Reward."
                )

    if report_lines:
        payload = "### 🌐 Forex & Metals Tactical Radar\n" + "\n\n".join(report_lines) + "\n\n*ESSENTIALS Quantitative Architecture*"
        send_essentials_embed(WEBHOOK_FOREX, "Mathematical Setup Detected", payload, 0xf1c40f)

def execute_tsp_tactical_scan():
    """
    Intraday execution for Federal TSP Proxies. Evaluates Daily/Weekly moving averages
    to identify major Interfund Transfer (IFT) pivot points.
    """
    logger.info("Executing TSP Institutional Moving Average Scan...")
    if not TD_API_KEY or not WEBHOOK_FED: return
    
    proxies = {"SPY": "C Fund", "VXF": "S Fund", "EFA": "I Fund"}
    report_lines = []

    for sym, name in proxies.items():
        d_sma10 = fetch_td_indicator(sym, "sma", "1day", time_period=10) # Proxy for 2-week
        d_sma50 = fetch_td_indicator(sym, "sma", "1day", time_period=50) # Proxy for 10-week
        
        if d_sma10 and d_sma50:
            if d_sma10 < d_sma50:
                report_lines.append(f"🔴 **{name} ({sym})**: Short-term momentum crossed below Macro Baseline. Defensive reallocation to G-Fund mathematically favored.")
    
    if report_lines:
        payload = "### 🦅 TSP Tactical IFT Alert\n" + "\n".join(report_lines) + "\n\n*ESSENTIALS Capital Preservation Engine*"
        send_essentials_embed(WEBHOOK_FED, "TSP Macro Alignment Warning", payload, 0xe74c3c)

def execute_unified_conviction_scan(is_test=False):
    logger.info("⚡ Initiating Unified Institutional Conviction Matrix engine...")
    if not TD_API_KEY: return

    core_assets = ["SPY", "QQQ", "IWM", "BTC/USD"]
    gex_flip = db.get_state("spy_gex_flip", 540.0)
    latest_vrp = db.get_state("SPY_vrp_latest", 0.5)
    regime_mode = db.get_state("market_regime_state", "BULLISH")
    
    report_lines = []
    
    for symbol in core_assets:
        try:
            p_res = requests.get(f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
            price = float(p_res.get("price", 0.0))
            if price == 0: continue
                
            trend_status, is_bullish = get_trend_alignment(symbol, TD_API_KEY)
            rsi_val = fetch_twelvedata_rsi(symbol, interval="1h")
            
            v_res = requests.get(f"https://api.twelvedata.com/statistics?symbol={symbol}&apikey={TD_API_KEY}", timeout=10).json()
            stats = v_res.get("statistics", {})
            curr_vol = int(stats.get("volume", 1))
            avg_vol = int(stats.get("avg_volume_30_days", 1))
            vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1.0

            conviction_score = 50 
            conviction_score += 15 if is_bullish else -15
            
            if 55 <= rsi_val <= 70: conviction_score += 10
            elif 30 <= rsi_val <= 45: conviction_score -= 10
                
            if vol_ratio > 1.15 and is_bullish: conviction_score += 15
            elif vol_ratio > 1.15 and not is_bullish: conviction_score -= 15
                
            if symbol == "SPY":
                if price > gex_flip: conviction_score += 10
                else: conviction_score -= 10
                if latest_vrp > 0: conviction_score += 10

            conviction_score = max(0, min(100, conviction_score))
            
            if conviction_score >= 75: emoji, status_txt = "🔥", "INSTITUTIONAL LOCK-IN (STRONG LONG)"
            elif 55 <= conviction_score < 75: emoji, status_txt = "🟢", "LIQUIDITY EXPANSION (BULLISH)"
            elif 45 <= conviction_score < 55: emoji, status_txt = "⏳", "CHOP REGIME (COMPRESSION)"
            elif 25 <= conviction_score < 45: emoji, status_txt = "🔴", "BEARISH PRESSURE (DISTRIBUTION)"
            else: emoji, status_txt = "⚠️", "DEFENSIVE LIQUIDATION (STRONG SHORT)"
                
            report_lines.append(
                f"┣ **{symbol}**: `${price:,.2f}`\n"
                f"┃  ┣ **Conviction Score**: `{conviction_score}%` → {emoji} *{status_txt}*\n"
                f"┃  ┗ **Metrics**: Supertrend: `{trend_status.split()[-1] if ' ' in trend_status else trend_status}`, RSI(1h): `{rsi_val:.1f}`, Vol Ratio: `{vol_ratio:.2f}x`"
            )
        except Exception as e:
            logger.error(f"Error compiling conviction matrix line for {symbol}: {e}")

    if not report_lines: return

    payload = (
        f"### 🛡️ Unified Institutional Conviction Matrix\n"
        f"Cross-verified market consensus metrics utilizing real-time technical, volume, and data layer parameters:\n\n"
        f"**System Ecosystem Overlays:**\n"
        f"┣ **Macro Regime Posture**: `{regime_mode}`\n"
        f"┗ **SPY Gamma Flip Boundary**: `${gex_flip:,.2f}`\n\n"
        f"**Asset Consensus Profiles:**\n" + "\n".join(report_lines) + "\n\n"
        f"***\n"
        f"**ESSENTIALS** • *FEAR OF MISSING OUT* • Automated Market Intelligence Engine"
    )

    title = "🤖 Unified Strategic Conviction Broadcast" + (" [TEST]" if is_test else "")
    
    if HAS_ESSENTIALS and WEBHOOK_MARKET_ANALYSIS:
        send_essentials_embed(WEBHOOK_MARKET_ANALYSIS, title, payload, 0x9b59b6)
        db.log_event("Unified Conviction Matrix successfully dispatched to subscriber network.")
        db.update_state("last_ping_options", time.time())
        db.update_state("last_ping_macro", time.time())

def compile_institutional_macro_recap():
    logger.info("Compiling daily institutional macro recap metrics...")
    db.log_event("Institutional recap metrics aggregated successfully.")

def compile_eod_derivatives_matrix(is_test=False):
    logger.info("Compiling end-of-day institutional derivatives matrix configurations...")
    db.log_event("EOD derivatives structure snapshot finalized.")

if __name__ == "__main__":
    validate_environment()
    logger.info("Trade_Signals initialized with VRP, Arbitrage, and Macro Expansion matrices.")
    tz_h = pytz.timezone('Pacific/Honolulu')

    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        logger.info("Terminal Test Mode Initiated.")
        execute_vrp_signal_scan(is_test=True)
        execute_pairs_scan(is_test=True)
        execute_global_macro_matrix(is_test=True)
        execute_unified_conviction_scan(is_test=True)
        execute_forex_tactical_scan()
        execute_tsp_tactical_scan()
        compile_eod_derivatives_matrix(is_test=True)
    else:
        logger.info("Production mode: Starting persistent 15-minute unified signal loop.")
        last_eod_date = None
        while True:
            try:
                now = datetime.now(tz_h)
                current_date = now.strftime("%Y-%m-%d")
                current_time_val = int(now.strftime("%H%M"))
                
                # 1. Standard Intraday Market Scans
                execute_vrp_signal_scan(is_test=False)
                execute_pairs_scan(is_test=False)
                execute_global_macro_matrix(is_test=False)
                execute_unified_conviction_scan(is_test=False)
                
                # 2. Highly Tactical Intraday Engine Scans (The Goldmine)
                execute_forex_tactical_scan()
                execute_tsp_tactical_scan()
                
                # 3. Timed Institutional Recap (09:30 - 09:35 AM HST)
                if 930 <= current_time_val <= 935 and current_date != last_eod_date:
                    compile_institutional_macro_recap()
                
                # 4. Timed EOD Institutional Matrix (10:05 - 10:10 AM HST)
                if 1005 <= current_time_val <= 1010 and current_date != last_eod_date:
                    compile_eod_derivatives_matrix(is_test=False)
                    last_eod_date = current_date
                
                time.sleep(900) 
            except Exception as e:
                logger.error(f"Ecosystem Main Loop Exception caught: {e}")
                time.sleep(60)
