import os
import time
import datetime
import logging
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from database import EcosystemDatabase
from essentials_tools import send_essentials_embed

logger = logging.getLogger("Cross_Asset_Expansion")
logging.basicConfig(level=logging.INFO)

# UPGRADE: Failsafe import to prevent task crashing if module is missing
try:
    import bt 
    HAS_BT = True
except ImportError:
    HAS_BT = False
    logger.error("CRITICAL: 'bt' module missing. Run 'pip3.10 install bt --user' in console.")

load_dotenv()
db = EcosystemDatabase()
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

WEBHOOKS = {
    "futures": os.getenv("WEBHOOK_FUTURES_TRADING"),
    "options": os.getenv("WEBHOOK_OPTIONS_SIGNALS"),
    "tsp": os.getenv("WEBHOOK_FED"),
    "forex": os.getenv("WEBHOOK_FOREX")
}

def fetch_twelve_data_close(symbol, interval="1day", outputsize=10):
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval={interval}&outputsize={outputsize}&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" in res:
            df = pd.DataFrame(res["values"])
            df["close"] = df["close"].astype(float)
            return df["close"].iloc[0], df
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
    return None, None

def broadcast_futures_snapshot():
    assets = {"/ES": "E-mini S&P 500", "/NQ": "E-mini Nasdaq 100", "/CL": "Crude Oil"}
    payload_lines = ["### 📊 Pre-Market Futures Flow Matrix"]
    
    for sym, name in assets.items():
        price, _ = fetch_twelve_data_close(sym, interval="5min", outputsize=2)
        if price:
            payload_lines.append(f"┣ **{name} ({sym})**: `${price:,.2f}`")
    
    payload_lines.append("\n*System Status: Continuous globally sourced pricing feeds operational.*")
    if WEBHOOKS["futures"]:
        send_essentials_embed(WEBHOOKS["futures"], "Futures Contract Dashboard", "\n".join(payload_lines), 0xf39c12)
        db.update_state("last_ping_macro", time.time())

def broadcast_options_signals():
    spy_vrp = db.get_state("SPY_vrp_latest", 0.0)
    spy_rv = db.get_state("SPY_rv_latest", 0.15)
    
    regime = "PREMIUM HARVESTING" if spy_vrp > 0 else "DEFENSIVE SHIELD ACTIVE"
    color = 0x2ecc71 if spy_vrp > 0 else 0xe74c3c
    
    payload = (
        f"### 🎛️ Derivative Volatility Environment\n"
        f"┣ **Calculated VRP Score:** `{spy_vrp:.4f}`\n"
        f"┣ **Annualized Realized Vol (RV):** `{spy_rv * 100:.2f}%`\n"
        f"┗ **Tactical Operation Mode:** `{regime}`\n\n"
        f"*Instruction:* Sell wide high-probability credit spreads when VRP remains structurally premium rich."
    )
    if WEBHOOKS["options"]:
        send_essentials_embed(WEBHOOKS["options"], "Proprietary Volatility State Engine", payload, color)
        db.update_state("last_ping_options", time.time())

def broadcast_tsp_allocation():
    if not HAS_BT:
        logger.warning("Skipping TSP Allocation Matrix: 'bt' backtesting module is not installed.")
        return

    proxies = {"SPY": "C Fund", "VXF": "S Fund", "EFA": "I Fund", "AGG": "F Fund"}
    data_dict = {}
    
    for symbol in proxies.keys():
        _, df = fetch_twelve_data_close(symbol, interval="1day", outputsize=60)
        if df is not None:
            df = df.set_index(pd.to_datetime(df["datetime"]))
            data_dict[symbol] = df["close"].sort_index()
            
    if len(data_dict) < len(proxies):
        logger.warning("Insufficient proxy history returned for target tactical matrix.")
        return

    price_data = pd.DataFrame(data_dict).dropna()
    
    try:
        strategy = bt.Strategy('tsp_momentum', [
            bt.algos.RunMonthly(),
            bt.algos.SelectAll(),
            bt.algos.SelectMomentum(n=1), 
            bt.algos.WeighEqually(),
            bt.algos.Rebalance()
        ])
        
        backtest = bt.Backtest(strategy, price_data)
        res = bt.run(backtest)
        
        weights = res.get_security_weights().iloc[-1]
        active_allocation = weights[weights > 0].index.tolist()
        recommended = proxies.get(active_allocation[0], "G Fund (Cash Preservation)") if active_allocation else "G Fund"
        
        payload = (
            f"### 🏛️ Systematic TSP Allocation Directive\n"
            f"┣ **Top Momentum Cross-Asset Target:** `{recommended}`\n"
            f"┗ **Algorithmic Model Composition:** Trailing Return Window Optimizer\n\n"
            f"*Execution Guideline: Reallocate capital blocks strictly inside official portals once per cycle.*"
        )
        if WEBHOOKS["tsp"]:
            send_essentials_embed(WEBHOOKS["tsp"], "Thrift Savings Plan Quantitative Strategy", payload, 0x9b59b6)
            db.update_state("last_ping_tsp", time.time())
    except Exception as e:
        logger.error(f"TSP Algorithmic computation failed: {e}")

def broadcast_forex_macro():
    net_liq = db.get_state("net_liquidity", 0.0)
    dxy_price, _ = fetch_twelve_data_close("DXY")
    eur_usd, _ = fetch_twelve_data_close("EUR/USD")
    
    payload = (
        f"### 🌐 Global Macro Currency Regime\n"
        f"┣ **Fed Net Liquidity Systemic Base:** `${net_liq:,.0f}B`\n"
        f"┣ **US Dollar Index (DXY):** `{dxy_price if dxy_price else 'N/A'}`\n"
        f"┗ **EUR/USD Macro Cross:** `{eur_usd if eur_usd else 'N/A'}`\n\n"
        f"*Interpretation: Declining liquidity reservoirs yield structural support vectors for safe-haven fiat complexes.*"
    )
    if WEBHOOKS["forex"]:
        send_essentials_embed(WEBHOOKS["forex"], "Macro Liquidity & Forex Pulse", payload, 0x34495e)
        db.update_state("last_ping_macro", time.time())

if __name__ == "__main__":
    logger.info("Executing comprehensive systematic data collection sweeps...")
    broadcast_futures_snapshot()
    broadcast_options_signals()
    broadcast_tsp_allocation()
    broadcast_forex_macro()
