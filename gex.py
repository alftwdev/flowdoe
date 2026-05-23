import os
import sys
import logging
import pandas as pd
import requests
from dotenv import load_dotenv
from ecosys import EcosystemState, log_event, logger as base_logger

# 1. Initialize Child Logger
logger = logging.getLogger("GEX_Engine")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

def validate_environment():
    required_keys = ["TWELVE_DATA_API_KEY"]
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        logger.error(f"CRITICAL: Missing environment variables: {missing}")
        sys.exit(1)

validate_environment()
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

def fetch_gex_data(symbol="SPY"):
    url = f"https://api.twelvedata.com/options/chain?symbol={symbol}&apikey={TD_API_KEY}"
    try:
        response = requests.get(url, timeout=15).json()
        if "data" not in response or not response["data"]:
            logger.warning(f"No option chain data returned for {symbol}.")
            return None
        
        df = pd.DataFrame(response["data"])
        df['strike'] = pd.to_numeric(df['strike'], errors='coerce')
        df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce')
        
        df['gex_proxy'] = df['open_interest'] * df['strike']
        net_gex = df.groupby('strike')['gex_proxy'].sum().cumsum()
        
        flip_lines = net_gex[net_gex > 0]
        if flip_lines.empty:
            return None
            
        flip_line = flip_lines.index.min()
        return flip_line
    except Exception as e:
        logger.error(f"Critical error calculating Gamma proxy for {symbol}: {e}")
        return None

if __name__ == "__main__":
    logger.info("GEX Options Sweep Initiated.")
    state = EcosystemState()
    gex_level = fetch_gex_data("SPY")
    
    if gex_level:
        state.update({"spy_gamma_flip": float(gex_level)})
        logger.info(f"✅ State Matrix Updated: Gamma Flip Proxied at {gex_level}")
