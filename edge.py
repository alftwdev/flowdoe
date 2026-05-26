import os
import requests
import pandas as pd
import numpy as np
import logging
from dotenv import load_dotenv

logger = logging.getLogger("Edge_Engine")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

def get_realized_volatility(symbol="SPY", window=21):
    """Fetches trailing price data and calculates annualized realized volatility."""
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=100&apikey={TD_API_KEY}"
    try:
        response = requests.get(url, timeout=10).json()
        if "values" not in response:
            logger.warning(f"Failed to fetch RV data for {symbol}.")
            return None
            
        df = pd.DataFrame(response['values'])
        df['close'] = df['close'].astype(float)
        
        # Calculate log returns for better statistical accuracy
        returns = np.log(df['close'] / df['close'].shift(-1)).dropna()
        rv = np.std(returns) * np.sqrt(252) # Annualized
        return rv
    except Exception as e:
        logger.error(f"RV calculation failed for {symbol}: {e}")
        return None

def calculate_vrp_score(symbol, current_iv):
    """
    Combines IV from Sentry with calculated RV from local data.
    Returns: VRP Score (Positive = Premium Rich, Negative = Underpriced Insurance)
    """
    rv = get_realized_volatility(symbol)
    if rv is None:
        return 0.0
    
    # Scale IV (e.g., 20.0) to decimal (0.20) to match RV
    iv_decimal = current_iv / 100.0
    vrp = iv_decimal - rv
    return vrp

def calculate_mean_reversion_edge(symbol="SPY"):
    """Calculates statistical edge for Mean Reversion strategy."""
    # (Original mean reversion logic preserved...)
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=100&apikey={TD_API_KEY}"
    try:
        response = requests.get(url, timeout=10).json()
        if "values" not in response:
            return f"{symbol} Edge: Data Unavailable"
            
        df = pd.DataFrame(response['values'])
        df['close'] = df['close'].astype(float)
        
        df['sma_200'] = df['close'].rolling(window=200).mean()
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        signals = df[(df['rsi'] < 30) & (df['close'] > df['sma_200'])]
        
        wins = 0
        total = len(signals)
        for idx in signals.index:
            if idx > 5:
                if df.iloc[idx-5]['close'] < df.iloc[idx]['close']:
                    wins += 1
        
        win_rate = (wins / total * 100) if total > 0 else 0
        ev = (df['close'].diff().mean() * 5)
        
        return f"{symbol} Mean Reversion Edge\n┣ Win Rate: {win_rate:.1f}%\n┣ Expected Value: {ev:.2f} pts\n┗ Signals found: {total}"
    except Exception as e:
        logger.error(f"Edge calculation failed: {e}")
        return f"{symbol} Edge: Error"

if __name__ == "__main__":
    print(calculate_mean_reversion_edge())
