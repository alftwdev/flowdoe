import os
import requests
import pandas as pd
import logging
from dotenv import load_dotenv

# Setup
logger = logging.getLogger("Edge_Engine")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

def calculate_mean_reversion_edge(symbol="SPY"):
    """Calculates statistical edge for Mean Reversion strategy."""
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=100&apikey={TD_API_KEY}"
    try:
        response = requests.get(url, timeout=10).json()
        if "values" not in response:
            return f"{symbol} Edge: Data Unavailable"
            
        df = pd.DataFrame(response['values'])
        df['close'] = df['close'].astype(float)
        
        # Calculate Indicators
        df['sma_200'] = df['close'].rolling(window=200).mean()
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # Signal: RSI < 30 and Price > 200SMA
        signals = df[(df['rsi'] < 30) & (df['close'] > df['sma_200'])]
        
        # Backtest (T+5 performance)
        wins = 0
        total = len(signals)
        for idx in signals.index:
            if idx > 5: # Need 5 days of lookahead
                if df.iloc[idx-5]['close'] < df.iloc[idx]['close']: # Check if it went up 5 days later
                    wins += 1
        
        win_rate = (wins / total * 100) if total > 0 else 0
        ev = (df['close'].diff().mean() * 5)
        
        return f"{symbol} Mean Reversion Edge\n┣ Win Rate: {win_rate:.1f}%\n┣ Expected Value: {ev:.2f} pts\n┗ Signals found: {total}"
    except Exception as e:
        logger.error(f"Edge calculation failed: {e}")
        return f"{symbol} Edge: Error"

if __name__ == "__main__":
    print(calculate_mean_reversion_edge())
