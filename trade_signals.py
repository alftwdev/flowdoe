import os
import time
import requests
from dotenv import load_dotenv
# Import the shared tool correctly
from essentials_tools import send_essentials_embed

load_dotenv()

# Configuration
# Note: Ensure these keys match your .env file names exactly
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
WEBHOOK_URL = os.getenv("WEBHOOK_TRADE_SIGNALS")

# Expanded watchlist for high-volume options trading
WATCHLIST = ["TQQQ", "NVDA", "TSLA", "PLTR", "SOFI", "AAPL", "AMD", "MARA"]

def get_rsi(symbol):
    """Fetches 15-minute RSI using Alpha Vantage Premium."""
    url = f"https://www.alphavantage.co/query?function=RSI&symbol={symbol}&interval=15min&time_period=14&series_type=close&apikey={API_KEY}"
    try:
        data = requests.get(url, timeout=15).json()
        # Get the latest RSI value from the nested dictionary
        latest_time = list(data['Technical Analysis: RSI'].keys())[0]
        return float(data['Technical Analysis: RSI'][latest_time]['RSI'])
    except Exception as e:
        print(f"    [AV] RSI Error for {symbol}: {e}")
        return None

def monitor_waves():
    print(f"--- STARTING TRADE SIGNAL SCAN: {time.ctime()} ---")
    for symbol in WATCHLIST:
        try:
            rsi_val = get_rsi(symbol)
            if rsi_val is None:
                continue
                
            # Essentials Algos Logic: Strike Zones
            if rsi_val <= 30:
                send_essentials_embed(
                    WEBHOOK_URL, 
                    f"🌊 {symbol} STRIKE ZONE: OVERSOLD",
                    f"{symbol} is currently hitting a high-conviction entry point.\n**RSI:** {rsi_val:.2f}",
                    0x2ecc71 # Green
                )
            elif rsi_val >= 70:
                send_essentials_embed(
                    WEBHOOK_URL, 
                    f"🚨 {symbol} ALERT: OVERBOUGHT",
                    f"{symbol} is reaching an exhaustion point. Watch for a pullback.\n**RSI:** {rsi_val:.2f}",
                    0xe74c3c # Red
                )
            
            # Respect API limits even with Premium (1 sec is safe)
            time.sleep(1) 
        except Exception as e:
            print(f"Error scanning {symbol}: {e}")
    print(f"--- SCAN COMPLETE ---")

if __name__ == "__main__":
    monitor_waves()
