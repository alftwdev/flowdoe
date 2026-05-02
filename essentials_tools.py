import os
import time
from dotenv import load_dotenv
from essentials_tools import send_essentials_embed
import requests

load_dotenv()

# Configuration
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")
WEBHOOK_URL = os.getenv("WEBHOOK_TRADE_SIGNALS")
# Expanded watchlist for high-volume options trading
WATCHLIST = ["TQQQ", "NVDA", "TSLA", "PLTR", "SOFI", "AAPL" "AMD", "MARA"]

def get_rsi(symbol):
    url = f"https://www.alphavantage.co/query?function=RSI&symbol={symbol}&interval=15min&time_period=14&series_type=close&apikey={API_KEY}"
    data = requests.get(url).json()
    # Get the latest RSI value
    latest_time = list(data['Technical Analysis: RSI'].keys())[0]
    return float(data['Technical Analysis: RSI'][latest_time]['RSI'])

def monitor_waves():
    for symbol in WATCHLIST:
        try:
            rsi_val = get_rsi(symbol)
            
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
                    f"{symbol} is reaching a exhaustion point. Watch for a pullback.\n**RSI:** {rsi_val:.2f}",
                    0xe74c3c # Red
                )
            
            # Respect API limits even with Premium
            time.sleep(1) 
        except Exception as e:
            print(f"Error scanning {symbol}: {e}")

if __name__ == "__main__":
    monitor_waves()
