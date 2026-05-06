import os
import csv
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE = os.path.join(BASE_DIR, 'macro_history.csv')

def fetch_td_data(url):
    try:
        response = requests.get(url, timeout=15)
        data = response.json()
        if data.get("status") == "error":
            return None
        return data
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return None

def get_market_data(symbol):
    """Fetches Price, EMA200, and Volume for Whale detection."""
    quote_url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
    ema_url = f"https://api.twelvedata.com/ema?symbol={symbol}&interval=1day&time_period=200&apikey={TD_API_KEY}"
    
    q = fetch_td_data(quote_url)
    e = fetch_td_data(ema_url)
    
    if q and e:
        try:
            return {
                "price": float(q['close']),
                "ema": float(e['values'][0]['ema']),
                "vol": int(q['volume']),
                "avg_vol": int(q['average_volume'])
            }
        except (KeyError, IndexError): pass
    return None

def save_to_history(vix, spy_price, regime):
    file_exists = os.path.isfile(HISTORY_FILE)
    with open(HISTORY_FILE, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['Date', 'VIX', 'Regime', 'spy_price']) 
        writer.writerow([datetime.now().strftime('%Y-%m-%d'), vix, regime, spy_price])

def run_macro_check():
    print(f"--- 🛰️ VENTURE MACRO RADAR: {datetime.now()} ---")
    
    spy = get_market_data("SPY")
    if not spy: return

    # Fixed VIX Symbol for Twelve Data
    vix_data = fetch_td_data(f"https://api.twelvedata.com/quote?symbol=VIX&apikey={TD_API_KEY}")
    if not vix_data:
        vix_data = fetch_td_data(f"https://api.twelvedata.com/quote?symbol=VIX:CBOE&apikey={TD_API_KEY}")
    
    vix_price = float(vix_data['close']) if vix_data else 20.0

    # Whale Alert Logic: Vol > 3x Average
    if spy['vol'] > (spy['avg_vol'] * 3):
        print("🚨 WHALE ALERT: Massive SPY Volume detected.")

    regime = "Risk-On" if (vix_price < 20 and spy['price'] > spy['ema']) else "Risk-Off"
    save_to_history(vix_price, spy['price'], regime)
    print(f"✅ Market Status: {regime}")

if __name__ == "__main__":
    run_macro_check()
