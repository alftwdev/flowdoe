import os
import requests
import json
import time
import traceback
from datetime import datetime
import pytz
from dotenv import load_dotenv

# --- 1. INITIALIZATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# API Keys & Webhooks
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MONITOR = os.getenv("WEBHOOK_DIVIDEND_CCETFS") # Using your existing naming
REGIME_LEDGER = os.path.join(BASE_DIR, "market_regime.json")

# Constants
PRIORITY_ASSETS = ["CLM", "CRF"]
# SEC EDGAR User-Agent (Required by SEC to prevent 403 Forbidden errors)
HEADERS = {"User-Agent": "Alwin Almazan (almazan.trading.bot@gmail.com)"} 

def get_detailed_intel(symbol):
    """Fetches price data and premium/discount metrics."""
    try:
        # Fetching Tape via Twelve Data
        url = f"https://api.twelvedata.com/quote?symbol={symbol}&apikey={TD_API_KEY}"
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if "price" not in data:
            print(f"    ❌ API Error for {symbol}: {data.get('message', 'Unknown Error')}")
            return None
            
        return {
            "symbol": symbol,
            "price": float(data['close']),
            "change": float(data['percent_change']),
            "high_52": data['fifty_two_week']['high'],
            "low_52": data['fifty_two_week']['low']
        }
    except Exception:
        print(f"    ⚠️ Exception during Tape Fetch for {symbol}")
        traceback.print_exc()
        return None

def scan_edgar_filings(symbol):
    """Placeholder for EDGAR scanning logic - specifically watching for N-PORT or N-CEN."""
    # Note: SEC requires a specific User-Agent. If this fails on PA, it's likely a whitelist issue.
    print(f"PROCESS: Scanning EDGAR for {symbol}...")
    return "No new structural filings detected."

def run_monitor():
    print(f"--- 🛡️ SENTRY START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    while True:
        now = datetime.now(pytz.timezone('US/Aleutian')) # Syncing with your HST/Hawaii context
        
        for ticker in PRIORITY_ASSETS:
            print(f"\nScanning {ticker} Tape...")
            
            # 1. Check structural filings
            scan_edgar_filings(ticker)
            
            # 2. Get Price Intelligence
            intel = get_detailed_intel(ticker)
            
            if intel:
                # Logic: If price is near 52-week high, alert for potential NAV Premium harvesting
                price_vs_high = (intel['price'] / float(intel['high_52'])) * 100
                
                if price_vs_high > 95:
                    alert_msg = (
                        f"⚠️ **High Premium Alert: ${ticker}**\n"
                        f"Price `${intel['price']}` is at **{price_vs_high:.1f}%** of 52W High.\n"
                        f"Check NAV alignment before Dividend Reinvestment."
                    )
                    
                    if WEBHOOK_MONITOR:
                        requests.post(WEBHOOK_MONITOR, json={"content": alert_msg})
                        print(f"✅ Alert dispatched for {ticker}")

        print(f"\n--- 🛡️ SENTRY CYCLE FINISHED: {now.strftime('%H:%M')} ---")
        print("Cycle sleeping for 10 minutes to maintain API integrity...")
        
        # 10-minute sleep (600 seconds) is the 'Sweet Spot' for PythonAnywhere Always-On tasks
        time.sleep(600)

if __name__ == "__main__":
    try:
        run_monitor()
    except KeyboardInterrupt:
        print("\nSentry gracefully deactivated.")
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        traceback.print_exc()
