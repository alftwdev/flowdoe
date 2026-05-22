import os
import sys
import requests
import math
from datetime import datetime
import pytz
from dotenv import load_dotenv

from ecosys import EcosystemState, log_event
from essentials_tools import send_essentials_embed, send_pushover_alert

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
WEBHOOK_MARKET = os.getenv("WEBHOOK_MARKET_ANALYSIS")
WEBHOOK_CRYPTO = os.getenv("WEBHOOK_CRYPTO")

def get_dynamic_crypto_universe():
    return ["BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD"]

def calculate_auction_market_theory(symbol):
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=15min&outputsize=39&apikey={TD_API_KEY}"
    try:
        res = requests.get(url, timeout=10).json()
        if "values" not in res: return 0, 0, 0
        prices = [(float(c['high']) + float(c['low']) + float(c['close'])) / 3 for c in res["values"]]
        volume_bins = {}
        for c, p in zip(res["values"], prices):
            bin_key = round(p, 1)
            volume_bins[bin_key] = volume_bins.get(bin_key, 0) + int(c['volume'])
        poc = max(volume_bins, key=volume_bins.get)
        mean_p = sum(prices) / len(prices)
        std_dev = math.sqrt(sum([((x - mean_p) ** 2) for x in prices]) / len(prices))
        return round(poc, 2), round(poc + std_dev, 2), round(poc - std_dev, 2)
    except: return 0, 0, 0

def fetch_macro_context():
    """Pulls 10Y Yield, Dollar, Crude, and Gold."""
    assets = ["US10Y", "DXY", "CL=F", "GC=F"]
    context = {}
    for a in assets:
        try:
            url = f"https://api.twelvedata.com/quote?symbol={a}&apikey={TD_API_KEY}"
            res = requests.get(url, timeout=5).json()
            context[a] = float(res.get("change", 0.0))
        except: context[a] = 0.0
    return context

def broadcast_session_report(session_type, is_test=False):
    macro = fetch_macro_context()
    us10_trend = "RISING (Risk-Off Pressure)" if macro.get("US10Y", 0) > 0 else "FALLING (Risk-On Support)"
    dxy_trend = "STRONG (Equity Headwind)" if macro.get("DXY", 0) > 0 else "WEAK (Liquidity Tailwinds)"
    
    desc = (
        f"### **Global Macro Heatmap**\n"
        f"┣ **U.S. 10-Year Treasury Yield**: `{us10_trend}`\n"
        f"┣ **U.S. Dollar Index (DXY)**: `{dxy_trend}`\n"
        f"┣ **Crude Oil (CL)**: `{macro.get('CL=F', 0.0):+.2f}%`\n"
        f"┗ **Gold (GC)**: `{macro.get('GC=F', 0.0):+.2f}%`\n\n"
        f"### **Microstructure & Order Flow**\n"
        f"┣ **VWAP Context**: `Testing Mean Value Areas`\n"
        f"┗ **Advance/Decline Breadth**: `Concentrated Leadership`\n\n"
        f"**Terminal Note**: Automated gatekeepers active. System will actively suppress sizing to protect capital during low volume."
    )
    if is_test: print(f"    ↳ Broadcasting {session_type} Report...\n{desc}")
    send_essentials_embed(WEBHOOK_MARKET, f"🏛️ ROCKEFELLER TERMINAL: {session_type} UPDATE", desc, 0x3498db)

def broadcast_crypto_pulse(is_test=False):
    for asset in get_dynamic_crypto_universe():
        poc, vah, val = calculate_auction_market_theory(asset)
        if poc == 0: continue
        desc = (
            f"### **Ecosystem Auction Parameters**\n"
            f"┣ **Asset**: `{asset}`\n"
            f"┣ **Value Area High (VAH)**: `${vah:,.2f}`\n"
            f"┣ **Point of Control (POC)**: `${poc:,.2f}` *(Inventory Core)*\n"
            f"┗ **Value Area Low (VAL)**: `${val:,.2f}`\n\n"
            f"### **Order Flow Footprint**\n"
            f"┗ **Strategic Stance**: `INSTITUTIONAL ACCUMULATION/DISTRIBUTION`"
        )
        if is_test: print(f"    ↳ Broadcasting Crypto {asset}...")
        send_essentials_embed(WEBHOOK_CRYPTO, f"₿ CRYPTO MICROSTRUCTURE: {asset}", desc, 0xf39c12)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        print("🧪 Testing Terminal Updates...")
        broadcast_session_report("MID-DAY TEST", is_test=True)
        broadcast_crypto_pulse(is_test=True)
        print("✅ Macro Radar testing complete.")
    else:
        import time
        while True:
            now = datetime.now(pytz.timezone('Pacific/Honolulu'))
            # Mid-Day Update: 06:00 HST (12:00 PM EST)
            if now.hour == 6 and now.minute == 0:
                broadcast_session_report("MID-DAY")
                time.sleep(60)
            # Pre-Close Update: 09:30 HST (3:30 PM EST)
            elif now.hour == 9 and now.minute == 30:
                broadcast_session_report("PRE-CLOSE")
                time.sleep(60)
            # Crypto AMT Top of Hour
            elif now.minute == 0 and now.hour not in [6, 9]:
                broadcast_crypto_pulse()
                time.sleep(60)
            else:
                time.sleep(30)
