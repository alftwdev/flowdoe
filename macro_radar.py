import requests
import os
import sys
from dotenv import load_dotenv

# --- LOAD SECURE VAULT ---
load_dotenv()

# --- CONFIG & CREDENTIALS ---
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
PUSHOVER_USER = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_TOKEN = os.getenv("PUSHOVER_API_TOKEN")

def get_av_data(symbol, function="GLOBAL_QUOTE", extra_params=None):
    url = f"https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={API_KEY}"
    if extra_params:
        for k, v in extra_params.items():
            url += f"&{k}={v}"
    try:
        r = requests.get(url, timeout=15)
        data = r.json()
        if "Note" in data:
            print(f"    [API LIMIT] Hit the AlphaVantage limit for {symbol}.")
        return data
    except Exception as e:
        print(f"    [ERROR] Connection failed: {e}")
        return {}

def main():
    print("    [1/3] Fetching Macro Breadth (VIX/SPY)...")
    vix_data = get_av_data("VIX").get("Global Quote", {})
    spy_data = get_av_data("SPY").get("Global Quote", {})
    
    vix = float(vix_data.get("05. price", 0))
    spy_pct = float(spy_data.get("10. change percent", "0%").strip('%'))

    print("    [2/3] Fetching TQQQ Technicals (RSI/VWAP)...")
    tqqq_data = get_av_data("TQQQ").get("Global Quote", {})
    tqqq_rsi_data = get_av_data("TQQQ", function="RSI", extra_params={"interval": "daily", "time_period": "14", "series_type": "close"})
    
    tqqq_price = float(tqqq_data.get("05. price", 0))
    tqqq_pct = float(tqqq_data.get("10. change percent", "0%").strip('%'))
    
    # Extract RSI
    rsi_series = tqqq_rsi_data.get("Technical Analysis: RSI", {})
    latest_date = next(iter(rsi_series)) if rsi_series else None
    rsi_val = float(rsi_series[latest_date]["RSI"]) if latest_date else 0.0

    # Status Logic
    status = "🔴 FEAR" if vix > 28 or rsi_val < 35 else "🟢 CALM"
    strike = "⚡ ACTIVE" if rsi_val < 30 and vix > 25 else "INACTIVE"

    report = (
        f"Status: {status}\n"
        f"VIX: {vix} | SPY: {spy_pct}%\n"
        f"-------------------\n"
        f"TQQQ: ${tqqq_price} ({tqqq_pct}%)\n"
        f"RSI: {rsi_val:.2f}\n"
        f"Strike Zone: {strike}"
    )

    print(f"\n{report}")

    # --- PUSHOVER ---
    if PUSHOVER_TOKEN and (strike == "⚡ ACTIVE" or "test" in sys.argv):
        print("    [3/3] Sending Pushover Notification...")
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": PUSHOVER_TOKEN,
            "user": PUSHOVER_USER,
            "title": "🌎 Market Macro Radar",
            "message": report
        })

if __name__ == "__main__":
    main()
