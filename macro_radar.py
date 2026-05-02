import requests
import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# --- LOAD SECURE VAULT ---
load_dotenv()

# --- CONFIG & CREDENTIALS ---
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
PUSHOVER_USER = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_TOKEN = os.getenv("PUSHOVER_API_TOKEN")

# File to track TQQQ entries
POSITIONS_FILE = "tqqq_positions.csv"

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

def check_profit_sentry():
    """Checks if any open TQQQ positions are in the 30-45 day exit window."""
    if not os.path.exists(POSITIONS_FILE):
        return ""
    
    try:
        df = pd.read_csv(POSITIONS_FILE)
        alerts = []
        for _, row in df.iterrows():
            entry_date = datetime.strptime(row['entry_date'], '%Y-%m-%d')
            days_held = (datetime.now() - entry_date).days
            if 30 <= days_held <= 45:
                alerts.append(f"⚠️ EXIT WATCH: {row['ticker']} held {days_held} days.")
        return "\n".join(alerts)
    except Exception as e:
        print(f"    [ERROR] Position file read failed: {e}")
        return ""

def main():
    print("    [1/4] Fetching Macro Breadth & 200MA...")
    vix_data = get_av_data("VIX").get("Global Quote", {})
    spy_data = get_av_data("SPY").get("Global Quote", {})
    spy_ma_data = get_av_data("SPY", function="SMA", extra_params={"interval": "daily", "time_period": "200", "series_type": "close"})
    
    # Extract Values with Safety Defaults
    try:
        vix = float(vix_data.get("05. price", 0))
        spy_price = float(spy_data.get("05. price", 0))
        spy_pct = spy_data.get("10. change percent", "0.00%")

        ma_series = spy_ma_data.get("Technical Analysis: SMA", {})
        latest_ma_date = next(iter(ma_series)) if ma_series else None
        spy_200ma = float(ma_series[latest_ma_date]["SMA"]) if latest_ma_date else 0.0
    except (ValueError, KeyError, StopIteration):
        print("    [CRITICAL] Data parsing failed. Likely API limit.")
        return

    # Safety Gate: Prevent false BEAR regime if API returns 0
    if spy_price == 0 or spy_200ma == 0:
        print("    [ABORT] Incomplete data. Notification cancelled.")
        return

    print("    [2/4] Fetching TQQQ Technicals (RSI)...")
    tqqq_data = get_av_data("TQQQ").get("Global Quote", {})
    tqqq_rsi_data = get_av_data("TQQQ", function="RSI", extra_params={"interval": "daily", "time_period": "14", "series_type": "close"})
    
    try:
        tqqq_price = float(tqqq_data.get("05. price", 0))
        tqqq_pct = tqqq_data.get("10. change percent", "0.00%")
        rsi_series = tqqq_rsi_data.get("Technical Analysis: RSI", {})
        latest_date = next(iter(rsi_series)) if rsi_series else None
        rsi_val = float(rsi_series[latest_date]["RSI"]) if latest_date else 0.0
    except (ValueError, KeyError, StopIteration):
        rsi_val = 0.0

    # --- STRATEGY LOGIC ---
    market_regime = "BULL" if spy_price > spy_200ma else "BEAR"
    status = "🔴 FEAR" if vix > 28 or rsi_val < 35 else "🟢 CALM"
    strike = "⚡ STRIKE ZONE" if (rsi_val < 32 and vix > 25) else "INACTIVE"
    profit_alert = check_profit_sentry()

    report = (
        f"Regime: {market_regime} (SPY vs 200MA)\n"
        f"Status: {status}\n"
        f"VIX: {vix} | SPY: {spy_pct}\n"
        f"-------------------\n"
        f"TQQQ: ${tqqq_price} ({tqqq_pct})\n"
        f"RSI: {rsi_val:.2f}\n"
        f"Strike Zone: {strike}\n"
        f"{'-------------------' if profit_alert else ''}\n"
        f"{profit_alert}"
    )

    print(f"\n{report}")

    # --- PUSHOVER (HEARTBEAT MODE) ---
    if PUSHOVER_TOKEN:
        print("    [4/4] Sending Pushover Heartbeat...")
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": PUSHOVER_TOKEN,
            "user": PUSHOVER_USER,
            "title": f"🌎 Macro Radar: {market_regime}",
            "message": report,
            "priority": 1 if strike == "⚡ STRIKE ZONE" else 0
        })

if __name__ == "__main__":
    main()
