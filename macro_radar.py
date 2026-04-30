import requests
import datetime
import sys
import os
import csv
from fredapi import Fred

# --- 1. CONFIG & CREDENTIALS ---
PUSHOVER_USER_KEY = "ua1tgyam2bd124756cuc1s5e16kxgt"
PUSHOVER_API_TOKEN = "a7dv58on4sgdyommmy72ygs6r63hsw"

# Replace with your newly generated keys
FRED_API_KEY = '58319998d168c380f60036032f43b0e2'
ALPHA_VANTAGE_KEY = 'E77PWEEST1CIFGU0'

# Path for your history log
BASE_PATH = "/home/alftw/scripts/"
LOG_FILE = os.path.join(BASE_PATH, "macro_history.csv")

fred = Fred(api_key=FRED_API_KEY)

def send_macro_pushover(message):
    """Dispatches the final report to your phone."""
    requests.post("https://api.pushover.net/1/messages.json", data={
        "token": PUSHOVER_API_TOKEN,
        "user": PUSHOVER_USER_KEY,
        "title": "🌎 Market Macro Radar",
        "message": message
    })

def safe_get_av(ticker_symbol):
    """Fetches ETF data via AlphaVantage."""
    url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker_symbol}&apikey={ALPHA_VANTAGE_KEY}'
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        if "Global Quote" in data and data["Global Quote"]:
            price = float(data["Global Quote"]["05. price"])
            change_pct = float(data["Global Quote"]["10. change percent"].strip('%'))
            return price, change_pct
    except Exception as e:
        print(f"   [AV Error] {ticker_symbol}: {e}")
    return None, 0.0

def log_macro_data(data_dict):
    """Appends daily data to macro_history.csv for future 'Conviction' analysis."""
    file_exists = os.path.isfile(LOG_FILE)
    with open(LOG_FILE, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=data_dict.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data_dict)

def get_monthly_pulse():
    """Checks FINRA Margin Debt - Returns data only once a month."""
    today = datetime.datetime.now()
    # FINRA usually updates around the 20th-25th for the previous month
    if today.day == 25:
        try:
            # Series for Security Brokers/Dealers; Margin Loans (Asset)
            debt_val = fred.get_series('BOGZ1FL663067103Q').iloc[-1]
            return f"\n📊 MONTHLY PULSE: Margin Debt is ${debt_val:,.0f}M"
        except:
            return ""
    return ""

def get_macro_data():
    # 1. Official Macro (VIX & 10Y Yield)
    print("   [FRED] Fetching VIX and 10Y Yield...")
    try:
        vix_val = fred.get_series('VIXCLS').iloc[-1]
    except:
        vix_val = 20.0 # Neutral Fallback

    try:
        tnx_val = fred.get_series('DGS10').iloc[-1]
    except:
        tnx_val = 4.30 # Neutral Fallback

    vix_status = "⚠️ HIGH FEAR" if vix_val > 30 else "✅ STABLE"

    # 2. ETFs (AlphaVantage)
    print("   [AV] Fetching Sector and Breadth data...")
    xlk_price, xlk_chg = safe_get_av("XLK")
    xlp_price, xlp_chg = safe_get_av("XLP")
    spy_price, spy_chg = safe_get_av("SPY")

    leader = "💻 TECH (Growth)" if xlk_chg > xlp_chg else "🛡️ DEFENSIVE (Value)"
    breadth = "🚀 BULLISH" if spy_chg > 0.5 else "📉 BEARISH" if spy_chg < -0.5 else "↔️ NEUTRAL"

    # 3. Monthly Pulse (FINRA Check)
    pulse_msg = get_monthly_pulse()

    # 4. Logging for Conviction Analysis
    log_entry = {
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "vix": f"{vix_val:.2f}",
        "tnx": f"{tnx_val:.2f}",
        "spy_chg": f"{spy_chg:.2f}",
        "leader": leader
    }
    log_macro_data(log_entry)

    # 5. Final Message
    msg = (
        f"Status: {breadth}\n"
        f"VIX: {vix_val:.2f} ({vix_status})\n"
        f"Fed/Rates (10Y): {tnx_val:.2f}%\n"
        f"Leading Sector: {leader}\n"
        f"S&P 500 Change: {spy_chg:.2f}%"
        f"{pulse_msg}"
    )
    return msg

if __name__ == "__main__":
    try:
        report = get_macro_data()
        send_macro_pushover(report)
        print("Macro Radar Dispatched and Logged.")
    except Exception as e:
        print(f"Macro Radar Error: {e}")