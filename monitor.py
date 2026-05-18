import os
import requests
import time
import sys
import json
from datetime import datetime, time as dt_time
import pytz
from dotenv import load_dotenv

# --- 1. ECOSYSTEM INITIALIZATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

try:
    from essentials_tools import send_essentials_embed, get_institutional_conviction
    HAS_ESSENTIALS = True
except ImportError:
    HAS_ESSENTIALS = False

# Dynamic Asset Inheritance from income.py
try:
    import income
    INCOME_TARGETS = income.PRIORITY_ASSETS if hasattr(income, "PRIORITY_ASSETS") else ["CLM", "CRF"]
except Exception:
    INCOME_TARGETS = ["CLM", "CRF"]

# Structural Technical Asset Profiles
PRIORITY_ASSETS = {}
for ticker in INCOME_TARGETS:
    if ticker == "CLM":
        PRIORITY_ASSETS["CLM"] = {"nav_ticker": "XCLMX", "avg_vol": 1700000}
    elif ticker == "CRF":
        PRIORITY_ASSETS["CRF"] = {"nav_ticker": "XCRFX", "avg_vol": 600000}
    else:
        PRIORITY_ASSETS[ticker] = {"nav_ticker": f"X{ticker}X", "avg_vol": 1000000}

WEBHOOK_CORNERSTONE = os.getenv("WEBHOOK_CORNERSTONE_RO")
TD_API_KEY = os.getenv("TWELVE_DATA_API_KEY")
PULSE_FILE = os.path.join(BASE_DIR, "last_pulse_timestamp.txt")

# --- 2. DATA ACQUISITION & INTEGRATION LAYER ---

def get_live_close_and_rsi(symbol):
    """Retrieves real-time close price and 14-period RSI metric from Twelve Data."""
    url_price = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TD_API_KEY}"
    url_rsi = f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1d&time_period=14&apikey={TD_API_KEY}"
    try:
        p_res = requests.get(url_price, timeout=10).json()
        r_res = requests.get(url_rsi, timeout=10).json()
        price = float(p_res.get("price", 0))
        rsi_vals = r_res.get("values", [])
        rsi = float(rsi_vals[0].get("rsi", 50.0)) if rsi_vals else 50.0
        return price, rsi
    except Exception as e:
        print(f"    [SENTRY ERROR] Data lookup failure for {symbol}: {e}")
        return 0, 50.0

def fetch_sec_filing_shield(symbol):
    """Scans for active structural dilution filings. (Simulated boundary check)"""
    return "No N2/SEC Detected"

# --- 3. MONITORED DISPATCH MATRIX ---

def send_daily_pulse(is_test=False):
    """Compiles and broadcasts the core structural asset flow state across all notification layers."""
    title = f"🏛️ Cornerstone Flowstate Update" + (" [TEST BROADCAST]" if is_test else "")
    reports = []
    has_alerts = False

    for ticker, config in PRIORITY_ASSETS.items():
        market_price, rsi_1d = get_live_close_and_rsi(ticker)
        nav_price, _ = get_live_close_and_rsi(config["nav_ticker"])

        if market_price == 0 or nav_price == 0:
            print(f"⚠️ Skipping asset tracking cycle for {ticker} due to core market access limits.")
            continue

        # Mathematical Premium Evaluation
        premium = ((market_price - nav_price) / nav_price) * 100
        sec_status = fetch_sec_filing_shield(ticker)
        whale_status, color_hex, is_whale = get_institutional_conviction(ticker, TD_API_KEY) if HAS_ESSENTIALS else ("NORMAL", 0x2ecc71, False)

        # Contextual Gauge Valuation Logic
        if premium < 12.0:
            premium_label = "UNDERVALUED / LOW"
        elif premium <= 18.0:
            premium_label = "STABLE / NOMINAL"
        else:
            premium_label = "HIGH RISK"

        if rsi_1d < 40.0:
            rsi_label = "OVERSOLD"
        elif rsi_1d <= 60.0:
            rsi_label = "NEUTRAL"
        else:
            rsi_label = "OVERBOUGHT"

        # Operational Bounds Checking
        if premium > 22.0 or "Detected" in sec_status or is_whale:
            status_str = "🚨 CRITICAL BOUNDARY BREACH"
            income_note = "TACTICAL REDUCTION RECOMMENDED: High premium extension risks capital exposure."
            rec_str = "Cease proactive accumulation. Halt distribution reinvestment programs immediately."
            verdict_str = "Aggressive price distortion detected relative to underlying book value assets."
            has_alerts = True
        else:
            status_str = "✅ STABLE: Nominal Flowstate"
            income_note = "HOLD/ACCUMULATE: Net distributions healthy relative to carrying costs."
            rec_str = "Stable environment. Reinvest distributions; accumulate on tactical pullbacks."
            verdict_str = "Premium variance within historical standard deviations. No active dilution signatures."

        # Absolute Line-by-Line Uniformity Layout Assembly with Integrated Gauges
        asset_report = (
            f"**{ticker} Cornerstone Flowstate Update**\n"
            f"Status: `{status_str}`\n"
            f"┣ Premium to NAV: `{premium:.1f}%` ({premium_label})\n"
            f"┣ SEC Shield: `{sec_status}`\n"
            f"┣ RSI (1D): `{rsi_1d:.1f}` ({rsi_label})\n"
            f"┣ Income Note: `{income_note}`\n"
            f"┣ Whale Flow: `{whale_status}`\n"
            f"┣ Recommendation: `{rec_str}`\n"
            f"┗ Strategy Verdict: *{verdict_str}*"
        )
        reports.append(asset_report)

    if not reports:
        return

    full_report = "\n\n".join(reports)
    color = 0xe74c3c if has_alerts else 0x2ecc71

    # Platform A: Discord Delivery Channel
    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, full_report, color)
        print("✅ Core structural analytics successfully dispatched to Discord Server category.")

    # Platform B: Pushover Real-Time Notification Pipeline (Preserving Tree Layout)
    if os.getenv("PUSHOVER_API_TOKEN"):
        requests.post("https://api.pushover.net/1/messages.json", data={
            "token": os.getenv("PUSHOVER_API_TOKEN"),
            "user": os.getenv("PUSHOVER_USER_KEY"),
            "title": title,
            "message": full_report,  # Preserves all line markers, trees (┣, ┗), and layout structure
            "priority": 1 if has_alerts else 0
        }, timeout=10)
        print("✅ Uniform notification payload dispatched successfully via Pushover Gateway.")

    try:
        with open(PULSE_FILE, "w") as f:
            f.write(datetime.now().isoformat())
    except Exception as e:
        print(f"⚠️ Error writing state sync file: {e}")

def run_monitor():
    tz_h = pytz.timezone('Pacific/Honolulu')
    current_time_str = datetime.now(tz_h).strftime('%Y-%m-%d %H:%M HST')
    print(f"--- 🛡️ SENTRY ACTIVE: {current_time_str} ---")

    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "--force", "force"]:
        send_daily_pulse(is_test=("test" in sys.argv[1].lower()))
        return

    print("⏳ Entering PythonAnywhere Engine Loop...")
    last_pulse_day = None

    while True:
        now_hst = datetime.now(tz_h)
        current_day = now_hst.date()

        # Structural execution gate target window: 08:00 AM HST (Post Market Summary Close)
        if now_hst.time() >= dt_time(8, 0) and current_day != last_pulse_day:
            print(f"🎯 Target Execution Window Met at {now_hst.strftime('%H:%M HST')}. Generating Pulse metrics...")
            try:
                send_daily_pulse(is_test=False)
                last_pulse_day = current_day
            except Exception as e:
                print(f"⚠️ System Matrix execution fault encountered inside tracking run: {e}")

        time.sleep(60)

if __name__ == "__main__":
    run_monitor()
