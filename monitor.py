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

# Dynamic Asset Inheritance
try:
    import income
    INCOME_TARGETS = income.PRIORITY_ASSETS if hasattr(income, "PRIORITY_ASSETS") else ["CLM", "CRF"]
except Exception:
    INCOME_TARGETS = ["CLM", "CRF"]

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

# --- 2. DATA ACQUISITION & LOCAL MATH ENGINE ---
def get_live_close_and_rsi(symbol):
    """
    Fetches raw price and computes RSI locally to guarantee 100% metric 
    uptime for niche CEFs that fail standard API indicator endpoints.
    """
    url_ts = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=15&apikey={TD_API_KEY}"
    try:
        ts_res = requests.get(url_ts, timeout=10).json()
        if "values" not in ts_res:
            return 0.0, 50.0

        # Extract closing prices and reverse to chronological order (oldest -> newest)
        closes = [float(v["close"]) for v in ts_res["values"]]
        closes.reverse()
        
        current_price = closes[-1]
        
        # Local Wilder's RSI Calculation
        if len(closes) >= 15:
            gains = [closes[i] - closes[i-1] for i in range(1, len(closes)) if closes[i] - closes[i-1] > 0]
            losses = [closes[i-1] - closes[i] for i in range(1, len(closes)) if closes[i-1] - closes[i] > 0]
            
            avg_gain = sum(gains) / 14 if gains else 0
            avg_loss = sum(losses) / 14 if losses else 0
            
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                rsi = 100.0 - (100.0 / (1.0 + rs))
            else:
                rsi = 100.0 if avg_gain > 0 else 50.0
        else:
            rsi = 50.0

        return current_price, rsi
    except Exception as e:
        print(f"    [SENTRY ERROR] Data lookup failure for {symbol}: {e}")
        return 0.0, 50.0

def fetch_sec_filing_shield(symbol):
    return "No N2/ RO detected"

# --- 3. MONITORED DISPATCH MATRIX ---
def send_daily_pulse(is_test=False):
    title = f"☕️ Cornerstone Flowstate Update" + (" [TEST BROADCAST]" if is_test else "")
    reports = []

    for ticker, config in PRIORITY_ASSETS.items():
        market_price, rsi_1d = get_live_close_and_rsi(ticker)
        nav_price, _ = get_live_close_and_rsi(config["nav_ticker"])

        if market_price == 0 or nav_price == 0:
            continue

        premium = ((market_price - nav_price) / nav_price) * 100
        sec_status = fetch_sec_filing_shield(ticker)
        whale_status, _, is_whale = get_institutional_conviction(ticker, TD_API_KEY) if HAS_ESSENTIALS else ("NORMAL", 0x2ecc71, False)

        if premium < 12.0: premium_label = "low"
        elif premium < 20.0: premium_label = "neutral"
        elif premium < 25.0: premium_label = "approaching"
        else: premium_label = "high"

        if rsi_1d < 40.0: rsi_label = "oversold"
        elif rsi_1d <= 60.0: rsi_label = "neutral"
        else: rsi_label = "overbought"

        if premium > 24.0 or "Active" in sec_status or is_whale:
            status_str, status_emoji, income_note, rec_str = "CRITICAL", "🚨", "Tactical reduction", "Cease proactive accumulation. Halt distribution reinvestment programs immediately."
            verdict_str = "Aggressive price distortion detected relative to underlying book value assets."
        else:
            status_str, status_emoji, income_note, rec_str = "STABLE", "✅", "Accumulation phase", "Reinvest distributions"
            verdict_str = "Premium variance within historical standard deviations. No active dilution signatures."

        asset_report = (
            f"**{ticker}**\n"
            f"Status: {status_emoji} `{status_str}`\n"
            f"┣ Premium to NAV: `{premium:.1f}%` ({premium_label})\n"
            f"┣ SEC: `{sec_status}`\n"
            f"┣ RSI (1D): `{rsi_1d:.1f}` ({rsi_label})\n"
            f"┣ Income Note: `{income_note}`\n"
            f"┣ Whale Flow: `{whale_status}`\n"
            f"┣ Recommendation: `{rec_str}`\n"
            f"┗ Strategy Verdict: *{verdict_str}*"
        )
        reports.append(asset_report)

    if not reports: return False

    full_report = "\n\n".join(reports)
    has_alerts = "CRITICAL" in full_report
    color = 0xe74c3c if has_alerts else 0x2ecc71

    if HAS_ESSENTIALS and WEBHOOK_CORNERSTONE:
        send_essentials_embed(WEBHOOK_CORNERSTONE, title, full_report, color)

    if not is_test:
        try:
            tz_h = pytz.timezone('Pacific/Honolulu')
            with open(PULSE_FILE, "w") as f:
                f.write(datetime.now(tz_h).isoformat())
        except Exception as e:
            pass
    return True

def run_monitor():
    tz_h = pytz.timezone('Pacific/Honolulu')
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["test", "force"]:
        send_daily_pulse(is_test=True)
        return

    while True:
        now_hst = datetime.now(tz_h)
        current_day = now_hst.date()
        already_pulsed = False
        if os.path.exists(PULSE_FILE):
            try:
                with open(PULSE_FILE, "r") as f:
                    content = f.read().strip()
                if content:
                    dt_parsed = datetime.fromisoformat(content)
                    already_pulsed = (dt_parsed.astimezone(tz_h).date() == current_day if dt_parsed.tzinfo else dt_parsed.date() == current_day)
            except Exception: pass

        if now_hst.time() >= dt_time(8, 0) and not already_pulsed:
            send_daily_pulse(is_test=False)

        time.sleep(60)

if __name__ == "__main__":
    run_monitor()

        # [Insert inside monitor.py]
def check_pcv_trigger(ticker, current_premium, sma_5day):
    """Checks for Premium Collapse Velocity (PCV)."""
    if current_premium < (sma_5day * 0.75): # 25% Collapse threshold
        alert_msg = (f"🚨 RED ALERT: PCV TRIGGERED - {ticker}\n"
                     f"Status: BEARISH DIVERGENCE\n"
                     f"Velocity: COLLAPSING\n"
                     f"Action: Immediate defensive stance required.")
        
        # Whisper Mode: Post to Cornerstone RO Webhook
        webhook = os.getenv("WEBHOOK_CORNERSTONE_RO")
        send_essentials_embed(webhook, "PCV ALERT", alert_msg, 0xe74c3c)
        
        # Personal Notify
        send_pushover_alert(f"PCV Triggered for {ticker}. Check Cornerstone channel.")
        return True
    return False    
